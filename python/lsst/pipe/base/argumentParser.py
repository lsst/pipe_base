#
# LSST Data Management System
# Copyright 2008-2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
from __future__ import absolute_import, division, print_function
import abc
import argparse
import collections
import fnmatch
import itertools
import os
import re
import shlex
import sys
import shutil
import textwrap

from builtins import zip
from builtins import str
from builtins import range
from builtins import object

import lsst.utils
import lsst.pex.config as pexConfig
import lsst.pex.config.history
import lsst.log as lsstLog
import lsst.daf.persistence as dafPersist
from future.utils import with_metaclass

__all__ = ["ArgumentParser", "ConfigFileAction", "ConfigValueAction", "DataIdContainer",
           "DatasetArgument", "ConfigDatasetType", "InputOnlyArgumentParser"]

DEFAULT_INPUT_NAME = "PIPE_INPUT_ROOT"
DEFAULT_CALIB_NAME = "PIPE_CALIB_ROOT"
DEFAULT_OUTPUT_NAME = "PIPE_OUTPUT_ROOT"


def _fixPath(defName, path):
    """Apply environment variable as default root, if present, and abspath.

    Parameters
    ----------
    defName : `str`
        Name of environment variable containing default root path; if the environment variable does not exist
        then the path is relative to the current working directory
    path : `str`
        Path relative to default root path.

    Returns
    -------
    abspath : `str`
        Path that has been expanded, or `None` if the environment variable does not exist and path is `None`.
    """
    defRoot = os.environ.get(defName)
    if defRoot is None:
        if path is None:
            return None
        return os.path.abspath(path)
    return os.path.abspath(os.path.join(defRoot, path or ""))


class DataIdContainer(object):
    """Container for data IDs and associated data references.

    Parameters
    ----------
    level
        Unknown.

    Notes
    -----
    Override for data IDs that require special handling to be converted to ``data references``, and specify
    the override class as ``ContainerClass`` for ``add_id_argument``. (If you don't want the argument parser
    to compute data references, you may use this class and specify ``doMakeDataRefList=False`` in
    ``add_id_argument``.)
    """

    def __init__(self, level=None):
        self.datasetType = None  # the actual dataset type, as specified on the command line (if dynamic)
        self.level = level
        self.idList = []
        self.refList = []

    def setDatasetType(self, datasetType):
        """Set actual dataset type, once it is known.

        Parameters
        ----------
        datasetType : `str`
            Dataset type.
        """
        self.datasetType = datasetType

    def castDataIds(self, butler):
        """Validate data IDs and cast them to the correct type (modify idList in place).

        Parameters
        ----------
        butler : `lsst.daf.persistence.Butler`
            Data butler.
        """
        if self.datasetType is None:
            raise RuntimeError("Must call setDatasetType first")
        try:
            idKeyTypeDict = butler.getKeys(datasetType=self.datasetType, level=self.level)
        except KeyError:
            raise KeyError("Cannot get keys for datasetType %s at level %s" % (self.datasetType, self.level))

        for dataDict in self.idList:
            for key, strVal in dataDict.items():
                try:
                    keyType = idKeyTypeDict[key]
                except KeyError:
                    # OK, assume that it's a valid key and guess that it's a string
                    keyType = str
                    
                    log = lsstLog.Log.getDefaultLogger()
                    log.warn("Unexpected ID %s; guessing type is \"%s\"" %
                             (key, 'str' if keyType == str else keyType))
                    idKeyTypeDict[key] = keyType

                if keyType != str:
                    try:
                        castVal = keyType(strVal)
                    except Exception:
                        raise TypeError("Cannot cast value %r to %s for ID key %r" % (strVal, keyType, key,))
                    dataDict[key] = castVal

    def makeDataRefList(self, namespace):
        """Compute refList based on idList.

        Parameters
        ----------
        namespace
            Results of parsing command-line (with ``butler`` and ``log`` elements).

        Notes
        -----
        Not called if ``add_id_argument`` called with ``doMakeDataRefList=False``.
        """
        if self.datasetType is None:
            raise RuntimeError("Must call setDatasetType first")
        butler = namespace.butler
        for dataId in self.idList:
            refList = list(butler.subset(datasetType=self.datasetType, level=self.level, dataId=dataId))
            # exclude nonexistent data
            # this is a recursive test, e.g. for the sake of "raw" data
            refList = [dr for dr in refList if dataExists(butler=butler, datasetType=self.datasetType,
                                                          dataRef=dr)]
            if not refList:
                namespace.log.warn("No data found for dataId=%s", dataId)
                continue
            self.refList += refList


class DataIdArgument(object):
    """data ID argument, used by `ArgumentParser.add_id_argument`.

    Parameters
    ----------
    name : `str`
        Name of identifier (argument name without dashes).
    datasetType : `str`
        Type of dataset; specify a string for a fixed dataset type or a `DatasetArgument` for a dynamic
        dataset type (e.g. one specified by a command-line argument).
    level
        Level of dataset, for `~lsst.daf.persistence.Butler`.
    doMakeDataRefList : `bool`, optional
        If `True` (default), construct data references.
    ContainerClass : class, optional
        Class to contain data IDs and data references; the default class will work for many kinds of data,
        but you may have to override to compute some kinds of data references. Default is `DataIdContainer`.
    """

    def __init__(self, name, datasetType, level, doMakeDataRefList=True, ContainerClass=DataIdContainer):
        if name.startswith("-"):
            raise RuntimeError("Name %s must not start with -" % (name,))
        self.name = name
        self.datasetType = datasetType
        self.level = level
        self.doMakeDataRefList = bool(doMakeDataRefList)
        self.ContainerClass = ContainerClass
        self.argName = name.lstrip("-")

    @property
    def isDynamicDatasetType(self):
        """`True` if the dataset type is dynamic (that is, specified on the command line)."""
        return isinstance(self.datasetType, DynamicDatasetType)

    def getDatasetType(self, namespace):
        """Get the dataset type as a string.

        Parameters
        ----------
        namespace
            Parsed command.

        Returns
        -------
        datasetType : `str`
            Dataset type.
        """
        if self.isDynamicDatasetType:
            return self.datasetType.getDatasetType(namespace)
        else:
            return self.datasetType


class DynamicDatasetType(with_metaclass(abc.ABCMeta, object)):
    """Abstract base class for a dataset type determined from parsed command-line arguments.
    """

    def addArgument(self, parser, idName):
        """Add a command-line argument to specify dataset type name, if wanted.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser to add the argument to.
        idName : `str`
            Name of data ID argument, without the leading ``"--"``, e.g. ``"id"``.

        Notes
        -----
        The default implementation does nothing
        """
        pass

    @abc.abstractmethod
    def getDatasetType(self, namespace):
        """Get the dataset type as a string, based on parsed command-line arguments.

        Returns
        -------
        namespace : `str`
            Parsed command.
        """
        raise NotImplementedError("Subclasses must override")


class DatasetArgument(DynamicDatasetType):
    """Dataset type specified by a command-line argument.

    Parameters
    ----------
    name : `str`, optional
        Name of command-line argument (including leading "--", if appropriate) whose value is the dataset
        type. If `None`, uses ``--idName_dstype`` where idName is the name of the data ID argument (e.g.
        "id").
    help : `str`, optional
        Help string for the command-line argument.
    default : obj, optional
        Default value. If `None`, then the command-line option is required. This argument isignored if the
        command-line argument is positional (name does not start with "-") because positional arguments do
        not support default values.
    """

    def __init__(self,
                 name=None,
                 help="dataset type to process from input data repository",
                 default=None,
                 ):
        DynamicDatasetType.__init__(self)
        self.name = name
        self.help = help
        self.default = default

    def getDatasetType(self, namespace):
        """Get the dataset type as a string, from the appropriate command-line argument.

        Parameters
        ----------
        namespace
            Parsed command.

        Returns
        -------
        datasetType : `str`
            Dataset type.
        """
        argName = self.name.lstrip("-")
        return getattr(namespace, argName)

    def addArgument(self, parser, idName):
        """Add a command-line argument to specify the dataset type name.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser.
        idName : `str`
            Data ID.

        Notes
        -----
        Also sets the `name` attribute if it is currently `None`.
        """
        help = self.help if self.help else "dataset type for %s" % (idName,)
        if self.name is None:
            self.name = "--%s_dstype" % (idName,)
        requiredDict = dict()
        if self.name.startswith("-"):
            requiredDict = dict(required=self.default is None)
        parser.add_argument(
            self.name,
            default=self.default,
            help=help,
            **requiredDict)  # cannot specify required=None for positional arguments


class ConfigDatasetType(DynamicDatasetType):
    """Dataset type specified by a config parameter.

    Parameters
    ----------
    name : `str`
        Name of config option whose value is the dataset type.
    """

    def __init__(self, name):
        DynamicDatasetType.__init__(self)
        self.name = name

    def getDatasetType(self, namespace):
        """Return the dataset type as a string, from the appropriate config field

        Parameters
        ----------
        namespace : `argparse.Namespace`
            Parsed command.
        """
        # getattr does not work reliably if the config field name is dotted,
        # so step through one level at a time
        keyList = self.name.split(".")
        value = namespace.config
        for key in keyList:
            try:
                value = getattr(value, key)
            except KeyError:
                raise RuntimeError("Cannot find config parameter %r" % (self.name,))
        return value


class ArgumentParser(argparse.ArgumentParser):
    """Argument parser for command-line tasks that is based on `argparse.ArgumentParser`.

    Parameters
    ----------
    name : `str`
        Name of top-level task; used to identify camera-specific override files.
    usage : `str`, optional
        Command-line usage signature.
    **kwargs
        Additional keyword arguments for `argparse.ArgumentParser`.

    Notes
    -----
    Users may wish to add additional arguments before calling `parse_args`.
    """
    # I would prefer to check data ID keys and values as they are parsed,
    # but the required information comes from the butler, so I have to construct a butler
    # before I do this checking. Constructing a butler is slow, so I only want do it once,
    # after parsing the command line, so as to catch syntax errors quickly.

    requireOutput = True
    """Require an output directory to be specified (`bool`)."""

    def __init__(self, name, usage="%(prog)s input [options]", **kwargs):
        self._name = name
        self._dataIdArgDict = {}  # Dict of data identifier specifications, by argument name
        argparse.ArgumentParser.__init__(self,
                                         usage=usage,
                                         fromfile_prefix_chars='@',
                                         epilog=textwrap.dedent("""Notes:
            * --config, --configfile, --id, --loglevel and @file may appear multiple times;
                all values are used, in order left to right
            * @file reads command-line options from the specified file:
                * data may be distributed among multiple lines (e.g. one option per line)
                * data after # is treated as a comment and ignored
                * blank lines and lines starting with # are ignored
            * To specify multiple values for an option, do not use = after the option name:
                * right: --configfile foo bar
                * wrong: --configfile=foo bar
            """),
                                         formatter_class=argparse.RawDescriptionHelpFormatter,
                                         **kwargs)
        self.add_argument(metavar='input', dest="rawInput",
                          help="path to input data repository, relative to $%s" % (DEFAULT_INPUT_NAME,))
        self.add_argument("--calib", dest="rawCalib",
                          help="path to input calibration repository, relative to $%s" %
                          (DEFAULT_CALIB_NAME,))
        self.add_argument("--output", dest="rawOutput",
                          help="path to output data repository (need not exist), relative to $%s" %
                          (DEFAULT_OUTPUT_NAME,))
        self.add_argument("--rerun", dest="rawRerun", metavar="[INPUT:]OUTPUT",
                          help="rerun name: sets OUTPUT to ROOT/rerun/OUTPUT; "
                               "optionally sets ROOT to ROOT/rerun/INPUT")
        self.add_argument("-c", "--config", nargs="*", action=ConfigValueAction,
                          help="config override(s), e.g. -c foo=newfoo bar.baz=3", metavar="NAME=VALUE")
        self.add_argument("-C", "--configfile", dest="configfile", nargs="*", action=ConfigFileAction,
                          help="config override file(s)")
        self.add_argument("-L", "--loglevel", nargs="*", action=LogLevelAction,
                          help="logging level; supported levels are [trace|debug|info|warn|error|fatal]",
                          metavar="LEVEL|COMPONENT=LEVEL")
        self.add_argument("--longlog", action="store_true", help="use a more verbose format for the logging")
        self.add_argument("--debug", action="store_true", help="enable debugging output?")
        self.add_argument("--doraise", action="store_true",
                          help="raise an exception on error (else log a message and continue)?")
        self.add_argument("--noExit", action="store_true",
                          help="Do not exit even upon failure (i.e. return a struct to the calling script)")
        self.add_argument("--profile", help="Dump cProfile statistics to filename")
        self.add_argument("--show", nargs="+", default=(),
                          help="display the specified information to stdout and quit "
                               "(unless run is specified).")
        self.add_argument("-j", "--processes", type=int, default=1, help="Number of processes to use")
        self.add_argument("-t", "--timeout", type=float,
                          help="Timeout for multiprocessing; maximum wall time (sec)")
        self.add_argument("--clobber-output", action="store_true", dest="clobberOutput", default=False,
                          help=("remove and re-create the output directory if it already exists "
                                "(safe with -j, but not all other forms of parallel execution)"))
        self.add_argument("--clobber-config", action="store_true", dest="clobberConfig", default=False,
                          help=("backup and then overwrite existing config files instead of checking them "
                                "(safe with -j, but not all other forms of parallel execution)"))
        self.add_argument("--no-backup-config", action="store_true", dest="noBackupConfig", default=False,
                          help="Don't copy config to file~N backup.")
        self.add_argument("--clobber-versions", action="store_true", dest="clobberVersions", default=False,
                          help=("backup and then overwrite existing package versions instead of checking"
                                "them (safe with -j, but not all other forms of parallel execution)"))
        self.add_argument("--no-versions", action="store_true", dest="noVersions", default=False,
                          help="don't check package versions; useful for development")
        lsstLog.configure_prop("""
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%c %p: %m%n
""")

    def add_id_argument(self, name, datasetType, help, level=None, doMakeDataRefList=True,
                        ContainerClass=DataIdContainer):
        """Add a data ID argument.


        Parameters
        ----------
        name : `str`
            Data ID argument (including leading dashes, if wanted).
        datasetType : `str` or `DynamicDatasetType`-type
            Type of dataset. Supply a string for a fixed dataset type. For a dynamically determined dataset
            type, supply a `DynamicDatasetType`, such a `DatasetArgument`.
        help : `str`
            Help string for the argument.
        level : object, optional
            Level of dataset, for the `~lsst.daf.persistence.Butler`.
        doMakeDataRefList : bool, optional
            If `True` (default), construct data references.
        ContainerClass : class, optional
            Data ID container class to use to contain results; override the default if you need a special
            means of computing data references from data IDs

        Notes
        -----
        If ``datasetType`` is an instance of `DatasetArgument`, then add a second argument to specify the
        dataset type.

        The associated data is put into ``namespace.<dataIdArgument.name>`` as an instance of ContainerClass;
        the container includes fields:

        - ``idList``: a list of data ID dicts.
        - ``refList``: a list of `~lsst.daf.persistence.Butler` data references (empty if
          ``doMakeDataRefList`` is  `False`).
        """
        argName = name.lstrip("-")

        if argName in self._dataIdArgDict:
            raise RuntimeError("Data ID argument %s already exists" % (name,))
        if argName in set(("camera", "config", "butler", "log", "obsPkg")):
            raise RuntimeError("Data ID argument %s is a reserved name" % (name,))

        self.add_argument(name, nargs="*", action=IdValueAction, help=help,
                          metavar="KEY=VALUE1[^VALUE2[^VALUE3...]")

        dataIdArgument = DataIdArgument(
            name=argName,
            datasetType=datasetType,
            level=level,
            doMakeDataRefList=doMakeDataRefList,
            ContainerClass=ContainerClass,
        )

        if dataIdArgument.isDynamicDatasetType:
            datasetType.addArgument(parser=self, idName=argName)

        self._dataIdArgDict[argName] = dataIdArgument

    def parse_args(self, config, args=None, log=None, override=None):
        """Parse arguments for a command-line task.

        Parameters
        ----------
        config : `lsst.pex.config.Config`
            Config for the task being run.
        args : `list`, optional
            Argument list; if `None` then ``sys.argv[1:]`` is used.
        log : `lsst.log.Log`, optional
            `~lsst.log.Log` instance; if `None` use the default log.
        override : callable, optional
            A config override function. It must take the root config object as its only argument and must
            modify the config in place. This function is called after camera-specific overrides files are
            applied, and before command-line config overrides are applied (thus allowing the user the final
            word).

        Returns
        -------
        namespace : `argparse.Namespace`
            A `~argparse.Namespace` instance containing fields:

            - ``camera``: camera name.
            - ``config``: the supplied config with all overrides applied, validated and frozen.
            - ``butler``: a `lsst.daf.persistence.Butler` for the data.
            - An entry for each of the data ID arguments registered by `add_id_argument`,
              the value of which is a `~lsst.pipe.base.DataIdArgument` that includes public elements
              ``idList`` and ``refList``.
            - ``log``: a `lsst.log` Log.
            - An entry for each command-line argument, with the following exceptions:
              - config is the supplied config, suitably updated.
              - configfile, id and loglevel are all missing.
            - ``obsPkg``: name of the ``obs_`` package for this camera.
        """
        if args is None:
            args = sys.argv[1:]

        if len(args) < 1 or args[0].startswith("-") or args[0].startswith("@"):
            self.print_help()
            if len(args) == 1 and args[0] in ("-h", "--help"):
                self.exit()
            else:
                self.exit("%s: error: Must specify input as first argument" % self.prog)

        # Note that --rerun may change namespace.input, but if it does we verify that the
        # new input has the same mapper class.
        namespace = argparse.Namespace()
        namespace.input = _fixPath(DEFAULT_INPUT_NAME, args[0])
        if not os.path.isdir(namespace.input):
            self.error("Error: input=%r not found" % (namespace.input,))

        namespace.config = config
        namespace.log = log if log is not None else lsstLog.Log.getDefaultLogger()
        mapperClass = dafPersist.Butler.getMapperClass(namespace.input)
        namespace.camera = mapperClass.getCameraName()
        namespace.obsPkg = mapperClass.getPackageName()

        self.handleCamera(namespace)

        self._applyInitialOverrides(namespace)
        if override is not None:
            override(namespace.config)

        # Add data ID containers to namespace
        for dataIdArgument in self._dataIdArgDict.values():
            setattr(namespace, dataIdArgument.name, dataIdArgument.ContainerClass(level=dataIdArgument.level))

        namespace = argparse.ArgumentParser.parse_args(self, args=args, namespace=namespace)
        del namespace.configfile

        self._parseDirectories(namespace)

        if namespace.clobberOutput:
            if namespace.output is None:
                self.error("--clobber-output is only valid with --output or --rerun")
            elif namespace.output == namespace.input:
                self.error("--clobber-output is not valid when the output and input repos are the same")
            if os.path.exists(namespace.output):
                namespace.log.info("Removing output repo %s for --clobber-output", namespace.output)
                shutil.rmtree(namespace.output)

        namespace.log.debug("input=%s", namespace.input)
        namespace.log.debug("calib=%s", namespace.calib)
        namespace.log.debug("output=%s", namespace.output)

        obeyShowArgument(namespace.show, namespace.config, exit=False)

        # No environment variable or --output or --rerun specified.
        if self.requireOutput and namespace.output is None and namespace.rerun is None:
            self.error("no output directory specified.\n"
                       "An output directory must be specified with the --output or --rerun\n"
                       "command-line arguments.\n")

        butlerArgs = {}  # common arguments for butler elements
        if namespace.calib:
            butlerArgs = {'mapperArgs': {'calibRoot': namespace.calib}}
        if namespace.output:
            outputs = {'root': namespace.output, 'mode': 'rw'}
            inputs = {'root': namespace.input}
            inputs.update(butlerArgs)
            outputs.update(butlerArgs)
            namespace.butler = dafPersist.Butler(inputs=inputs, outputs=outputs)
        else:
            outputs = {'root': namespace.input, 'mode': 'rw'}
            outputs.update(butlerArgs)
            namespace.butler = dafPersist.Butler(outputs=outputs)

        # convert data in each of the identifier lists to proper types
        # this is done after constructing the butler, hence after parsing the command line,
        # because it takes a long time to construct a butler
        self._processDataIds(namespace)
        if "data" in namespace.show:
            for dataIdName in self._dataIdArgDict.keys():
                for dataRef in getattr(namespace, dataIdName).refList:
                    print("%s dataRef.dataId = %s" % (dataIdName, dataRef.dataId))

        if namespace.show and "run" not in namespace.show:
            sys.exit(0)

        if namespace.debug:
            try:
                import debug
                assert debug  # silence pyflakes
            except ImportError:
                sys.stderr.write("Warning: no 'debug' module found\n")
                namespace.debug = False

        del namespace.loglevel

        if namespace.longlog:
            lsstLog.configure_prop("""
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.err
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-5p %d{yyyy-MM-ddThh:mm:ss.sss} %c (%X{LABEL})(%F:%L)- %m%n
""")
        del namespace.longlog

        namespace.config.validate()
        namespace.config.freeze()

        return namespace

    def _parseDirectories(self, namespace):
        """Parse input, output and calib directories

        This allows for hacking the directories, e.g., to include a "rerun".
        Modifications are made to the 'namespace' object in-place.
        """
        mapperClass = dafPersist.Butler.getMapperClass(_fixPath(DEFAULT_INPUT_NAME, namespace.rawInput))
        namespace.calib = _fixPath(DEFAULT_CALIB_NAME, namespace.rawCalib)

        # If an output directory is specified, process it and assign it to the namespace
        if namespace.rawOutput:
            namespace.output = _fixPath(DEFAULT_OUTPUT_NAME, namespace.rawOutput)
        else:
            namespace.output = None

        # This section processes the rerun argument, if rerun is specified as a colon separated
        # value, it will be parsed as an input and output. The input value will be overridden if
        # previously specified (but a check is made to make sure both inputs use the same mapper)
        if namespace.rawRerun:
            if namespace.output:
                self.error("Error: cannot specify both --output and --rerun")
            namespace.rerun = namespace.rawRerun.split(":")
            rerunDir = [os.path.join(namespace.input, "rerun", dd) for dd in namespace.rerun]
            modifiedInput = False
            if len(rerunDir) == 2:
                namespace.input, namespace.output = rerunDir
                modifiedInput = True
            elif len(rerunDir) == 1:
                namespace.output = rerunDir[0]
                if os.path.exists(os.path.join(namespace.output, "_parent")):
                    namespace.input = os.path.realpath(os.path.join(namespace.output, "_parent"))
                    modifiedInput = True
            else:
                self.error("Error: invalid argument for --rerun: %s" % namespace.rerun)
            if modifiedInput and dafPersist.Butler.getMapperClass(namespace.input) != mapperClass:
                self.error("Error: input directory specified by --rerun must have the same mapper as INPUT")
        else:
            namespace.rerun = None
        del namespace.rawInput
        del namespace.rawCalib
        del namespace.rawOutput
        del namespace.rawRerun

    def _processDataIds(self, namespace):
        """Process the parsed data for each data ID argument in a `~argparse.Namespace`.

        Processing includes:

        - Validate data ID keys.
        - Cast the data ID values to the correct type.
        - Compute data references from data IDs.

        Parameters
        ----------
        namespace : parsed namespace (an argparse.Namespace);
            Parsed namespace. These attributes are read:

            - ``butler``
            - ``log``
            - ``config``, if any dynamic dataset types are set by a config parameter.
            - Dataset type arguments (e.g. ``id_dstype``), if any dynamic dataset types are specified by such
              and modifies these attributes:
            - ``<name>`` for each data ID argument registered using `add_id_argument`.
        """
        for dataIdArgument in self._dataIdArgDict.values():
            dataIdContainer = getattr(namespace, dataIdArgument.name)
            dataIdContainer.setDatasetType(dataIdArgument.getDatasetType(namespace))
            if dataIdArgument.doMakeDataRefList:
                try:
                    dataIdContainer.castDataIds(butler=namespace.butler)
                except (KeyError, TypeError) as e:
                    # failure of castDataIds indicates invalid command args
                    self.error(e)

                # failure of makeDataRefList indicates a bug that wants a traceback
                dataIdContainer.makeDataRefList(namespace)

    def _applyInitialOverrides(self, namespace):
        """Apply obs-package-specific and camera-specific config override files, if found

        Parameters
        ----------
        namespace : `argparse.Namespace`
            Parsed namespace. These attributes are read:

            - ``obsPkg``

            Look in the package namespace.obsPkg for files:

            - ``config/<task_name>.py``
            - ``config/<camera_name>/<task_name>.py`` and load if found.
        """
        obsPkgDir = lsst.utils.getPackageDir(namespace.obsPkg)
        fileName = self._name + ".py"
        for filePath in (
            os.path.join(obsPkgDir, "config", fileName),
            os.path.join(obsPkgDir, "config", namespace.camera, fileName),
        ):
            if os.path.exists(filePath):
                namespace.log.info("Loading config overrride file %r", filePath)
                namespace.config.load(filePath)
            else:
                namespace.log.debug("Config override file does not exist: %r", filePath)

    def handleCamera(self, namespace):
        """Perform camera-specific operations before parsing the command-line.

        Parameters
        ----------
        namespace : `argparse.Namespace`
            Namespace (an ) with the following fields:

            - ``camera``: the camera name.
            - ``config``: the config passed to parse_args, with no overrides applied.
            - ``obsPkg``: the ``obs_`` package for this camera.
            - ``log``: a `lsst.log` Log.

        Notes
        -----
        The default implementation does nothing.
        """
        pass

    def convert_arg_line_to_args(self, arg_line):
        """Allow files of arguments referenced by ``@<path>`` to contain multiple values on each line.

        Parameters
        ----------
        arg_line : `str`
            Line of text read from an argument file.
        """
        arg_line = arg_line.strip()
        if not arg_line or arg_line.startswith("#"):
            return
        for arg in shlex.split(arg_line, comments=True, posix=True):
            if not arg.strip():
                continue
            yield arg


class InputOnlyArgumentParser(ArgumentParser):
    """`ArgumentParser` for command-line tasks that don't write any output.
    """

    requireOutput = False  # We're not going to write anything


def getTaskDict(config, taskDict=None, baseName=""):
    """Get a dictionary of task info for all subtasks in a config

    Parameters
    ----------
    config : `lsst.pex.config.Config`
        Configuration to process.
    taskDict : `dict`, optional
        Users should not specify this argument. Supports recursion; if provided, taskDict is updated in
        place, else a new `dict` is started).
    baseName : `str`, optional
        Users should not specify this argument. It is only used for recursion: if a non-empty string then a
        period is appended and the result is used as a prefix for additional entries in taskDict; otherwise
        no prefix is used.

    Returns
    -------
    taskDict : `dict`
        Keys are config field names, values are task names.

    Notes
    -----
    This function is designed to be called recursively. The user should call with only a config
    (leaving taskDict and baseName at their default values).
    """
    if taskDict is None:
        taskDict = dict()
    for fieldName, field in config.items():
        if hasattr(field, "value") and hasattr(field, "target"):
            subConfig = field.value
            if isinstance(subConfig, pexConfig.Config):
                subBaseName = "%s.%s" % (baseName, fieldName) if baseName else fieldName
                try:
                    taskName = "%s.%s" % (field.target.__module__, field.target.__name__)
                except Exception:
                    taskName = repr(field.target)
                taskDict[subBaseName] = taskName
                getTaskDict(config=subConfig, taskDict=taskDict, baseName=subBaseName)
    return taskDict


def obeyShowArgument(showOpts, config=None, exit=False):
    """Process arguments specified with ``--show`` (but ignores ``"data"``).

    Parameters
    ----------
    showOpts : `list` of `str`
        List of options passed to ``--show``.
    config : optional
        The provided config.
    exit : bool, optional
        Exit if ``"run"`` isn't included in ``showOpts``.

    Parameters
    ----------
    Supports the following options in showOpts:

    - ``config[=PAT]``. Dump all the config entries, or just the ones that match the glob pattern.
    - ``history=PAT``. Show where the config entries that match the glob pattern were set.
    - ``tasks``. Show task hierarchy.
    - ``data``. Ignored; to be processed by caller.
    - ``run``. Keep going (the default behaviour is to exit if --show is specified).

    Calls ``sys.exit(1)`` if any other option found.
    """
    if not showOpts:
        return

    for what in showOpts:
        showCommand, showArgs = what.split("=", 1) if "=" in what else (what, "")

        if showCommand == "config":
            matConfig = re.search(r"^(?:config.)?(.+)?", showArgs)
            pattern = matConfig.group(1)
            if pattern:
                class FilteredStream(object):
                    """A file object that only prints lines that match the glob "pattern"

                    N.b. Newlines are silently discarded and reinserted;  crude but effective.
                    """

                    def __init__(self, pattern):
                        # obey case if pattern isn't lowecase or requests NOIGNORECASE
                        mat = re.search(r"(.*):NOIGNORECASE$", pattern)

                        if mat:
                            pattern = mat.group(1)
                            self._pattern = re.compile(fnmatch.translate(pattern))
                        else:
                            if pattern != pattern.lower():
                                print(u"Matching \"%s\" without regard to case "
                                      "(append :NOIGNORECASE to prevent this)" % (pattern,), file=sys.stdout)
                            self._pattern = re.compile(fnmatch.translate(pattern), re.IGNORECASE)

                    def write(self, showStr):
                        showStr = showStr.rstrip()
                        # Strip off doc string line(s) and cut off at "=" for string matching
                        matchStr = showStr.split("\n")[-1].split("=")[0]
                        if self._pattern.search(matchStr):
                            print(u"\n" + showStr)

                fd = FilteredStream(pattern)
            else:
                fd = sys.stdout

            config.saveToStream(fd, "config")
        elif showCommand == "history":
            matHistory = re.search(r"^(?:config.)?(.+)?", showArgs)
            pattern = matHistory.group(1)
            if not pattern:
                print("Please provide a value with --show history (e.g. history=XXX)", file=sys.stderr)
                sys.exit(1)

            pattern = pattern.split(".")
            cpath, cname = pattern[:-1], pattern[-1]
            hconfig = config            # the config that we're interested in
            for i, cpt in enumerate(cpath):
                try:
                    hconfig = getattr(hconfig, cpt)
                except AttributeError:
                    print("Error: configuration %s has no subconfig %s" %
                          (".".join(["config"] + cpath[:i]), cpt), file=sys.stderr)

                    sys.exit(1)

            try:
                print(pexConfig.history.format(hconfig, cname))
            except KeyError:
                print("Error: %s has no field %s" % (".".join(["config"] + cpath), cname), file=sys.stderr)
                sys.exit(1)

        elif showCommand == "data":
            pass
        elif showCommand == "run":
            pass
        elif showCommand == "tasks":
            showTaskHierarchy(config)
        else:
            print(u"Unknown value for show: %s (choose from '%s')" %
                  (what, "', '".join("config[=XXX] data history=XXX tasks run".split())), file=sys.stderr)
            sys.exit(1)

    if exit and "run" not in showOpts:
        sys.exit(0)


def showTaskHierarchy(config):
    """Print task hierarchy to stdout.

    Parameters
    ----------
    config : `lsst.pex.config.Config`
        Configuration to process.
    """
    print(u"Subtasks:")
    taskDict = getTaskDict(config=config)

    fieldNameList = sorted(taskDict.keys())
    for fieldName in fieldNameList:
        taskName = taskDict[fieldName]
        print(u"%s: %s" % (fieldName, taskName))


class ConfigValueAction(argparse.Action):
    """argparse action callback to override config parameters using name=value pairs from the command-line.
    """

    def __call__(self, parser, namespace, values, option_string):
        """Override one or more config name value pairs.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. The ``namespace.config`` attribute is updated.
        values : `list`
            A list of ``configItemName=value`` pairs.
        option_string : `str`
            Option value specified by the user.
        """
        if namespace.config is None:
            return
        for nameValue in values:
            name, sep, valueStr = nameValue.partition("=")
            if not valueStr:
                parser.error("%s value %s must be in form name=value" % (option_string, nameValue))

            # see if setting the string value works; if not, try eval
            try:
                setDottedAttr(namespace.config, name, valueStr)
            except AttributeError:
                parser.error("no config field: %s" % (name,))
            except Exception:
                try:
                    value = eval(valueStr, {})
                except Exception:
                    parser.error("cannot parse %r as a value for %s" % (valueStr, name))
                try:
                    setDottedAttr(namespace.config, name, value)
                except Exception as e:
                    parser.error("cannot set config.%s=%r: %s" % (name, value, e))


class ConfigFileAction(argparse.Action):
    """argparse action to load config overrides from one or more files.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        """Load one or more files of config overrides.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. The following attributes are updated by this method: ``namespace.config``.
        values : `list`
            A list of data config file paths.
        option_string : `str`, optional
            Option value specified by the user.
        """
        if namespace.config is None:
            return
        for configfile in values:
            try:
                namespace.config.load(configfile)
            except Exception as e:
                parser.error("cannot load config file %r: %s" % (configfile, e))


class IdValueAction(argparse.Action):
    """argparse action callback to process a data ID into a dict.
    """

    def __call__(self, parser, namespace, values, option_string):
        """Parse ``--id`` data and append results to ``namespace.<argument>.idList``.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command (an instance of argparse.Namespace). The following attributes are updated:

            - ``<idName>.idList``, where ``<idName>`` is the name of the ID argument, for instance ``"id"``
              for ID argument ``--id``.
        values : `list`
            A list of data IDs; see Notes below.
        option_string : `str`
            Option value specified by the user.

        Notes
        -----
        The data format is::

            key1=value1_1[^value1_2[^value1_3...] key2=value2_1[^value2_2[^value2_3...]...

        The values (e.g. ``value1_1``) may either be a string, or of the form ``"int..int"``
        (e.g. ``"1..3"``) which is interpreted as ``"1^2^3"`` (inclusive, unlike a python range).
        So ``"0^2..4^7..9"`` is equivalent to ``"0^2^3^4^7^8^9"``.  You may also specify a stride:
        ``"1..5:2"`` is ``"1^3^5"``.

        The cross product is computed for keys with multiple values. For example::

            --id visit 1^2 ccd 1,1^2,2

        results in the following data ID dicts being appended to ``namespace.<argument>.idList``:

            {"visit":1, "ccd":"1,1"}
            {"visit":2, "ccd":"1,1"}
            {"visit":1, "ccd":"2,2"}
            {"visit":2, "ccd":"2,2"}
        """
        if namespace.config is None:
            return
        idDict = collections.OrderedDict()
        for nameValue in values:
            name, sep, valueStr = nameValue.partition("=")
            if name in idDict:
                parser.error("%s appears multiple times in one ID argument: %s" % (name, option_string))
            idDict[name] = []
            for v in valueStr.split("^"):
                mat = re.search(r"^(\d+)\.\.(\d+)(?::(\d+))?$", v)
                if mat:
                    v1 = int(mat.group(1))
                    v2 = int(mat.group(2))
                    v3 = mat.group(3)
                    v3 = int(v3) if v3 else 1
                    for v in range(v1, v2 + 1, v3):
                        idDict[name].append(str(v))
                else:
                    idDict[name].append(v)

        iterList = [idDict[key] for key in idDict.keys()]
        idDictList = [collections.OrderedDict(zip(idDict.keys(), valList))
                      for valList in itertools.product(*iterList)]

        argName = option_string.lstrip("-")
        ident = getattr(namespace, argName)
        ident.idList += idDictList


class LogLevelAction(argparse.Action):
    """argparse action to set log level.
    """

    def __call__(self, parser, namespace, values, option_string):
        """Set trace level.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. This argument is not used.
        values : `list`
            List of trace levels; each item must be of the form ``component_name=level`` or ``level``,
            where ``level`` is a keyword (not case sensitive) or an integer.
        option_string : `str`
            Option value specified by the user.
        """
        permittedLevelList = ('TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')
        permittedLevelSet = set(permittedLevelList)
        for componentLevel in values:
            component, sep, levelStr = componentLevel.partition("=")
            if not levelStr:
                levelStr, component = component, None
            logLevelUpr = levelStr.upper()
            if logLevelUpr in permittedLevelSet:
                logLevel = getattr(lsstLog.Log, logLevelUpr)
            else:
                parser.error("loglevel=%r not one of %s" % (levelStr, permittedLevelList))
            if component is None:
                namespace.log.setLevel(logLevel)
            else:
                lsstLog.Log.getLogger(component).setLevel(logLevel)


def setDottedAttr(item, name, value):
    """Set an instance attribute (like `setattr` but accepting hierarchical names such as ``foo.bar.baz``).

    Parameters
    ----------
    item : obj
        Object whose attribute is to be set.
    name : `str`
        Name of attribute to set.
    value : obj
        New value for the attribute.

    Notes
    -----
    For example if name is ``foo.bar.baz`` then ``item.foo.bar.baz`` is set to the specified value.
    """
    subitem = item
    subnameList = name.split(".")
    for subname in subnameList[:-1]:
        subitem = getattr(subitem, subname)
    setattr(subitem, subnameList[-1], value)


def getDottedAttr(item, name):
    """Get an attribute (like `getattr` but accepts hierarchical names such as ``foo.bar.baz``).

    Parameters
    ----------
    item : obj
        Object whose attribute is to be returned.
    name : `str`
        Name of the attribute to get.

    Returns
    -------
    itemAttr : obj
        If name is ``foo.bar.baz then the return value is ``item.foo.bar.baz``.
    """
    subitem = item
    for subname in name.split("."):
        subitem = getattr(subitem, subname)
    return subitem


def dataExists(butler, datasetType, dataRef):
    """Determine if data exists at the current level or any data exists at a deeper level.

    Parameters
    ----------
    butler : `lsst.daf.persistence.Butler`
        The Butler.
    datasetType : `str`
        Dataset type.
    dataRef : `lsst.daf.persistence.ButlerDataRef`
        Butler data reference.

    Returns
    -------
    exists : `bool`
        Return value is `True` if data exists, `False` otherwise.
    """
    subDRList = dataRef.subItems()
    if subDRList:
        for subDR in subDRList:
            if dataExists(butler, datasetType, subDR):
                return True
        return False
    else:
        return butler.datasetExists(datasetType=datasetType, dataId=dataRef.dataId)
