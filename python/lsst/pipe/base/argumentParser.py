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
__all__ = [
    "ArgumentParser",
    "ConfigFileAction",
    "ConfigValueAction",
    "DataIdContainer",
    "DatasetArgument",
    "ConfigDatasetType",
    "InputOnlyArgumentParser",
]

import abc
import argparse
import collections
import fnmatch
import itertools
import logging
import os
import re
import shlex
import shutil
import sys
import textwrap

import lsst.daf.persistence as dafPersist
import lsst.log as lsstLog
import lsst.pex.config as pexConfig
import lsst.pex.config.history
import lsst.utils
import lsst.utils.logging

DEFAULT_INPUT_NAME = "PIPE_INPUT_ROOT"
DEFAULT_CALIB_NAME = "PIPE_CALIB_ROOT"
DEFAULT_OUTPUT_NAME = "PIPE_OUTPUT_ROOT"


def _fixPath(defName, path):
    """Apply environment variable as default root, if present, and abspath.

    Parameters
    ----------
    defName : `str`
        Name of environment variable containing default root path;
        if the environment variable does not exist
        then the path is relative to the current working directory
    path : `str`
        Path relative to default root path.

    Returns
    -------
    abspath : `str`
        Path that has been expanded, or `None` if the environment variable
        does not exist and path is `None`.
    """
    defRoot = os.environ.get(defName)
    if defRoot is None:
        if path is None:
            return None
        return os.path.abspath(path)
    return os.path.abspath(os.path.join(defRoot, path or ""))


class DataIdContainer:
    """Container for data IDs and associated data references.

    Parameters
    ----------
    level : `str`
        The lowest hierarchy level to descend to for this dataset type,
        for example `"amp"` for `"raw"` or `"ccd"` for `"calexp"`.
        Use `""` to use the mapper's default for the dataset type.
        This class does not support `None`, but if it did, `None`
        would mean the level should not be restricted.

    Notes
    -----
    Override this class for data IDs that require special handling to be
    converted to ``data references``, and specify the override class
    as ``ContainerClass`` for ``add_id_argument``.

    If you don't want the argument parser to compute data references,
    specify ``doMakeDataRefList=False`` in ``add_id_argument``.
    """

    def __init__(self, level=None):
        self.datasetType = None
        """Dataset type of the data references (`str`).
        """
        self.level = level
        """See parameter ``level`` (`str`).
        """
        self.idList = []
        """List of data IDs specified on the command line for the
        appropriate data ID argument (`list` of `dict`).
        """
        self.refList = []
        """List of data references for the data IDs in ``idList``
        (`list` of `lsst.daf.persistence.ButlerDataRef`).
        Elements will be omitted if the corresponding data is not found.
        The list will be empty when returned by ``parse_args`` if
        ``doMakeDataRefList=False`` was specified in ``add_id_argument``.
        """

    def setDatasetType(self, datasetType):
        """Set actual dataset type, once it is known.

        Parameters
        ----------
        datasetType : `str`
            Dataset type.

        Notes
        -----
        The reason ``datasetType`` is not a constructor argument is that
        some subclasses do not know the dataset type until the command
        is parsed. Thus, to reduce special cases in the code,
        ``datasetType`` is always set after the command is parsed.
        """
        self.datasetType = datasetType

    def castDataIds(self, butler):
        """Validate data IDs and cast them to the correct type
        (modify idList in place).

        This code casts the values in the data IDs dicts in `dataIdList`
        to the type required by the butler. Data IDs are read from the
        command line as `str`, but the butler requires some values to be
        other types. For example "visit" values should be `int`.

        Parameters
        ----------
        butler : `lsst.daf.persistence.Butler`
            Data butler.
        """
        if self.datasetType is None:
            raise RuntimeError("Must call setDatasetType first")
        try:
            idKeyTypeDict = butler.getKeys(datasetType=self.datasetType, level=self.level)
        except KeyError as e:
            msg = f"Cannot get keys for datasetType {self.datasetType} at level {self.level}"
            raise KeyError(msg) from e

        for dataDict in self.idList:
            for key, strVal in dataDict.items():
                try:
                    keyType = idKeyTypeDict[key]
                except KeyError:
                    # OK, assume that it's a valid key and guess that it's a
                    # string
                    keyType = str

                    log = lsst.utils.logging.getLogger()
                    log.warning(
                        'Unexpected ID %s; guessing type is "%s"', key, "str" if keyType == str else keyType
                    )
                    idKeyTypeDict[key] = keyType

                if keyType != str:
                    try:
                        castVal = keyType(strVal)
                    except Exception:
                        raise TypeError(f"Cannot cast value {strVal!r} to {keyType} for ID key {key}")
                    dataDict[key] = castVal

    def makeDataRefList(self, namespace):
        """Compute refList based on idList.

        Parameters
        ----------
        namespace : `argparse.Namespace`
            Results of parsing command-line. The ``butler`` and ``log``
            elements must be set.

        Notes
        -----
        Not called if ``add_id_argument`` was called with
        ``doMakeDataRefList=False``.
        """
        if self.datasetType is None:
            raise RuntimeError("Must call setDatasetType first")
        butler = namespace.butler
        for dataId in self.idList:
            refList = dafPersist.searchDataRefs(
                butler, datasetType=self.datasetType, level=self.level, dataId=dataId
            )
            if not refList:
                namespace.log.warning("No data found for dataId=%s", dataId)
                continue
            self.refList += refList


class DataIdArgument:
    """data ID argument, used by `ArgumentParser.add_id_argument`.

    Parameters
    ----------
    name : `str`
        Name of identifier (argument name without dashes).
    datasetType : `str`
        Type of dataset; specify a string for a fixed dataset type
        or a `DatasetArgument` for a dynamic dataset type (e.g.
        one specified by a command-line argument).
    level : `str`
        The lowest hierarchy level to descend to for this dataset type,
        for example `"amp"` for `"raw"` or `"ccd"` for `"calexp"`.
        Use `""` to use the mapper's default for the dataset type.
        Some container classes may also support `None`, which means
        the level should not be restricted; however the default class,
        `DataIdContainer`, does not support `None`.
    doMakeDataRefList : `bool`, optional
        If `True` (default), construct data references.
    ContainerClass : `class`, optional
        Class to contain data IDs and data references; the default class
        `DataIdContainer` will work for many, but not all, cases.
        For example if the dataset type is specified on the command line
        then use `DynamicDatasetType`.
    """

    def __init__(self, name, datasetType, level, doMakeDataRefList=True, ContainerClass=DataIdContainer):
        if name.startswith("-"):
            raise RuntimeError(f"Name {name} must not start with -")
        self.name = name
        self.datasetType = datasetType
        self.level = level
        self.doMakeDataRefList = bool(doMakeDataRefList)
        self.ContainerClass = ContainerClass
        self.argName = name.lstrip("-")

    @property
    def isDynamicDatasetType(self):
        """`True` if the dataset type is dynamic (that is, specified
        on the command line).
        """
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


class DynamicDatasetType(metaclass=abc.ABCMeta):
    """Abstract base class for a dataset type determined from parsed
    command-line arguments.
    """

    def addArgument(self, parser, idName):
        """Add a command-line argument to specify dataset type name,
        if wanted.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser to add the argument to.
        idName : `str`
            Name of data ID argument, without the leading ``"--"``,
            e.g. ``"id"``.

        Notes
        -----
        The default implementation does nothing
        """
        pass

    @abc.abstractmethod
    def getDatasetType(self, namespace):
        """Get the dataset type as a string, based on parsed command-line
        arguments.

        Returns
        -------
        datasetType : `str`
            Dataset type.
        """
        raise NotImplementedError("Subclasses must override")


class DatasetArgument(DynamicDatasetType):
    """Dataset type specified by a command-line argument.

    Parameters
    ----------
    name : `str`, optional
        Name of command-line argument (including leading "--",
        if appropriate) whose value is the dataset type.
        If `None`, uses ``--idName_dstype`` where idName
        is the name of the data ID argument (e.g. "id").
    help : `str`, optional
        Help string for the command-line argument.
    default : `object`, optional
        Default value. If `None`, then the command-line option is required.
        This argument isignored if the command-line argument is positional
        (name does not start with "-") because positional arguments do
        not support default values.
    """

    def __init__(
        self,
        name=None,
        help="dataset type to process from input data repository",
        default=None,
    ):
        DynamicDatasetType.__init__(self)
        self.name = name
        self.help = help
        self.default = default

    def getDatasetType(self, namespace):
        """Get the dataset type as a string, from the appropriate
        command-line argument.

        Parameters
        ----------
        namespace :
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
        help = self.help if self.help else f"dataset type for {idName}"
        if self.name is None:
            self.name = f"--{idName}_dstype"
        requiredDict = dict()
        if self.name.startswith("-"):
            requiredDict = dict(required=self.default is None)
        parser.add_argument(self.name, default=self.default, help=help, **requiredDict)


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
        """Return the dataset type as a string, from the appropriate
        config field.

        Parameters
        ----------
        namespace : `argparse.Namespace`
            Parsed command.
        """
        # getattr does not work reliably if the config field name is
        # dotted, so step through one level at a time
        keyList = self.name.split(".")
        value = namespace.config
        for key in keyList:
            try:
                value = getattr(value, key)
            except KeyError:
                raise RuntimeError(f"Cannot find config parameter {self.name!r}")
        return value


class ArgumentParser(argparse.ArgumentParser):
    """Argument parser for command-line tasks that is based on
    `argparse.ArgumentParser`.

    Parameters
    ----------
    name : `str`
        Name of top-level task; used to identify camera-specific override
        files.
    usage : `str`, optional
        Command-line usage signature.
    **kwargs
        Additional keyword arguments for `argparse.ArgumentParser`.

    Notes
    -----
    Users may wish to add additional arguments before calling `parse_args`.
    """

    # I would prefer to check data ID keys and values as they are parsed,
    # but the required information comes from the butler, so I have to
    # construct a butler before I do this checking. Constructing a butler
    # is slow, so I only want do it once, after parsing the command line,
    # so as to catch syntax errors quickly.

    requireOutput = True
    """Require an output directory to be specified (`bool`)."""

    def __init__(self, name, usage="%(prog)s input [options]", **kwargs):
        self._name = name
        self._dataIdArgDict = {}  # Dict of data identifier specifications, by argument name
        argparse.ArgumentParser.__init__(
            self,
            usage=usage,
            fromfile_prefix_chars="@",
            epilog=textwrap.dedent(
                """Notes:
            * --config, --config-file or --configfile, --id, --loglevel and @file may appear multiple times;
                all values are used, in order left to right
            * @file reads command-line options from the specified file:
                * data may be distributed among multiple lines (e.g. one option per line)
                * data after # is treated as a comment and ignored
                * blank lines and lines starting with # are ignored
            * To specify multiple values for an option, do not use = after the option name:
                * right: --config-file foo bar
                * wrong: --config-file=foo bar
            """
            ),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            **kwargs,
        )
        self.add_argument(
            metavar="input",
            dest="rawInput",
            help=f"path to input data repository, relative to ${DEFAULT_INPUT_NAME}",
        )
        self.add_argument(
            "--calib",
            dest="rawCalib",
            help=f"path to input calibration repository, relative to ${DEFAULT_CALIB_NAME}",
        )
        self.add_argument(
            "--output",
            dest="rawOutput",
            help=f"path to output data repository (need not exist), relative to ${DEFAULT_OUTPUT_NAME}",
        )
        self.add_argument(
            "--rerun",
            dest="rawRerun",
            metavar="[INPUT:]OUTPUT",
            help="rerun name: sets OUTPUT to ROOT/rerun/OUTPUT; optionally sets ROOT to ROOT/rerun/INPUT",
        )
        self.add_argument(
            "-c",
            "--config",
            nargs="*",
            action=ConfigValueAction,
            help="config override(s), e.g. -c foo=newfoo bar.baz=3",
            metavar="NAME=VALUE",
        )
        self.add_argument(
            "-C",
            "--config-file",
            "--configfile",
            dest="configfile",
            nargs="*",
            action=ConfigFileAction,
            help="config override file(s)",
        )
        self.add_argument(
            "-L",
            "--loglevel",
            nargs="*",
            action=LogLevelAction,
            help="logging level; supported levels are [trace|debug|info|warn|error|fatal]",
            metavar="LEVEL|COMPONENT=LEVEL",
        )
        self.add_argument(
            "--longlog",
            nargs=0,
            action=LongLogAction,
            help="use a more verbose format for the logging",
        )
        self.add_argument("--debug", action="store_true", help="enable debugging output?")
        self.add_argument(
            "--doraise",
            action="store_true",
            help="raise an exception on error (else log a message and continue)?",
        )
        self.add_argument(
            "--noExit",
            action="store_true",
            help="Do not exit even upon failure (i.e. return a struct to the calling script)",
        )
        self.add_argument("--profile", help="Dump cProfile statistics to filename")
        self.add_argument(
            "--show",
            nargs="+",
            default=(),
            help="display the specified information to stdout and quit "
            "(unless run is specified); information is "
            "(config[=PATTERN]|history=PATTERN|tasks|data|run)",
        )
        self.add_argument("-j", "--processes", type=int, default=1, help="Number of processes to use")
        self.add_argument(
            "-t", "--timeout", type=float, help="Timeout for multiprocessing; maximum wall time (sec)"
        )
        self.add_argument(
            "--clobber-output",
            action="store_true",
            dest="clobberOutput",
            default=False,
            help=(
                "remove and re-create the output directory if it already exists "
                "(safe with -j, but not all other forms of parallel execution)"
            ),
        )
        self.add_argument(
            "--clobber-config",
            action="store_true",
            dest="clobberConfig",
            default=False,
            help=(
                "backup and then overwrite existing config files instead of checking them "
                "(safe with -j, but not all other forms of parallel execution)"
            ),
        )
        self.add_argument(
            "--no-backup-config",
            action="store_true",
            dest="noBackupConfig",
            default=False,
            help="Don't copy config to file~N backup.",
        )
        self.add_argument(
            "--clobber-versions",
            action="store_true",
            dest="clobberVersions",
            default=False,
            help=(
                "backup and then overwrite existing package versions instead of checking"
                "them (safe with -j, but not all other forms of parallel execution)"
            ),
        )
        self.add_argument(
            "--no-versions",
            action="store_true",
            dest="noVersions",
            default=False,
            help="don't check package versions; useful for development",
        )
        lsstLog.configure_prop(
            """
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.out
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%c %p: %m%n
"""
        )

        # Forward all Python logging to lsst.log
        lgr = logging.getLogger()
        lgr.setLevel(logging.INFO)  # same as in log4cxx config above
        lgr.addHandler(lsstLog.LogHandler())

    def add_id_argument(
        self, name, datasetType, help, level=None, doMakeDataRefList=True, ContainerClass=DataIdContainer
    ):
        """Add a data ID argument.


        Parameters
        ----------
        name : `str`
            Data ID argument (including leading dashes, if wanted).
        datasetType : `str` or `DynamicDatasetType`-type
            Type of dataset. Supply a string for a fixed dataset type.
            For a dynamically determined dataset type, supply
            a `DynamicDatasetType`, such a `DatasetArgument`.
        help : `str`
            Help string for the argument.
        level : `str`
            The lowest hierarchy level to descend to for this dataset type,
            for example `"amp"` for `"raw"` or `"ccd"` for `"calexp"`.
            Use `""` to use the mapper's default for the dataset type.
            Some container classes may also support `None`, which means
            the level should not be restricted; however the default class,
            `DataIdContainer`, does not support `None`.
        doMakeDataRefList : bool, optional
            If `True` (default), construct data references.
        ContainerClass : `class`, optional
        Class to contain data IDs and data references; the default class
        `DataIdContainer` will work for many, but not all, cases.
        For example if the dataset type is specified on the command line
        then use `DynamicDatasetType`.

        Notes
        -----
        If ``datasetType`` is an instance of `DatasetArgument`,
        then add a second argument to specify the dataset type.

        The associated data is put into ``namespace.<dataIdArgument.name>``
        as an instance of `ContainerClass`; the container includes fields:

        - ``idList``: a list of data ID dicts.
        - ``refList``: a list of `~lsst.daf.persistence.Butler`
            data references (empty if ``doMakeDataRefList`` is  `False`).
        """
        argName = name.lstrip("-")

        if argName in self._dataIdArgDict:
            raise RuntimeError(f"Data ID argument {name} already exists")
        if argName in set(("camera", "config", "butler", "log", "obsPkg")):
            raise RuntimeError(f"Data ID argument {name} is a reserved name")

        self.add_argument(
            name, nargs="*", action=IdValueAction, help=help, metavar="KEY=VALUE1[^VALUE2[^VALUE3...]"
        )

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
        log : `lsst.log.Log` or `logging.Logger`, optional
            Logger instance; if `None` use the default log.
        override : callable, optional
            A config override function. It must take the root config object
            as its only argument and must modify the config in place.
            This function is called after camera-specific overrides files
            are applied, and before command-line config overrides
            are applied (thus allowing the user the final word).

        Returns
        -------
        namespace : `argparse.Namespace`
            A `~argparse.Namespace` instance containing fields:

            - ``camera``: camera name.
            - ``config``: the supplied config with all overrides applied,
                validated and frozen.
            - ``butler``: a `lsst.daf.persistence.Butler` for the data.
            - An entry for each of the data ID arguments registered by
                `add_id_argument`, of the type passed to its ``ContainerClass``
                keyword (`~lsst.pipe.base.DataIdContainer` by default). It
                includes public elements ``idList`` and ``refList``.
            - ``log``: a `lsst.pipe.base.TaskLogAdapter` log.
            - An entry for each command-line argument,
                with the following exceptions:

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
                self.exit(f"{self.prog}: error: Must specify input as first argument")

        # Note that --rerun may change namespace.input, but if it does
        # we verify that the new input has the same mapper class.
        namespace = argparse.Namespace()
        namespace.input = _fixPath(DEFAULT_INPUT_NAME, args[0])
        if not os.path.isdir(namespace.input):
            self.error(f"Error: input={namespace.input!r} not found")

        namespace.config = config
        # Ensure that the external logger is converted to the expected
        # logger class.
        namespace.log = (
            lsst.utils.logging.getLogger(log.name) if log is not None else lsst.utils.logging.getLogger()
        )
        mapperClass = dafPersist.Butler.getMapperClass(namespace.input)
        if mapperClass is None:
            self.error(f"Error: no mapper specified for input repo {namespace.input!r}")

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
            self.error(
                "no output directory specified.\n"
                "An output directory must be specified with the --output or --rerun\n"
                "command-line arguments.\n"
            )

        butlerArgs = {}  # common arguments for butler elements
        if namespace.calib:
            butlerArgs = {"mapperArgs": {"calibRoot": namespace.calib}}
        if namespace.output:
            outputs = {"root": namespace.output, "mode": "rw"}
            inputs = {"root": namespace.input}
            inputs.update(butlerArgs)
            outputs.update(butlerArgs)
            namespace.butler = dafPersist.Butler(inputs=inputs, outputs=outputs)
        else:
            outputs = {"root": namespace.input, "mode": "rw"}
            outputs.update(butlerArgs)
            namespace.butler = dafPersist.Butler(outputs=outputs)

        # convert data in each of the identifier lists to proper types
        # this is done after constructing the butler,
        # hence after parsing the command line,
        # because it takes a long time to construct a butler
        self._processDataIds(namespace)
        if "data" in namespace.show:
            for dataIdName in self._dataIdArgDict.keys():
                for dataRef in getattr(namespace, dataIdName).refList:
                    print(f"{dataIdName} dataRef.dataId = {dataRef.dataId}")

        if namespace.show and "run" not in namespace.show:
            sys.exit(0)

        if namespace.debug:
            try:
                import debug  # type: ignore

                assert debug  # silence pyflakes (above silences mypy)
            except ImportError:
                print("Warning: no 'debug' module found", file=sys.stderr)
                namespace.debug = False

        del namespace.loglevel
        del namespace.longlog

        namespace.config.validate()
        namespace.config.freeze()

        return namespace

    def _parseDirectories(self, namespace):
        """Parse input, output and calib directories

        This allows for hacking the directories, e.g., to include a
        "rerun".
        Modifications are made to the 'namespace' object in-place.
        """
        mapperClass = dafPersist.Butler.getMapperClass(_fixPath(DEFAULT_INPUT_NAME, namespace.rawInput))
        namespace.calib = _fixPath(DEFAULT_CALIB_NAME, namespace.rawCalib)

        # If an output directory is specified, process it and assign it to the
        # namespace
        if namespace.rawOutput:
            namespace.output = _fixPath(DEFAULT_OUTPUT_NAME, namespace.rawOutput)
        else:
            namespace.output = None

        # This section processes the rerun argument.
        # If rerun is specified as a colon separated value,
        # it will be parsed as an input and output.
        # The input value will be overridden if previously specified
        # (but a check is made to make sure both inputs use
        # the same mapper)
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
                self.error(f"Error: invalid argument for --rerun: {namespace.rerun}")
            if modifiedInput and dafPersist.Butler.getMapperClass(namespace.input) != mapperClass:
                self.error("Error: input directory specified by --rerun must have the same mapper as INPUT")
        else:
            namespace.rerun = None
        del namespace.rawInput
        del namespace.rawCalib
        del namespace.rawOutput
        del namespace.rawRerun

    def _processDataIds(self, namespace):
        """Process the parsed data for each data ID argument in an
        `~argparse.Namespace`.

        Processing includes:

        - Validate data ID keys.
        - Cast the data ID values to the correct type.
        - Compute data references from data IDs.

        Parameters
        ----------
        namespace : `argparse.Namespace`
            Parsed namespace. These attributes are read:

            - ``butler``
            - ``log``
            - ``config``, if any dynamic dataset types are set by
              a config parameter.
            - Dataset type arguments (e.g. ``id_dstype``), if any dynamic
              dataset types are specified by such

            These attributes are modified:

            - ``<name>`` for each data ID argument registered using
                `add_id_argument` with name ``<name>``.
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

                # failure of makeDataRefList indicates a bug
                # that wants a traceback
                dataIdContainer.makeDataRefList(namespace)

    def _applyInitialOverrides(self, namespace):
        """Apply obs-package-specific and camera-specific config
        override files, if found

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
            - ``config``: the config passed to parse_args, with no overrides
              applied.
            - ``obsPkg``: the ``obs_`` package for this camera.
            - ``log``: a `lsst.pipe.base.TaskLogAdapter` Log.

        Notes
        -----
        The default implementation does nothing.
        """
        pass

    def convert_arg_line_to_args(self, arg_line):
        """Allow files of arguments referenced by ``@<path>`` to contain
        multiple values on each line.

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

    def addReuseOption(self, choices):
        """Add a "--reuse-outputs-from SUBTASK" option to the argument
        parser.

        CmdLineTasks that can be restarted at an intermediate step using
        outputs from earlier (but still internal) steps should use this
        method to allow the user to control whether that happens when
        outputs from earlier steps are present.

        Parameters
        ----------
        choices : sequence
            A sequence of string names (by convention, top-level subtasks)
            that identify the steps that could be skipped when their
            outputs are already present.  The list is ordered, so when the
            user specifies one step on the command line, all previous steps
            may be skipped as well.  In addition to the choices provided,
            users may pass "all" to indicate that all steps may be thus
            skipped.

        When this method is called, the ``namespace`` object returned by
        ``parse_args`` will contain a ``reuse`` attribute containing
        a list of all steps that should be skipped if their outputs
        are already present.
        If no steps should be skipped, the ``reuse`` will be an empty list.
        """
        choices = list(choices)
        choices.append("all")
        self.add_argument(
            "--reuse-outputs-from",
            dest="reuse",
            choices=choices,
            default=[],
            action=ReuseAction,
            help=(
                "Skip the given subtask and its predecessors and reuse their outputs "
                "if those outputs already exist.  Use 'all' to specify all subtasks."
            ),
        )


class InputOnlyArgumentParser(ArgumentParser):
    """`ArgumentParser` for command-line tasks that don't write any output."""

    requireOutput = False  # We're not going to write anything


def getTaskDict(config, taskDict=None, baseName=""):
    """Get a dictionary of task info for all subtasks in a config

    Parameters
    ----------
    config : `lsst.pex.config.Config`
        Configuration to process.
    taskDict : `dict`, optional
        Users should not specify this argument. Supports recursion.
        If provided, taskDict is updated in place, else a new `dict`
        is started.
    baseName : `str`, optional
        Users should not specify this argument. It is only used for
        recursion: if a non-empty string then a period is appended
        and the result is used as a prefix for additional entries
        in taskDict; otherwise no prefix is used.

    Returns
    -------
    taskDict : `dict`
        Keys are config field names, values are task names.

    Notes
    -----
    This function is designed to be called recursively.
    The user should call with only a config (leaving taskDict and baseName
    at their default values).
    """
    if taskDict is None:
        taskDict = dict()
    for fieldName, field in config.items():
        if hasattr(field, "value") and hasattr(field, "target"):
            subConfig = field.value
            if isinstance(subConfig, pexConfig.Config):
                subBaseName = f"{baseName}.{fieldName}" if baseName else fieldName
                try:
                    taskName = f"{field.target.__module__}.{field.target.__name__}"
                except Exception:
                    taskName = repr(field.target)
                taskDict[subBaseName] = taskName
                getTaskDict(config=subConfig, taskDict=taskDict, baseName=subBaseName)
    return taskDict


def obeyShowArgument(showOpts, config=None, exit=False):
    """Process arguments specified with ``--show`` (but ignores
    ``"data"``).

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

    - ``config[=PAT]``. Dump all the config entries, or just the ones that
        match the glob pattern.
    - ``history=PAT``. Show where the config entries that match the glob
        pattern were set.
    - ``tasks``. Show task hierarchy.
    - ``data``. Ignored; to be processed by caller.
    - ``run``. Keep going (the default behaviour is to exit if
        ``--show`` is specified).

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

                class FilteredStream:
                    """A file object that only prints lines
                    that match the glob "pattern".

                    N.b. Newlines are silently discarded and reinserted;
                    crude but effective.
                    """

                    def __init__(self, pattern):
                        # obey case if pattern isn't lowecase or requests
                        # NOIGNORECASE
                        mat = re.search(r"(.*):NOIGNORECASE$", pattern)

                        if mat:
                            pattern = mat.group(1)
                            self._pattern = re.compile(fnmatch.translate(pattern))
                        else:
                            if pattern != pattern.lower():
                                print(
                                    f"Matching {pattern!r} without regard to case "
                                    "(append :NOIGNORECASE to prevent this)",
                                    file=sys.stdout,
                                )
                            self._pattern = re.compile(fnmatch.translate(pattern), re.IGNORECASE)

                    def write(self, showStr):
                        showStr = showStr.rstrip()
                        # Strip off doc string line(s) and cut off
                        # at "=" for string matching
                        matchStr = showStr.split("\n")[-1].split("=")[0]
                        if self._pattern.search(matchStr):
                            print("\n" + showStr)

                fd = FilteredStream(pattern)
            else:
                fd = sys.stdout

            config.saveToStream(fd, "config")
        elif showCommand == "history":
            matHistory = re.search(r"^(?:config.)?(.+)?", showArgs)
            globPattern = matHistory.group(1)
            if not globPattern:
                print("Please provide a value with --show history (e.g. history=*.doXXX)", file=sys.stderr)
                sys.exit(1)

            error = False
            for i, pattern in enumerate(fnmatch.filter(config.names(), globPattern)):
                if i > 0:
                    print("")

                pattern = pattern.split(".")
                cpath, cname = pattern[:-1], pattern[-1]
                hconfig = config  # the config that we're interested in
                for i, cpt in enumerate(cpath):
                    try:
                        hconfig = getattr(hconfig, cpt)
                    except AttributeError:
                        config_path = ".".join(["config"] + cpath[:i])
                        print(f"Error: configuration {config_path} has no subconfig {cpt}", file=sys.stderr)
                        error = True

                try:
                    print(pexConfig.history.format(hconfig, cname))
                except KeyError:
                    config_path = ".".join(["config"] + cpath)
                    print(f"Error: {config_path} has no field {cname}", file=sys.stderr)
                    error = True

            if error:
                sys.exit(1)

        elif showCommand == "data":
            pass
        elif showCommand == "run":
            pass
        elif showCommand == "tasks":
            showTaskHierarchy(config)
        else:
            choices = "', '".join("config[=XXX] data history=XXX tasks run".split())
            print(f"Unknown value for show: {what} (choose from {choices!r})", file=sys.stderr)
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
    print("Subtasks:")
    taskDict = getTaskDict(config=config)

    fieldNameList = sorted(taskDict.keys())
    for fieldName in fieldNameList:
        taskName = taskDict[fieldName]
        print(f"{fieldName}: {taskName}")


class ConfigValueAction(argparse.Action):
    """argparse action callback to override config parameters using
    name=value pairs from the command-line.
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
                parser.error(f"{option_string} value {nameValue} must be in form name=value")

            # see if setting the string value works; if not, try eval
            try:
                setDottedAttr(namespace.config, name, valueStr)
            except AttributeError:
                parser.error(f"no config field: {name}")
            except Exception:
                try:
                    value = eval(valueStr, {})
                except Exception:
                    parser.error(f"cannot parse {valueStr!r} as a value for {name}")
                try:
                    setDottedAttr(namespace.config, name, value)
                except Exception as e:
                    parser.error(f"cannot set config.{name}={value!r}: {e}")


class ConfigFileAction(argparse.Action):
    """argparse action to load config overrides from one or more files."""

    def __call__(self, parser, namespace, values, option_string=None):
        """Load one or more files of config overrides.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. The following attributes are updated by this
            method: ``namespace.config``.
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
                parser.error(f"cannot load config file {configfile!r}: {e}")


class IdValueAction(argparse.Action):
    """argparse action callback to process a data ID into a dict."""

    def __call__(self, parser, namespace, values, option_string):
        """Parse ``--id`` data and append results to
        ``namespace.<argument>.idList``.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command (an instance of argparse.Namespace).
            The following attributes are updated:

            - ``<idName>.idList``, where ``<idName>`` is the name of the
              ID argument, for instance ``"id"`` for ID argument ``--id``.
        values : `list`
            A list of data IDs; see Notes below.
        option_string : `str`
            Option value specified by the user.

        Notes
        -----
        The data format is::

            key1=value1_1[^value1_2[^value1_3...]
            key2=value2_1[^value2_2[^value2_3...]...

        The values (e.g. ``value1_1``) may either be a string,
        or of the form ``"int..int"`` (e.g. ``"1..3"``) which is
        interpreted as ``"1^2^3"`` (inclusive, unlike a python range).
        So ``"0^2..4^7..9"`` is equivalent to ``"0^2^3^4^7^8^9"``.
        You may also specify a stride: ``"1..5:2"`` is ``"1^3^5"``.

        The cross product is computed for keys with multiple values.
        For example::

            --id visit 1^2 ccd 1,1^2,2

        results in the following data ID dicts being appended to
        ``namespace.<argument>.idList``:

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
                parser.error(f"{name} appears multiple times in one ID argument: {option_string}")
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
        idDictList = [
            collections.OrderedDict(zip(idDict.keys(), valList)) for valList in itertools.product(*iterList)
        ]

        argName = option_string.lstrip("-")
        ident = getattr(namespace, argName)
        ident.idList += idDictList


class LongLogAction(argparse.Action):
    """argparse action to make logs verbose.

    An action so that it can take effect before log level options.
    """

    def __call__(self, parser, namespace, values, option_string):
        """Set long log.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. This argument is not used.
        values : `list`
            Unused.
        option_string : `str`
            Option value specified by the user (unused).
        """
        lsstLog.configure_prop(
            """
log4j.rootLogger=INFO, A1
log4j.appender.A1=ConsoleAppender
log4j.appender.A1.Target=System.out
log4j.appender.A1.layout=PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-5p %d{yyyy-MM-ddTHH:mm:ss.SSSZ} %c (%X{LABEL})(%F:%L)- %m%n
"""
        )


class LogLevelAction(argparse.Action):
    """argparse action to set log level."""

    def __call__(self, parser, namespace, values, option_string):
        """Set trace level.

        Parameters
        ----------
        parser : `ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. This argument is not used.
        values : `list`
            List of trace levels; each item must be of the form
            ``component_name=level`` or ``level``, where ``level``
            is a keyword (not case sensitive) or an integer.
        option_string : `str`
            Option value specified by the user.
        """
        permittedLevelList = ("TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL")
        permittedLevelSet = set(permittedLevelList)
        for componentLevel in values:
            component, sep, levelStr = componentLevel.partition("=")
            if not levelStr:
                levelStr, component = component, None
            logLevelUpr = levelStr.upper()

            if component is None:
                logger = namespace.log
            else:
                logger = lsst.utils.logging.getLogger(component)

            if logLevelUpr in permittedLevelSet:
                logLevel = getattr(logger, logLevelUpr)
            else:
                parser.error(f"loglevel={levelStr!r} not one of {permittedLevelList}")

            logger.setLevel(logLevel)

            # Set logging level for whatever logger this wasn't.
            if isinstance(logger, lsstLog.Log):
                pyLevel = lsstLog.LevelTranslator.lsstLog2logging(logLevel)
                logging.getLogger(component or None).setLevel(pyLevel)
            else:
                # Need to set lsstLog level
                lsstLogLevel = lsstLog.LevelTranslator.logging2lsstLog(logLevel)
                lsstLog.getLogger(component or "").setLevel(lsstLogLevel)


class ReuseAction(argparse.Action):
    """argparse action associated with ArgumentPraser.addReuseOption."""

    def __call__(self, parser, namespace, value, option_string):
        if value == "all":
            value = self.choices[-2]
        index = self.choices.index(value)
        namespace.reuse = self.choices[: index + 1]


def setDottedAttr(item, name, value):
    """Set an instance attribute (like `setattr` but accepting
    hierarchical names such as ``foo.bar.baz``).

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
    For example if name is ``foo.bar.baz`` then ``item.foo.bar.baz``
    is set to the specified value.
    """
    subitem = item
    subnameList = name.split(".")
    for subname in subnameList[:-1]:
        subitem = getattr(subitem, subname)
    setattr(subitem, subnameList[-1], value)


def getDottedAttr(item, name):
    """Get an attribute (like `getattr` but accepts hierarchical names
    such as ``foo.bar.baz``).

    Parameters
    ----------
    item : obj
        Object whose attribute is to be returned.
    name : `str`
        Name of the attribute to get.

    Returns
    -------
    itemAttr : obj
        If name is ``foo.bar.baz then the return value is
        ``item.foo.bar.baz``.
    """
    subitem = item
    for subname in name.split("."):
        subitem = getattr(subitem, subname)
    return subitem
