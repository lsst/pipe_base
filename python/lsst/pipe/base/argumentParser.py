from __future__ import absolute_import, division
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
import argparse
import collections
import fnmatch
import itertools
import os
import re
import shlex
import sys
import shutil

import lsst.utils
import lsst.pex.config as pexConfig
import lsst.pex.logging as pexLog
import lsst.daf.persistence as dafPersist

__all__ = ["ArgumentParser", "ConfigFileAction", "ConfigValueAction", "DataIdContainer", "DatasetArgument"]

DEFAULT_INPUT_NAME = "PIPE_INPUT_ROOT"
DEFAULT_CALIB_NAME = "PIPE_CALIB_ROOT"
DEFAULT_OUTPUT_NAME = "PIPE_OUTPUT_ROOT"

def _fixPath(defName, path):
    """!Apply environment variable as default root, if present, and abspath

    @param[in] defName  name of environment variable containing default root path;
        if the environment variable does not exist then the path is relative
        to the current working directory
    @param[in] path     path relative to default root path
    @return abspath: path that has been expanded, or None if the environment variable does not exist
        and path is None
    """
    defRoot = os.environ.get(defName)
    if defRoot is None:
        if path is None:
            return None
        return os.path.abspath(path)
    return os.path.abspath(os.path.join(defRoot, path or ""))


class DataIdContainer(object):
    """!A container for data IDs and associated data references

    Override for data IDs that require special handling to be converted to data references,
    and specify the override class as ContainerClass for add_id_argument.
    (If you don't want the argument parser to compute data references, you may use this class
    and specify doMakeDataRefList=False in add_id_argument.)
    """
    def __init__(self, level=None):
        """!Construct a DataIdContainer"""
        self.datasetType = None # the actual dataset type, as specified on the command line (if dynamic)
        self.level = level
        self.idList = []
        self.refList = []

    def setDatasetType(self, datasetType):
        """!Set actual dataset type, once it is known"""
        self.datasetType = datasetType

    def castDataIds(self, butler):
        """!Validate data IDs and cast them to the correct type (modify idList in place).

        @param[in] butler       data butler (a \ref lsst.daf.persistence.butler.Butler
            "lsst.daf.persistence.Butler")
        """
        if self.datasetType is None:
            raise RuntimeError("Must call setDatasetType first")
        try:
            idKeyTypeDict = butler.getKeys(datasetType=self.datasetType, level=self.level)
        except KeyError:
            raise KeyError("Cannot get keys for datasetType %s at level %s" % (self.datasetType, self.level))

        for dataDict in self.idList:
            for key, strVal in dataDict.iteritems():
                try:
                    keyType = idKeyTypeDict[key]
                except KeyError:
                    validKeys = sorted(idKeyTypeDict.keys())
                    raise KeyError("Unrecognized ID key %r; valid keys are: %s" % (key, validKeys))
                if keyType != str:
                    try:
                        castVal = keyType(strVal)
                    except Exception:
                        raise TypeError("Cannot cast value %r to %s for ID key %r" % (strVal, keyType, key,))
                    dataDict[key] = castVal

    def makeDataRefList(self, namespace):
        """!Compute refList based on idList

        Not called if add_id_argument called with doMakeDataRef=False

        @param[in] namespace    results of parsing command-line (with 'butler' and 'log' elements)
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
                namespace.log.warn("No data found for dataId=%s" % (dataId,))
                continue
            self.refList += refList


class DataIdArgument(object):
    """!Glorified struct for data about id arguments, used by ArgumentParser.add_id_argument"""
    def __init__(self, name, datasetType, level, doMakeDataRefList=True, ContainerClass=DataIdContainer):
        """!Constructor

        @param[in] name         name of identifier (argument name without dashes)
        @param[in] datasetType  type of dataset; specify a string for a fixed dataset type
            or a DatasetArgument for a dynamic dataset type (one specified on the command line),
            in which case an argument is added by name --\<name>_dstype
        @param[in] level        level of dataset, for butler
        @param[in] doMakeDataRefList    construct data references?
        @param[in] ContainerClass   class to contain data IDs and data references;
            the default class will work for many kinds of data, but you may have to override
            to compute some kinds of data references.
        """
        if name.startswith("-"):
            raise RuntimeError("Name %s must not start with -" % (name,))
        self.name = name
        self.datasetType = datasetType
        self.level = level
        self.doMakeDataRefList = bool(doMakeDataRefList)
        self.ContainerClass = ContainerClass
        self.argName = name.lstrip("-")
        if self.isDynamicDatasetType():
            self.datasetTypeName = datasetType.name if datasetType.name else self.name + "_dstype"
        else:
            self.datasetTypeName = None

    def isDynamicDatasetType(self):
        """!Is the dataset type dynamic (specified on the command line)?"""
        return isinstance(self.datasetType, DatasetArgument)

    def getDatasetType(self, namespace):
        """!Get the dataset type

        @param[in] namespace    parsed command created by argparse parse_args;
            if the dataset type is dynamic then it is read from namespace.\<name>_dstype
            else namespace is ignored
        """
        return getattr(namespace, self.datasetTypeName) if self.isDynamicDatasetType() else self.datasetType

class DatasetArgument(object):
    """!Specify that the dataset type should be a command-line option.

    Somewhat more heavyweight than just using, e.g., None as a signal, but
    provides the ability to have more informative help and a default.  Also
    more extensible in the future.

    @param[in] name     name of command-line argument (including leading "--", if wanted);
        if omitted a suitable default is chosen
    @param[in] help     help string for the command-line option
    @param[in] default  default value; if None, then the option is required
    """
    def __init__(self,
        name = None,
        help="dataset type to process from input data repository",
        default=None,
    ):
        self.name = name
        self.help = help
        self.default = default

    @property
    def required(self):
        return self.default is None

class ArgumentParser(argparse.ArgumentParser):
    """!An argument parser for pipeline tasks that is based on argparse.ArgumentParser

    Users may wish to add additional arguments before calling parse_args.

    @note
    - I would prefer to check data ID keys and values as they are parsed,
      but the required information comes from the butler, so I have to construct a butler
      before I do this checking. Constructing a butler is slow, so I only want do it once,
      after parsing the command line, so as to catch syntax errors quickly.
    """
    def __init__(self, name, usage = "%(prog)s input [options]", **kwargs):
        """!Construct an ArgumentParser

        @param[in] name     name of top-level task; used to identify camera-specific override files
        @param[in] usage    usage string
        @param[in] **kwargs additional keyword arguments for argparse.ArgumentParser
        """
        self._name = name
        self._dataIdArgDict = {} # Dict of data identifier specifications, by argument name
        argparse.ArgumentParser.__init__(self,
            usage = usage,
            fromfile_prefix_chars = '@',
            epilog = """Notes:
* --config, --configfile, --id, --loglevel and @file may appear multiple times;
    all values are used, in order left to right
* @file reads command-line options from the specified file:
    * data may be distributed among multiple lines (e.g. one option per line)
    * data after # is treated as a comment and ignored
    * blank lines and lines starting with # are ignored
* To specify multiple values for an option, do not use = after the option name:
    * right: --configfile foo bar
    * wrong: --configfile=foo bar
""",
            formatter_class = argparse.RawDescriptionHelpFormatter,
        **kwargs)
        self.add_argument("input",
            help="path to input data repository, relative to $%s" % (DEFAULT_INPUT_NAME,))
        self.add_argument("--calib",
            help="path to input calibration repository, relative to $%s" % (DEFAULT_CALIB_NAME,))
        self.add_argument("--output",
            help="path to output data repository (need not exist), relative to $%s" % (DEFAULT_OUTPUT_NAME,))
        self.add_argument("-c", "--config", nargs="*", action=ConfigValueAction,
            help="config override(s), e.g. -c foo=newfoo bar.baz=3", metavar="NAME=VALUE")
        self.add_argument("-C", "--configfile", dest="configfile", nargs="*", action=ConfigFileAction,
            help="config override file(s)")
        self.add_argument("-L", "--loglevel", nargs="*", action=LogLevelAction,
            help="logging level; supported levels are [debug|warn|info|fatal] or an integer; "
                "trace level is negative log level, e.g. use level -3 for trace level 3",
            metavar="LEVEL|COMPONENT=LEVEL")
        self.add_argument("--debug", action="store_true", help="enable debugging output?")
        self.add_argument("--doraise", action="store_true",
            help="raise an exception on error (else log a message and continue)?")
        self.add_argument("--profile", help="Dump cProfile statistics to filename")
        self.add_argument("--logdest", help="logging destination")
        self.add_argument("--show", nargs="+", default=(),
            help="display the specified information to stdout and quit (unless run is specified).")
        self.add_argument("-j", "--processes", type=int, default=1, help="Number of processes to use")
        self.add_argument("-t", "--timeout", type=float,
                          help="Timeout for multiprocessing; maximum wall time (sec)")
        self.add_argument("--clobber-output", action="store_true", dest="clobberOutput", default=False,
                          help=("remove and re-create the output directory if it already exists "
                                "(safe with -j, but not all other forms of parallel execution)"))
        self.add_argument("--clobber-config", action="store_true", dest="clobberConfig", default=False,
                          help=("backup and then overwrite existing config files instead of checking them "
                                "(safe with -j, but not all other forms of parallel execution)"))

    def add_id_argument(self, name, datasetType, help, level=None, doMakeDataRefList=True,
        ContainerClass=DataIdContainer):
        """!Add a data ID argument

        Add an argument to specify data IDs. If datasetType is an instance of DatasetArgument,
        then add a second argument to specify the dataset type.

        @param[in] name                 name of name (including leading dashes, if wanted)
        @param[in] datasetType          type of dataset; supply a string for a fixed dataset type,
            or a DatasetArgument for a dynamically determined dataset type
        @param[in] help                 help string for the argument
        @param[in] level                level of dataset, for butler
        @param[in] doMakeDataRefList    construct data references?
        @param[in] ContainerClass       data ID container class to use to contain results;
            override the default if you need a special means of computing data references from data IDs

        The associated data is put into namespace.<dataIdArgument.name> as an instance of ContainerClass;
        the container includes fields:
        - idList: a list of data ID dicts
        - refList: a list of butler data references (empty if doMakeDataRefList false)
        """
        argName = name.lstrip("-")

        if argName in self._dataIdArgDict:
            raise RuntimeError("Data ID argument %s already exists" % (name,))
        if argName in set(("camera", "config", "butler", "log", "obsPkg")):
            raise RuntimeError("Data ID argument %s is a reserved name" % (name,))

        self.add_argument(name, nargs="*", action=IdValueAction, help=help,
                          metavar="KEY=VALUE1[^VALUE2[^VALUE3...]")

        dataIdArgument = DataIdArgument(
            name = argName,
            datasetType = datasetType,
            level = level,
            doMakeDataRefList = doMakeDataRefList,
            ContainerClass = ContainerClass,
        )

        if dataIdArgument.isDynamicDatasetType():
            datasetType = dataIdArgument.datasetType
            help = datasetType.help if datasetType.help else "dataset type for %s" % (name,)
            self.add_argument(
                "--" + dataIdArgument.datasetTypeName,
                default = datasetType.default,
                required = datasetType.required,
                help = help,
            )
        self._dataIdArgDict[argName] = dataIdArgument

    def parse_args(self, config, args=None, log=None, override=None):
        """!Parse arguments for a pipeline task

        @param[in,out] config   config for the task being run
        @param[in] args         argument list; if None use sys.argv[1:]
        @param[in] log          log (instance pex_logging Log); if None use the default log
        @param[in] override     a config override function; it must take the root config object
            as its only argument and must modify the config in place.
            This function is called after camera-specific overrides files are applied, and before
            command-line config overrides are applied (thus allowing the user the final word).

        @return namespace: an argparse.Namespace containing many useful fields including:
        - camera: camera name
        - config: the supplied config with all overrides applied, validated and frozen
        - butler: a butler for the data
        - an entry for each of the data ID arguments registered by add_id_argument(),
          the value of which is a DataIdArgument that includes public elements 'idList' and 'refList'
        - log: a pex_logging log
        - an entry for each command-line argument, with the following exceptions:
          - config is the supplied config, suitably updated
          - configfile, id and loglevel are all missing
        - obsPkg: name of obs_ package for this camera
        """
        if args == None:
            args = sys.argv[1:]

        if len(args) < 1 or args[0].startswith("-") or args[0].startswith("@"):
            self.print_help()
            if len(args) == 1 and args[0] in ("-h", "--help"):
                self.exit()
            else:
                self.exit("%s: error: Must specify input as first argument" % self.prog)

        # note: don't set namespace.input until after running parse_args, else it will get overwritten
        inputRoot = _fixPath(DEFAULT_INPUT_NAME, args[0])
        if not os.path.isdir(inputRoot):
            self.error("Error: input=%r not found" % (inputRoot,))

        namespace = argparse.Namespace()
        namespace.config = config
        namespace.log = log if log is not None else pexLog.Log.getDefaultLog()
        mapperClass = dafPersist.Butler.getMapperClass(inputRoot)
        namespace.camera = mapperClass.getCameraName()
        namespace.obsPkg = mapperClass.getPackageName()

        self.handleCamera(namespace)

        self._applyInitialOverrides(namespace)
        if override is not None:
            override(namespace.config)

        # Add data ID containers to namespace
        for dataIdArgument in self._dataIdArgDict.itervalues():
            setattr(namespace, dataIdArgument.name, dataIdArgument.ContainerClass(level=dataIdArgument.level))

        namespace = argparse.ArgumentParser.parse_args(self, args=args, namespace=namespace)
        namespace.input = inputRoot
        del namespace.configfile

        namespace.calib  = _fixPath(DEFAULT_CALIB_NAME,  namespace.calib)
        namespace.output = _fixPath(DEFAULT_OUTPUT_NAME, namespace.output)

        if namespace.clobberOutput:
            if namespace.output is None:
                self.error("--clobber-output is only valid with --output")
            elif namespace.output == namespace.input:
                self.error("--clobber-output is not valid when the output and input repos are the same")
            if os.path.exists(namespace.output):
                namespace.log.info("Removing output repo %s for --clobber-output" % namespace.output)
                shutil.rmtree(namespace.output)

        namespace.log.info("input=%s"  % (namespace.input,))
        namespace.log.info("calib=%s"  % (namespace.calib,))
        namespace.log.info("output=%s" % (namespace.output,))

        obeyShowArgument(namespace.show, namespace.config, exit=False)

        namespace.butler = dafPersist.Butler(
            root = namespace.input,
            calibRoot = namespace.calib,
            outputRoot = namespace.output,
        )

        # convert data in each of the identifier lists to proper types
        # this is done after constructing the butler, hence after parsing the command line,
        # because it takes a long time to construct a butler
        self._processDataIds(namespace)
        if "data" in namespace.show:
            for dataIdName in self._dataIdArgDict.iterkeys():
                for dataRef in getattr(namespace, dataIdName).refList:
                    print dataIdName + " dataRef.dataId =", dataRef.dataId

        if namespace.show and "run" not in namespace.show:
            sys.exit(0)

        if namespace.debug:
            try:
                import debug
                assert debug # silence pyflakes
            except ImportError:
                sys.stderr.write("Warning: no 'debug' module found\n")
                namespace.debug = False

        if namespace.logdest:
            namespace.log.addDestination(namespace.logdest)
        del namespace.logdest
        del namespace.loglevel

        namespace.config.validate()
        namespace.config.freeze()

        return namespace

    def _processDataIds(self, namespace):
        """!Process the parsed data for each data ID argument

        Processing includes:
        - Validate data ID keys
        - Cast the data ID values to the correct type
        - Compute data references from data IDs

        @param[in,out] namespace    parsed namespace (an argparse.Namespace);
            reads these attributes:
            - butler
            - log
            - \<name_dstype> for each data ID argument with a dynamic dataset type registered using
                add_id_argument
            and modifies these attributes:
            - \<name> for each data ID argument registered using add_id_argument
        """
        for dataIdArgument in self._dataIdArgDict.itervalues():
            dataIdContainer = getattr(namespace, dataIdArgument.name)
            dataIdContainer.setDatasetType(dataIdArgument.getDatasetType(namespace))
            try:
                dataIdContainer.castDataIds(butler = namespace.butler)
            except (KeyError, TypeError) as e:
                # failure of castDataIds indicates invalid command args
                self.error(e)
            # failure of makeDataRefList indicates a bug that wants a traceback
            if dataIdArgument.doMakeDataRefList:
                dataIdContainer.makeDataRefList(namespace)

    def _applyInitialOverrides(self, namespace):
        """!Apply obs-package-specific and camera-specific config override files, if found

        @param[in] namespace    parsed namespace (an argparse.Namespace);
            reads these attributes:
            - obsPkg

        Look in the package namespace.obsPkg for files:
        - config/\<task_name>.py
        - config/\<camera_name>/\<task_name>.py
        and load if found
        """
        obsPkgDir = lsst.utils.getPackageDir(namespace.obsPkg)
        fileName = self._name + ".py"
        for filePath in (
            os.path.join(obsPkgDir, "config", fileName),
            os.path.join(obsPkgDir, "config", namespace.camera, fileName),
        ):
            if os.path.exists(filePath):
                namespace.log.info("Loading config overrride file %r" % (filePath,))
                namespace.config.load(filePath)
            else:
                namespace.log.info("Config override file does not exist: %r" % (filePath,))

    def handleCamera(self, namespace):
        """!Perform camera-specific operations before parsing the command line.

        The default implementation does nothing.

        @param[in,out] namespace    namespace (an argparse.Namespace) with the following fields:
            - camera: the camera name
            - config: the config passed to parse_args, with no overrides applied
            - obsPkg: the obs_ package for this camera
            - log: a pex_logging log
        """
        pass

    def convert_arg_line_to_args(self, arg_line):
        """!Allow files of arguments referenced by `@<path>` to contain multiple values on each line

        @param[in] arg_line     line of text read from an argument file
        """
        arg_line = arg_line.strip()
        if not arg_line or arg_line.startswith("#"):
            return
        for arg in shlex.split(arg_line, comments=True, posix=True):
            if not arg.strip():
                continue
            yield arg

def getTaskDict(config, taskDict=None, baseName=""):
    """!Get a dictionary of task info for all subtasks in a config

    Designed to be called recursively; the user should call with only a config
    (leaving taskDict and baseName at their default values).

    @param[in] config           configuration to process, an instance of lsst.pex.config.Config
    @param[in,out] taskDict     users should not specify this argument;
        (supports recursion; if provided, taskDict is updated in place, else a new dict is started)
    @param[in] baseName         users should not specify this argument.
        (supports recursion: if a non-empty string then a period is appended and the result is used
        as a prefix for additional entries in taskDict; otherwise no prefix is used)
    @return taskDict: a dict of config field name: task name
    """
    if taskDict is None:
        taskDict = dict()
    for fieldName, field in config.iteritems():
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
    """!Process arguments specified with --show (but ignores "data")

    @param showOpts  List of options passed to --show
    @param config    The provided config
    @param exit      Exit if "run" isn't included in showOpts

    Supports the following options in showOpts:
    - config[=PAT]   Dump all the config entries, or just the ones that match the glob pattern
    - tasks      Show task hierarchy
    - data       Ignored; to be processed by caller
    - run        Keep going (the default behaviour is to exit if --show is specified)

    Calls sys.exit(1) if any other option found.
    """
    if not showOpts:
        return

    for what in showOpts:
        mat = re.search(r"^config(?:=(.+))?", what)
        if mat:
            pattern = mat.group(1)
            if pattern:
                class FilteredStream(object):
                    """A file object that only prints lines that match the glob "pattern"

                    N.b. Newlines are silently discarded and reinserted;  crude but effective.
                    """
                    def __init__(self, pattern):
                        self._pattern = pattern

                    def write(self, showStr):
                        showStr = showStr.rstrip()
                        # Strip off doc string line(s) and cut off at "=" for string matching
                        matchStr = showStr.split("\n")[-1].split("=")[0]
                        if fnmatch.fnmatch(matchStr, self._pattern):
                            print "\n" + showStr

                fd = FilteredStream(pattern)
            else:
                fd = sys.stdout

            config.saveToStream(fd, "config")
        elif what == "data":
            pass
        elif what == "run":
            pass
        elif what == "tasks":
            showTaskHierarchy(config)
        else:
            print >> sys.stderr, "Unknown value for show: %s (choose from '%s')" % \
                (what, "', '".join("config[=XXX] data tasks run".split()))
            sys.exit(1)

    if exit and "run" not in showOpts:
        sys.exit(0)

def showTaskHierarchy(config):
    """!Print task hierarchy to stdout

    @param[in] config: configuration to process (an lsst.pex.config.Config)
    """
    print "Subtasks:"
    taskDict = getTaskDict(config=config)

    fieldNameList = sorted(taskDict.keys())
    for fieldName in fieldNameList:
        taskName = taskDict[fieldName]
        print "%s: %s" % (fieldName, taskName)

class ConfigValueAction(argparse.Action):
    """!argparse action callback to override config parameters using name=value pairs from the command line
    """
    def __call__(self, parser, namespace, values, option_string):
        """!Override one or more config name value pairs

        @param[in] parser           argument parser (instance of ArgumentParser)
        @param[in,out] namespace    parsed command (an instance of argparse.Namespace);
            updated values:
            - namespace.config
        @param[in] values           a list of configItemName=value pairs
        @param[in] option_string    option value specified by the user (a str)
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
                except Exception, e:
                    parser.error("cannot set config.%s=%r: %s" % (name, value, e))

class ConfigFileAction(argparse.Action):
    """!argparse action to load config overrides from one or more files
    """
    def __call__(self, parser, namespace, values, option_string=None):
        """!Load one or more files of config overrides

        @param[in] parser           argument parser (instance of ArgumentParser)
        @param[in,out] namespace    parsed command (an instance of argparse.Namespace);
            updated values:
            - namespace.config
        @param[in] values           a list of data config file paths
        @param[in] option_string    option value specified by the user (a str)
        """
        if namespace.config is None:
            return
        for configfile in values:
            try:
                namespace.config.load(configfile)
            except Exception, e:
                parser.error("cannot load config file %r: %s" % (configfile, e))

class IdValueAction(argparse.Action):
    """!argparse action callback to process a data ID into a dict
    """
    def __call__(self, parser, namespace, values, option_string):
        """!Parse --id data and append results to namespace.\<argument>.idList

        @param[in] parser           argument parser (instance of ArgumentParser)
        @param[in,out] namespace    parsed command (an instance of argparse.Namespace);
            updated values:
            - \<idName>.idList, where \<idName> is the name of the ID argument,
                for instance "id" for ID argument --id
        @param[in] values           a list of data IDs; see data format below
        @param[in] option_string    option value specified by the user (a str)

        The data format is:
        key1=value1_1[^value1_2[^value1_3...] key2=value2_1[^value2_2[^value2_3...]...

        The values (e.g. value1_1) may either be a string, or of the form "int..int" (e.g. "1..3")
        which is interpreted as "1^2^3" (inclusive, unlike a python range). So "0^2..4^7..9" is
        equivalent to "0^2^3^4^7^8^9".  You may also specify a stride: "1..5:2" is "1^3^5"

        The cross product is computed for keys with multiple values. For example:
            --id visit 1^2 ccd 1,1^2,2
        results in the following data ID dicts being appended to namespace.\<argument>.idList:
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
                    v3 = mat.group(3); v3 = int(v3) if v3 else 1
                    for v in range(v1, v2 + 1, v3):
                        idDict[name].append(str(v))
                else:
                    idDict[name].append(v)

        keyList = idDict.keys()
        iterList = [idDict[key] for key in keyList]
        idDictList = [collections.OrderedDict(zip(keyList, valList))
            for valList in itertools.product(*iterList)]

        argName = option_string.lstrip("-")
        ident = getattr(namespace, argName)
        ident.idList += idDictList

class LogLevelAction(argparse.Action):
    """!argparse action to set log level
    """
    def __call__(self, parser, namespace, values, option_string):
        """!Set trace level

        @param[in] parser       argument parser (instance of ArgumentParser)
        @param[in] namespace    parsed command (an instance of argparse.Namespace); ignored
        @param[in] values       a list of trace levels;
            each item must be of the form 'component_name=level' or 'level',
            where level is a keyword (not case sensitive) or an integer
        @param[in] option_string    option value specified by the user (a str)
        """
        permittedLevelList = ('DEBUG', 'INFO', 'WARN', 'FATAL')
        permittedLevelSet = set(permittedLevelList)
        for componentLevel in values:
            component, sep, levelStr = componentLevel.partition("=")
            if not levelStr:
                levelStr, component = component, None
            logLevelUpr = levelStr.upper()
            if logLevelUpr in permittedLevelSet:
                logLevel = getattr(namespace.log, logLevelUpr)
            else:
                try:
                    logLevel = int(levelStr)
                except Exception:
                    parser.error("loglevel=%r not int or one of %s" % (namespace.loglevel, permittedLevelList))
            if component is None:
                namespace.log.setThreshold(logLevel)
            else:
                namespace.log.setThresholdFor(component, logLevel)

def setDottedAttr(item, name, value):
    """!Like setattr, but accepts hierarchical names, e.g. foo.bar.baz

    @param[in,out] item     object whose attribute is to be set
    @param[in] name         name of item to set
    @param[in] value        new value for the item

    For example if name is foo.bar.baz then item.foo.bar.baz is set to the specified value.
    """
    subitem = item
    subnameList = name.split(".")
    for subname in subnameList[:-1]:
        subitem = getattr(subitem, subname)
    setattr(subitem, subnameList[-1], value)

def getDottedAttr(item, name):
    """!Like getattr, but accepts hierarchical names, e.g. foo.bar.baz

    @param[in] item         object whose attribute is to be returned
    @param[in] name         name of item to get

    For example if name is foo.bar.baz then returns item.foo.bar.baz
    """
    subitem = item
    for subname in name.split("."):
        subitem = getattr(subitem, subname)
    return subitem

def dataExists(butler, datasetType, dataRef):
    """!Return True if data exists at the current level or any data exists at a deeper level, False otherwise

    @param[in] butler       data butler (a \ref lsst.daf.persistence.butler.Butler
        "lsst.daf.persistence.Butler")
    @param[in] datasetType  dataset type (a str)
    @param[in] dataRef      butler data reference (a \ref lsst.daf.persistence.butlerSubset.ButlerDataRef
        "lsst.daf.persistence.ButlerDataRef")
    """
    subDRList = dataRef.subItems()
    if subDRList:
        for subDR in subDRList:
            if dataExists(butler, datasetType, subDR):
                return True
        return False
    else:
        return butler.datasetExists(datasetType = datasetType, dataId = dataRef.dataId)
