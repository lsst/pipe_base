#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import argparse
import itertools
import os.path
import re
import shlex
import sys

import lsst.pex.logging as pexLog
import lsst.daf.persistence as dafPersist

__all__ = ["ArgumentParser"]

class ArgumentParser(argparse.ArgumentParser):
    """An argument parser for pipeline tasks that is based on argparse.ArgumentParser
    
    Users may wish to add additional arguments before calling parse_args.
    
    @notes
    * The need to specify camera name will go away once the repository format allows
      constructing a butler without knowing it.
    * I would prefer to check data ID keys and values as they are parsed,
      but the required information comes from the butler, so I have to construct a butler
      before I do this checking. Constructing a butler is slow, so I only want do it once,
      after parsing the command line.
    """
    def __init__(self,
        usage = "%(prog)s camera dataSource [options]",
        datasetType = "raw",
        dataRefLevel = None,
    **kwargs):
        """Construct an ArgumentParser
        
        @param usage: usage string
        @param datasetType: dataset type appropriate to the task at hand;
            this affects which data ID keys are recognized.
        @param dataRefLevel: the level of the data references returned in dataRefList;
            None uses the data mapper's default, which is usually sensor.
            Warning: any value other than None is likely to be repository-specific.
        @param **kwargs: additional keyword arguments for argparse.ArgumentParser
        """
        self._datasetType = datasetType
        self._dataRefLevel = dataRefLevel
        argparse.ArgumentParser.__init__(self,
            usage = usage,
            fromfile_prefix_chars = '@',
            epilog = """Notes:
* --config, --configfile, --id, --trace and @file may appear multiple times;
    all values are used, in order left to right
* @file reads command-line options from the specified file:
    * data may be distributed among multiple lines (e.g. one option per line)
    * data after # is treated as a comment and ignored
    * blank lines and lines starting with # are ignored
* To specify multiple values for an option, do not use = after the option name:
    * wrong: --configfile=foo bar
    * right: --configfile foo bar
* The need to specify camera is temporary
""",
            formatter_class = argparse.RawDescriptionHelpFormatter,
        **kwargs)
        self.add_argument("camera", help="""name of camera (e.g. lsstSim or suprimecam)
            (WARNING: this must appear before any options or @file)""")
        self.add_argument("dataPath", help="path to data repository")
        self.add_argument("--output", dest="outPath", help="output root directory")
        self.add_argument("--calib", dest="calibPath", help="calibration root directory")
        self.add_argument("--id", nargs="*", action=IdValueAction,
            help="data ID, e.g. --id visit=12345 ccd=1,2", metavar="KEY=VALUE1[^VALUE2[^VALUE3...]")
        self.add_argument("-c", "--config", nargs="*", action=ConfigValueAction,
            help="config override(s), e.g. -c foo=newfoo bar.baz=3", metavar="NAME=VALUE")
        self.add_argument("-C", "--configfile", dest="configFile", nargs="*", action=ConfigFileAction,
            help="config override file(s)")
        self.add_argument("-L", "--log-level", dest="logLevel", help="logging level")
        self.add_argument("-T", "--trace", nargs="*", action=TraceLevelAction,
            help="trace level for component", metavar="COMPONENT=LEVEL")
        self.add_argument("--debug", action="store_true", help="enable debugging output?")
        self.add_argument("--doraise", dest="doRaise", action="store_true",
            help="raise an exception on error (else log a message and continue)?")
        self.add_argument("--log", dest="logDest", help="logging destination")

    def parse_args(self, config, args=None, log=None):
        """Parse arguments for a pipeline task

        @params config: config for the task being run
        @params args: args to parse; if None then sys.argv[1:] is used
        
        @return namespace: a struct containing many useful fields including:
        - config: the supplied config with all overrides applied
        - butler: a butler for the data
        - dataIdList: a list of data ID dicts
        - dataRefList: a list of butler data references
        - log: a pex_logging log
        - an entry for each command-line argument, with a few exceptions such as configFile and logDest
        """
        if args == None:
            args = sys.argv[1:]

        if len(args) < 1 or args[0].startswith("-") or args[0].startswith("@"):
            self.error("Must specify camera as first argument")

        namespace = argparse.Namespace()
        namespace.camera = args[0]
        namespace.config = config
        if log is None:
            log = pexLog.Log.getDefaultLog()
        namespace.log = log

        self.handleCamera(namespace)

        namespace.dataIdList = []
        
        namespace = argparse.ArgumentParser.parse_args(self, args=args, namespace=namespace)
        del namespace.configFile
        del namespace.id
        
        if not os.path.isdir(namespace.dataPath):
            self.error("Error: dataPath=%r not found" % (namespace.dataPath,))
        
        mapper = self._createMapper(namespace)
        butlerFactory = dafPersist.ButlerFactory(mapper = mapper)
        namespace.butler = butlerFactory.create()
        idKeyTypeDict = namespace.butler.getKeys(datasetType=self._datasetType, level=self._dataRefLevel)       
        
        # convert data in namespace.dataIdList to proper types
        # this is done after constructing the butler, hence after parsing the command line,
        # because it takes a long time to construct a butler
        for dataDict in namespace.dataIdList:
            for key, strVal in dataDict.iteritems():
                try:
                    keyType = idKeyTypeDict[key]
                except KeyError:
                    validKeys = sorted(idKeyTypeDict.keys())
                    self.error("Unrecognized ID key %r; valid keys are: %s" % (key, validKeys))
                if keyType != str:
                    try:
                        castVal = keyType(strVal)
                    except Exception:
                        self.error("Cannot cast value %r to %s for ID key %r" % (strVal, keyType, key,))
                    dataDict[key] = castVal

        namespace.dataRefList = []
        for dataId in namespace.dataIdList:
            dataRefList = list(namespace.butler.subset(
                datasetType = self._datasetType,
                level = self._dataRefLevel,
                **dataId))
            if not dataRefList:
                namespace.log.log(pexLog.Log.WARN, "No data references for dataId=%s" % (dataId,))
                continue
            namespace.dataRefList += dataRefList
        if not namespace.dataRefList:
            namespace.log.log(pexLog.Log.WARN, "No data references")

        if namespace.debug:
            try:
                import debug
            except ImportError:
                sys.stderr.write("Warning: no 'debug' module found\n")
                namespace.debug = False

        if namespace.logDest:
            namespace.log.addDestination(namespace.logDest)
        del namespace.logDest
        
        if namespace.logLevel:
            permitted = ('DEBUG', 'INFO', 'WARN', 'FATAL')
            if namespace.logLevel.upper() in permitted:
                value = getattr(pexLog.Log, namespace.logLevel.upper())
            else:
                try:
                    value = int(namespace.logLevel)
                except ValueError:
                    self.error("log-level=%s not int or one of %s" % (namespace.logLevel, permitted))
            namespace.log.setThreshold(value)
        del namespace.logLevel
        
        namespace.config.validate()

        return namespace
    
    def handleCamera(self, namespace):
        """Perform camera-specific operations before parsing the command line.
        
        @param[inout] namespace: namespace object with the following fields:
            - config: the config passed to parse_args, with no overrides applied
            - camera: the camera name
            - log: a pex_logging log
        """
        pass

    def _createMapper(self, namespace):
        """Construct namespace.mapper based on namespace.camera, dataPath and calibPath.
        
        This is a temporary hack to set self._mapperClass; this will go away once the butler
        renders it unnecessary, and the user will no longer have to supply the camera name.
        """
        lowCamera = namespace.camera.lower()
        if lowCamera == "lsstsim":
            try:
                from lsst.obs.lsstSim import LsstSimMapper as Mapper
            except ImportError:
                self.error("Must setup obs_lsstSim to use lsstSim")
        elif lowCamera == "hscsim":
            try:
                from lsst.obs.hscSim.hscSimMapper import HscSimMapper as Mapper
            except ImportError, e:
                self.error("Must setup obs_subaru to use hscSim: %s" % e)
        elif lowCamera == "suprimecam":
            try:
                from lsst.obs.suprimecam.suprimecamMapper import SuprimecamMapper as Mapper
            except ImportError, e:
                self.error("Must setup obs_subaru to use suprimecam: %s" % e)
        elif lowCamera == "cfht":
            try:
                from lsst.obs.cfht import CfhtMapper as Mapper
            except ImportError:
                self.error("Must setup obs_cfht to use CFHT")
        else:
            self.error("Unsupported camera: %s" % namespace.camera)
        return Mapper(
            root = namespace.dataPath,
            calibRoot = namespace.calibPath,
            outputRoot = namespace.outPath,
        )

    def convert_arg_line_to_args(self, arg_line):
        """Allow files of arguments referenced by @file to contain multiple values on each line
        """
        arg_line = arg_line.strip()
        if not arg_line or arg_line.startswith("#"):
            return
        for arg in shlex.split(arg_line, comments=True, posix=True):
            if not arg.strip():
                continue
            yield arg        

class ConfigValueAction(argparse.Action):
    """argparse action callback to override config parameters using name=value pairs from the command line
    """
    def __call__(self, parser, namespace, values, option_string):
        """Override one or more config name value pairs
        """
        for nameValue in values:
            name, sep, valueStr = nameValue.partition("=")
            if not valueStr:
                parser.error("%s value %s must be in form name=value" % (option_string, nameValue))

            # see if setting the string value works; if not, try eval
            try:
                setDottedAttr(namespace.config, name, valueStr)
            except Exception:
                try:
                    value = eval(valueStr, {})
                except Exception:
                    parser.error("Cannot parse %r as a value for %s" % (valueStr, name))
                try:
                    setDottedAttr(namespace.config, name, value)
                except Exception, e:
                    parser.error("Cannot set config.%s=%r: %s" % (name, value, e))

class ConfigFileAction(argparse.Action):
    """argparse action to load config overrides from one or more files
    """
    def __call__(self, parser, namespace, values, option_string=None):
        """Load one or more files of config overrides
        """
        for configFile in values:
            try:
                namespace.config.load(configFile)
            except Exception, e:
                parser.error("Cannot load config file %r: %s" % (configFile, e))
                

class IdValueAction(argparse.Action):
    """argparse action callback to add one data ID dict to namespace.dataIdList
    """
    def __call__(self, parser, namespace, values, option_string):
        """Parse --id data and append results to namespace.dataIdList
        
        The data format is:
        key1=value1_1[^value1_2[^value1_3...] key2=value2_1[^value2_2[^value2_3...]...

        The values (e.g. value1_1) may either be a string, or of the form "int...int" (e.g. "1..3")
        which is interpreted as "1^2^3" (inclusive, unlike a python range). So "0^2..4^7..9" is
        equivalent to "0^2^3^4^7^8^9"
        
        The cross product is computed for keys with multiple values. For example:
            --id visit 1^2 ccd 1,1^2,2
        results in the following data ID dicts being appended to namespace.dataIdList:
            {"visit":1, "ccd":"1,1"}
            {"visit":2, "ccd":"1,1"}
            {"visit":1, "ccd":"2,2"}
            {"visit":2, "ccd":"2,2"}
        """
        idDict = dict()
        for nameValue in values:
            name, sep, valueStr = nameValue.partition("=")
            idDict[name] = []
            for v in valueStr.split("^"):
                mat = re.search(r"^(\d+)\.\.(\d+)$", v)
                if mat:
                    v1 = int(mat.group(1))
                    v2 = int(mat.group(2))
                    for v in range(v1, v2 + 1):
                        idDict[name].append(str(v))
                else:
                    idDict[name].append(v)

        keyList = idDict.keys()
        iterList = [idDict[key] for key in keyList]
        idDictList = [dict(zip(keyList, valList)) for valList in itertools.product(*iterList)]

        namespace.dataIdList += idDictList

class TraceLevelAction(argparse.Action):
    """argparse action to set trace level"""
    def __call__(self, parser, namespace, values, option_string):
        for componentLevel in values:
            component, sep, levelStr = componentLevel.partition("=")
            if not levelStr:
                parser.error("%s level %s must be in form component=level" % (option_string, componentLevel))
            try:
                level = int(levelStr)
            except Exception:
                parser.error("Cannot parse %r as an integer level for %s" % (levelStr, component))
            pexLog.Trace.setVerbosity(component, level)

def setDottedAttr(item, name, value):
    """Like setattr, but accepts hierarchical names, e.g. foo.bar.baz
    """
    subitem = item
    subnameList = name.split(".")
    for subname in subnameList[:-1]:
        subitem = getattr(subitem, subname)
    setattr(subitem, subnameList[-1], value)

def getDottedAttr(item, name):
    """Like getattr, but accepts hierarchical names, e.g. foo.bar.baz
    """
    subitem = item
    for subname in name.split("."):
        subitem = getattr(subitem, subname)
    return subitem
