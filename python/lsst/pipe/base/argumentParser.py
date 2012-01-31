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
import os.path
import sys

import lsst.pex.logging as pexLog
import lsst.daf.persistence as dafPersist

__all__ = ["ArgumentParser"]

class ArgumentParser(argparse.ArgumentParser):
    """ArgumentParser is an argparse.ArgumentParser that provides standard arguments for pipe_tasks tasks.

    These are used to populate butler, config and idList attributes,
    in addition to standard argparse behavior.
    
    @note:
    * --configfile and --config both may be specified multiple times on the command line;
      every instance is applied in order (left to right as it appears on the command line).
    * Other command-line arguments are only applied once, using the right-most instance
      (except I'm not yet sure about @file).
    * To specify an option with multiple values do NOT use = (a limitation of argparse):
        this is OK: --filter g r
        this is NOT OK: --filter=g r
    * The camera name must be specified before any options. (The need to specify a camera name
        should go away with the new butler.)
    
    @todo adapt for new butler:
    - Get camera name from data repository
    - Use mapper or camera name to obtain the names of the camera ID elements
    @todo: adapt for new Config
    """
    def __init__(self, usage="usage: %(prog)s camera dataSource [options]", **kwargs):
        argparse.ArgumentParser.__init__(self,
            usage = usage,
            fromfile_prefix_chars='@',
            epilog="@file reads command-line options from the specified file (one option per line)",
            **kwargs)
        self.add_argument("camera", help="""name of camera (e.g. lsstSim or suprimecam)
            (WARNING: this must appear before any options)""")
        self.add_argument("dataPath", help="path to data repository")
        self.add_argument("-c", "--config", nargs="*", action=ConfigValueAction,
                        help="command-line config overrides", metavar="NAME=VALUE")
        self.add_argument("-C", "--configfile", dest="configFile", nargs="*", action=ConfigFileAction,
                        help="file of config overrides")
        self.add_argument("-R", "--rerun", dest="rerun", default=os.getenv("USER", default="rerun"),
                        help="rerun name")
        self.add_argument("-L", "--log-level", action=LogLevelAction, help="Set logging level")
        self.add_argument("-T", "--trace", nargs=2, action=TraceLevelAction,
                          help="Set trace level for component")
        self.add_argument("--output", dest="outPath", help="output root directory")
        self.add_argument("--calib", dest="calibPath", help="calibration root directory")
        self.add_argument("--debug", action="store_true", help="enable debugging output?")
        self.add_argument("--log", dest="logDest", help="logging destination")

    def parse_args(self, config, argv=None):
        """Parse arguments for a command-line-driven task

        @params config: config for the task being run
        @params argv: argv to parse; if None then sys.argv[1:] is used
        
        @return namespace: a struct containing many useful fields including:
        - config: the supplied config with all overrides applied
        - butler: a butler for the data
        - idList: a list of data IDs as specified by the user and found with the butler
        - mapper: a mapper for the data
        - log: a log
        - an entry for each command-line argument, with a few exceptions such as configFile and logDest
        """
        if argv == None:
            argv = sys.argv[1:]

        if len(argv) < 1:
            sys.stderr.write("Error: must specify camera as first argument\n")
            self.print_usage()
            sys.exit(1)
        try:
            self._handleCamera(argv[0])
        except Exception, e:
            sys.stderr.write("%s\n" % e)
            sys.exit(1)
            
        inNamespace = argparse.Namespace
        inNamespace.config = config
        namespace = argparse.ArgumentParser.parse_args(self, args=argv)
        del namespace.configFile
        
        if not os.path.isdir(namespace.dataPath):
            sys.stderr.write("Error: dataPath=%r not found\n" % (namespace.dataPath,))
            sys.exit(1)

        namespace.mapper = self._mapperClass(root=namespace.dataPath, calibRoot=namespace.calibPath)
        butlerFactory = dafPersist.ButlerFactory(mapper = namespace.mapper)
        namespace.butler = butlerFactory.create()
        
        self._setIdList(namespace)

        if namespace.debug:
            try:
                import debug
            except ImportError:
                sys.stderr.write("Warning: no 'debug' module found\n")
                namespace.debug = False

        log = pexLog.Log.getDefaultLog()
        if namespace.logDest:
            log.addDestination(namespace.logDest)
        namespace.log = log
        del namespace.logDest

        return namespace
    
    def _setIdList(self, namespace):
        """Determine the valid data IDs that match the user's specification
        
        @param[inout] namespace: must contain butler and may contain
            entries and values for the data ID names in self._idNameCharTypeList
            Sets a new attribute idList.
        """
        argList = list()
        iterList = list()
        idDict = dict()

        idNameList = [item[0] for item in self._idNameCharTypeList]
        
        for idName, idChar, idType in self._idNameCharTypeList:
            strValues = getattr(namespace, idName)
            if not strValues:
                continue
            idDict[idName] = [idType(item) for item in strValues]
            argList.append("%s=%sItem" % (idName, idName))
            iterList.append("for %sItem in idDict['%s']" % (idName, idName))
        queryIdListExpr = "[dict(%s) %s]" % (", ".join(argList), " ".join(iterList))
        queryIdList = eval(queryIdListExpr)
        
        butler = namespace.butler

        goodTupleSet = set() # use set to avoid duplicates but sets cannot contain ID dicts so store tuples
        for queryId in queryIdList:
            # queryMetadata finds all possible matches, even ones that don't exist
            # (and it only works for raw, not calexp)
            # so follow it with datasetExists to find the good data IDs
            candidateTupleList = butler.queryMetadata("raw", None, idNameList, **queryId)
            newGoodIdSet = set(candTup for candTup in candidateTupleList
                if butler.datasetExists("calexp", dict(zip(idNameList, candTup))))
            goodTupleSet |= newGoodIdSet
            
        namespace.idList = [dict(zip(idNameList, goodTup)) for goodTup in goodTupleSet]

    def _handleCamera(self, camera):
        """Configure the command parser for the chosen camera.
        
        Called by parse_args before parsing the command (beyond getting the camera name).
        
        This will be radically rewritten once I can get this information from the data repository,
        but the subroutine must continue to live on to support things like camera-specific defaults.
        """
        if camera in ("-h", "--help"):
            self.print_help()
            print "\nFor more complete help, specify camera (e.g. lsstSim or suprimecam) as first argument\n"
            sys.exit(1)
        
        lowCamera = camera.lower()
        if lowCamera == "lsstsim":
            try:
                import lsst.obs.lsstSim
            except ImportError:
                self.error("Must setup obs_lsstSim to use lsstSim")
            self._mapperClass = lsst.obs.lsstSim.LsstSimMapper
            self._idNameCharTypeList = (
                ("visit",  "V", int),
                ("filter", "f", str),
                ("raft",   "r", str),
                ("sensor", "s", str),
            )
            self._extraFileKeys = ["channel"]
        elif lowCamera == "suprimecam":
            try:
                import lsst.obs.suprimecam
            except ImportError:
                self.error("Must setup obs_suprimecam to use suprimecam")
            self._mapperClass = lsst.obs.suprimecam.SuprimecamMapper
            self._idNameCharTypeList = (
                ("visit",  "V", int),
                ("ccd", "s", str),
            )
            self._extraFileKeys = []
        else:
            self.error("Unsupported camera: %s" % camera)

        for idName, idChar, idType in self._idNameCharTypeList:
            argList = []
            if idChar:
                argList.append("-%s" % (idChar,))
            argList.append("--%s" % (idName,))
            self.add_argument(*argList, dest=idName, nargs="*", default=[],
                help="%ss to to process" % (idName,))

        self._camera = camera

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
            try:
                value = eval(valueStr)
            except Exception:
                parser.error("Cannot parse %r as a value for %s" % (valueStr, name))
            setattr(namespace.config, name, value)

class ConfigFileAction(argparse.Action):
    """argparse action to load config overrides from one or more files
    """
    def __call__(self, parser, namespace, values, option_string=None):
        """Load one or more files of config overrides
        """
        for configFile in values:
            namespace.config.load(configFile)

class LogLevelAction(argparse.Action):
    """argparse action to set log level"""
    def __call__(self, parser, namespace, value, option_string):
        permitted = ('DEBUG', 'INFO', 'WARN', 'FATAL')
        if value.upper() in permitted:
            value = getattr(pexLog.Log, value.upper())
        else:
            try:
                value = int(value)
            except ValueError:
                parser.error("Cannot parse %s a logging level %s" % (value, permitted))
        log = pexLog.getDefaultLog()
        log.setThreshold(value)

class TraceLevelAction(argparse.Action):
    """argparse action to set trace level"""
    def __call__(self, parser, namespace, values, option_string):
        try:
            component, level = values
        except TypeError:
            parser.error("Cannont parse %s as component and level" % values)
        pexLog.Trace.setVerbosity(component, level)
