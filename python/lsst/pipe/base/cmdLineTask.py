# 
# LSST Data Management System
# Copyright 2008, 2009, 2010, 2011 LSST Corporation.
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
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import sys
import traceback

from .task import Task, TaskError
from .struct import Struct
from .argumentParser import ArgumentParser

__all__ = ["CmdLineTask"]

class CmdLineTask(Task):
    """A task that can be executed from the command line
    
    Subclasses must specify the following attribute:
    _DefaultName: default name used for this task
    """

    @classmethod
    def applyOverrides(cls, config):
        """A hook to allow a task to change the values of its config *after* the camera-specific
        overrides are loaded but before any command-line overrides are applied.  This is invoked
        only when parseAndRun is used; other ways of constructing a config will not apply these
        overrides.

        This is necessary in some cases because the camera-specific overrides may retarget subtasks,
        wiping out changes made in ConfigClass.setDefaults.  See LSST ticket #2282 for more discussion.
        """
        pass

    @classmethod
    def parseAndRun(cls, args=None, config=None, log=None):
        """Parse an argument list and run the command

        @param args: list of command-line arguments; if None use sys.argv
        @param config: config for task (instance of pex_config Config); if None use cls.ConfigClass()
        @param log: log (instance of pex_logging Log); if None use the default log

        @return a Struct containing:
        - argumentParser: the argument parser
        - parsedCmd: the parsed command returned by argumentParser.parse_args
        - task: the instantiated task
        The return values are primarily for testing and debugging
        """
        argumentParser = cls._makeArgumentParser()
        if config is None:
            config = cls.ConfigClass()
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log, override=cls.applyOverrides)
        task = cls(name = cls._DefaultName, config = parsedCmd.config, log = parsedCmd.log)
        task.runParsedCmd(parsedCmd)
        return Struct(
            argumentParser = argumentParser,
            parsedCmd = parsedCmd,
            task = task,
        )

    @classmethod
    def _makeArgumentParser(cls):
        """Create an argument parser

        Subclasses may wish to override, e.g. to change the dataset type or data ref level
        """
        return ArgumentParser(name=cls._DefaultName)

    def runParsedCmd(self, parsedCmd):
        """Run the task, given the results of parsing a command line."""
        self.runDataRefList(parsedCmd.dataRefList, doRaise=parsedCmd.doraise)
    
    def runDataRefList(self, dataRefList, doRaise=False):
        """Execute the parsed command on a sequence of dataRefs,
        including writing the config and metadata.
        """
        name = self._DefaultName
        result = []
        for dataRef in dataRefList:
            try:
                configName = self._getConfigName()
                if configName is not None:
                    dataRef.put(self.config, configName)
            except Exception, e:
                self.log.log(self.log.WARN, "Could not persist config for dataId=%s: %s" % \
                    (dataRef.dataId, e,))
            if doRaise:
                result.append(self.run(dataRef))
            else:
                try:
                    result.append(self.run(dataRef))
                except Exception, e:
                    self.log.log(self.log.FATAL, "Failed on dataId=%s: %s" % (dataRef.dataId, e))
                    if not isinstance(e, TaskError):
                        traceback.print_exc(file=sys.stderr)
            try:
                metadataName = self._getMetadataName()
                if metadataName is not None:
                    dataRef.put(self.getFullMetadata(), metadataName)
            except Exception, e:
                self.log.log(self.log.WARN, "Could not persist metadata for dataId=%s: %s" % \
                    (dataRef.dataId, e,))
        return result
            
    
    def _getConfigName(self):
        """Return the name of the config dataset, or None if config is not persisted
        """
        return self._DefaultName + "_config"
    
    def _getMetadataName(self):
        """Return the name of the metadata dataset, or None if metadata is not persisted
        """
        return self._DefaultName + "_metadata"
