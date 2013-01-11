# 
# LSST Data Management System
# Copyright 2008-2013 LSST Corporation.
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

__all__ = ["CmdLineTask", "TaskRunner"]

class TaskRunner(object):
    """Class whose instances will run a Task

    Instances must be picklable in order to be compatible
    with the use of multiprocessing in CmdLineTask.runParsedCmd.
    The 'prepareForMultiProcessing' method will be called by the
    constructor if multiprocessing is configured (as determined
    by the 'getMultiProcessing' method), which gives the
    opportunity to jettison optional non-picklable elements.
    """
    def __init__(self, TaskClass, parsedCmd):
        """Constructor

        @param CmdLineTaskClass   The class we're to run
        @param parsedCmd          The parsed command-line arguments
        """
        self.cls = TaskClass
        self.name = TaskClass._DefaultName
        self.config = parsedCmd.config
        self.log = parsedCmd.log
        self.doraise = parsedCmd.doraise
        if useMultiProcessing(parsedCmd):
            self.prepareMultiProcessing()

    def prepareForMultiProcessing(self):
        self.parsedCmd.log = None

    def __call__(self, dataRef):
        """Run the Task on a single target.

        Here, the target is assumed to be a dataRef.
        """
        task = self.cls(name=self.name, config=self.config, log=self.log)
        task.writeConfig(dataRef)
        task.runDataRef(dataRef, doraise=self.doraise)
        task.writeMetadata(dataRef)

    @staticmethod
    def getMultiProcessing(parsedCmd):
        """Determine whether we're using multiprocessing,
        based on the parsed command-line arguments."""
        return hasattr(parsedCmd, 'processes') and args.parsedCmd > 1

    @staticmethod
    def getTargetList(self, parsedCmd):
        """Provide the list of targets to be processed,
        based on the command-line arguments."""
        return parsedCmd.dataRefList


class CmdLineTask(Task):
    """A task that can be executed from the command line
    
    Subclasses must specify the following attribute:
    _DefaultName: default name used for this task
    """
    RunnerClass = TaskRunner

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
        cls.runParsedCmd(parsedCmd)
        return Struct(
            argumentParser = argumentParser,
            parsedCmd = parsedCmd,
            )

    @classmethod
    def runParsedCmd(cls, parsedCmd):
        useMP = cls.RunnerClass.getMultiProcessing(parsedCmd)
        if useMP:
            try:
                import multiprocessing
            except ImportError, e:
                parsedCmd.log.warn("Unable to import multiprocessing: %s" % e)
                useMP = False
        if useMP:
            pool = multiprocessing.Pool(processes=parsedCmd.processes, maxtasksperchild=1)
            mapFunc = pool.map
        else:
            mapFunc = map

        runner = cls.RunnerClass(cls, parsedCmd)
        mapFunc(runner, runner.getTargetList(parsedCmd))

        if useMP:
            pool.close()
            pool.join()

    @classmethod
    def _makeArgumentParser(cls):
        """Create an argument parser

        Subclasses may wish to override, e.g. to change the dataset type or data ref level
        """
        return ArgumentParser(name=cls._DefaultName)

    def writeConfig(self, dataRef):
        """Write the configuration used for processing the data"""
        try:
            configName = self._getConfigName()
            if configName is not None:
                dataRef.put(self.config, configName)
        except Exception, e:
            self.log.warn("Could not persist config for dataId=%s: %s" % (dataRef.dataId, e,))

    def writeMetadata(self, dataRef):
        """Write the metadata produced from processing the data"""
        try:
            metadataName = self._getMetadataName()
            if metadataName is not None:
                dataRef.put(self.getFullMetadata(), metadataName)
        except Exception, e:
            self.log.warn("Could not persist metadata for dataId=%s: %s" % (dataRef.dataId, e,))

    def runDataRef(self, dataRef, doraise=False):
        """Execute the task on the data reference

        If you want to override this method with different inputs, you're
        also going to want to provide a different RunnerClass for your
        subclass.

        @param dataRef   Data reference to process
        @param doraise   Allow exceptions to float up?
        """
        if doraise:
            self.run(dataRef)
        else:
            try:
                self.run(dataRef)
            except Exception, e:
                self.log.fatal("Failed on dataId=%s: %s" % (dataRef.dataId, e))
                if not isinstance(e, TaskError):
                    traceback.print_exc(file=sys.stderr)


    @classmethod
    def useMultiProcessing(cls, parsedCmd):
        """Determine whether we're using multiprocessing, based on the parsed command.
        
        @params[in] parsedCmd: parsed command
        """
        return getattr(parsedCmd, "processes", 1) > 1

    def _getConfigName(self):
        """Return the name of the config dataset, or None if config is not persisted
        """
        return self._DefaultName + "_config"

    def _getMetadataName(self):
        """Return the name of the metadata dataset, or None if metadata is not persisted
        """
        return self._DefaultName + "_metadata"
