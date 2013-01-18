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
    """Class whose instances run a Task
    
    This version assumes your task has a method runDataRef(dataRef, doRaise).
    If that is not so for your task then you may inherit from TaskRunner and override getTargetList
    and (if your task's runDataRef does not accept dataRef and doRaise arguments) __call__ 

    Instances of this class must be picklable in order to be compatible with the use of multiprocessing
    to call TaskClass.runParsedCmd. The 'prepareForMultiProcessing' method is called by the run() method
    if multiprocessing is configured, which jettisons optional non-picklable elements.
    """
    def __init__(self, TaskClass, parsedCmd, doReturnResults=False):
        """Constructor
        
        You will need to provide your own version of this class if:
        - Your task's runDataRef method does not have dataRef and doRaise arguments.
            You may inherit and override getTargetList and __call__
        - Your task's runDataRef method accepts additional arguments beyond dataRef and doRaise.
            You may inherit and override getTargetList.

        Instances of this class must be pickable if the task supports multiprocessing.
        See prepareForMultiprocessing for more information.
        
        We don't want to store parsedCmd here, as this instance will be pickled (if multiprocessing)
        and parsedCmd may contain non-picklable elements. Furthermore, parsedCmd contains the dataRefList
        and we don't want to have each process re-instantiating the entire dataRefList.

        @param TaskClass    The class we're to run
        @param parsedCmd    The parsed command-line arguments
        @param doReturnResults    Return the collected result from each invocation of the task?
            This is only intended for unit tests and similar use.
            It can easily exhaust memory (if the task returns enough data and you call it enough times)
            and it will fail when using multiprocessing if the returned data cannot be pickled.
        
        @raise ImportError if multiprocessing requested (and the task supports it)
        but the multiprocessing library cannot be imported.
        """
        self.TaskClass = TaskClass
        self.doReturnResults = bool(doReturnResults)
        self.config = parsedCmd.config
        self.log = parsedCmd.log
        self.doRaise = bool(parsedCmd.doraise)
        self.numProcesses = int(getattr(parsedCmd, 'processes', 1))
        if self.numProcesses > 1:
            if not TaskClass.canMultiprocess:
                self.log.warn("This task does not support multiprocessing; using one process")
                self.numProcesses = 1

    def prepareForMultiProcessing(self):
        """Prepare this instance for multiprocessing by removing optional non-picklable elements.
        """
        self.log = None

    def run(self, parsedCmd):
        """Run the task on all targets.

        The task is run under multiprocessing if numProcesses > 1; otherwise processing is serial.

        @return a list of results returned by __call__ (typically None unless doReturnResults is true).
        """
        if self.numProcesses > 1:
            import multiprocessing
            self.prepareForMultiProcessing()
            pool = multiprocessing.Pool(processes=self.numProcesses, maxtasksperchild=1)
            mapFunc = pool.map
        else:
            pool = None
            mapFunc = map

        resultList = mapFunc(self, self.getTargetList(parsedCmd))

        if pool is not None:
            pool.close()
            pool.join()

        return resultList

    @staticmethod
    def getTargetList(parsedCmd):
        """Return a list of targets (argument dicts for __call__); one entry per invocation
        """
        return [dict(dataRef=dr) for dr in parsedCmd.dataRefList]

    def __call__(self, argDict):
        """Run the Task on a single target.
        
        @param argDict: argument dict for runDataRef; doRaise is added by this method.
            This implementation assumes that argDict includes the key "dataRef".

        @warning if you wish to return something when doReturnResults is false
        then it must be picklable to support multiprocessing and it should be small
        enough that pickling and unpickling do not add excessive overhead.
        The default is to ruturn
        
        @return:
        - None if doReturnResults false
        - A pipe_base Struct containing these fields if doReturnResults true:
            - argDict: the argument dict sent to runDataRef
            - metadata: task metadata after execution of runDataRef
            - result: result returned by task runDataRef
        """
        argDict["doRaise"] = self.doRaise
        task = self.TaskClass(config=self.config, log=self.log)
        task.writeConfig(argDict["dataRef"])
        result = task.runDataRef(**argDict)
        task.writeMetadata(argDict["dataRef"])
        
        if self.doReturnResults:
            return Struct(
                argDict = argDict,
                metadata = task.metadata,
                result = result,
            )


class CmdLineTask(Task):
    """A task that can be executed from the command line
    
    Subclasses must specify the following attribute:
    _DefaultName: default name used for this task
    """
    # Specify the task runner class.
    # You may use TaskRunner, the default, if your task has method runDataRef(dataRef, doRaise);
    # otherwise you must use a version of TaskRunner specialized for your task.
    RunnerClass = TaskRunner

    # Specify whether your task supports multiprocessing; most tasks do.
    canMultiprocess = True

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
    def parseAndRun(cls, args=None, config=None, log=None, doReturnResults=False):
        """Parse an argument list and run the command

        @param args     list of command-line arguments; if None use sys.argv
        @param config   config for task (instance of pex_config Config); if None use cls.ConfigClass()
        @param log      log (instance of pex_logging Log); if None use the default log
        @param doReturnResults  Return the collected results from each invocation of the task?
            This is only intended for unit tests and similar use.
            It can easily exhaust memory (if the task returns enough data and you call it enough times)
            and it will fail when using multiprocessing if the returned data cannot be pickled.

        @return a Struct containing:
        - argumentParser: the argument parser
        - parsedCmd: the parsed command returned by argumentParser.parse_args
        - taskRunner: the task runner used to run the task
        - resultList: results returned by cls.RunnerClass.run, one entry per invocation.
            This will typically be a list of None unless doReturnResults is True.
            see cls.RunnerClass (TaskRunner by default) for more information.
            The return values are primarily for testing and debugging
        """
        argumentParser = cls._makeArgumentParser()
        if config is None:
            config = cls.ConfigClass()
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log, override=cls.applyOverrides)
        taskRunner = cls.RunnerClass(TaskClass=cls, parsedCmd=parsedCmd, doReturnResults=doReturnResults)
        resultList = taskRunner.run(parsedCmd)
        return Struct(
            argumentParser = argumentParser,
            parsedCmd = parsedCmd,
            taskRunner = taskRunner,
            resultList = resultList,
        )

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

    def runDataRef(self, dataRef, doRaise=False):
        """Execute the task on the data reference

        If you want to override this method with different inputs, you must also provide
        a different RunnerClass for your subclass.

        @param dataRef   Data reference to process
        @param doRaise   Allow exceptions to float up?
        """
        if doRaise:
            result = self.run(dataRef)
        else:
            try:
                result = self.run(dataRef)
            except Exception, e:
                self.log.fatal("Failed on dataId=%s: %s" % (dataRef.dataId, e))
                if not isinstance(e, TaskError):
                    traceback.print_exc(file=sys.stderr)
        return result

    def _getConfigName(self):
        """Return the name of the config dataset, or None if config is not persisted
        """
        return self._DefaultName + "_config"

    def _getMetadataName(self):
        """Return the name of the metadata dataset, or None if metadata is not persisted
        """
        return self._DefaultName + "_metadata"
