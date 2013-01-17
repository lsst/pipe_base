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
    
    This version assumes your task has a method named runDataRef that has exactly
    one argument: a data reference. If that is not the case for your task, you may
    inherit from this class and override __call__ and getTargetList.
    
    __call__ could be made to support any runDataRef method by receiving and passing to runDataRef
    an argument list (commonly named *args) and argument dict (commonly named **kwargs).
    However, that requires pickling the argument list and argument dict for each invocation of __call__
    It is more efficient to only pass those arguments to __call__ that runDataRef requires,
    and thus make the user override both getTargetList and __call__.

    Instances must be picklable in order to be compatible
    with the use of multiprocessing in CmdLineTask.runParsedCmd.
    The 'prepareForMultiProcessing' method will be called by the
    run() method if multiprocessing is configured, which gives the
    opportunity to jettison optional non-picklable elements.
    """
    def __init__(self, TaskClass, parsedCmd, returnResults=False):
        """Constructor

        We don't want to store parsedCmd here, as this instance
        will be pickled and parsedCmd may contain non-picklable
        elements.  Furthermore, parsedCmd contains the dataRefList
        and we don't want to have each process re-instantiating the
        entire dataRefList.

        @param TaskClass    The class we're to run
        @param parsedCmd    The parsed command-line arguments
        @param[in] returnResults    Return the collected results from each invocation of the task?
            This is only intended for unit tests and similar use.
            It can easily exhaust memory (if the task returns enough data and you call it enough times)
            and it will fail when using multiprocessing if the returned data cannot be pickled.
        """
        self.TaskClass = TaskClass
        self.returnResults = bool(returnResults)
        self.name = TaskClass._DefaultName
        self.config = parsedCmd.config
        self.log = parsedCmd.log
        self.doraise = parsedCmd.doraise
        self.numProcesses = getattr(parsedCmd, 'processes', 1)

    def prepareForMultiProcessing(self):
        """Prepare the instance for multiprocessing

        This provides an opportunity to remove optional non-picklable elements.
        """
        self.log = None

    def run(self, parsedCmd):
        """Run the task on all targets.

        The task will be run under multiprocessing if configured; otherwise
        processing will be serial.

        @return a list of results returned by __call__ (typically None unless returnResults is true).
        """
        if self.numProcesses > 1:
            try:
                import multiprocessing
            except ImportError, e:
                raise RuntimeError("Unable to import multiprocessing: %s" % e)
            self.prepareForMultiProcessing()
            pool = multiprocessing.Pool(processes=self.numProcesses, maxtasksperchild=1)
            mapFunc = pool.map
        else:
            pool = None
            mapFunc = map

        results = mapFunc(self, self.getTargetList(parsedCmd))

        if pool is not None:
            pool.close()
            pool.join()

        return results

    @staticmethod
    def getTargetList(parsedCmd):
        """Return a list of targets (arguments for __call__); one entry per invocation
        """
        return parsedCmd.dataRefList

    def __call__(self, dataRef):
        """Run the Task on a single target.

        The argument(s) is/are one element of the list returned by getTargetList.
        Here, the target is assumed to be a dataRef, but subclasses may override
        (typically both __call__ and getTargetList).

        Note that whatever is returned by this method needs to be picklable in
        order to support multiprocessing.  Returning large structures is not
        advisable, due to the additional overhead of pickling and unpickling.
        Because some Tasks have rather large return values (e.g., images),
        this implementation does not return anything by default.
        """
        task = self.TaskClass(name=self.name, config=self.config, log=self.log)
        task.writeConfig(dataRef)
        results = task.runDataRef(dataRef, doraise=self.doraise)
        task.writeMetadata(dataRef)
        
        if self.returnResults:
            return Struct(
                dataRef = dataRef,
                metadata = task.metadata,
                results = results,
            )


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
    def parseAndRun(cls, args=None, config=None, log=None, returnResults=False):
        """Parse an argument list and run the command

        @param args: list of command-line arguments; if None use sys.argv
        @param config: config for task (instance of pex_config Config); if None use cls.ConfigClass()
        @param log: log (instance of pex_logging Log); if None use the default log

        @return a Struct containing:
        - argumentParser: the argument parser
        - parsedCmd: the parsed command returned by argumentParser.parse_args
        - results: results returned by cls.RunnerClass.run (cls.RunnerClass is TaskRunner by default).
        The return values are primarily for testing and debugging
        """
        argumentParser = cls._makeArgumentParser()
        if config is None:
            config = cls.ConfigClass()
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log, override=cls.applyOverrides)
        runner = cls.RunnerClass(TaskClass=cls, parsedCmd=parsedCmd, returnResults=returnResults)
        results = runner.run(parsedCmd)
        return Struct(
            argumentParser = argumentParser,
            parsedCmd = parsedCmd,
            results = results,
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

    def _getConfigName(self):
        """Return the name of the config dataset, or None if config is not persisted
        """
        return self._DefaultName + "_config"

    def _getMetadataName(self):
        """Return the name of the metadata dataset, or None if metadata is not persisted
        """
        return self._DefaultName + "_metadata"
