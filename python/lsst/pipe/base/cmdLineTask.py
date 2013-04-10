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

import lsst.afw.table as afwTable

from .task import Task, TaskError
from .struct import Struct
from .argumentParser import ArgumentParser

__all__ = ["CmdLineTask", "TaskRunner"]

class TaskRunner(object):
    """Run a Task, using multiprocessing if requested.
    
    Each command-line task must have this task runner or a subclass as its RunnerClass.
    See CmdLineTask.parseAndRun to see how a task runner is used.
    
    You may use this task runner for your command line task if your task has a run method
    that takes exactly one argument: a butler data reference. Otherwise you must
    provide a task-specific subclass of this runner for your task's RunnerClass
    that overrides getTargetList and possibly __call__. See getTargetList for details.

    This design matches the common pattern for command-line tasks: the run method takes a single
    data reference, of some suitable name. Additional arguments are rare, and if present, require
    a subclass of TaskRunner that calls these additional arguments by name.

    Instances of this class must be picklable in order to be compatible with multiprocessing.
    If multiprocessing is requested (parsedCmd.numProcesses > 1) then run() calls prepareForMultiProcessing
    to jettison optional non-picklable elements.
    """
    def __init__(self, TaskClass, parsedCmd, doReturnResults=False):
        """Construct a TaskRunner
        
        Do not store parsedCmd, as this instance is pickled (if multiprocessing) and parsedCmd may contain
        non-picklable elements and it certainly contains more data than we need to send to each
        iteration of the task.

        @param TaskClass    The class of the task to run
        @param parsedCmd    The parsed command-line arguments, as returned by the task's argument parser's
                            parse_args method.
        @param doReturnResults    Should run return the collected result from each invocation of the task?
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
        self.clobberConfig = bool(parsedCmd.clobberConfig)
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

        @return a list of results returned by __call__; see __call__ for details.
        """
        if self.numProcesses > 1:
            import multiprocessing
            self.prepareForMultiProcessing()
            pool = multiprocessing.Pool(processes=self.numProcesses, maxtasksperchild=1)
            mapFunc = pool.map
        else:
            pool = None
            mapFunc = map

        self.precall(parsedCmd)
        resultList = mapFunc(self, self.getTargetList(parsedCmd))

        if pool is not None:
            pool.close()
            pool.join()

        return resultList

    @staticmethod
    def getTargetList(parsedCmd, **kwargs):
        """Return a list of (dataRef, kwargs) to be used as arguments for __call__.
        
        @param parsedCmd the parsed command object returned by ArgumentParser.parse_args
        @param **kwargs any additional keyword arguments. In the default TaskRunner
        this is an empty dict, but having it simplifies overriding TaskRunner for tasks
        whose run method takes additional arguments (see case (1) below).
        
        The default implementation of getTargetList and __call__ works for any task
        that has a run method that takes exactly one argument: a data reference.
        Otherwise you must provide a variant of TaskRunner that overrides getTargetList
        and possibly __call__. There are two cases:
        
        (1) If your task has a run method that takes one data reference followed by additional arguments
        then you need only override getTargetList to return the additional arguments an argument dict:

        @staticmethod
        def getTargetList(parsedCmd):
            return TaskRunner.getTargetList(parsedCmd, calExpList=parsedCmd.calexp.idList)
        
        which is equivalent to:
        
        @staticmethod
        def getTargetList(parsedCmd):
            argDict = dict(calExpList=parsedCmd.calexp.idList)
            return [(dataId, argDict) for dataId in parsedCmd.id.idList]
            
        (2) If your task does not meet condition (1) then you must override both getTargetList
        and __call__. You may do this however you see fit, so long as getTargetList returns
        a list, each of whose elements is sent to __call__, which runs your task.
        """
        return [(ref, kwargs) for ref in parsedCmd.id.refList]

    def precall(self, parsedCmd):
        """Hook for code that should run exactly once, before multiprocessing is invoked.

        Implementations must take care to ensure that no unpicklable attributes are added to
        the TaskRunner itself.

        The default implementation writes schemas and configs (and compares them to existing
        files on disk if present).
        """
        task = self.TaskClass(config=self.config, log=self.log)
        task.writeConfig(parsedCmd.butler, clobber=self.clobberConfig)
        task.writeSchemas(parsedCmd.butler, clobber=self.clobberConfig)

    def __call__(self, args):
        """Run the Task on a single target.

        This default implementation assumes that the 'args' is a tuple
        containing a data reference and a dict of keyword arguments.

        @warning if you override this method and wish to return something when
        doReturnResults is false, then it must be picklable to support
        multiprocessing and it should be small enough that pickling and
        unpickling do not add excessive overhead.

        @param args: Arguments for Task.run()
        @return:
        - None if doReturnResults false
        - A pipe_base Struct containing these fields if doReturnResults true:
            - dataRef: the provided data reference
            - metadata: task metadata after execution of run
            - result: result returned by task run
        """
        dataRef, kwargs = args
        task = self.TaskClass(config=self.config, log=self.log)
        if self.doRaise:
            result = task.run(dataRef, **kwargs)
        else:
            try:
                result = task.run(dataRef, **kwargs)
            except Exception, e:
                task.log.fatal("Failed on dataId=%s: %s" % (dataRef.dataId, e))
                if not isinstance(e, TaskError):
                    traceback.print_exc(file=sys.stderr)
        task.writeMetadata(dataRef)
        
        if self.doReturnResults:
            return Struct(
                dataRef = dataRef,
                metadata = task.metadata,
                result = result,
            )


class CmdLineTask(Task):
    """A task that can be executed from the command line
    
    Subclasses must specify the following attribute:
    * ConfigClass: configuration class for your task (an instance of pex_config Config)
    * _DefaultName: default name used for this task
    
    Subclasses may also specify the following attribute:
    * RunnerClass: a task runner class. The default is TaskRunner, which works for any task
      with a run method that takes exactly one argument: a data reference. If your task does
      not meet this requirement then you must supply a variant of TaskRunner; see TaskRunner
      for more information.
    * canMultiprocess: the default is True; set False if your task does not support multiprocessing.
    """
    RunnerClass = TaskRunner
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
            This will typically be a list of None unless doReturnResults is True;
            see cls.RunnerClass (TaskRunner by default) for more information.
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

        Subclasses may wish to override, e.g. to change the dataset type or data
        ref level or add additional identifiers.  If additional identifiers are
        added, you might also want to adjust the cls.RunnerClass.getTargetList()
        method (and perhaps cls.RunnerClass.__call__()) to get additional data
        into our run method.
        """
        parser = ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument(name="--id", datasetType="raw", help="data ID, e.g. --id visit=12345 ccd=1,2")
        return parser

    def writeConfig(self, butler, clobber=False):
        """Write the configuration used for processing the data, or check that an existing
        one is equal to the new one if present.
        """
        configName = self._getConfigName()
        if configName is None:
            return
        if clobber:
            butler.put(self.config, configName, doBackup=True)
        elif butler.datasetExists(configName):
            # this may be subject to a race condition; see #2789
            oldConfig = butler.get(configName, immediate=True)
            output = lambda msg: self.log.fatal("Comparing configuration: " + msg)
            if not self.config.compare(oldConfig, shortcut=False, output=output):
                raise TaskError(
                    "Config does match existing config on disk for this task; tasks configurations "
                    "must be consistent within the same output repo (override with --clobber-config)"
                    )
        else:
            butler.put(self.config, configName)

    def writeSchemas(self, butler, clobber=False):
        """Write any catalogs returned by getSchemaCatalogs()."""
        for dataset, catalog in self.getAllSchemaCatalogs().iteritems():
            schemaDataset = dataset + "_schema"
            if clobber:
                butler.put(catalog, schemaDataset, doBackup=True)
            elif butler.datasetExists(schemaDataset):
                oldSchema = butler.get(schemaDataset, immediate=True).getSchema()
                if not oldSchema.compare(catalog.getSchema(), afwTable.Schema.IDENTICAL):
                    raise TaskError(
                        "New schema does not match schema on disk for %r; schemas must be "
                        " consistent within the same output repo (override with --clobber-config)"
                        % dataset
                        )
            else:
                butler.put(catalog, schemaDataset)

    def writeMetadata(self, dataRef):
        """Write the metadata produced from processing the data"""
        try:
            metadataName = self._getMetadataName()
            if metadataName is not None:
                dataRef.put(self.getFullMetadata(), metadataName)
        except Exception, e:
            self.log.warn("Could not persist metadata for dataId=%s: %s" % (dataRef.dataId, e,))

    def _getConfigName(self):
        """Return the name of the config dataset, or None if config is not persisted
        """
        return self._DefaultName + "_config"

    def _getMetadataName(self):
        """Return the name of the metadata dataset, or None if metadata is not persisted
        """
        return self._DefaultName + "_metadata"
