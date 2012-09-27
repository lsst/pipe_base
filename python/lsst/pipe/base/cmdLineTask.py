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
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log)
        task = cls(name=cls._DefaultName, config=parsedCmd.config, log=parsedCmd.log)
        
        useMP = useMultiProcessing(parsedCmd)
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

        runInfo = cls._getRunInfo(parsedCmd)
        mapFunc(runInfo.func, runInfo.inputs)

        if useMP:
            pool.close()
            pool.join()

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

    @classmethod
    def _getRunInfo(cls, parsedCmd):
        """Construct information necessary to run the task from the command-line arguments

        For multiprocessing to work, the 'func' returned must be picklable
        (i.e., typically a named function rather than anonymous function or
        method).  Thus, an extra level of indirection is typically required,
        so that the 'func' will create the Task from the 'inputs', and run.
        Because the 'func' is executed using 'map', it should not return any
        large data structures (which will require transmission between
        processes, and long-term memory storage).

        @param parsedCmd   Results of the argument parser
        @return Struct(func: Function to receive 'inputs';
                       inputs: List of Structs to be passed to the 'func')
        """
        log = parsedCmd.log if not useMultiProcessing(parsedCmd) else None# XXX pexLogging is not yet picklable
        inputs = [Struct(cls=cls, config=parsedCmd.config, log=log, doraise=parsedCmd.doraise, dataRef=dataRef)
                  for dataRef in parsedCmd.dataRefList]
        return Struct(func=runTask, inputs=inputs)
    
    def _runDataRef(self, dataRef, doraise=False):
        """Execute the task on the data reference

        @param dataRef   Data reference to process
        @param doraise   Allow exceptions to float up?
        """
        try:
            configName = self._getConfigName()
            if configName is not None:
                dataRef.put(self.config, configName)
        except Exception, e:
            self.log.log(self.log.WARN, "Could not persist config for dataId=%s: %s" % \
                (dataRef.dataId, e,))
        if doraise:
            self.run(dataRef)
        else:
            try:
                self.run(dataRef)
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

    def _getConfigName(self):
        """Return the name of the config dataset, or None if config is not persisted
        """
        return self._DefaultName + "_config"
    
    def _getMetadataName(self):
        """Return the name of the metadata dataset, or None if metadata is not persisted
        """
        return self._DefaultName + "_metadata"


def useMultiProcessing(args):
    """Determine whether we're using multiprocessing,
    based on the parsed command-line arguments."""
    return hasattr(args, 'processes') and args.processes > 1

def runTask(args):
    """Run task, by forwarding to CmdLineTask._runDataRef.

    This forwarding is necessary because multiprocessing requires
    that the function used is picklable, which means it must be a
    named function, rather than an anonymous function (lambda) or
    method.
    """
    task = args.cls(name = args.cls._DefaultName, config=args.config, log=args.log)
    task._runDataRef(args.dataRef, doraise=args.doraise)

