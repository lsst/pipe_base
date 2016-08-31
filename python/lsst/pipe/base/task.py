#
# LSST Data Management System
# Copyright 2008-2016 AURA/LSST.
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
from __future__ import absolute_import, division
import contextlib

from builtins import object

import lsstDebug
from lsst.pex.config import ConfigurableField
from lsst.log import Log
import lsst.daf.base as dafBase
from .timer import logInfo

__all__ = ["Task", "TaskError"]


class TaskError(Exception):
    """!Use to report errors for which a traceback is not useful.

    Examples of such errors:
    - processCcd is asked to run detection, but not calibration, and no calexp is found.
    - coadd finds no valid images in the specified patch.
    """
    pass


class Task(object):
    """!Base class for data processing tasks

    See \ref pipeBase_introduction "pipe_base introduction" to learn what tasks are,
    and \ref pipeTasks_writeTask "how to write a task" for more information about writing tasks.
    If the second link is broken (as it will be before the documentation is cross-linked)
    then look at the main page of pipe_tasks documentation for a link.

    Useful attributes include:
    * log: an lsst.log.Log
    * config: task-specific configuration; an instance of ConfigClass (see below)
    * metadata: an lsst.daf.base.PropertyList for collecting task-specific metadata,
        e.g. data quality and performance metrics. This is data that is only meant to be
        persisted, never to be used by the task.

    Subclasses typically have a method named "run" to perform the main data processing. Details:
    * run should process the minimum reasonable amount of data, typically a single CCD.
      Iteration, if desired, is performed by a caller of the run method. This is good design and allows
      multiprocessing without the run method having to support it directly.
    * If "run" can persist or unpersist data:
        * "run" should accept a butler data reference (or a collection of data references, if appropriate,
            e.g. coaddition).
        * There should be a way to run the task without persisting data. Typically the run method returns all
            data, even if it is persisted, and the task's config method offers a flag to disable persistence.

    \deprecated Tasks other than cmdLineTask.CmdLineTask%s should \em not accept a blob such as a butler data
    reference.  How we will handle data references is still TBD, so don't make changes yet! RHL 2014-06-27

    Subclasses must also have an attribute ConfigClass that is a subclass of lsst.pex.config.Config
    which configures the task. Subclasses should also have an attribute _DefaultName:
    the default name if there is no parent task. _DefaultName is required for subclasses of
    \ref cmdLineTask.CmdLineTask "CmdLineTask" and recommended for subclasses of Task because it simplifies
    construction (e.g. for unit tests).

    Tasks intended to be run from the command line should be subclasses of \ref cmdLineTask.CmdLineTask
    "CmdLineTask", not Task.
    """

    def __init__(self, config=None, name=None, parentTask=None, log=None):
        """!Create a Task

        @param[in] config       configuration for this task (an instance of self.ConfigClass,
            which is a task-specific subclass of lsst.pex.config.Config), or None. If None:
            - If parentTask specified then defaults to parentTask.config.\<name>
            - If parentTask is None then defaults to self.ConfigClass()
        @param[in] name         brief name of task, or None; if None then defaults to self._DefaultName
        @param[in] parentTask   the parent task of this subtask, if any.
            - If None (a top-level task) then you must specify config and name is ignored.
            - If not None (a subtask) then you must specify name
        @param[in] log          log (an lsst.log.Log) whose name is used as a log name prefix,
            or None for no prefix. Ignored if parentTask specifie, in which case parentTask.log's
            name is used as a prefix.
            The task's log name is `prefix + "." + name` if a prefix exists, else `name`.

        @throw RuntimeError if parentTask is None and config is None.
        @throw RuntimeError if parentTask is not None and name is None.
        @throw RuntimeError if name is None and _DefaultName does not exist.
        """
        self.metadata = dafBase.PropertyList()
        self._parentTask = parentTask

        if parentTask is not None:
            if name is None:
                raise RuntimeError("name is required for a subtask")
            self._name = name
            self._fullName = parentTask._computeFullName(name)
            if config is None:
                config = getattr(parentTask.config, name)
            self._taskDict = parentTask._taskDict
            loggerName = parentTask.log.getName() + '.' + name
        else:
            if name is None:
                name = getattr(self, "_DefaultName", None)
                if name is None:
                    raise RuntimeError("name is required for a task unless it has attribute _DefaultName")
                name = self._DefaultName
            self._name = name
            self._fullName = self._name
            if config is None:
                config = self.ConfigClass()
            self._taskDict = dict()
            loggerName = self._fullName
            if log is not None:
                loggerName = log.getName() + '.' + loggerName

        self.log = Log.getLogger(loggerName)
        self.config = config
        self._display = lsstDebug.Info(self.__module__).display
        self._taskDict[self._fullName] = self

    def emptyMetadata(self):
        """!Empty (clear) the metadata for this Task and all sub-Tasks."""
        for subtask in self._taskDict.values():
            subtask.metadata = dafBase.PropertyList()

    def getSchemaCatalogs(self):
        """!Return the schemas generated by this task

        @warning Subclasses the use schemas must override this method. The default implemenation
        returns an empty dict.

        @return a dict of butler dataset type: empty catalog (an instance of the appropriate
            lsst.afw.table Catalog type) for this task

        This method may be called at any time after the Task is constructed, which means that
        all task schemas should be computed at construction time, __not__ when data is actually
        processed. This reflects the philosophy that the schema should not depend on the data.

        Returning catalogs rather than just schemas allows us to save e.g. slots for SourceCatalog as well.

        See also Task.getAllSchemaCatalogs
        """
        return {}

    def getAllSchemaCatalogs(self):
        """!Call getSchemaCatalogs() on all tasks in the hiearchy, combining the results into a single dict.

        @return a dict of butler dataset type: empty catalog (an instance of the appropriate
            lsst.afw.table Catalog type) for all tasks in the hierarchy, from the top-level task down
            through all subtasks

        This method may be called on any task in the hierarchy; it will return the same answer, regardless.

        The default implementation should always suffice. If your subtask uses schemas the override
        Task.getSchemaCatalogs, not this method.
        """
        schemaDict = self.getSchemaCatalogs()
        for subtask in self._taskDict.values():
            schemaDict.update(subtask.getSchemaCatalogs())
        return schemaDict

    def getFullMetadata(self):
        """!Get metadata for all tasks

        The returned metadata includes timing information (if \@timer.timeMethod is used)
        and any metadata set by the task. The name of each item consists of the full task name
        with "." replaced by ":", followed by "." and the name of the item, e.g.:
            topLeveltTaskName:subtaskName:subsubtaskName.itemName
        using ":" in the full task name disambiguates the rare situation that a task has a subtask
        and a metadata item with the same name.

        @return metadata: an lsst.daf.base.PropertySet containing full task name: metadata
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        fullMetadata = dafBase.PropertySet()
        for fullName, task in self.getTaskDict().items():
            fullMetadata.set(fullName.replace(".", ":"), task.metadata)
        return fullMetadata

    def getFullName(self):
        """!Return the task name as a hierarchical name including parent task names

        The full name consists of the name of the parent task and each subtask separated by periods.
        For example:
        - The full name of top-level task "top" is simply "top"
        - The full name of subtask "sub" of top-level task "top" is "top.sub"
        - The full name of subtask "sub2" of subtask "sub" of top-level task "top" is "top.sub.sub2".
        """
        return self._fullName

    def getName(self):
        """!Return the name of the task

        See getFullName to get a hierarchical name including parent task names
        """
        return self._name

    def getTaskDict(self):
        """!Return a dictionary of all tasks as a shallow copy.

        @return taskDict: a dict containing full task name: task object
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        return self._taskDict.copy()

    def makeSubtask(self, name, **keyArgs):
        """!Create a subtask as a new instance self.\<name>

        The subtask must be defined by self.config.\<name>, an instance of pex_config ConfigurableField
        or RegistryField.

        @param name         brief name of subtask
        @param **keyArgs    extra keyword arguments used to construct the task.
            The following arguments are automatically provided and cannot be overridden:
            "config" and "parentTask".
        """
        taskField = getattr(self.config, name, None)
        if taskField is None:
            raise KeyError("%s's config does not have field %r" % (self.getFullName(), name))
        subtask = taskField.apply(name=name, parentTask=self, **keyArgs)
        setattr(self, name, subtask)

    @contextlib.contextmanager
    def timer(self, name, logLevel=Log.DEBUG):
        """!Context manager to log performance data for an arbitrary block of code

        @param[in] name         name of code being timed;
            data will be logged using item name: \<name>Start\<item> and \<name>End\<item>
        @param[in] logLevel     one of the lsst.log.Log level constants

        Example of use:
        \code
        with self.timer("someCodeToTime"):
            ...code to time...
        \endcode

        See timer.logInfo for the information logged
        """
        logInfo(obj=self, prefix=name + "Start", logLevel=logLevel)
        try:
            yield
        finally:
            logInfo(obj=self, prefix=name + "End", logLevel=logLevel)

    @classmethod
    def makeField(cls, doc):
        """!Make an lsst.pex.config.ConfigurableField for this task

        Provides a convenient way to specify this task is a subtask of another task.
        Here is an example of use:
        \code
        class OtherTaskConfig(lsst.pex.config.Config)
            aSubtask = ATaskClass.makeField("a brief description of what this task does")
        \endcode

        @param[in] cls      this class
        @param[in] doc      help text for the field
        @return a lsst.pex.config.ConfigurableField for this task
        """
        return ConfigurableField(doc=doc, target=cls)

    def _computeFullName(self, name):
        """!Compute the full name of a subtask or metadata item, given its brief name

        For example: if the full name of this task is "top.sub.sub2"
        then _computeFullName("subname") returns "top.sub.sub2.subname".

        @param[in] name     brief name of subtask or metadata item
        @return the full name: the "name" argument prefixed by the full task name and a period.
        """
        return "%s.%s" % (self._fullName, name)

    def __reduce__(self):
        """Pickler"""
        return self.__class__, (self.config, self._name, self._parentTask, None)
