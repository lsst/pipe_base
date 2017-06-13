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

"""
superTask, base module that includes all classes and functions
for SuperTask. Documentation and example use can be found at
http://dmtn-002.lsst.io
"""

from __future__ import absolute_import, division, print_function

__all__ = ["SuperTask"]  # Classes in this module

from builtins import str

import lsst.afw.table as afwTable
from lsst.pipe.base.task import Task, TaskError


class SuperTask(Task):

    """
    Base class for Super-Tasks, which are extended classes of pipe.base.Task that can be called
    using activators, in particular from the cmdLineActivator so these can be executed in the
    command line. See the technical document at
    http://dmtn-002.lsst.io for more information on how to use this class.

    There are some small differences with respect Task itself, the main one is the new method
    execute() which for now takes a dataRef as input where the information is extracted and
    prepared to be parsed to run() which actually performs the operation

    Ideally all Task should be (and will be) SuperTasks, which provides the abstraction and
    expose the run method of the Task itself.

    Subclasses may also specify the following class variables:
    * `canMultiprocess`: the default is True; set False if your task does not support multiprocessing.
    """

    canMultiprocess = True

    def __init__(self, config=None, name=None, parentTask=None, log=None, butler=None):
        """
        Creates the SuperTask, the parameters are the same as Task.

        The inputs are (some of them taken from task.py):

        :param config:      configuration for this task (an instance of self.ConfigClass,
                            which is a task-specific subclass of lsst.pex.config.Config), or None.
                            If None:
                              - If parentTask specified then defaults to parentTask.config.`name`
                              - If parentTask is None then defaults to self.ConfigClass()
        :param name:        brief name of super_task, or None; if None then defaults to
                            self._DefaultName
        :param parentTask:  the parent task of this subtask, if any.
                             - If None (a top-level task) then you must specify config and name is
                               ignored.
                             - If not None (a subtask) then you must specify name
        :param log:         log (an lsst.log.Log) whose name is used as a log name prefix,
                            or None for no prefix. Ignored if parentTask specified, in which case
                            parentTask.log's name is used as a prefix.
        :param butler:      data butler instance (this is not used by this class, it should
                            probably be removed)
        :return: The SuperTask Class
        """

        # No spaces allowed in names for SuperTasks
        # TO DO: need better validation here, some other characters may be disallowed
        if name is None:
            name = getattr(self, "_DefaultName", None)
        if name is not None:
            name = name.replace(" ", "_")
        super(SuperTask, self).__init__(config=config, name=name, parentTask=parentTask, log=log)

        self._completed = False  # Hook to indicate whether a SuperTask was completed
        self._list_config = None  # List of configuration items
        self._task_kind = 'SuperTask'  # To differentiate between this and Task

    def run(self, *args, **kwargs):
        """
        Run task algorithm on in-memory data.

        This function is the one that actually operates on the data (exposed by execute) and
        usually returning a Struct with the collected results. This method will be overiden by
        every subclass. It operates on in-memory data structures (or data proxies) and cannot
        access any external data such as data butler or databases. All interaction with external
        data happens in `execute()` method.
        """
        pass

    def defineQuanta(self, repoGraph, butler):
        """Produce set of execution quanta.

        Purpose of this method is to split the whole workload (as defined
        by already existing data in `datasets`) into individual units of
        work that SuperTask can handle.

        Task is allowed to add task-specific data to returned quanta which
        may be useful for `runQuantum()` method. Any type of serializable
        data object can be added as `extras` attribute of quanta.

        Parameters
        ----------
        repoGraph : `obs.base.repodb.RepoGraph`
            A RepoGraph containing only Units matching the data ID
            expression supplied by the user and Datasets that should be
            present in the repository when all previous SuperTasks in the same
            pipeline have been run.  Any Datasets produced by this SuperTask
            should be added to the graph on return.
        butler : object
            Data butler instance.

        Returns
        -------
        Return a list of Quantum objects representing the inputs and outputs
        of a single invocation of runQuantum().
        """
        raise NotImplementedError("defineQuanta() is not implemented")

    def runQuantum(self, quantum, butler):
        """Execute SuperTask algorithm on single quantum of data.

        Typical implementation of this method will use inputs from quantum
        to retrieve Python-domain objects from data butler and call `run()`
        method on that data. Extra information in quantum can be passed to
        `run()` method as well. On return from `run()` this method will
        extract data from returned `Struct` instance and save that data
        to butler.

        This method does not return anything to the caller, on errors
        corresponding exception is raised.

        Note that `defineQuanta()` and `runQuantum()` in general will be
        executed by the different instances of SuperTask, very likely in
        separate processes or hosts. Thus it is not possible to share
        information between these methods via instance or class members,
        the only way to propagate knowledge from one method to another is
        by using `Quantum.extras` member.

        Parameters
        ----------
        quantum : `Quantum`
            Object describing input and output corresponding to this
            invocation of SuperTask instance.
        butler : object
            Data butler instance.

        Raises
        ------
        Any exceptions that happen in data butler or in `run()` method.
        """
        raise NotImplementedError("runQuantum() is not implemented")


    def getDatasetClasses(self):
        """Return a pair of dictionaries containing all of the concrete Dataset
        classes used by this SuperTask as inputs and outputs.

        Either inputs or outputs can be empty or None.

        Returns
        -------
        inputs : dict {str : type}
            Dictionary mapping Dataset type name onto Dataset class for all
            input datasets.
        outputs : dict {str : type}
            Dictionary mapping Dataset type name onto Dataset class for all
            output datasets.
        """
        # code below needs DatasetField class which I have no idea yet
        # as to where it comes from, for now say it's not implemented
        # and subclasses need to implement it.
        raise NotImplementedError("getDatasetClasses() is not implemented")
        inputs = {}
        outputs = {}
        for fieldName in self.config:
            cls = getattr(self.ConfigClass, fieldName)
            if issubclass(cls, DatasetField):
                p = getattr(self.config, fieldName)
                if p.mode == 'r':
                    inputs[p.name] = p.type
                else:
                    outputs[p.name] = p.type
        return inputs, outputs

    def write_config(self, butler, clobber=False, do_backup=True):
        """!Write the configuration used for processing the data, or check that an existing
        one is equal to the new one if present.

        @param[in] butler   data butler used to write the config.
            The config is written to dataset type self._get_config_name()
        @param[in] clobber  a boolean flag that controls what happens if a config already has been saved:
            - True: overwrite the existing config
            - False: raise TaskError if this config does not match the existing config
        """
        config_name = self._get_config_name()
        if config_name is None:
            return
        if clobber:
            butler.put(self.config, config_name, doBackup=do_backup)
        elif butler.datasetExists(config_name):
            # this may be subject to a race condition; see #2789
            try:
                old_config = butler.get(config_name, immediate=True)
            except Exception as exc:
                raise type(exc)("Unable to read stored config file %s (%s); consider using --clobber-config" %
                                (config_name, exc))

            def output(msg): return self.log.fatal("Comparing configuration: " + msg)
            if not self.config.compare(old_config, shortcut=False, output=output):
                raise TaskError(
                    ("Config does not match existing task config %r on disk; tasks configurations " +
                     "must be consistent within the same output repo (override with --clobber-config)") %
                    (config_name,))
        else:
            butler.put(self.config, config_name)

    def write_schemas(self, butler, clobber=False, do_backup=True):
        """!Write the schemas returned by \ref task.Task.getAllSchemaCatalogs "getAllSchemaCatalogs"

        @param[in] butler   data butler used to write the schema.
            Each schema is written to the dataset type specified as the key in the dict returned by
            \ref task.Task.getAllSchemaCatalogs "getAllSchemaCatalogs".
        @param[in] clobber  a boolean flag that controls what happens if a schema already has been saved:
            - True: overwrite the existing schema
            - False: raise TaskError if this schema does not match the existing schema

        @warning if clobber is False and an existing schema does not match a current schema,
        then some schemas may have been saved successfully and others may not, and there is no easy way to
        tell which is which.
        """
        for dataset, catalog in self.getAllSchemaCatalogs().items():
            schema_dataset = dataset + "_schema"
            if clobber:
                self.log.info("Writing schema %s", schema_dataset)
                butler.put(catalog, schema_dataset, doBackup=do_backup)
            elif butler.datasetExists(schema_dataset):
                self.log.info("Getting schema %s", schema_dataset)
                old_schema = butler.get(schema_dataset, immediate=True).getSchema()
                if not old_schema.compare(catalog.getSchema(), afwTable.Schema.IDENTICAL):
                    raise TaskError(
                        ("New schema does not match schema %r on disk; schemas must be " +
                         " consistent within the same output repo (override with --clobber-config)") %
                        (dataset,))
            else:
                self.log.info("Writing schema %s" % schema_dataset)
                butler.put(catalog, schema_dataset)

    def get_resource_config(self):
        """Return resource configuration for this task.

        Returns object of type `resource_config.ResourceConfig`or None if resource configuration
        is not defined for this task.
        """
        return getattr(self.config, "resources", None)

    def _get_config_name(self):
        """!Return the name of the config dataset type, or None if config is not to be persisted
        @note The name may depend on the config; that is why this is not a class method.
        """
        return self.getName() + "_config"
