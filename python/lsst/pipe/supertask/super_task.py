"""
super_task, base module that includes all classes and functions
for SuperTask. Documentation and examnple use can be found at
http://dmtn-002.lsst.io
"""
from __future__ import absolute_import, division, print_function
from builtins import str
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
import lsst.afw.table as afwTable
from lsst.pipe.base.argumentParser import ArgumentParser
from lsst.pipe.base.task import Task, TaskError


__all__ = ["SuperTask"]  # Classes in this module


class SuperTask(Task):

    """
    Base class for Super-Tasks, which are extended classes of pipe.base.Task that can be called
    using activators, in particular from the cmdLineActivator so these can be executed in the
    command line. See the technical document at
    http://dmtn-002.lsst.io for more information on how to use this class.

    There are some small differences with respect Task itself, the main one is the new method
    execute() which for now takes a dataRef as input where the information is extracted and
    prepared to be parsed to run() which actually performs the operation

    The name is taken from _default_name (a more pythonic way to deal with variable names) but is
    still backwards compatible with Task.

    Ideally all Task should be (and will be) SuperTasks, which provides the abstraction and
    expose the run method of the Task itself.
    """

    _default_name = None

    def __init__(self, config=None, name=None, parent_task=None, log=None, butler=None):
        """
        Creates the SuperTask, the parameters are the same as Task.

        The inputs are (some of them taken from task.py):

        :param config:      configuration for this task (an instance of self.ConfigClass,
                            which is a task-specific subclass of lsst.pex.config.Config), or None.
                            If None:
                              - If parentTask specified then defaults to parentTask.config.name
                              - If parentTask is None then defaults to self.ConfigClass()
        :param name:        brief name of super_task, or None; if None then defaults to
                            self._default_name or self._DefaultName
        :param parent_task: the parent task of this subtask, if any.
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

        if name is None:
            name = getattr(self, "_default_name", None)
        if name is not None:
            name = name.replace(" ", "_")  # No spaces allowed in names for SuperTasks
        super(SuperTask, self).__init__(config, name, parent_task, log)  # Initiate Task

        self._completed = False  # Hook to indicate whether a SuperTask was completed
        self._list_config = None  # List of configuration items
        self._task_kind = 'SuperTask'  # To differentiate between this and Task

        self.log.info('%s was initiated' % self.name)  # For debugging only,  to be removed

    @property
    def name(self):
        """
        Name of this super-task.

        Note that base class defines `getName()` method which returns the same information,
        need to think if this property is needed.
        """
        return self._name

    @property
    def task_kind(self):
        """
        (obsolete) String representing kind othis task (e.g. "SuperTask")
        """
        return self._task_kind

    @property
    def completed(self):
        """
        Status of (Super)Task, True if task has completed.
        """
        return self._completed

    @completed.setter
    def completed(self, completed):
        """
        Sets the status of (Super)Task. The Task should be able to report whether it ran
        successfully or not, this is useful for workflow and freeze-dry runs or to make checkpoints.

        :param completed: New task completion status.
        """
        self._completed = completed

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

    def execute(self, dataRef):
        """
        Retrieve data and run task algorithm on it.

        This method is responsible for handling access to external data. It retrieves (probably
        lazily) data defined by `dataRef`, calls `run()` method on that data, and stores
        resulting data in butler. This method needs to be overriden by subclasses.

        :param dataRef: Butler dataRef (or list of dataRef eventually) which will be inspected to
        get necessary inputs for run.
        :return: the results from run()
        """
        pass

    def get_conf_list(self):
        """
        Get the configuration items for this (Super)Task.

        TODO: Review the format of the returned strings or get rid of this methof altogether
        (or make it private).

        :return: list of configuration and their values for this task
        """
        if self._list_config is None:
            self._list_config = [self.name + '.config.' + key + ' = ' + str(val)
                                 for key, val in self.config.items()]
        return self._list_config

    def print_config(self):
        """
        Print Configuration parameters for this (Super)Task
        :return:
        """
        print()
        print('* Configuration * :')
        print()
        for branch in self.get_conf_list():
            print(branch)
        print()

    @classmethod
    def _makeArgumentParser(cls):
        """Create argument parser used by command line activator.

        TO DO: we need more generic method which can work with all activators.
        """
        # Allow either _default_name or _DefaultName
        task_name = cls._default_name
        if task_name is None:
            task_name = getattr(cls, "_DefaultName", None)
        if task_name is None:
            raise RuntimeError("_default_name or _DefaultName is required for a task")
        parser = ArgumentParser(name=task_name)
        parser.add_id_argument("--id", "raw", help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser

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
            output = lambda msg: self.log.fatal("Comparing configuration: " + msg)
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
        for dataset, catalog in self.getAllSchemaCatalogs().iteritems():
            schema_dataset = dataset + "_schema"
            if clobber:
                print("Writing schema %s" % schema_dataset)
                butler.put(catalog, schema_dataset, doBackup=do_backup)
            elif butler.datasetExists(schema_dataset):
                print("Getting schema %s" % schema_dataset)
                old_schema = butler.get(schema_dataset, immediate=True).getSchema()
                if not old_schema.compare(catalog.getSchema(), afwTable.Schema.IDENTICAL):
                    raise TaskError(
                        ("New schema does not match schema %r on disk; schemas must be " +
                         " consistent within the same output repo (override with --clobber-config)") %
                        (dataset,))
            else:
                print("Writing schema %s" % schema_dataset)
                butler.put(catalog, schema_dataset)

    def _get_config_name(self):
        """!Return the name of the config dataset type, or None if config is not to be persisted
        @note The name may depend on the config; that is why this is not a class method.
        """
        return self.name + "_config"