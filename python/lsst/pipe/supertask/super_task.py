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
import inspect
import lsst.afw.table as afwTable
from lsst.pipe.base.argumentParser import ArgumentParser
from lsst.pipe.base.task import Task, TaskError
from lsst.pipe.base.struct import Struct


__all__ = ["SuperTask"]  # Classes in this module


def wraprun(func):
    """
    Wrapper around the run method within a Task Class, It is just a hook to add a pre_run() method
    and a post_run() method that will run before and after the invocation of run() respectively,
    this is used as a decorator on run(),
    i.e.

    @wraprun
    def run(): pass
    ...

    :return: The decorated function
    """

    def inner(instance, *args, **kwargs):
        """
        This is the function that calls pre_run, run and post_run in that order and return the
        results of the main function run()
        """
        instance.pre_run(*args, **kwargs)
        temp = func(instance, *args, **kwargs)
        instance.post_run(*args, **kwargs)
        return temp

    return inner


def wrapclass(decorator):
    """
    A Wrapper decorator that acts directly on a class, it takes the decorator function and applies
    it to every (should be one) run method inside the class, this is used when defined a
    SuperTask or Task class adding the decorator at the moment when the class is defined, i.e.,

    @wrapclass(wraprun)
    class MyTask(Task) :


    :param decorator: the decorator that will take the run() method as input
    :return: The decorated class
    """

    def innerclass(cls):
        """
        This function looks for a method run() within the class, then apply the parsed decorator
        to the method function
        """
        for name, method in inspect.getmembers(cls, inspect.ismethod):
            if name == 'run':
                setattr(cls, name, decorator(method))
        return cls

    return innerclass


@wrapclass(wraprun)
class SuperTask(Task):

    """
    Base class for Super-Tasks, which are extended classes of pipe.base.Task that can be called
    using activators, in particular from the cmdLineActivator so these can be executed in the
    command line. See the technical document at
    http://dmtn-002.lsst.io for more information on how to use this class.

    There are some small differences with respect Task itself, the main one is the new method
    execute() which for now takes a dataRef as input where the information is extracted and
    prepared to be parsed to run() which actually
    performs the operation

    The name is taken from _default_name (a more pythonic way to deal with variable names) but is
    still backwards compatible with Task.

    Ideally all Task should be (and will be) SuperTasks, which provides the abstraction and
    expose the run method of the Task itself.
    """

    _default_name = None
    _parent_name = None

    def __init__(self, config=None, name=None, parent_task=None, log=None, activator=None):
        """
        Creates the SuperTask, the parameters are the same as Task, except by activator which is a
        hook for the class activator that calls this Task, for cmdLineActivator is the only one
        available

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
        :param log:         pexLog log; if None then the default is used;
                            in either case a copy is made using the full task name.
        :param activator:   name of activator calling this task, default is None,
                            there is only one kind of activator (cmdLineActivator)
        :return: The SuperTask Class
        """

        if name is None:
            name = getattr(self, "_default_name", None)
        super(SuperTask, self).__init__(config, name, parent_task, log)  # Initiate Task

        self._parser = None
        self.input = Struct()  # input and output will be deprecated
        self.output = None
        self._activator = activator
        self._completed = False  # Hook to indicate whether a SuperTask was completed
        self.list_config = None  # List of configuration items
        self.name = self.name.replace(" ", "_")  # No spaces allowed in names for SuperTasks
        self._task_kind = 'SuperTask'  # To differentiate between this and Task

        self.log.info('%s was initiated' % self.name)  # For debugging only,  to be removed

    @property
    def name(self):
        """
        name of task as property
        :return: Name of (Super)Task
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        Sets the name of task
        :param name: new name for (Super)Task
        """
        self._name = name

    @property
    def activator(self):
        """
        Hook for the name of activator as a property
        :return: Name of activator invoking this (Super)Task
        """
        return self._activator

    @activator.setter
    def activator(self, activator):
        """
        Sets the name of the activator
        :param activator:  new name of activator
        """
        self._activator = activator

    @property
    def task_kind(self):
        """
        Defined task_kind as a property
        :return: Name the kind of task SuperTask or Task
        """
        return self._task_kind

    @task_kind.setter
    def task_kind(self, task_kind):
        """
        task_kind setter
        :param task_kind: kind of task, SuperTask or Task
        """
        self._task_kind = task_kind

    @property
    def parent_name(self):
        """
        parent name as property
        """
        return self._parent_name

    @property
    def completed(self):
        """
        Return the status of (Super)Task
        """
        return self._completed

    @completed.setter
    def completed(self, completed):
        """
        Sets the status of (Super)Task. The Task should be able to report whether it ran
        successfully or not, this is useful for workflow and freeze-dry runs or to make checkpoints.

        :param completed: Current status, usually this is done in post_run()
        """
        self._completed = completed

    def pre_run(self, *args, **kwargs):
        """
        Prerun method, which given the decorator @wraprun will run before run()
        This method can be used to prepare the data or logs before running the task
        """
        pass

    def post_run(self, *args, **kwargs):
        """
        Postrun method, this hook method can be used to define the completion of a task and to
        gather information about the run()
        """
        # By default, if used, this methods set the completion to be true.
        if args is None and kwargs in None:
            self.completed = True

    def run(self, *args, **kwargs):
        """
        This function is the one that actually operates on teh data (exposed by execute) and usually
        returning a Struct with the collected results
        """
        pass

    @property
    def parser(self):
        """
        Hook to handle the parser, it might be deprecated soon
        :return: the parser used to call this task
        """
        return self._parser

    @parser.setter
    def parser(self, parser):
        """
        parser setter
        """
        self._parser = parser

    def execute(self, dataRef):
        """
        execute method: This is the method that differentiate Task from SuperTask, this will
        probably change in the future, but as for now this method takes a dataRef (it might be a
        list of dataRef) and then gets the images, tables or anything needed for the run method to
        operate. The execute method should NOT process the object but it should ALWAYS call the
        run method with the necessary inputs.

        :param dataRef: Butler dataRef (or list of dataRef eventually) which will be inspect to
        get necessary inputs for run. All SuperTask need to define a execute and call run from
        inside.
        :return: the results from run()
        """
        pass

    def get_conf_list(self):
        """
        Get the configuration items for this (Super)Task
        :return: list of configuration and their values for this task
        """
        if self.list_config is None:
            self.list_config = []
        root_name = self.name + '.'
        for key, val in self.config.items():
            self.list_config.append(root_name + 'config.' + key + ' = ' + str(val))
        return self.list_config

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
        # Allow either _default_name or _DefaultName
        if cls._default_name is not None:
            task_name = cls._default_name
        elif cls._DefaultName is not None:
            task_name = cls._DefaultName
        else:
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
                oldSchema = butler.get(schema_dataset, immediate=True).getSchema()
                if not oldSchema.compare(catalog.getSchema(), afwTable.Schema.IDENTICAL):
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
