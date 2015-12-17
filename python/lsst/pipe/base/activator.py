"""
activator: This module contains the activator classes which are used
to execute (Super)Task in different context/environments
"""
from __future__ import absolute_import, division, print_function
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
import inspect
import pkgutil
import pyclbr
import contextlib
from .argumentParser import ArgumentParser
import argparse as argp

import importlib


__all__ = ["Activator", "CmdLineActivator"]

# This is the temporary place for all the packages with (Super)tasks inside
TASK_PACKAGES = {'lsst.pipe.base.examples': None, 'lsst.pipe.tasks': None}

for pkg in TASK_PACKAGES.keys():
    # This get the list of modules inside package, from the __init__.py inside package
    TASK_PACKAGES[pkg] = importlib.import_module(pkg)


class ActivatorParser(argp.ArgumentParser):
    """
    Small ActivatorParser handler to print error message to add an error method
    """

    def error(self, message):
        """
        Writes outs the message error in the argument parser
        """
        sys.stderr.write('\n*ERROR* : %s\n\n' % message)
        self.print_help()
        sys.exit(2)


@contextlib.contextmanager
def profile(filename, log=None):
    """!Context manager for profiling with cProfile

    @param filename     filename to which to write profile (profiling disabled if None or empty)
    @param log          log object for logging the profile operations

    If profiling is enabled, the context manager returns the cProfile.Profile object (otherwise
    it returns None), which allows additional control over profiling.  You can obtain this using
    the "as" clause, e.g.:

        with profile(filename) as prof:
            runYourCodeHere()

    The output cumulative profile can be printed with a command-line like:

        python -c 'import pstats; pstats.Stats("<filename>").sort_stats("cumtime").print_stats(30)'
    """
    if not filename:
        # Nothing to do
        yield
        return
    from cProfile import Profile

    profile = Profile()
    if log is not None:
        log.info("Enabling cProfile profiling")
    profile.enable()
    yield profile
    profile.disable()
    profile.dump_stats(filename)
    if log is not None:
        log.info("cProfile stats written to %s" % filename)


class TaskNameError(Exception):
    """
    Exception Class to handle conflict with task and config names,
    might be replaced for a more general one, or even RunTimeError

    TODO: Decide on final Exception handler for name conflicts
     I though about creating a new Exception to deal with name
     conflicts in general (that differs from NameError)
    """

    def __init__(self, msg, errs):
        """
        Initiate the class, gets the errs and message
        """
        super(TaskNameError, self).__init__(msg)
        self.errs = errs


class Activator(object):
    """
    Hook for other activators
    """

    def __init__(self):
        """
        This might grow into a common initialization, right now
        is overridden
        """
        pass

    def activate(self):
        """
        This is the required method to execute the activator
        """
        pass


class CmdLineActivator(Activator):
    """
    Activator for the command line: This Class takes the higher level SuperTask as input and
    executes in the command line, similarly to CmdLineTask but this is a generic way to run (
    Super)Task. It takes the name of the task from the command line along all the arguments to
    run the task.
    Look http://dmtn-002.lsst.io to see examples on how to invoke the activator
    """

    def __init__(self, SuperTask, parsed_cmd, return_results=False):
        """
        Constructs the CmdLineActivator for the SuperTask Class parsed, given the command line
        options

        :param SuperTask: The actual SuperTask Class that the activator will execute activate
        :param parsed_cmd: The parsed arguments in the command line, relevant to the task,
        not to the activator
        :param return_results: Whether or not return the results, similar to CmdLineTask
        """
        super(CmdLineActivator, self).__init__()

        self.SuperTask = SuperTask
        self.return_results = bool(return_results)
        if parsed_cmd:
            self.config = parsed_cmd.config
            self.log = parsed_cmd.log
            self.do_raise = bool(parsed_cmd.doraise)
            self.clobber_config = bool(parsed_cmd.clobberConfig)
            # self.do_backup = not bool(parsed_cmd.noBackupConfig)
            self.parsed_cmd = parsed_cmd
        self.num_processes = 1  # int(getattr(parsed_cmd, 'processes', 1)), lets fix it for now


    def make_task(self):
        """
        It creates another instance of the SuperTask passed to the activator, it returns the
        SuperTask instance. Similar to MakeTask in CmdlineTask
        """
        return self.SuperTask.__class__(config=self.config, log=self.log, activator='cmdLine')

    def precall(self):
        """
        A hook similar to the CmdLineTask one, it returns True by default. Need to implement this
        from CmdLineActivator.py

        TODO: Need to add writeConfig, writeSchema and writeMetadata to activator from
        """
        return True

    def activate(self):
        """
        This functions run the execute method of the super task by looping through all the data ids
        parsed from the command line, similar to run a cmdlinetask directly, see the
        documentation to get more information on how to run a SuperTask
        """
        # result_list = []

        if self.precall():
            profile_name = self.parsed_cmd.profile if hasattr(self.parsed_cmd, "profile") else None
            log = self.parsed_cmd.log
            target_list = self.get_target_list(self.parsed_cmd)

            if len(target_list) > 0:
                with profile(profile_name, log):
                    # Run the task using self.__call__
                    # result_list = map(self, target_list)
                    for target in target_list:
                        data_ref, kwargs = target
                        results = None
                        super_task = self.make_task()

                        results = super_task.execute(data_ref, **kwargs)

            else:
                log.warn("Not running the task because there is no data to process; "
                         "you may preview data using \"--show data\"")


    def display_tree(self):
        """
        This is for WorkflowTask, if a tree structure is present, then it can be outputted in the
        stdout
        """
        if hasattr(self.SuperTask, 'print_tree'):
            self.SuperTask.print_tree()

    def display_config(self):
        """
        Display configuration for new framework, i.e., SuperTask created from scratch and linked
         together in a WorkFlowTask
        """
        if hasattr(self.SuperTask, 'print_config'):
            self.SuperTask.print_config()

    def generate_dot(self):
        """
        It generates a dot file structure of the SuperTask and WorkFlowTask if the methods is
        defined
        """
        if hasattr(self.SuperTask, 'write_tree'):
            self.SuperTask.write_tree()


    @staticmethod
    def get_target_list(parsed_cmd, **kwargs):
        """
        Get the target list from the command  line argument parser, taken from CmdLineTask,
        getTargetList function.

        :param parsed_cmd: the parsed command object (an argparse.Namespace) returned by
                           argumentParser.ArgumentParser.parse_args
                           "ArgumentParser.parse_args".
        :param kwargs:     any additional keyword arguments. In the default TaskRunner
                           this is an empty dict, but having it simplifies overriding TaskRunner
                           for tasks whose run method takes additional arguments
        :return: A target list
        """
        return [(ref, kwargs) for ref in parsed_cmd.id.refList]


    @staticmethod
    def load_super_task(super_taskname):
        """
        It search and loads the supertask parsed in the command line. To avoid importing every
        module on every package defined in TASK_PACKAGES, which introduces and extra overhead and
        might be of potential conflict, it uses pyclbr which finds classes and functions by looking
        at the source code of the modules. It is much faster than importing all modules and its
        much simpler as well. An alternative solution would be to have a predefined register of all
        task and the modules they live in, or, to have a naming convention where the source file
        and the Task Class inside have the same name. Currently this is not true for all packages.

        :param super_taskname: The name of the SuperTask
        :return: the Task and Config instances of the supertask
        """
        class_task_instance = None
        class_config_instance = None
        super_task_module = None

        for package in TASK_PACKAGES.itervalues():  # Loop over all packages
            for _, module_name, _ in pkgutil.iter_modules(package.__path__):  # Loop over modules
                # Get a dictionary of  ALL classes defined inside module
                classes_map = pyclbr.readmodule(module_name, path=package.__path__)
                mod_classes = [class_key for class_key in classes_map.keys() if
                               classes_map[class_key].module.upper() == module_name.upper()]
                if super_taskname.upper() in map(lambda x: x.upper(), mod_classes):
                    super_task_module = module_name  # SuperTask Class Found
                    super_taskname = mod_classes[map(lambda x: x.upper(), mod_classes).index(
                        super_taskname.upper())]  # Get correct name
                    break
                    # Breaks after first instance is found, There should not be SuperTask Classes
                    # with the same name.
                    # I might add an Exception raise if more than one is found

            if super_task_module:  # It was found, hence it breaks this loop as well
                print(package.__name__ + '.' + super_task_module + '.' + super_taskname +
                      ' found!')  # Debug only
                break

        if super_task_module:
            # Need to add a try -- except here
            python_mod_stask = importlib.import_module(package.__name__ + '.' + super_task_module)
        else:
            raise RuntimeError("Super Task %s not found!" % super_taskname)  # more to be added

        # print is for debug only
        # We look for SuperTask Class and Config Class to have matching names
        # i.e., ExampleTask and ExampleConfig should both exists in same module
        # It is the case for I think all cmdlinetasks, is this convention?
        print('\nClasses inside module %s : \n ' % (package.__name__ + '.' + super_task_module))
        for _, obj in inspect.getmembers(python_mod_stask):
            if inspect.isclass(obj):
                if obj.__module__ == python_mod_stask.__name__:
                    print(super_task_module + '.' + obj.__name__)
                if obj.__name__.upper() == super_taskname.upper():
                    class_task_instance = obj
                if obj.__name__.upper() == (super_taskname[:-4] + 'Config').upper():
                    class_config_instance = obj
        print()

        if class_task_instance is None:
            # No SuperTask found (although to this point this shouldn't happen)
            raise TaskNameError(' No SuperTaskClass %s found: Task or similiar' % (super_taskname),
                                None)

        if class_config_instance is None:
            # No ConfigClass associated to SuperTask was found
            raise TaskNameError(
                ' No SuperConfig Class %s found: Task or similiar' % (
                    super_taskname.replace('Task', 'Config')), None)

        return class_task_instance


    @staticmethod
    def get_tasks(modules_only=False):
        """
        Get all the tasks available to the activator, basically it returns all the tasks defined as
        (Super)Task classes inside all the packages defined in TASK_PACKAGES

        TODO: Use this function to locate package.module for a given (Super)Task  and use it
        above to avoid code duplication

        :param modules_only: If True, return only the modules, not the tasks
        :return: a list of task or modules inside every package from TASK_PACKAGES
        """

        tasks_list = []
        module_list = []
        for package in TASK_PACKAGES.itervalues():
            for _, module_name, _ in pkgutil.iter_modules(package.__path__):

                task_module = package.__name__ + '.' + module_name
                module_list.append(task_module)
                if not modules_only:
                    classes_map = pyclbr.readmodule(module_name, path=package.__path__)
                    module_classes = [class_key for class_key in classes_map.keys() if
                                      classes_map[class_key].module == module_name]
                    for mod_cls in module_classes:
                        # Get all task except the base classes
                        if mod_cls[-4:].upper() == 'TASK' and mod_cls not in [
                            'WorkFlowParTask', 'WorkFlowSeqTask', 'Task', 'SuperTask',
                            'WorkFlowTask', 'CmdLineTask']:
                            tasks_list.append(task_module + '.' + mod_cls)
        if modules_only:
            return module_list
        else:
            return tasks_list


    @classmethod
    def parse_and_run(cls):
        """
        Similar to the CmdLineTasks, this classmethod parse the arguments and execute this
        activator, this classmethod will create a instance of the activator class. This in
        invoked from the executable cmdLineActivator located in the /bin/ folder

        """

        # Basic arguments to the CmdLineActivator
        # This might be added to rhe argumentParser or be in a different module
        # Arguments for the CmdLineActivator and for the Task it self (as it has been run before)
        # are separated by the --extras argument. This might change in the future
        parser_activator = ActivatorParser(description='CmdLine Activator')
        parser_activator.add_argument('taskname', nargs='?', type=str, help='name of the ('
                                                                            'Super)Task')
        parser_activator.add_argument('-lt', '--list_tasks', action="store_true", default=False,
                                      help='list tasks available to this activator')
        parser_activator.add_argument('-lm', '--list_modules', action="store_true", default=False,
                                      help='list modules available to this activator')
        parser_activator.add_argument('-lp', '--list_packages', action="store_true", default=False,
                                      help='list packages available to this activator, defined by'
                                           'TASK_PACKAGES')
        parser_activator.add_argument('--show', nargs='+', default=(),
                                      help='shows {tree} and/or {config}')
        parser_activator.add_argument('--extras', action="store_true", default=False,
                                      help='Add extra arguments for the (Super)Task itself')
        parser_activator.add_argument('--packages', nargs='+', default=(),
                                      help="Manually adds packages to look for task, overrides "
                                           "TASK_PACKAGES, to see the available packages use -lp "
                                           "option")

        try:
            idx = sys.argv.index('--extras')
            args1 = sys.argv[1:idx]
            args = parser_activator.parse_args(args1)
            args2 = sys.argv[idx + 1:]
        except ValueError:
            args = parser_activator.parse_args()
            args2 = None

        if args.packages:
            TASK_PACKAGES.clear()
            for package in args.packages:
                TASK_PACKAGES[package] = importlib.import_module(package)

        if args.list_modules:
            print("\n List of available modules: \n")
            list_modules = cls.get_tasks(modules_only=True)
            if len(list_modules) == 0:
                print('No modules in these packages: ')
                for package in TASK_PACKAGES.keys():
                    print(package)
            for modules in list_modules:
                print(modules)
            print()
            sys.exit()

        if args.list_tasks:
            print("\n List of available tasks: \n")
            list_tasks = cls.get_tasks()
            if len(list_tasks) == 0:
                print('No Tasks in these packages: ')
                for package in TASK_PACKAGES.keys():
                    print(package)
            for task in list_tasks:
                print(task)
            print()
            sys.exit()

        if args.list_packages:
            print("\n List of available packages see by this activator: \n")
            for package in TASK_PACKAGES.keys():
                print(package)
            print()
            sys.exit()

        if args.show:
            super_taskname = args.taskname
            super_task_class = cls.load_super_task(super_taskname)
            super_task = super_task_class(activator='cmdLine')
            CmdLineClass = cls(super_task, None)

            for show in args.show:
                if show == 'tree':
                    CmdLineClass.display_tree()
                    CmdLineClass.generate_dot()
                elif show == 'config':
                    CmdLineClass.display_config()
                else:
                    print('\nUnknown value for show, use {tree or config}')
            sys.exit()

        if args.taskname is None:
            parser_activator.error('Need to specify a task, use --list_task to see the avaiables '
                                   'ones')
        super_taskname = args.taskname.split('.')[-1]  # TODO: take full path instead of cutting it
        super_task_class = cls.load_super_task(super_taskname)

        # Before creating an instance of the class, check whether --extras was indeed parsed
        if args2 is None:
            parser_activator.error('Missing --extras arguments before inputs and options for the '
                                   'task')
        super_task = super_task_class(activator='cmdLine')
        argparse = ArgumentParser(name=super_task.name)
        argparse.add_id_argument(name="--id", datasetType="raw",
                                 help="data ID, e.g. --id visit=12345 ccd=1,2")
        # Parsing the second set of arguments to the usual argparse
        parser = argparse.parse_args(config=super_task.ConfigClass(), args=args2)

        # Create the Class, display tree and generate dot (locally for now) and execute the
        # activator
        CmdLineClass = cls(super_task, parser)
        CmdLineClass.display_tree()
        CmdLineClass.generate_dot()
        CmdLineClass.activate()




