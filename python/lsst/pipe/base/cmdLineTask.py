#
# LSST Data Management System
# Copyright 2008-2015 AURA/LSST.
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
# see <https://www.lsstcorp.org/LegalNotices/>.
#
from __future__ import absolute_import, division
import sys
import traceback
import functools
import contextlib

from builtins import str
from builtins import object

import lsst.utils
from lsst.base import disableImplicitThreading
import lsst.afw.table as afwTable
from .task import Task, TaskError
from .struct import Struct
from .argumentParser import ArgumentParser
from lsst.base import Packages
from lsst.log import Log

__all__ = ["CmdLineTask", "TaskRunner", "ButlerInitializedTaskRunner"]


def _poolFunctionWrapper(function, arg):
    """Wrapper around function to catch exceptions that don't inherit from `Exception`.

    Such exceptions aren't caught by multiprocessing, which causes the slave process to crash and you end up
    hitting the timeout.
    """
    try:
        return function(arg)
    except Exception:
        raise  # No worries
    except:
        # Need to wrap the exception with something multiprocessing will recognise
        cls, exc, tb = sys.exc_info()
        log = Log.getDefaultLogger()
        log.warn("Unhandled exception %s (%s):\n%s" % (cls.__name__, exc, traceback.format_exc()))
        raise Exception("Unhandled exception: %s (%s)" % (cls.__name__, exc))


def _runPool(pool, timeout, function, iterable):
    """Wrapper around ``pool.map_async``, to handle timeout

    This is required so as to trigger an immediate interrupt on the KeyboardInterrupt (Ctrl-C); see
    http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool

    Further wraps the function in ``_poolFunctionWrapper`` to catch exceptions
    that don't inherit from `Exception`.
    """
    return pool.map_async(functools.partial(_poolFunctionWrapper, function), iterable).get(timeout)


@contextlib.contextmanager
def profile(filename, log=None):
    """Context manager for profiling with cProfile.


    Parameters
    ----------
    filename : `str`
        Filename to which to write profile (profiling disabled if `None` or empty).
    log : `lsst.log.Log`, optional
        Log object for logging the profile operations.

    If profiling is enabled, the context manager returns the cProfile.Profile object (otherwise
    it returns None), which allows additional control over profiling.  You can obtain this using
    the "as" clause, e.g.:

        with profile(filename) as prof:
            runYourCodeHere()

    The output cumulative profile can be printed with a command-line like::

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


class TaskRunner(object):
    """Run a command-line task, using `multiprocessing` if requested.

    Parameters
    ----------
    TaskClass : `lsst.pipe.base.Task` subclass
        The class of the task to run.
    parsedCmd : `argparse.Namespace`
        The parsed command-line arguments, as returned by the task's argument parser's
        `~lsst.pipe.base.ArgumentParser.parse_args` method.

        .. warning::

           Do not store ``parsedCmd``, as this instance is pickled (if multiprocessing) and parsedCmd may
           contain non-picklable elements. It certainly contains more data than we need to send to each
           instance of the task.
    doReturnResults : `bool`, optional
        Should run return the collected result from each invocation of the task? This is only intended for
        unit tests and similar use. It can easily exhaust memory (if the task returns enough data and you
        call it enough times) and it will fail when using multiprocessing if the returned data cannot be
        pickled.

        Note that even if ``doReturnResults`` is False a struct with a single member "exitStatus" is returned,
        with value 0 or 1 to be returned to the unix shell.

    Raises
    ------
    ImportError
        If multiprocessing is requested (and the task supports it) but the multiprocessing library cannot be
        imported.

    Notes
    -----
    Each command-line task (subclass of `lsst.pipe.base.CmdLineTask`) has a task runner. By default it is this
    class, but some tasks require a subclass. See the manual :ref:`creating-a-command-line-task` for more
    information. See `CmdLineTask.parseAndRun` to see how a task runner is used.

    You may use this task runner for your command-line task if your task has a run method that takes exactly
    one argument: a butler data reference. Otherwise you must provide a task-specific subclass of this runner
    for your task's ``RunnerClass`` that overrides `TaskRunner.getTargetList` and possibly
    `TaskRunner.__call__`. See `TaskRunner.getTargetList` for details.

    This design matches the common pattern for command-line tasks: the run method takes a single data
    reference, of some suitable name. Additional arguments are rare, and if present, require a subclass of
    `TaskRunner` that calls these additional arguments by name.

    Instances of this class must be picklable in order to be compatible with multiprocessing. If
    multiprocessing is requested (``parsedCmd.numProcesses > 1``) then `run` calls `prepareForMultiProcessing`
    to jettison optional non-picklable elements. If your task runner is not compatible with multiprocessing
    then indicate this in your task by setting class variable ``canMultiprocess=False``.

    Due to a `python bug`__, handling a `KeyboardInterrupt` properly `requires specifying a timeout`__. This
    timeout (in sec) can be specified as the ``timeout`` element in the output from
    `~lsst.pipe.base.ArgumentParser` (the ``parsedCmd``), if available, otherwise we use `TaskRunner.TIMEOUT`.

    .. __: http://bugs.python.org/issue8296
    .. __:  http://stackoverflow.com/questions/1408356/keyboard-interrupts-with-pythons-multiprocessing-pool
    """

    TIMEOUT = 3600*24*30
    """Default timeout (seconds) for multiprocessing."""

    def __init__(self, TaskClass, parsedCmd, doReturnResults=False):
        self.TaskClass = TaskClass
        self.doReturnResults = bool(doReturnResults)
        self.config = parsedCmd.config
        self.log = parsedCmd.log
        self.doRaise = bool(parsedCmd.doraise)
        self.clobberConfig = bool(parsedCmd.clobberConfig)
        self.doBackup = not bool(parsedCmd.noBackupConfig)
        self.numProcesses = int(getattr(parsedCmd, 'processes', 1))

        self.timeout = getattr(parsedCmd, 'timeout', None)
        if self.timeout is None or self.timeout <= 0:
            self.timeout = self.TIMEOUT

        if self.numProcesses > 1:
            if not TaskClass.canMultiprocess:
                self.log.warn("This task does not support multiprocessing; using one process")
                self.numProcesses = 1

    def prepareForMultiProcessing(self):
        """Prepare this instance for multiprocessing

        Optional non-picklable elements are removed.

        This is only called if the task is run under multiprocessing.
        """
        self.log = None

    def run(self, parsedCmd):
        """Run the task on all targets.

        Parameters
        ----------
        parsedCmd : `argparse.Namespace`
            Parsed command `argparse.Namespace`.

        Returns
        -------
        resultList : `list`
            A list of results returned by `TaskRunner.__call__`, or an empty list if `TaskRunner.__call__`
            is not called (e.g. if `TaskRunner.precall` returns `False`). See `TaskRunner.__call__`
            for details.

        Notes
        -----
        The task is run under multiprocessing if `TaskRunner.numProcesses` is more than 1; otherwise
        processing is serial.
        """
        resultList = []
        if self.numProcesses > 1:
            disableImplicitThreading()  # To prevent thread contention
            import multiprocessing
            self.prepareForMultiProcessing()
            pool = multiprocessing.Pool(processes=self.numProcesses, maxtasksperchild=1)
            mapFunc = functools.partial(_runPool, pool, self.timeout)
        else:
            pool = None
            mapFunc = map

        if self.precall(parsedCmd):
            profileName = parsedCmd.profile if hasattr(parsedCmd, "profile") else None
            log = parsedCmd.log
            targetList = self.getTargetList(parsedCmd)
            if len(targetList) > 0:
                with profile(profileName, log):
                    # Run the task using self.__call__
                    resultList = list(mapFunc(self, targetList))
            else:
                log.warn("Not running the task because there is no data to process; "
                         "you may preview data using \"--show data\"")

        if pool is not None:
            pool.close()
            pool.join()

        return resultList

    @staticmethod
    def getTargetList(parsedCmd, **kwargs):
        """Get a list of (dataRef, kwargs) for `TaskRunner.__call__`.

        Parameters
        ----------
        parsedCmd : `argparse.Namespace`
            The parsed command object returned by `lsst.pipe.base.argumentParser.ArgumentParser.parse_args`.
        kwargs
            Any additional keyword arguments. In the default `TaskRunner` this is an empty dict, but having
            it simplifies overriding `TaskRunner` for tasks whose run method takes additional arguments
            (see case (1) below).

        Notes
        -----
        The default implementation of `TaskRunner.getTargetList` and `TaskRunner.__call__` works for any
        command-line task whose run method takes exactly one argument: a data reference. Otherwise you
        must provide a variant of TaskRunner that overrides `TaskRunner.getTargetList` and possibly
        `TaskRunner.__call__`. There are two cases.

        **Case 1**

        If your command-line task has a ``run`` method that takes one data reference followed by additional
        arguments, then you need only override `TaskRunner.getTargetList` to return the additional arguments
        as an argument dict. To make this easier, your overridden version of `~TaskRunner.getTargetList` may
        call `TaskRunner.getTargetList` with the extra arguments as keyword arguments. For example, the
        following adds an argument dict containing a single key: "calExpList", whose value is the list of data
        IDs for the calexp ID argument::

            def getTargetList(parsedCmd):
                return TaskRunner.getTargetList(
                    parsedCmd,
                    calExpList=parsedCmd.calexp.idList
                )

        It is equivalent to this slightly longer version::

            @staticmethod
            def getTargetList(parsedCmd):
                argDict = dict(calExpList=parsedCmd.calexp.idList)
                return [(dataId, argDict) for dataId in parsedCmd.id.idList]

        **Case 2**

        If your task does not meet condition (1) then you must override both TaskRunner.getTargetList and
        `TaskRunner.__call__`. You may do this however you see fit, so long as `TaskRunner.getTargetList`
        returns a list, each of whose elements is sent to `TaskRunner.__call__`, which runs your task.
        """
        return [(ref, kwargs) for ref in parsedCmd.id.refList]

    def makeTask(self, parsedCmd=None, args=None):
        """Create a Task instance.

        Parameters
        ----------
        parsedCmd
            Parsed command-line options (used for extra task args by some task runners).
        args
            Args tuple passed to `TaskRunner.__call__` (used for extra task arguments by some task runners).

        Notes
        -----
        ``makeTask`` can be called with either the ``parsedCmd`` argument or ``args`` argument set to None,
        but it must construct identical Task instances in either case.

        Subclasses may ignore this method entirely if they reimplement both `TaskRunner.precall` and
        `TaskRunner.__call__`.
        """
        return self.TaskClass(config=self.config, log=self.log)

    def _precallImpl(self, task, parsedCmd):
        """The main work of `precall`.

        We write package versions, schemas and configs, or compare these to existing files on disk if present.
        """
        if not parsedCmd.noVersions:
            task.writePackageVersions(parsedCmd.butler, clobber=parsedCmd.clobberVersions)
        task.writeConfig(parsedCmd.butler, clobber=self.clobberConfig, doBackup=self.doBackup)
        task.writeSchemas(parsedCmd.butler, clobber=self.clobberConfig, doBackup=self.doBackup)

    def precall(self, parsedCmd):
        """Hook for code that should run exactly once, before multiprocessing.

        Notes
        -----
        Must return True if `TaskRunner.__call__` should subsequently be called.

        .. warning::

           Implementations must take care to ensure that no unpicklable
           attributes are added to the TaskRunner itself, for compatibility
           with multiprocessing.

        The default implementation writes package versions, schemas and configs, or compares them to existing
        files on disk if present.
        """
        task = self.makeTask(parsedCmd=parsedCmd)

        if self.doRaise:
            self._precallImpl(task, parsedCmd)
        else:
            try:
                self._precallImpl(task, parsedCmd)
            except Exception as e:
                task.log.fatal("Failed in task initialization: %s", e)
                if not isinstance(e, TaskError):
                    traceback.print_exc(file=sys.stderr)
                return False
        return True

    def __call__(self, args):
        """Run the Task on a single target.

        Parameters
        ----------
        args
            Arguments for Task.run()

        Returns
        -------
        struct : `lsst.pipe.base.Struct`
            Contains these fields if ``doReturnResults`` is `True`:

            - ``dataRef``: the provided data reference.
            - ``metadata``: task metadata after execution of run.
            - ``result``: result returned by task run, or `None` if the task fails.

            If ``doReturnResults`` is `False` the struct contains:

            - ``exitStatus``.

        Notes
        -----
        This default implementation assumes that the ``args`` is a tuple
        containing a data reference and a dict of keyword arguments.

        .. warning::

           If you override this method and wish to return something when ``doReturnResults`` is `False`,
           then it must be picklable to support multiprocessing and it should be small enough that pickling
           and unpickling do not add excessive overhead.
        """
        dataRef, kwargs = args
        if self.log is None:
            self.log = Log.getDefaultLogger()
        if hasattr(dataRef, "dataId"):
            self.log.MDC("LABEL", str(dataRef.dataId))
        elif isinstance(dataRef, (list, tuple)):
            self.log.MDC("LABEL", str([ref.dataId for ref in dataRef if hasattr(ref, "dataId")]))
        task = self.makeTask(args=args)
        result = None                   # in case the task fails
        exitStatus = 0                  # exit status for the shell
        if self.doRaise:
            result = task.run(dataRef, **kwargs)
        else:
            try:
                result = task.run(dataRef, **kwargs)
            except Exception as e:
                exitStatus = 1          # n.b. The shell exit value is the number of dataRefs returning
                                        # non-zero, so the actual value used here is lost
                
                # don't use a try block as we need to preserve the original exception
                if hasattr(dataRef, "dataId"):
                    task.log.fatal("Failed on dataId=%s: %s", dataRef.dataId, e)
                elif isinstance(dataRef, (list, tuple)):
                    task.log.fatal("Failed on dataId=[%s]: %s",
                                   ", ".join(str(ref.dataId) for ref in dataRef), e)
                else:
                    task.log.fatal("Failed on dataRef=%s: %s", dataRef, e)

                if not isinstance(e, TaskError):
                    traceback.print_exc(file=sys.stderr)
        task.writeMetadata(dataRef)

        # remove MDC so it does not show up outside of task context
        self.log.MDCRemove("LABEL")

        if self.doReturnResults:
            return Struct(
                exitStatus=exitStatus,
                dataRef=dataRef,
                metadata=task.metadata,
                result=result,
            )
        else:
            return Struct(
                exitStatus=exitStatus,
            )


class ButlerInitializedTaskRunner(TaskRunner):
    """A TaskRunner for `CmdLineTask`\ s that require a ``butler`` keyword argument to be passed to
    their constructor.
    """

    def makeTask(self, parsedCmd=None, args=None):
        """A variant of the base version that passes a butler argument to the task's constructor.

        Parameters
        ----------
        parsedCmd : `argparse.Namespace`
            Parsed command-line options, as returned by the `~lsst.pipe.base.ArgumentParser`; if specified
            then args is ignored.
        args
            Other arguments; if ``parsedCmd`` is `None` then this must be specified.

        Raises
        ------
        RuntimeError
            Raised if ``parsedCmd`` and ``args`` are both `None`.
        """
        if parsedCmd is not None:
            butler = parsedCmd.butler
        elif args is not None:
            dataRef, kwargs = args
            butler = dataRef.butlerSubset.butler
        else:
            raise RuntimeError("parsedCmd or args must be specified")
        return self.TaskClass(config=self.config, log=self.log, butler=butler)


class CmdLineTask(Task):
    """Base class for command-line tasks: tasks that may be executed from the command-line.

    Notes
    -----
    See :ref:`task-framework-overview` to learn what tasks are and :ref:`creating-a-command-line-task` for
    more information about writing command-line tasks.

    Subclasses must specify the following class variables:

    - ``ConfigClass``: configuration class for your task (a subclass of `lsst.pex.config.Config`, or if your
      task needs no configuration, then `lsst.pex.config.Config` itself).
    - ``_DefaultName``: default name used for this task (a str).

    Subclasses may also specify the following class variables:

    - ``RunnerClass``: a task runner class. The default is ``TaskRunner``, which works for any task
      with a run method that takes exactly one argument: a data reference. If your task does
      not meet this requirement then you must supply a variant of ``TaskRunner``; see ``TaskRunner``
      for more information.
    - ``canMultiprocess``: the default is `True`; set `False` if your task does not support multiprocessing.

    Subclasses must specify a method named ``run``:

    - By default ``run`` accepts a single butler data reference, but you can specify an alternate task runner
      (subclass of ``TaskRunner``) as the value of class variable ``RunnerClass`` if your run method needs
      something else.
    - ``run`` is expected to return its data in a `lsst.pipe.base.Struct`. This provides safety for evolution
      of the task since new values may be added without harming existing code.
    - The data returned by ``run`` must be picklable if your task is to support multiprocessing.
    """
    RunnerClass = TaskRunner
    canMultiprocess = True

    @classmethod
    def applyOverrides(cls, config):
        """A hook to allow a task to change the values of its config *after* the camera-specific
        overrides are loaded but before any command-line overrides are applied.

        Parameters
        ----------
        config : instance of task's ``ConfigClass``
            Task configuration.

        Notes
        -----
        This is necessary in some cases because the camera-specific overrides may retarget subtasks,
        wiping out changes made in ConfigClass.setDefaults. See LSST Trac ticket #2282 for more discussion.

        .. warning::

           This is called by CmdLineTask.parseAndRun; other ways of constructing a config will not apply
           these overrides.
        """
        pass

    @classmethod
    def parseAndRun(cls, args=None, config=None, log=None, doReturnResults=False):
        """Parse an argument list and run the command.

        Parameters
        ----------
        args : `list`, optional
            List of command-line arguments; if `None` use `sys.argv`.
        config : `lsst.pex.config.Config`-type, optional
            Config for task. If `None` use `Task.ConfigClass`.
        log : `lsst.log.Log`-type, optional
            Log. If `None` use the default log.
        doReturnResults : `bool`, optional
            If `True`, return the results of this task. Default is `False`. This is only intended for
            unit tests and similar use. It can easily exhaust memory (if the task returns enough data and you
            call it enough times) and it will fail when using multiprocessing if the returned data cannot be
            pickled.

        Returns
        -------
        struct : `lsst.pipe.base.Struct`
            Fields are:

            - ``argumentParser``: the argument parser.
            - ``parsedCmd``: the parsed command returned by the argument parser's
              `lsst.pipe.base.ArgumentParser.parse_args` method.
            - ``taskRunner``: the task runner used to run the task (an instance of `Task.RunnerClass`).
            - ``resultList``: results returned by the task runner's ``run`` method, one entry per invocation.
                This will typically be a list of `None` unless ``doReturnResults`` is `True`;
                see `Task.RunnerClass` (`TaskRunner` by default) for more information.

        Notes
        -----
        Calling this method with no arguments specified is the standard way to run a command-line task
        from the command-line. For an example see ``pipe_tasks`` ``bin/makeSkyMap.py`` or almost any other
        file in that directory.

        If one or more of the dataIds fails then this routine will exit (with a status giving the
        number of failed dataIds) rather than returning this struct;  this behaviour can be
        overridden by specifying the ``--noExit`` command-line option.
        """
        if args is None:
            commandAsStr = " ".join(sys.argv)
            args = sys.argv[1:]
        else:
            commandAsStr = "{}{}".format(lsst.utils.get_caller_name(skip=1), tuple(args))

        argumentParser = cls._makeArgumentParser()
        if config is None:
            config = cls.ConfigClass()
        parsedCmd = argumentParser.parse_args(config=config, args=args, log=log, override=cls.applyOverrides)
        # print this message after parsing the command so the log is fully configured
        parsedCmd.log.info("Running: %s", commandAsStr)

        taskRunner = cls.RunnerClass(TaskClass=cls, parsedCmd=parsedCmd, doReturnResults=doReturnResults)
        resultList = taskRunner.run(parsedCmd)

        try:
            nFailed = sum(((res.exitStatus != 0) for res in resultList))
        except Exception as e:
            parsedCmd.log.warn("Unable to retrieve exit status (%s); assuming success", e)
            nFailed = 0

        if nFailed > 0:
            if parsedCmd.noExit:
                parsedCmd.log.warn("%d dataRefs failed; not exiting as --noExit was set", nFailed)
            else:
                sys.exit(nFailed)

        return Struct(
            argumentParser=argumentParser,
            parsedCmd=parsedCmd,
            taskRunner=taskRunner,
            resultList=resultList,
        )

    @classmethod
    def _makeArgumentParser(cls):
        """Create and return an argument parser.

        Returns
        -------
        parser : `lsst.pipe.base.ArgumentParser`
            The argument parser for this task.

        Notes
        -----
        By default this returns an `~lsst.pipe.base.ArgumentParser` with one ID argument named `--id` of
        dataset type ``raw``.

        Your task subclass may need to override this method to change the dataset type or data ref level,
        or to add additional data ID arguments. If you add additional data ID arguments or your task's
        run method takes more than a single data reference then you will also have to provide a task-specific
        task runner (see TaskRunner for more information).
        """
        parser = ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument(name="--id", datasetType="raw",
                               help="data IDs, e.g. --id visit=12345 ccd=1,2^0,3")
        return parser

    def writeConfig(self, butler, clobber=False, doBackup=True):
        """Write the configuration used for processing the data, or check that an existing
        one is equal to the new one if present.

        Parameters
        ----------
        butler : `lsst.daf.persistence.Butler`
            Data butler used to write the config. The config is written to dataset type
            `CmdLineTask._getConfigName`.
        clobber : `bool`, optional
            A boolean flag that controls what happens if a config already has been saved:
            - `True`: overwrite or rename the existing config, depending on ``doBackup``.
            - `False`: raise `TaskError` if this config does not match the existing config.
        doBackup : bool, optional
            Set to `True` to backup the config files if clobbering.
        """
        configName = self._getConfigName()
        if configName is None:
            return
        if clobber:
            butler.put(self.config, configName, doBackup=doBackup)
        elif butler.datasetExists(configName):
            # this may be subject to a race condition; see #2789
            try:
                oldConfig = butler.get(configName, immediate=True)
            except Exception as exc:
                raise type(exc)("Unable to read stored config file %s (%s); consider using --clobber-config" %
                                (configName, exc))

            def logConfigMismatch(msg):
                self.log.fatal("Comparing configuration: %s", msg)

            if not self.config.compare(oldConfig, shortcut=False, output=logConfigMismatch):
                raise TaskError(
                    ("Config does not match existing task config %r on disk; tasks configurations " +
                     "must be consistent within the same output repo (override with --clobber-config)") %
                    (configName,))
        else:
            butler.put(self.config, configName)

    def writeSchemas(self, butler, clobber=False, doBackup=True):
        """Write the schemas returned by `lsst.pipe.base.Task.getAllSchemaCatalogs`.

        Parameters
        ----------
        butler : `lsst.daf.persistence.Butler`
            Data butler used to write the schema. Each schema is written to the dataset type specified as the
            key in the dict returned by `~lsst.pipe.base.Task.getAllSchemaCatalogs`.
        clobber : `bool`, optional
            A boolean flag that controls what happens if a schema already has been saved:
            - `True`: overwrite or rename the existing schema, depending on ``doBackup``.
            - `False`: raise `TaskError` if this schema does not match the existing schema.
        doBackup : `bool`, optional
            Set to `True` to backup the schema files if clobbering.

        Notes
        -----
        If ``clobber`` is `False` and an existing schema does not match a current schema,
        then some schemas may have been saved successfully and others may not, and there is no easy way to
        tell which is which.
        """
        for dataset, catalog in self.getAllSchemaCatalogs().items():
            schemaDataset = dataset + "_schema"
            if clobber:
                butler.put(catalog, schemaDataset, doBackup=doBackup)
            elif butler.datasetExists(schemaDataset):
                oldSchema = butler.get(schemaDataset, immediate=True).getSchema()
                if not oldSchema.compare(catalog.getSchema(), afwTable.Schema.IDENTICAL):
                    raise TaskError(
                        ("New schema does not match schema %r on disk; schemas must be " +
                         " consistent within the same output repo (override with --clobber-config)") %
                        (dataset,))
            else:
                butler.put(catalog, schemaDataset)

    def writeMetadata(self, dataRef):
        """Write the metadata produced from processing the data.

        Parameters
        ----------
        dataRef
            Butler data reference used to write the metadata.
            The metadata is written to dataset type `CmdLineTask._getMetadataName`.
        """
        try:
            metadataName = self._getMetadataName()
            if metadataName is not None:
                dataRef.put(self.getFullMetadata(), metadataName)
        except Exception as e:
            self.log.warn("Could not persist metadata for dataId=%s: %s", dataRef.dataId, e)

    def writePackageVersions(self, butler, clobber=False, doBackup=True, dataset="packages"):
        """Compare and write package versions.

        Parameters
        ----------
        butler : `lsst.daf.persistence.Butler`
            Data butler used to read/write the package versions.
        clobber : `bool`, optional
            A boolean flag that controls what happens if versions already have been saved:
            - `True`: overwrite or rename the existing version info, depending on ``doBackup``.
            - `False`: raise `TaskError` if this version info does not match the existing.
        doBackup : `bool`, optional
            If `True` and clobbering, old package version files are backed up.
        dataset : `str`, optional
            Name of dataset to read/write.

        Raises
        ------
        TaskError
            Raised if there is a version mismatch with current and persisted lists of package versions.

        Notes
        -----
        Note that this operation is subject to a race condition.
        """
        packages = Packages.fromSystem()

        if clobber:
            return butler.put(packages, dataset, doBackup=doBackup)
        if not butler.datasetExists(dataset):
            return butler.put(packages, dataset)

        try:
            old = butler.get(dataset, immediate=True)
        except Exception as exc:
            raise type(exc)("Unable to read stored version dataset %s (%s); "
                            "consider using --clobber-versions or --no-versions" %
                            (dataset, exc))
        # Note that because we can only detect python modules that have been imported, the stored
        # list of products may be more or less complete than what we have now.  What's important is
        # that the products that are in common have the same version.
        diff = packages.difference(old)
        if diff:
            raise TaskError(
                "Version mismatch (" +
                "; ".join("%s: %s vs %s" % (pkg, diff[pkg][1], diff[pkg][0]) for pkg in diff) +
                "); consider using --clobber-versions or --no-versions")
        # Update the old set of packages in case we have more packages that haven't been persisted.
        extra = packages.extra(old)
        if extra:
            old.update(packages)
            butler.put(old, dataset, doBackup=doBackup)

    def _getConfigName(self):
        """Get the name of the config dataset type, or `None` if config is not to be persisted.

        Notes
        -----
        The name may depend on the config; that is why this is not a class method.
        """
        return self._DefaultName + "_config"

    def _getMetadataName(self):
        """Get the name of the metadata dataset type, or `None` if metadata is not to be persisted.

        Notes
        -----
        The name may depend on the config; that is why this is not a class method.
        """
        return self._DefaultName + "_metadata"
