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
__all__ = ["CmdLineTask", "TaskRunner", "ButlerInitializedTaskRunner"]

import contextlib

from deprecated.sphinx import deprecated

from .task import Task


@deprecated(
    reason="Replaced by lsst.utils.timer.profile().  Will be removed after v26.0",
    version="v25.0",
    category=FutureWarning,
)
@contextlib.contextmanager
def profile(filename, log=None):
    """Context manager for profiling with cProfile.


    Parameters
    ----------
    filename : `str`
        Filename to which to write profile (profiling disabled if `None` or
        empty).
    log : `logging.Logger`, optional
        Log object for logging the profile operations.

    If profiling is enabled, the context manager returns the cProfile.Profile
    object (otherwise it returns None), which allows additional control over
    profiling.  You can obtain this using the "as" clause, e.g.:

    .. code-block:: python

        with profile(filename) as prof:
            runYourCodeHere()

    The output cumulative profile can be printed with a command-line like:

    .. code-block:: bash

        python -c 'import pstats; \
            pstats.Stats("<filename>").sort_stats("cumtime").print_stats(30)'
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
        log.info("cProfile stats written to %s", filename)


@deprecated(
    reason="Gen2 task runners are no longer supported. This functionality has been disabled.",
    version="v23.0",
    category=FutureWarning,
)
class TaskRunner:
    """Run a command-line task, using `multiprocessing` if requested.

    Parameters
    ----------
    TaskClass : `lsst.pipe.base.Task` subclass
        The class of the task to run.
    parsedCmd : `argparse.Namespace`
        The parsed command-line arguments, as returned by the task's argument
        parser's `~lsst.pipe.base.ArgumentParser.parse_args` method.

        .. warning::

           Do not store ``parsedCmd``, as this instance is pickled (if
           multiprocessing) and parsedCmd may contain non-picklable elements.
           It certainly contains more data than we need to send to each
           instance of the task.
    doReturnResults : `bool`, optional
        Should run return the collected result from each invocation of the
        task? This is only intended for unit tests and similar use. It can
        easily exhaust memory (if the task returns enough data and you call it
        enough times) and it will fail when using multiprocessing if the
        returned data cannot be pickled.

        Note that even if ``doReturnResults`` is False a struct with a single
        member "exitStatus" is returned, with value 0 or 1 to be returned to
        the unix shell.

    Raises
    ------
    ImportError
        Raised if multiprocessing is requested (and the task supports it) but
        the multiprocessing library cannot be imported.

    Notes
    -----
    Each command-line task (subclass of `lsst.pipe.base.CmdLineTask`) has a
    task runner. By default it is this class, but some tasks require a
    subclass.

    You may use this task runner for your command-line task if your task has a
    ``runDataRef`` method that takes exactly one argument: a butler data
    reference. Otherwise you must provide a task-specific subclass of
    this runner for your task's ``RunnerClass`` that overrides
    `TaskRunner.getTargetList` and possibly
    `TaskRunner.__call__`. See `TaskRunner.getTargetList` for details.

    This design matches the common pattern for command-line tasks: the
    ``runDataRef`` method takes a single data reference, of some suitable name.
    Additional arguments are rare, and if present, require a subclass of
    `TaskRunner` that calls these additional arguments by name.

    Instances of this class must be picklable in order to be compatible with
    multiprocessing. If multiprocessing is requested
    (``parsedCmd.numProcesses > 1``) then `runDataRef` calls
    `prepareForMultiProcessing` to jettison optional non-picklable elements.
    If your task runner is not compatible with multiprocessing then indicate
    this in your task by setting class variable ``canMultiprocess=False``.

    Due to a `python bug`__, handling a `KeyboardInterrupt` properly `requires
    specifying a timeout`__. This timeout (in sec) can be specified as the
    ``timeout`` element in the output from `~lsst.pipe.base.ArgumentParser`
    (the ``parsedCmd``), if available, otherwise we use `TaskRunner.TIMEOUT`.

    By default, we disable "implicit" threading -- ie, as provided by
    underlying numerical libraries such as MKL or BLAS. This is designed to
    avoid thread contention both when a single command line task spawns
    multiple processes and when multiple users are running on a shared system.
    Users can override this behaviour by setting the
    ``LSST_ALLOW_IMPLICIT_THREADS`` environment variable.

    .. __: http://bugs.python.org/issue8296
    .. __: http://stackoverflow.com/questions/1408356/
    """

    pass


@deprecated(
    reason="Gen2 task runners are no longer supported. This functionality has been disabled.",
    version="v23.0",
    category=FutureWarning,
)
class ButlerInitializedTaskRunner(TaskRunner):
    r"""A `TaskRunner` for `CmdLineTask`\ s that require a ``butler`` keyword
    argument to be passed to their constructor.
    """
    pass


@deprecated(
    reason="CmdLineTask is no longer supported. This functionality has been disabled. Use Gen3.",
    version="v23.0",
    category=FutureWarning,
)
class CmdLineTask(Task):
    """Base class for command-line tasks: tasks that may be executed from the
    command-line.

    Notes
    -----
    See :ref:`task-framework-overview` to learn what tasks.

    Subclasses must specify the following class variables:

    - ``ConfigClass``: configuration class for your task (a subclass of
      `lsst.pex.config.Config`, or if your task needs no configuration, then
      `lsst.pex.config.Config` itself).
    - ``_DefaultName``: default name used for this task (a `str`).

    Subclasses may also specify the following class variables:

    - ``RunnerClass``: a task runner class. The default is ``TaskRunner``,
      which works for any task with a runDataRef method that takes exactly one
      argument: a data reference. If your task does not meet this requirement
      then you must supply a variant of ``TaskRunner``; see ``TaskRunner``
      for more information.
    - ``canMultiprocess``: the default is `True`; set `False` if your task
      does not support multiprocessing.

    Subclasses must specify a method named ``runDataRef``:

    - By default ``runDataRef`` accepts a single butler data reference, but
      you can specify an alternate task runner (subclass of ``TaskRunner``) as
      the value of class variable ``RunnerClass`` if your run method needs
      something else.
    - ``runDataRef`` is expected to return its data in a
      `lsst.pipe.base.Struct`. This provides safety for evolution of the task
      since new values may be added without harming existing code.
    - The data returned by ``runDataRef`` must be picklable if your task is to
      support multiprocessing.
    """

    pass
