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

from __future__ import annotations

__all__ = ["Task", "TaskError"]

import contextlib
import logging
import weakref
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import lsst.utils
import lsst.utils.logging
from lsst.pex.config import ConfigurableField
from lsst.utils.timer import logInfo

if TYPE_CHECKING:
    from lsst.pex.config import Config

try:
    import lsstDebug  # type: ignore
except ImportError:
    lsstDebug = None

from ._task_metadata import TaskMetadata

# This defines the Python type to use for task metadata. It is a private
# class variable that can be accessed by other closely-related middleware
# code and test code.
_TASK_METADATA_TYPE = TaskMetadata
_TASK_FULL_METADATA_TYPE = TaskMetadata


class TaskError(Exception):
    """Use to report errors for which a traceback is not useful.

    Notes
    -----
    Examples of such errors:

    - processCcd is asked to run detection, but not calibration, and no calexp
      is found.
    - coadd finds no valid images in the specified patch.
    """

    pass


class Task:
    r"""Base class for data processing tasks.

    See :ref:`task-framework-overview` to learn what tasks are, and
    :ref:`creating-a-task` for more information about writing tasks.

    Parameters
    ----------
    config : `Task.ConfigClass` instance, optional
        Configuration for this task (an instance of Task.ConfigClass, which
        is a task-specific subclass of `lsst.pex.config.Config`, or `None`.
        If `None`:

        - If parentTask specified then defaults to parentTask.config.\<name>
        - If parentTask is None then defaults to self.ConfigClass()

    name : `str`, optional
        Brief name of task, or `None`; if `None` then defaults to
        `Task._DefaultName`
    parentTask : `Task`-type, optional
        The parent task of this subtask, if any.

        - If `None` (a top-level task) then you must specify config and name
          is ignored.
        - If not `None` (a subtask) then you must specify name.
    log : `logging.Logger` or subclass, optional
        Log whose name is used as a log name prefix, or `None` for no prefix.
        Ignored if is parentTask specified, in which case
        ``parentTask.log``\ 's name is used as a prefix. The task's log name is
        ``prefix + "." + name`` if a prefix exists, else ``name``. The task's
        log is then a child logger of ``parentTask.log`` (if ``parentTask``
        specified), or a child logger of the log from the argument
        (if ``log`` is not `None`).

    Raises
    ------
    RuntimeError
        Raised under these circumstances:

        - If ``parentTask`` is `None` and ``config`` is `None`.
        - If ``parentTask`` is not `None` and ``name`` is `None`.
        - If ``name`` is `None` and ``_DefaultName`` does not exist.

    Notes
    -----
    Useful attributes include:

    - ``log``: an `logging.Logger` or subclass.
    - ``config``: task-specific configuration; an instance of ``ConfigClass``
      (see below).
    - ``metadata``: a `TaskMetadata` for
      collecting task-specific metadata, e.g. data quality and performance
      metrics. This is data that is only meant to be persisted, never to be
      used by the task.

    Use a `lsst.pipe.base.PipelineTask` subclass to perform I/O with a
    Butler.

    Subclasses must also have an attribute ``ConfigClass`` that is a subclass
    of `lsst.pex.config.Config` which configures the task. Subclasses should
    also have an attribute ``_DefaultName``: the default name if there is no
    parent task. ``_DefaultName`` is required for subclasses of
    `~lsst.pipe.base.PipeLineTask` and recommended for subclasses of Task
    because it simplifies construction (e.g. for unit tests).
    """

    ConfigClass: ClassVar[Type[Config]]
    _DefaultName: ClassVar[str]

    _add_module_logger_prefix: bool = True
    """Control whether the module prefix should be prepended to default
    logger names."""

    def __init__(
        self,
        config: Optional[Config] = None,
        name: Optional[str] = None,
        parentTask: Optional[Task] = None,
        log: Optional[Union[logging.Logger, lsst.utils.logging.LsstLogAdapter]] = None,
    ):
        self.metadata = _TASK_METADATA_TYPE()
        self.__parentTask: Optional[weakref.ReferenceType]
        self.__parentTask = parentTask if parentTask is None else weakref.ref(parentTask)

        if parentTask is not None:
            if name is None:
                raise RuntimeError("name is required for a subtask")
            self._name = name
            self._fullName = parentTask._computeFullName(name)
            if config is None:
                config = getattr(parentTask.config, name)
            self._taskDict: Dict[str, weakref.ReferenceType[Task]] = parentTask._taskDict
            loggerName = parentTask.log.getChild(name).name
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
            if log is not None and log.name:
                loggerName = log.getChild(loggerName).name
            elif self._add_module_logger_prefix:
                # Prefix the logger name with the root module name.
                # We want all Task loggers to have this prefix to make
                # it easier to control them. This can be disabled by
                # a Task setting the class property _add_module_logger_prefix
                # to False -- in which case the logger name will not be
                # modified.
                module_name = self.__module__
                module_root = module_name.split(".")[0] + "."
                if not loggerName.startswith(module_root):
                    loggerName = module_root + loggerName

        # Get a logger (that might be a subclass of logging.Logger).
        self.log: lsst.utils.logging.LsstLogAdapter = lsst.utils.logging.getLogger(loggerName)
        self.config: Config = config
        self.config.validate()
        if lsstDebug:
            self._display = lsstDebug.Info(self.__module__).display
        else:
            self._display = None
        self._taskDict[self._fullName] = weakref.ref(self)

    @property
    def _parentTask(self) -> Optional[Task]:
        return self.__parentTask if self.__parentTask is None else self.__parentTask()

    def emptyMetadata(self) -> None:
        """Empty (clear) the metadata for this Task and all sub-Tasks."""
        for wref in self._taskDict.values():
            subtask = wref()
            assert subtask is not None, "Unexpected garbage collection of subtask."
            subtask.metadata = _TASK_METADATA_TYPE()

    def getFullMetadata(self) -> TaskMetadata:
        """Get metadata for all tasks.

        Returns
        -------
        metadata : `TaskMetadata`
            The keys are the full task name.
            Values are metadata for the top-level task and all subtasks,
            sub-subtasks, etc.

        Notes
        -----
        The returned metadata includes timing information (if
        ``@timer.timeMethod`` is used) and any metadata set by the task. The
        name of each item consists of the full task name with ``.`` replaced
        by ``:``, followed by ``.`` and the name of the item, e.g.::

            topLevelTaskName:subtaskName:subsubtaskName.itemName

        using ``:`` in the full task name disambiguates the rare situation
        that a task has a subtask and a metadata item with the same name.
        """
        fullMetadata = _TASK_FULL_METADATA_TYPE()
        for fullName, wref in self.getTaskDict().items():
            subtask = wref()
            assert subtask is not None, "Unexpected garbage collection of subtask."
            fullMetadata[fullName.replace(".", ":")] = subtask.metadata
        return fullMetadata

    def getFullName(self) -> str:
        """Get the task name as a hierarchical name including parent task
        names.

        Returns
        -------
        fullName : `str`
            The full name consists of the name of the parent task and each
            subtask separated by periods. For example:

            - The full name of top-level task "top" is simply "top".
            - The full name of subtask "sub" of top-level task "top" is
              "top.sub".
            - The full name of subtask "sub2" of subtask "sub" of top-level
              task "top" is "top.sub.sub2".
        """
        return self._fullName

    def getName(self) -> str:
        """Get the name of the task.

        Returns
        -------
        taskName : `str`
            Name of the task.

        See also
        --------
        getFullName
        """
        return self._name

    def getTaskDict(self) -> Dict[str, weakref.ReferenceType[Task]]:
        """Get a dictionary of all tasks as a shallow copy.

        Returns
        -------
        taskDict : `dict`
            Dictionary containing full task name: task object for the top-level
            task and all subtasks, sub-subtasks, etc.
        """
        return self._taskDict.copy()

    def makeSubtask(self, name: str, **keyArgs: Any) -> None:
        """Create a subtask as a new instance as the ``name`` attribute of this
        task.

        Parameters
        ----------
        name : `str`
            Brief name of the subtask.
        keyArgs
            Extra keyword arguments used to construct the task. The following
            arguments are automatically provided and cannot be overridden:

            - "config".
            - "parentTask".

        Notes
        -----
        The subtask must be defined by ``Task.config.name``, an instance of
        `~lsst.pex.config.ConfigurableField` or
        `~lsst.pex.config.RegistryField`.
        """
        taskField = getattr(self.config, name, None)
        if taskField is None:
            raise KeyError(f"{self.getFullName()}'s config does not have field {name!r}")
        subtask = taskField.apply(name=name, parentTask=self, **keyArgs)
        setattr(self, name, subtask)

    @contextlib.contextmanager
    def timer(self, name: str, logLevel: int = logging.DEBUG) -> Iterator[None]:
        """Context manager to log performance data for an arbitrary block of
        code.

        Parameters
        ----------
        name : `str`
            Name of code being timed; data will be logged using item name:
            ``Start`` and ``End``.
        logLevel
            A `logging` level constant.

        Examples
        --------
        Creating a timer context:

        .. code-block:: python

            with self.timer("someCodeToTime"):
                pass  # code to time

        See also
        --------
        timer.logInfo
        """
        logInfo(obj=self, prefix=name + "Start", logLevel=logLevel)
        try:
            yield
        finally:
            logInfo(obj=self, prefix=name + "End", logLevel=logLevel)

    @classmethod
    def makeField(cls, doc: str) -> ConfigurableField:
        """Make a `lsst.pex.config.ConfigurableField` for this task.

        Parameters
        ----------
        doc : `str`
            Help text for the field.

        Returns
        -------
        configurableField : `lsst.pex.config.ConfigurableField`
            A `~ConfigurableField` for this task.

        Examples
        --------
        Provides a convenient way to specify this task is a subtask of another
        task.

        Here is an example of use:

        .. code-block:: python

            class OtherTaskConfig(lsst.pex.config.Config):
                aSubtask = ATaskClass.makeField("brief description of task")
        """
        return ConfigurableField(doc=doc, target=cls)

    def _computeFullName(self, name: str) -> str:
        """Compute the full name of a subtask or metadata item, given its brief
        name.

        Parameters
        ----------
        name : `str`
            Brief name of subtask or metadata item.

        Returns
        -------
        fullName : `str`
            The full name: the ``name`` argument prefixed by the full task name
            and a period.

        Notes
        -----
        For example: if the full name of this task is "top.sub.sub2"
        then ``_computeFullName("subname")`` returns
        ``"top.sub.sub2.subname"``.
        """
        return f"{self._fullName}.{name}"

    @staticmethod
    def _unpickle_via_factory(
        factory: Callable[..., Task], args: Sequence[Any], kwargs: Dict[str, Any]
    ) -> Task:
        """Unpickle something by calling a factory

        Allows subclasses to unpickle using `__reduce__` with keyword
        arguments as well as positional arguments.
        """
        return factory(*args, **kwargs)

    def _reduce_kwargs(self) -> Dict[str, Any]:
        """Returns a dict of the keyword arguments that should be used
        by `__reduce__`.

        Subclasses with additional arguments should always call the parent
        class method to ensure that the standard parameters are included.

        Returns
        -------
        kwargs : `dict`
            Keyword arguments to be used when pickling.
        """
        return dict(
            config=self.config,
            name=self._name,
            parentTask=self._parentTask,
        )

    def __reduce__(
        self,
    ) -> Tuple[
        Callable[[Callable[..., Task], Sequence[Any], Dict[str, Any]], Task],
        Tuple[Type[Task], Sequence[Any], Dict[str, Any]],
    ]:
        """Pickler."""
        return self._unpickle_via_factory, (self.__class__, [], self._reduce_kwargs())
