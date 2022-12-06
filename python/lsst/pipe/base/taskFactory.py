# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module defining TaskFactory interface.
"""

from __future__ import annotations

__all__ = ["TaskFactory"]

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Optional, Type

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DatasetRef, LimitedButler

    from .config import PipelineTaskConfig
    from .configOverrides import ConfigOverrides
    from .pipeline import TaskDef
    from .pipelineTask import PipelineTask


class TaskFactory(metaclass=ABCMeta):
    """Abstract base class for task factory.

    Task factory is responsible for creating instances of PipelineTask
    subclasses.
    """

    @abstractmethod
    def makeTask(
        self,
        taskClass: Type[PipelineTask],
        name: Optional[str],
        config: Optional[PipelineTaskConfig],
        overrides: Optional[ConfigOverrides],
        butler: Optional[Butler],
    ) -> PipelineTask:
        """Create new PipelineTask instance from its class.

        Parameters
        ----------
        taskClass : `type`
            `PipelineTask` sub-class.
        name : `str` or `None`
            The name of the new task; if `None` then use
            ``taskClass._DefaultName``.
        config : `pex.Config` or `None`
            Configuration object, if `None` then use task-defined
            configuration class (``taskClass.ConfigClass``) to create new
            instance.
        overrides : `ConfigOverrides` or `None`
            Configuration overrides, this should contain all overrides to be
            applied to a default task config, including instrument-specific,
            obs-package specific, and possibly command-line overrides. This
            parameter is exclusive with ``config``, only one of the two can be
            specified as not-`None`.
        butler : `~lsst.daf.butler.Butler` or None
            Butler instance used to obtain initialization inputs for
            PipelineTasks. If `None`, some PipelineTasks will not be usable

        Returns
        -------
        task : `PipelineTask`
            Instance of a `PipelineTask` class.

        Raises
        ------
        Any exceptions that are raised by PipelineTask constructor or its
        configuration class are propagated back to caller.
        """
        raise NotImplementedError()

    @abstractmethod
    def makeTaskFromLimited(
        self, taskDef: TaskDef, butler: LimitedButler, initInputRefs: list[DatasetRef] | None
    ) -> PipelineTask:
        """Create new PipelineTask instance using limited Butler.

        Parameters
        ----------
        taskDef : `~lsst.pipe.base.TaskDef`
            Task definition structure.
        butler : `lsst.daf.butler.LimitedButler`
            Butler instance used to obtain initialization inputs for task.
        initInputRefs : `list` of `~lsst.daf.butler.DatasetRef` or None
            List of resolved dataset references for init inputs for this task.

        Returns
        -------
        task : `PipelineTask`
            Instance of a PipelineTask class.

        Raises
        ------
        Any exceptions that are raised by PipelineTask constructor or its
        configuration class are propagated back to caller.

        Notes
        -----
        Compared to the `makeTask` method this one works with `LimitedButler`.
        """
        raise NotImplementedError()
