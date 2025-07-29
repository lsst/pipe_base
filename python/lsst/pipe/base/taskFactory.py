# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

from __future__ import annotations

__all__ = ["TaskFactory"]

from abc import ABCMeta
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef, LimitedButler

    from .pipeline_graph import TaskNode
    from .pipelineTask import PipelineTask


class TaskFactory(metaclass=ABCMeta):
    """A helper class for creating instances of PipelineTask subclasses."""

    def makeTask(
        self,
        task_node: TaskNode,
        /,
        butler: LimitedButler,
        initInputRefs: Iterable[DatasetRef] | None,
    ) -> PipelineTask:
        """Create new PipelineTask instance from its
        `~.pipeline_graph.TaskNode`.

        Parameters
        ----------
        task_node : `~pipeline_graph.TaskNode`
            Task definition structure.
        butler : `lsst.daf.butler.LimitedButler`
            Butler instance used to obtain initialization inputs for task.
        initInputRefs : `~collections.abc.Iterable` of \
                `~lsst.daf.butler.DatasetRef` or `None`
            List of resolved dataset references for init inputs for this task.

        Returns
        -------
        task : `PipelineTask`
            Instance of a PipelineTask class.

        Raises
        ------
        Any exceptions that are raised by PipelineTask constructor or its
        configuration class are propagated back to caller.
        """
        config = task_node.config
        init_inputs: dict[str, Any] = {}
        init_input_refs_by_dataset_type = {}
        if initInputRefs is not None:
            init_input_refs_by_dataset_type = {ref.datasetType.name: ref for ref in initInputRefs}
        task_class = task_node.task_class
        if init_input_refs_by_dataset_type:
            for read_edge in task_node.init.inputs.values():
                init_inputs[read_edge.connection_name] = butler.get(
                    init_input_refs_by_dataset_type[read_edge.dataset_type_name]
                )
        task = task_class(config=config, initInputs=init_inputs, name=task_node.label)
        return task
