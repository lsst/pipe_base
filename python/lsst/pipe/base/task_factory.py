# This file is part of ctrl_mpexec.
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

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from lsst.pipe.base import TaskFactory as BaseTaskFactory
from lsst.pipe.base.pipeline_graph import TaskNode

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef, LimitedButler
    from lsst.pipe.base import PipelineTask

_LOG = logging.getLogger(__name__)


class TaskFactory(BaseTaskFactory):
    """Class instantiating PipelineTasks."""

    def makeTask(
        self,
        task_node: TaskNode,
        /,
        butler: LimitedButler,
        initInputRefs: Iterable[DatasetRef] | None,
    ) -> PipelineTask:
        # docstring inherited
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
        # make task instance
        task = task_class(config=config, initInputs=init_inputs, name=task_node.label)
        return task
