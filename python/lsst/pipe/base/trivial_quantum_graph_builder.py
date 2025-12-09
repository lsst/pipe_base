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

"""The standard, general-purpose implementation of the QuantumGraph-generation
algorithm.
"""

from __future__ import annotations

__all__ = "TrivialQuantumGraphBuilder"

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, final, Mapping

from lsst.daf.butler import (
    Butler,
)
from lsst.utils.timer import timeMethod

from .quantum_graph_builder import QuantumGraphBuilder
from .quantum_graph_skeleton import QuantumGraphSkeleton

if TYPE_CHECKING:
    from lsst.daf.butler import DataCoordinate, DatasetRef

    from .pipeline_graph import PipelineGraph


@final
class TrivialQuantumGraphBuilder(QuantumGraphBuilder):
    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        butler: Butler,
        *,
        data_ids: Iterable[DataCoordinate],
        input_refs: Mapping[str, list[DatasetRef]],
        **kwargs: Any,
    ):
        super().__init__(pipeline_graph, butler, **kwargs)
        self.data_ids = {d.dimensions: d for d in data_ids}
        self.data_ids[self.empty_data_id.dimensions] = self.empty_data_id
        self.input_refs = input_refs

    @timeMethod
    def process_subgraph(self, subgraph: PipelineGraph) -> QuantumGraphSkeleton:
        skeleton = QuantumGraphSkeleton(subgraph.tasks)
        for task_node in subgraph.tasks.values():
            quantum_key = skeleton.add_quantum_node(task_node.label, self.data_ids[task_node.dimensions])
            for read_edge in task_node.iter_all_inputs():
                if (input_refs := self.input_refs.get(read_edge.parent_dataset_type_name)) is not None:
                    for input_ref in input_refs:
                        if read_edge.is_prerequisite:
                            prereq_key = skeleton.add_prerequisite_node(input_ref)
                            skeleton.add_input_edge(quantum_key, prereq_key)
                        else:
                            input_key = skeleton.add_input_node(input_ref)
                            skeleton.add_input_edge(quantum_key, input_key)

                if read_edge.is_prerequisite:
                    continue
                dataset_type_node = subgraph.dataset_types[read_edge.parent_dataset_type_name]
                input_key = skeleton.add_dataset_node(
                    read_edge.parent_dataset_type_name, self.data_ids[dataset_type_node.dimensions]
                )
                skeleton.add_input_edge(quantum_key, input_key)

            for write_edge in task_node.iter_all_outputs():
                dataset_type_node = subgraph.dataset_types[write_edge.parent_dataset_type_name]
                input_key = skeleton.add_dataset_node(
                    write_edge.parent_dataset_type_name, self.data_ids[dataset_type_node.dimensions]
                )
                skeleton.add_output_edge(quantum_key, input_key)
