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

__all__ = "TrivialQuantumGraphBuilder"

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, final

from lsst.daf.butler import Butler, DataCoordinate, DatasetIdGenEnum, DatasetRef, DimensionGroup
from lsst.utils.timer import timeMethod

from .quantum_graph_builder import QuantumGraphBuilder
from .quantum_graph_skeleton import QuantumGraphSkeleton

if TYPE_CHECKING:
    from .pipeline_graph import PipelineGraph


@final
class TrivialQuantumGraphBuilder(QuantumGraphBuilder):
    """An optimized quantum-graph builder for pipelines that operate on only
    a single data ID or a closely related set of data IDs.

    Parameters
    ----------
    pipeline_graph
        Pipeline to build a quantum graph from, as a graph.  Will be resolved
        in-place with the given butler (any existing resolution is ignored).
    butler
        Client for the data repository.  Should be read-only.
    data_ids
        Mapping from dimension group to the data ID to use for that dimension
        group.  This is intended to allow the pipeline to switch between
        effectively-equivalent dimensions (e.g. ``group``, ``visit``
        ``exposure``).
    input_refs
        References for input datasets, keyed by task label and then connection
        name.  This should include all regular overall-input datasets whose
        data IDs are not included in ``data_ids``.  It may (but need not)
        include prerequisite inputs.  Existing intermediate datasets should
        also be provided when they need to be clobbered or used in skip logic.
    dataset_id_modes
        Mapping from dataset type name to the ID generation mode for that
        dataset type.  They default is to generate random UUIDs.
    **kwargs
        Forwarded to the base `.quantum_graph_builder.QuantumGraphBuilder`.

    Notes
    -----
    If ``dataset_id_modes`` is provided, ``clobber=True`` will be passed to
    the base builder's constructor, as is this is necessary to avoid spurious
    errors about the affected datasets already existing.  The only effect of
    this to silence *other* errors about datasets in the output run existing
    unexpectedly.
    """

    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        butler: Butler,
        *,
        data_ids: Mapping[DimensionGroup, DataCoordinate],
        input_refs: Mapping[str, Mapping[str, Sequence[DatasetRef]]] | None = None,
        dataset_id_modes: Mapping[str, DatasetIdGenEnum] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(pipeline_graph, butler, **kwargs)
        if dataset_id_modes:
            self.clobber = True
        self.data_ids = dict(data_ids)
        self.data_ids[self.empty_data_id.dimensions] = self.empty_data_id
        self.input_refs = input_refs or {}
        self.dataset_id_modes = dataset_id_modes or {}

    def _get_data_id(self, dimensions: DimensionGroup, context: str) -> DataCoordinate:
        try:
            return self.data_ids[dimensions]
        except KeyError as e:
            e.add_note(context)
            raise

    @timeMethod
    def process_subgraph(self, subgraph: PipelineGraph) -> QuantumGraphSkeleton:
        skeleton = QuantumGraphSkeleton(subgraph.tasks)
        for task_node in subgraph.tasks.values():
            quantum_key = skeleton.add_quantum_node(
                task_node.label, self._get_data_id(task_node.dimensions, context=f"task {task_node.label!r}")
            )
            input_refs_for_task = self.input_refs.get(task_node.label, {})

            for read_edge in task_node.iter_all_inputs():
                if (input_refs := input_refs_for_task.get(read_edge.connection_name)) is not None:
                    for input_ref in input_refs:
                        if read_edge.is_prerequisite:
                            prereq_key = skeleton.add_prerequisite_node(input_ref)
                            skeleton.add_input_edge(quantum_key, prereq_key)
                            self.log.info(
                                f"Added prereq {task_node.label}.{read_edge.connection_name} "
                                f"for {input_ref.dataId} from input_refs"
                            )
                        else:
                            input_key = skeleton.add_dataset_node(
                                read_edge.parent_dataset_type_name,
                                input_ref.dataId,
                                ref=input_ref,
                            )
                            skeleton.add_input_edge(quantum_key, input_key)
                            self.log.info(
                                f"Added regular input {task_node.label}.{read_edge.connection_name} "
                                f"for {input_ref.dataId} from input_refs"
                            )

                if read_edge.is_prerequisite:
                    continue
                dataset_type_node = subgraph.dataset_types[read_edge.parent_dataset_type_name]
                data_id = self._get_data_id(
                    dataset_type_node.dimensions,
                    context=f"input {task_node.label}.{read_edge.connection_name}",
                )
                input_key = skeleton.add_dataset_node(
                    read_edge.parent_dataset_type_name,
                    data_id,
                )
                skeleton.add_input_edge(quantum_key, input_key)
                if subgraph.producer_of(read_edge.parent_dataset_type_name) is None:
                    if skeleton.get_dataset_ref(input_key) is None:
                        ref = self.butler.find_dataset(dataset_type_node.dataset_type, data_id)
                        if ref is not None:
                            skeleton.set_dataset_ref(ref)
                self.log.info(
                    f"Added regular input {task_node.label}.{read_edge.connection_name} for {data_id}"
                )

            for write_edge in task_node.iter_all_outputs():
                dataset_type_node = subgraph.dataset_types[write_edge.parent_dataset_type_name]
                data_id = self._get_data_id(
                    dataset_type_node.dimensions,
                    context=f"output {task_node.label}.{write_edge.connection_name}",
                )
                output_key = skeleton.add_dataset_node(write_edge.parent_dataset_type_name, data_id)
                skeleton.add_output_edge(quantum_key, output_key)
                self.log.info(f"Added output {task_node.label}.{write_edge.connection_name} for {data_id}")
                if mode := self.dataset_id_modes.get(write_edge.parent_dataset_type_name):
                    ref = DatasetRef(
                        dataset_type_node.dataset_type,
                        data_id,
                        run=self.output_run,
                        id_generation_mode=mode,
                    )
                    skeleton.set_dataset_ref(ref)
                    skeleton.set_output_in_the_way(ref)
                    self.log.info(
                        f"Added ref for output {task_node.label}.{write_edge.connection_name} for "
                        f"{data_id} with {mode=}"
                    )

        return skeleton
