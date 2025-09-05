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

__all__ = ()

import dataclasses
import uuid
from collections.abc import Iterable

import networkx

from .._common import DatasetTypeName
from ._worker import ScannerWorker


@dataclasses.dataclass
class DeletionTracker:
    xgraph: networkx.DiGraph
    dataset_types: set[DatasetTypeName]

    @classmethod
    def load(
        cls,
        worker: ScannerWorker,
        dataset_types: Iterable[DatasetTypeName] = (),
        all_metadata: bool = True,
        all_log: bool = True,
    ) -> DeletionTracker:
        dataset_types = set(dataset_types)
        pipeline_graph = worker.reader.components.pipeline_graph
        for task_label, n_quanta in worker.reader.components.header.n_task_quanta.items():
            if not n_quanta:
                continue
            task_node = pipeline_graph.tasks[task_label]
            if all_metadata:
                dataset_types.add(task_node.metadata_output.dataset_type_name)
            if all_log and task_node.log_output is not None:
                dataset_types.add(task_node.log_output.dataset_type_name)
        worker.reader.read_thin_graph()
        uuid_by_index = {
            quantum_index: quantum_id
            for quantum_id, quantum_index in worker.reader.components.quantum_indices.items()
        }
        xgraph = networkx.DiGraph()
        for dataset_type_name in dataset_types:
            for read_edge in pipeline_graph.consuming_edges_of(dataset_type_name):
                consuming_quanta = {
                    uuid_by_index[thin_quantum.quantum_index]
                    for thin_quantum in worker.reader.components.thin_graph.quanta[read_edge.task_label]
                }
                worker.reader.read_quantum_datasets(consuming_quanta)
                for quantum_id in consuming_quanta:
                    predicted_quantum_datasets = worker.reader.components.quantum_datasets[quantum_id]
                    for predicted_dataset in predicted_quantum_datasets.inputs[read_edge.connection_name]:
                        xgraph.add_node(
                            predicted_dataset.dataset_id,
                            json_ref=worker.make_ref(predicted_dataset).to_json().encode(),
                        )
                        xgraph.add_edge(predicted_dataset.dataset_id, quantum_id)
        return cls(xgraph, dataset_types)

    def mark_dataset_complete(
        self, dataset_type_name: str, dataset_id: uuid.UUID, json_ref: bytes
    ) -> dict[uuid.UUID, bytes]:
        safe_to_delete: dict[uuid.UUID, bytes] = {}
        if dataset_type_name in self.dataset_types and dataset_id not in self.xgraph:
            safe_to_delete[dataset_id] = json_ref
        return safe_to_delete

    def mark_quantum_complete(self, quantum_id: uuid.UUID) -> dict[uuid.UUID, bytes]:
        safe_to_delete: dict[uuid.UUID, bytes] = {}
        if quantum_id not in self.xgraph:
            return safe_to_delete
        consumed_dataset_ids = list(self.xgraph.predecessors(quantum_id))
        self.xgraph.remove_node(quantum_id)
        for dataset_id in consumed_dataset_ids:
            if not self.xgraph.out_degree(dataset_id):
                safe_to_delete[dataset_id] = self.xgraph.nodes[dataset_id]["json_ref"]
        return safe_to_delete
