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
from __future__ import annotations

import dataclasses
import itertools
import uuid
from collections.abc import Iterable, Iterator

import networkx
from lsst.daf.butler import Butler, DataCoordinate, DatasetRef
from lsst.resources import ResourcePath, ResourcePathExpression

from .graph import QuantumGraph
from .pipeline import PipelineDatasetTypes


@dataclasses.dataclass
class TaskExecutionReport:
    failed: set[DataCoordinate] = dataclasses.field(default_factory=set)
    failed_upstream: set[DataCoordinate] = dataclasses.field(default_factory=set)
    missing_datasets_failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    missing_datasets_not_produced: dict[DatasetRef, bool] = dataclasses.field(default_factory=dict)
    # bool: predicted inputs to this task were not produced
    missing_upstream_failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    datasets_produced: dict[str, int] = dataclasses.field(default_factory=dict)

    def inspect_quantum(self, quantum_node, status_graph: networkx.DiGraph, refs, metadata_name):
        quantum = quantum_node.quantum
        (ref,) = quantum.outputs[metadata_name]
        if ref.id not in refs[metadata_name]:
            if any(
                status_graph.nodes[upstream_quantum_id]["failed"]
                for upstream_dataset_id in status_graph.predecessors(quantum_node.nodeId)
                for upstream_quantum_id in status_graph.predecessors(upstream_dataset_id)
            ):
                self.failed_upstream.add(quantum.dataId)
            else:
                self.failed.add(quantum.dataId)
            failed = True
        else:
            failed = False
        status_graph.nodes[quantum_node.nodeId]["failed"] = failed
        for ref in itertools.chain.from_iterable(quantum.outputs.values()):
            if ref.id not in refs[ref.datasetType.name]:
                if failed:
                    for upstream_quantum_id in status_graph.predecessors(ref.id):
                        if status_graph.nodes[upstream_quantum_id]["failed"]:
                            self.missing_upstream_failed.add(ref)
                            break
                    else:
                        self.missing_datasets_failed.add(ref)
                else:
                    status_graph.nodes[ref.id]["not_produced"] = True
                    self.missing_datasets_not_produced[ref] = any(
                        status_graph.nodes[upstream_dataset_id].get("not_produced", False)
                        for upstream_quantum_id in status_graph.predecessors(ref.id)
                        for upstream_dataset_id in status_graph.predecessors(upstream_quantum_id)
                    )

            else:
                status_graph.nodes[ref.id]["not_produced"] = False
                self.datasets_produced[ref.datasetType.name] = (
                    self.datasets_produced.setdefault(ref.datasetType.name, 0) + 1
                )

    def __str__(self) -> str:
        return f"failed: {len(self.failed)}\nfailed upstream: {len(self.failed_upstream)}\n"


@dataclasses.dataclass
class QuantumGraphExecutionReport:
    uri: ResourcePath
    tasks: dict[str, TaskExecutionReport] = dataclasses.field(default_factory=dict)

    @classmethod
    def make_reports(
        cls,
        butler: Butler,
        graph_uris: Iterable[ResourcePathExpression],
        collections: Iterable[str] | None = None,
    ) -> Iterator[QuantumGraphExecutionReport]:
        pipeline_dataset_types = None
        refs = {}
        status_graph = networkx.DiGraph()
        for graph_uri in graph_uris:
            graph_uri = ResourcePath(graph_uri)
            qg = QuantumGraph.loadUri(graph_uri)
            report = cls(graph_uri)
            if pipeline_dataset_types is None:
                task_defs = list(qg.iterTaskGraph())
                pipeline_dataset_types = PipelineDatasetTypes.fromPipeline(
                    task_defs, registry=butler.registry
                )
                for dataset_type in itertools.chain(
                    pipeline_dataset_types.initIntermediates,
                    pipeline_dataset_types.initOutputs,
                    pipeline_dataset_types.intermediates,
                    pipeline_dataset_types.outputs,
                ):
                    refs[dataset_type.name] = {
                        ref.id: ref
                        for ref in butler.registry.queryDatasets(
                            dataset_type.name, collections=collections, findFirst=False
                        )
                    }
            for taskDef in qg.iterTaskGraph():
                for node in qg.getNodesForTask(taskDef):
                    status_graph.add_node(node.nodeId)
                    for ref in itertools.chain.from_iterable(node.quantum.outputs.values()):
                        status_graph.add_edge(node.nodeId, ref.id)
                    for ref in itertools.chain.from_iterable(node.quantum.inputs.values()):
                        status_graph.add_edge(ref.id, node.nodeId)

            for taskDef in qg.iterTaskGraph():
                task_report = TaskExecutionReport()
                for node in qg.getNodesForTask(taskDef):
                    task_report.inspect_quantum(
                        node, status_graph, refs, metadata_name=taskDef.metadataDatasetName
                    )
                report.tasks[taskDef.label] = task_report
            yield report

    def __str__(self) -> str:
        return "\n".join(f"{tasklabel}:{report}" for tasklabel, report in self.tasks.items())


def lookup_quantum_dataId(graph_uri: ResourcePathExpression, nodes: Iterable[uuid.UUID]):
    qg = QuantumGraph.loadUri(graph_uri, nodes=nodes)
    return [qg.getQuantumNodeByNodeId(node).quantum.dataId for node in nodes]
