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
from collections.abc import Iterable
from typing import Any

import networkx
import yaml
from lsst.daf.butler import Butler, DataCoordinate, DatasetRef
from lsst.resources import ResourcePath, ResourcePathExpression

from .graph import QuantumGraph
from .pipeline import PipelineDatasetTypes


@dataclasses.dataclass
class DatasetTypeExecutionReport:
    missing_failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets not produced because their quanta failed directly in this run.
    """
    missing_not_produced: dict[DatasetRef, bool] = dataclasses.field(default_factory=dict)
    """Datasets not produced because their inputs were not produced or not
    found
    """
    # bool: predicted inputs to this task were not produced
    missing_upstream_failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets not produced due to an upstream failure
    """
    n_produced: int = 0
    """Counts of datasets produced by this run.
    """

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "produced": self.n_produced,
            "missing_failed": len(self.missing_failed),
            "missing_not_produced": len(self.missing_not_produced),
            "missing_upstream_failed": len(self.missing_upstream_failed),
        }

    def handle_missing_dataset(self, output_ref: DatasetRef, failed: bool, status_graph: networkx.DiGraph):
        if failed:
            for upstream_quantum_id in status_graph.predecessors(output_ref.id):
                if status_graph.nodes[upstream_quantum_id]["failed"]:
                    self.missing_upstream_failed.add(output_ref)
                    break
            else:
                self.missing_failed.add(output_ref)
        else:
            status_graph.nodes[output_ref.id]["not_produced"] = True
            self.missing_not_produced[output_ref] = any(
                status_graph.nodes[upstream_dataset_id].get("not_produced", False)
                for upstream_quantum_id in status_graph.predecessors(output_ref.id)
                for upstream_dataset_id in status_graph.predecessors(upstream_quantum_id)
            )

    def handle_produced_dataset(self, output_ref: DatasetRef, status_graph: networkx.DiGraph):
        status_graph.nodes[output_ref.id]["not_produced"] = False
        self.n_produced += 1


@dataclasses.dataclass
class TaskExecutionReport:
    failed: dict[uuid.UUID, DatasetRef] = dataclasses.field(default_factory=dict)
    """A mapping from quantum data ID to log dataset reference for quanta that
    failed directly in this run.
    """
    failed_upstream: dict[uuid.UUID, DataCoordinate] = dataclasses.field(default_factory=dict)
    """A mapping of data IDs of quanta that were not attempted due to an
    upstream failure.
    """
    output_datasets: dict[str, DatasetTypeExecutionReport] = dataclasses.field(default_factory=dict)
    """Reports of the missing and produced outputs of each DatasetType
    """

    def inspect_quantum(self, quantum_node, status_graph: networkx.DiGraph, refs, metadata_name, log_name):
        quantum = quantum_node.quantum
        (metadata_ref,) = quantum.outputs[metadata_name]
        (log_ref,) = quantum.outputs[log_name]
        if metadata_ref.id not in refs[metadata_name]:
            if any(
                status_graph.nodes[upstream_quantum_id]["failed"]
                for upstream_dataset_id in status_graph.predecessors(quantum_node.nodeId)
                for upstream_quantum_id in status_graph.predecessors(upstream_dataset_id)
            ):
                self.failed_upstream[quantum_node.nodeId] = quantum.dataId
            else:
                self.failed[quantum_node.nodeId] = log_ref
                # note to self: log_ref may or may not actually exist
            failed = True
        else:
            failed = False
        status_graph.nodes[quantum_node.nodeId]["failed"] = failed
        for output_ref in itertools.chain.from_iterable(quantum.outputs.values()):
            if (dataset_type_report := self.output_datasets.get(output_ref.datasetType.name)) is None:
                dataset_type_report = DatasetTypeExecutionReport()
                self.output_datasets[output_ref.datasetType.name] = dataset_type_report
            if output_ref.id not in refs[output_ref.datasetType.name]:
                dataset_type_report.handle_missing_dataset(output_ref, failed, status_graph)
            else:
                dataset_type_report.handle_produced_dataset(output_ref, status_graph)

    def to_summary_dict(self, butler: Butler) -> dict[str, Any]:
        return {
            "outputs": {name: r.to_summary_dict() for name, r in self.output_datasets.items()},
            "failed_quanta": {
                str(node_id): {"data_id": log_ref.dataId.byName(), "log_uri": str(butler.getURI(log_ref))}
                for node_id, log_ref in self.failed.items()
            },
            "n_quanta_blocked": len(self.failed_upstream),
        }

    def __str__(self) -> str:
        return f"failed: {len(self.failed)}\nfailed upstream: {len(self.failed_upstream)}\n"


@dataclasses.dataclass
class QuantumGraphExecutionReport:
    uri: ResourcePath
    tasks: dict[str, TaskExecutionReport] = dataclasses.field(default_factory=dict)

    def to_summary_dict(self, butler: Butler) -> dict[str, Any]:
        return {task: report.to_summary_dict(butler) for task, report in self.tasks.items()}

    def write_summary_yaml(self, butler: Butler, filename: str) -> None:
        with open(filename, "w") as stream:
            yaml.dump(self.to_summary_dict(butler), stream)

    @classmethod
    def make_reports(
        cls,
        butler: Butler,
        graph_uri: ResourcePathExpression,
    ) -> QuantumGraphExecutionReport:
        refs = {}
        status_graph = networkx.DiGraph()
        graph_uri = ResourcePath(graph_uri)
        qg = QuantumGraph.loadUri(graph_uri)
        collection = qg.metadata["output_run"]
        report = cls(graph_uri)
        task_defs = list(qg.iterTaskGraph())
        pipeline_dataset_types = PipelineDatasetTypes.fromPipeline(task_defs, registry=butler.registry)
        for dataset_type in itertools.chain(
            pipeline_dataset_types.initIntermediates,
            pipeline_dataset_types.initOutputs,
            pipeline_dataset_types.intermediates,
            pipeline_dataset_types.outputs,
        ):
            refs[dataset_type.name] = {
                ref.id: ref
                for ref in butler.registry.queryDatasets(
                    dataset_type.name, collections=collection, findFirst=False
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
                    node,
                    status_graph,
                    refs,
                    metadata_name=taskDef.metadataDatasetName,
                    log_name=taskDef.logOutputDatasetName,
                )
            report.tasks[taskDef.label] = task_report
        return report

    def __str__(self) -> str:
        return "\n".join(f"{tasklabel}:{report}" for tasklabel, report in self.tasks.items())


def lookup_quantum_dataId(graph_uri: ResourcePathExpression, nodes: Iterable[uuid.UUID]):
    qg = QuantumGraph.loadUri(graph_uri, nodes=nodes)
    return [qg.getQuantumNodeByNodeId(node).quantum.dataId for node in nodes]
