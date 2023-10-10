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

__all__ = (
    "QuantumGraphExecutionReport",
    "TaskExecutionReport",
    "DatasetTypeExecutionReport",
    "lookup_quantum_data_id",
)

import dataclasses
import itertools
import logging
import uuid
from collections.abc import Iterable, Mapping
from typing import Any

import networkx
import yaml
from lsst.daf.butler import Butler, DataCoordinate, DatasetRef
from lsst.resources import ResourcePathExpression

from .graph import QuantumGraph, QuantumNode
from .pipeline import PipelineDatasetTypes


@dataclasses.dataclass
class DatasetTypeExecutionReport:
    """A report on the number of produced datasets as well as the status of
    missing datasets based on metadata.

    A `DatasetTypeExecutionReport` is created for each `DatasetType` in a
    `TaskExecutionReport`.
    """

    missing_failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets not produced because their quanta failed directly in this
    run (`set`).
    """

    missing_not_produced: dict[DatasetRef, bool] = dataclasses.field(default_factory=dict)
    """Missing datasets which were not produced due either missing inputs or a
    failure in finding inputs (`dict`).
        bool: were predicted inputs produced?
    """

    missing_upstream_failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets not produced due to an upstream failure (`set`).
    """

    n_produced: int = 0
    """Count of datasets produced (`int`).
    """

    def to_summary_dict(self) -> dict[str, Any]:
        """Summarize the DatasetTypeExecutionReport in a dictionary.

        Returns
        -------
        summary_dict : `dict`
            A count of the datasets with each outcome; the number of
            produced, `missing_failed`, `missing_not_produced`, and
            `missing_upstream_failed` `DatasetTypes`. See above for attribute
            descriptions.
        """
        return {
            "produced": self.n_produced,
            "missing_failed": len(self.missing_failed),
            "missing_not_produced": len(self.missing_not_produced),
            "missing_upstream_failed": len(self.missing_upstream_failed),
        }

    def handle_missing_dataset(
        self, output_ref: DatasetRef, failed: bool, status_graph: networkx.DiGraph
    ) -> None:
        """Sort missing datasets into outcomes.

        Parameters
        ----------
        output_ref : `~lsst.daf.butler.DatasetRef`
            Dataset reference of the missing dataset.
        failed : `bool`
            Whether the task associated with the missing dataset failed.
        status_graph : `networkx.DiGraph`
            The quantum graph produced by `TaskExecutionReport.inspect_quantum`
            which steps through the run quantum graph and logs the status of
            each quanta.
        """
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

    def handle_produced_dataset(self, output_ref: DatasetRef, status_graph: networkx.DiGraph) -> None:
        """Account for produced datasets.

        Parameters
        ----------
        output_ref : `~lsst.daf.butler.DatasetRef`
            Dataset reference of the dataset.
        status_graph : `networkx.DiGraph`
            The quantum graph produced by
            `QuantumGraphExecutionReport.make_reports` which steps through the
            quantum graph of a run and logs the status of each quantum.

        See Also
        --------
        TaskExecutionReport.inspect_quantum
        """
        status_graph.nodes[output_ref.id]["not_produced"] = False
        self.n_produced += 1


@dataclasses.dataclass
class TaskExecutionReport:
    """A report on the status and content of a task in an executed quantum
    graph.

    Use task metadata to identify and inspect failures and report on output
    datasets.

    See Also
    --------
    QuantumGraphExecutionReport
    DatasetTypeExecutionReport
    """

    failed: dict[uuid.UUID, DatasetRef] = dataclasses.field(default_factory=dict)
    """A mapping from quantum data ID to log dataset reference for quanta that
    failed directly in this run (`dict`).
    """

    failed_upstream: dict[uuid.UUID, DataCoordinate] = dataclasses.field(default_factory=dict)
    """A mapping of data IDs of quanta that were not attempted due to an
    upstream failure (`dict`).
    """

    output_datasets: dict[str, DatasetTypeExecutionReport] = dataclasses.field(default_factory=dict)
    """Missing and produced outputs of each `DatasetType` (`dict`).
    """

    def inspect_quantum(
        self,
        quantum_node: QuantumNode,
        status_graph: networkx.DiGraph,
        refs: Mapping[str, Mapping[uuid.UUID, DatasetRef]],
        metadata_name: str,
        log_name: str,
    ) -> None:
        """Inspect a quantum of a quantum graph and ascertain the status of
        each associated data product.

        Parameters
        ----------
        quantum_node : `QuantumNode`
            The specific node of the quantum graph to be inspected.
        status_graph : `networkx.DiGraph`
            The quantum graph produced by
            `QuantumGraphExecutionReport.make_reports` which steps through the
            quantum graph of a run and logs the status of each quantum.
        refs : `~collections.abc.Mapping` [ `str`,\
                `~collections.abc.Mapping` [ `uuid.UUID`,\
                `~lsst.daf.butler.DatasetRef` ] ]
            The DatasetRefs of each of the DatasetTypes produced by the task.
            Includes initialization, intermediate and output data products.
        metadata_name : `str`
            The metadata dataset name for the node.
        log_name : `str`
            The name of the log files for the node.

        See Also
        --------
        DatasetTypeExecutionReport.handle_missing_dataset
        DatasetTypeExecutionReport.handle_produced_dataset
        QuantumGraphExecutionReport.make_reports
        """
        quantum = quantum_node.quantum
        (metadata_ref,) = quantum.outputs[metadata_name]
        (log_ref,) = quantum.outputs[log_name]
        if metadata_ref.id not in refs[metadata_name]:
            if any(
                status_graph.nodes[upstream_quantum_id]["failed"]
                for upstream_dataset_id in status_graph.predecessors(quantum_node.nodeId)
                for upstream_quantum_id in status_graph.predecessors(upstream_dataset_id)
            ):
                assert quantum.dataId is not None
                self.failed_upstream[quantum_node.nodeId] = quantum.dataId
            else:
                self.failed[quantum_node.nodeId] = log_ref
                # note: log_ref may or may not actually exist
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

    def to_summary_dict(self, butler: Butler, logs: bool = True) -> dict[str, Any]:
        """Summarize the results of the TaskExecutionReport in a dictionary.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        logs : `bool`
            Store the logs in the summary dictionary.

        Returns
        -------
        summary_dict : `dict`
            A dictionary containing:

            - outputs: A dictionary summarizing the
              DatasetTypeExecutionReport for each DatasetType associated with
              the task
            - failed_quanta: A dictionary of quanta which failed and their
              dataIDs by quantum graph node id
            - n_quanta_blocked: The number of quanta which failed due to
              upstream failures.

        """
        failed_quanta = {}
        for node_id, log_ref in self.failed.items():
            quantum_info: dict[str, Any] = {"data_id": log_ref.dataId.byName()}
            if logs:
                try:
                    log = butler.get(log_ref)
                except LookupError:
                    quantum_info["error"] = []
                else:
                    quantum_info["error"] = [
                        record.message for record in log if record.levelno >= logging.ERROR
                    ]
            failed_quanta[str(node_id)] = quantum_info
        return {
            "outputs": {name: r.to_summary_dict() for name, r in self.output_datasets.items()},
            "failed_quanta": failed_quanta,
            "n_quanta_blocked": len(self.failed_upstream),
        }

    def __str__(self) -> str:
        """Return a count of the failed and failed_upstream tasks in the
        TaskExecutionReport.
        """
        return f"failed: {len(self.failed)}\nfailed upstream: {len(self.failed_upstream)}\n"


@dataclasses.dataclass
class QuantumGraphExecutionReport:
    """A report on the execution of a quantum graph.

    Report the detailed status of each failure; whether tasks were not run,
    data is missing from upstream failures, or specific errors occurred during
    task execution (and report the errors). Contains a count of expected,
    produced DatasetTypes for each task. This report can be output as a
    dictionary or a yaml file.

    Parameters
    ----------
    tasks : `dict`
        A dictionary of TaskExecutionReports by task label.

    See Also
    --------
    TaskExecutionReport
    DatasetTypeExecutionReport
    """

    tasks: dict[str, TaskExecutionReport] = dataclasses.field(default_factory=dict)
    """A dictionary of TaskExecutionReports by task label (`dict`).
    """

    def to_summary_dict(self, butler: Butler, logs: bool = True) -> dict[str, Any]:
        """Summarize the results of the `QuantumGraphExecutionReport` in a
        dictionary.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        logs : `bool`
            Store the logs in the summary dictionary.

        Returns
        -------
        summary_dict : `dict`
            A dictionary containing a summary of a `TaskExecutionReport` for
            each task in the quantum graph.
        """
        return {task: report.to_summary_dict(butler, logs=logs) for task, report in self.tasks.items()}

    def write_summary_yaml(self, butler: Butler, filename: str, logs: bool = True) -> None:
        """Take the dictionary from
        `QuantumGraphExecutionReport.to_summary_dict` and store its contents in
        a yaml file.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        filename : `str`
            The name to be used for the summary yaml file.
        logs : `bool`
            Store the logs in the summary dictionary.
        """
        with open(filename, "w") as stream:
            yaml.safe_dump(self.to_summary_dict(butler, logs=logs), stream)

    @classmethod
    def make_reports(
        cls,
        butler: Butler,
        graph: QuantumGraph | ResourcePathExpression,
    ) -> QuantumGraphExecutionReport:
        """Make a `QuantumGraphExecutionReport`.

        Step through the quantum graph associated with a run, creating a
        `networkx.DiGraph` called status_graph to annotate the status of each
        quantum node. For each task in the quantum graph, use
        `TaskExecutionReport.inspect_quantum` to make a `TaskExecutionReport`
        based on the status of each node. Return a `TaskExecutionReport` for
        each task in the quantum graph.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.
        graph : `QuantumGraph` | `ResourcePathExpression`
            Either the associated quantum graph object or the uri of the
            location of said quantum graph.

        Returns
        -------
        report: `QuantumGraphExecutionReport`
            The `TaskExecutionReport` for each task in the quantum graph.
        """
        refs = {}  # type: dict[str, Any]
        status_graph = networkx.DiGraph()
        if not isinstance(graph, QuantumGraph):
            qg = QuantumGraph.loadUri(graph)
        else:
            qg = graph
        assert qg.metadata is not None, "Saved QGs always have metadata."
        collection = qg.metadata["output_run"]
        report = cls()
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
        for task_def in qg.iterTaskGraph():
            for node in qg.getNodesForTask(task_def):
                status_graph.add_node(node.nodeId)
                for ref in itertools.chain.from_iterable(node.quantum.outputs.values()):
                    status_graph.add_edge(node.nodeId, ref.id)
                for ref in itertools.chain.from_iterable(node.quantum.inputs.values()):
                    status_graph.add_edge(ref.id, node.nodeId)

        for task_def in qg.iterTaskGraph():
            task_report = TaskExecutionReport()
            if task_def.logOutputDatasetName is None:
                raise RuntimeError("QG must have log outputs to use execution reports.")
            for node in qg.getNodesForTask(task_def):
                task_report.inspect_quantum(
                    node,
                    status_graph,
                    refs,
                    metadata_name=task_def.metadataDatasetName,
                    log_name=task_def.logOutputDatasetName,
                )
            report.tasks[task_def.label] = task_report
        return report

    def __str__(self) -> str:
        return "\n".join(f"{tasklabel}:{report}" for tasklabel, report in self.tasks.items())


def lookup_quantum_data_id(
    graph_uri: ResourcePathExpression, nodes: Iterable[uuid.UUID]
) -> list[DataCoordinate | None]:
    """Look up a dataId from a quantum graph and a list of quantum graph
    nodeIDs.

    Parameters
    ----------
    graph_uri : `ResourcePathExpression`
        URI of the quantum graph of the run.
    nodes : `~collections.abc.Iterable` [ `uuid.UUID` ]
        Quantum graph nodeID.

    Returns
    -------
    data_ids : `list` [ `lsst.daf.butler.DataCoordinate` ]
        A list of human-readable dataIDs which map to the nodeIDs on the
        quantum graph at graph_uri.
    """
    qg = QuantumGraph.loadUri(graph_uri, nodes=nodes)
    return [qg.getQuantumNodeByNodeId(node).quantum.dataId for node in nodes]
