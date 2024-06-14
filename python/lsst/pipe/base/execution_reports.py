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
from lsst.daf.butler import Butler, DataCoordinate, DatasetRef, Quantum
from lsst.resources import ResourcePathExpression

from .graph import QuantumGraph


@dataclasses.dataclass
class DatasetTypeExecutionReport:
    """A report on the number of produced datasets as well as the status of
    missing datasets based on metadata.

    A `DatasetTypeExecutionReport` is created for each
    `~lsst.daf.butler.DatasetType` in a `TaskExecutionReport`.
    """

    failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets not produced because their quanta failed directly in this
    run (`set`).
    """

    not_produced: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Missing datasets which were not produced by successful quanta.
    """

    blocked: set[DatasetRef] = dataclasses.field(default_factory=set)
    """Datasets not produced due to an upstream failure (`set`).
    """

    n_produced: int = 0
    """Count of datasets produced (`int`).
    """

    n_expected: int = 0
    """Count of datasets expected (`int`)
    """

    def to_summary_dict(self) -> dict[str, Any]:
        r"""Summarize the DatasetTypeExecutionReport in a dictionary.

        Returns
        -------
        summary_dict : `dict`
            A count of the datasets with each outcome; the number of
            produced, ``failed``, ``not_produced``, and ``blocked``
            `~lsst.daf.butler.DatasetType`\ s.
            See above for attribute descriptions.
        """
        return {
            "produced": self.n_produced,
            "failed": len(self.failed),
            "not_produced": len(self.not_produced),
            "blocked": len(self.blocked),
            "expected": self.n_expected,
        }


@dataclasses.dataclass
class TaskExecutionReport:
    """A report on the status and content of a task in an executed quantum
    graph.

    Use task metadata to identify and inspect failures and report on output
    datasets.

    See Also
    --------
    QuantumGraphExecutionReport : Quantum graph report.
    DatasetTypeExecutionReport : DatasetType report.
    """

    failed: dict[uuid.UUID, DatasetRef] = dataclasses.field(default_factory=dict)
    """A mapping from quantum data ID to log dataset reference for quanta that
    failed directly in this run (`dict`).
    """

    n_succeeded: int = 0
    """A count of successful quanta.

    This may include quanta that did not produce any datasets; ie, raised
    `NoWorkFound`.
    """

    n_expected: int = 0
    """A count of expected quanta.
    """

    blocked: dict[uuid.UUID, DataCoordinate] = dataclasses.field(default_factory=dict)
    """A mapping of data IDs of quanta that were not attempted due to an
    upstream failure (`dict`).
    """

    output_datasets: dict[str, DatasetTypeExecutionReport] = dataclasses.field(default_factory=dict)
    """Missing and produced outputs of each `~lsst.daf.butler.DatasetType`
    (`dict`).
    """

    def inspect_quantum(
        self,
        quantum_id: uuid.UUID,
        quantum: Quantum,
        status_graph: networkx.DiGraph,
        refs: Mapping[str, Mapping[uuid.UUID, DatasetRef]],
        metadata_name: str,
        log_name: str,
    ) -> None:
        """Inspect a quantum of a quantum graph and ascertain the status of
        each associated data product.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            Unique identifier for the quantum to inspect.
        quantum : `Quantum`
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
        QuantumGraphExecutionReport.make_reports : Make reports.
        """
        (metadata_ref,) = quantum.outputs[metadata_name]
        (log_ref,) = quantum.outputs[log_name]
        blocked = False
        if metadata_ref.id not in refs[metadata_name]:
            if any(
                status_graph.nodes[upstream_quantum_id]["failed"]
                for upstream_dataset_id in status_graph.predecessors(quantum_id)
                for upstream_quantum_id in status_graph.predecessors(upstream_dataset_id)
            ):
                assert quantum.dataId is not None
                self.blocked[quantum_id] = quantum.dataId
                blocked = True
            else:
                self.failed[quantum_id] = log_ref
                # note: log_ref may or may not actually exist
            failed = True
        else:
            failed = False
            self.n_succeeded += 1
        status_graph.nodes[quantum_id]["failed"] = failed

        # Now, loop over the datasets to make a DatasetTypeExecutionReport.
        for output_ref in itertools.chain.from_iterable(quantum.outputs.values()):
            if output_ref == metadata_ref or output_ref == log_ref:
                continue
            if (dataset_type_report := self.output_datasets.get(output_ref.datasetType.name)) is None:
                dataset_type_report = DatasetTypeExecutionReport()
                self.output_datasets[output_ref.datasetType.name] = dataset_type_report
            if output_ref.id not in refs[output_ref.datasetType.name]:
                if failed:
                    if blocked:
                        dataset_type_report.blocked.add(output_ref)
                    else:
                        dataset_type_report.failed.add(output_ref)
                else:
                    dataset_type_report.not_produced.add(output_ref)
            else:
                dataset_type_report.n_produced += 1
            dataset_type_report.n_expected += 1

    def to_summary_dict(
        self, butler: Butler, do_store_logs: bool = True, human_readable: bool = False
    ) -> dict[str, Any]:
        """Summarize the results of the TaskExecutionReport in a dictionary.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        do_store_logs : `bool`
            Store the logs in the summary dictionary.
        human_readable : `bool`
            Store more human-readable information to be printed out to the
            command-line.

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
            - n_succeded: The number of quanta which succeeded.

            And possibly, if human-readable is passed:

            - errors: A dictionary of data ids associated with each error
              message. If `human-readable` and `do_store_logs`, this is stored
              here. Otherwise, if `do_store_logs`, it is stored in
              `failed_quanta` keyed by the quantum graph node id.
        """
        failed_quanta = {}
        failed_data_ids = []
        errors = []
        for node_id, log_ref in self.failed.items():
            data_id = dict(log_ref.dataId.required)
            quantum_info: dict[str, Any] = {"data_id": data_id}
            if do_store_logs:
                try:
                    log = butler.get(log_ref)
                except LookupError:
                    quantum_info["error"] = []
                except FileNotFoundError:
                    quantum_info["error"] = None
                else:
                    quantum_info["error"] = [
                        record.message for record in log if record.levelno >= logging.ERROR
                    ]
            if human_readable:
                failed_data_ids.append(data_id)
                if do_store_logs:
                    errors.append(quantum_info)

            else:
                failed_quanta[str(node_id)] = quantum_info
        result = {
            "outputs": {name: r.to_summary_dict() for name, r in self.output_datasets.items()},
            "n_quanta_blocked": len(self.blocked),
            "n_succeeded": self.n_succeeded,
            "n_expected": self.n_expected,
        }
        if human_readable:
            result["failed_quanta"] = failed_data_ids
            result["errors"] = errors
        else:
            result["failed_quanta"] = failed_quanta
        return result

    def __str__(self) -> str:
        """Return a count of the failed and blocked tasks in the
        TaskExecutionReport.
        """
        return f"failed: {len(self.failed)}\nblocked: {len(self.blocked)}\n"


@dataclasses.dataclass
class QuantumGraphExecutionReport:
    """A report on the execution of a quantum graph.

    Report the detailed status of each failure; whether tasks were not run,
    data is missing from upstream failures, or specific errors occurred during
    task execution (and report the errors). Contains a count of expected,
    produced DatasetTypes for each task. This report can be output as a
    dictionary or a yaml file.

    Attributes
    ----------
    tasks : `dict`
        A dictionary of TaskExecutionReports by task label.

    See Also
    --------
    TaskExecutionReport : A task report.
    DatasetTypeExecutionReport : A dataset type report.
    """

    tasks: dict[str, TaskExecutionReport] = dataclasses.field(default_factory=dict)
    """A dictionary of TaskExecutionReports by task label (`dict`)."""

    def to_summary_dict(
        self, butler: Butler, do_store_logs: bool = True, human_readable: bool = False
    ) -> dict[str, Any]:
        """Summarize the results of the `QuantumGraphExecutionReport` in a
        dictionary.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        do_store_logs : `bool`
            Store the logs in the summary dictionary.
        human_readable : `bool`
            Store more human-readable information to be printed out to the
            command-line.

        Returns
        -------
        summary_dict : `dict`
            A dictionary containing a summary of a `TaskExecutionReport` for
            each task in the quantum graph.
        """
        return {
            task: report.to_summary_dict(butler, do_store_logs=do_store_logs, human_readable=human_readable)
            for task, report in self.tasks.items()
        }

    def write_summary_yaml(self, butler: Butler, filename: str, do_store_logs: bool = True) -> None:
        """Take the dictionary from
        `QuantumGraphExecutionReport.to_summary_dict` and store its contents in
        a yaml file.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        filename : `str`
            The name to be used for the summary yaml file.
        do_store_logs : `bool`
            Store the logs in the summary dictionary.
        """
        with open(filename, "w") as stream:
            yaml.safe_dump(self.to_summary_dict(butler, do_store_logs=do_store_logs), stream)

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
        for dataset_type_node in qg.pipeline_graph.dataset_types.values():
            if qg.pipeline_graph.producer_of(dataset_type_node.name) is None:
                continue
            refs[dataset_type_node.name] = {
                ref.id: ref
                for ref in butler.registry.queryDatasets(
                    dataset_type_node.name, collections=collection, findFirst=False
                )
            }
        for task_node in qg.pipeline_graph.tasks.values():
            for quantum_id, quantum in qg.get_task_quanta(task_node.label).items():
                status_graph.add_node(quantum_id)
                for ref in itertools.chain.from_iterable(quantum.outputs.values()):
                    status_graph.add_edge(quantum_id, ref.id)
                for ref in itertools.chain.from_iterable(quantum.inputs.values()):
                    status_graph.add_edge(ref.id, quantum_id)
        for task_node in qg.pipeline_graph.tasks.values():
            task_report = TaskExecutionReport()
            if task_node.log_output is None:
                raise RuntimeError("QG must have log outputs to use execution reports.")
            for quantum_id, quantum in qg.get_task_quanta(task_node.label).items():
                task_report.inspect_quantum(
                    quantum_id,
                    quantum,
                    status_graph,
                    refs,
                    metadata_name=task_node.metadata_output.dataset_type_name,
                    log_name=task_node.log_output.dataset_type_name,
                )
                task_report.n_expected = len(qg.get_task_quanta(task_node.label).items())
            report.tasks[task_node.label] = task_report
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
