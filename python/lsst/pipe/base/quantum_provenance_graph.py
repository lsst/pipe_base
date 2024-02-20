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

"""A set of already-run, merged quantum graphs with provenance information
which can be used to compose a report on the status of multi-attempt
processing.
"""

from __future__ import annotations

__all__ = (
    "QuantumProvenanceGraph",
    "QuantumKey",
    "DatasetKey",
    "PrerequisiteDatasetKey",
)

import dataclasses
import itertools
import logging
import uuid
from collections.abc import Iterator, Sequence, Set
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple

import networkx
from lsst.daf.butler import Butler, DataIdValue
from lsst.daf.butler.nonempty_mapping import NonemptyMapping
from lsst.resources import ResourcePathExpression
from lsst.utils.logging import getLogger

from .graph import QuantumGraph

if TYPE_CHECKING:
    pass

_LOG = getLogger(__name__)


class QuantumKey(NamedTuple):
    """Identifier type for quantum keys in a `QuantumProvenanceGraph`."""

    task_label: str
    """Label of the task in the pipeline."""

    data_id_values: tuple[DataIdValue, ...]
    """Data ID values of the quantum.

    Note that keys are fixed given `task_label`, so using only the values here
    speeds up comparisons.
    """

    is_task: ClassVar[Literal[True]] = True
    """Whether this node represents a quantum rather
    than a dataset (always `True`).
    """

    def to_summary_dict(self, xgraph: networkx.DiGraph) -> dict[str, Any]:
        return {
            "task": self.task_label,
            "data_id": xgraph.nodes[self]["data_id"],
        }


class DatasetKey(NamedTuple):
    """Identifier type for dataset keys in a `QuantumProvenanceGraph`."""

    parent_dataset_type_name: str
    """Name of the dataset type (never a component)."""

    data_id_values: tuple[DataIdValue, ...]
    """Data ID values of the dataset.

    Note that keys are fixed given `parent_dataset_type_name`, so using only
    the values here speeds up comparisons.
    """

    is_task: ClassVar[Literal[False]] = False
    """Whether this node represents a quantum rather
    than a dataset (always `False`).
    """

    is_prerequisite: ClassVar[Literal[False]] = False


class PrerequisiteDatasetKey(NamedTuple):
    """Identifier type for prerequisite dataset keys in a
    `QuantumProvenanceGraph`.

    Unlike regular datasets, prerequisites are not actually required to come
    from a find-first search of `input_collections`, so we don't want to
    assume that the same data ID implies the same dataset.  Happily we also
    don't need to search for them by data ID in the graph, so we can use the
    dataset ID (UUID) instead.
    """

    parent_dataset_type_name: str
    """Name of the dataset type (never a component)."""

    dataset_id_bytes: bytes
    """Dataset ID (UUID) as raw bytes."""

    is_task: ClassVar[Literal[False]] = False
    """Whether this node represents a quantum rather
    than a dataset (always `False`).
    """

    is_prerequisite: ClassVar[Literal[True]] = True


@dataclasses.dataclass
class QuantumRun:
    """Information about a quantum in a given run collection."""

    id: uuid.UUID
    """The quantum graph node ID associated with the dataId in a specific run.
    """

    status: Literal["failed", "not_attempted", "successful", "logs_missing"] = "not_attempted"
    """The status of the quantum in that run.
    """


@dataclasses.dataclass
class DatasetRun:
    """Information about a dataset in a given run collection."""

    id: uuid.UUID
    """The dataset ID associated with the dataset in a specific run.
    """

    produced: bool = False
    """Whether the specific run produced the dataset.
    """

    published: bool = False
    """Whether this dataset was published in the final output collection.
    """


@dataclasses.dataclass(frozen=True, order=True)
class ResolvedDatasetKey:
    """A combination of a dataset key and a particular dataset run to be used
    for recording specific instances of issues.
    """

    key: DatasetKey
    run: str
    id: uuid.UUID

    def to_summary_dict(self, xgraph: networkx.DiGraph) -> dict[str, Any]:
        return {
            "dataset_type": self.key.parent_dataset_type_name,
            "data_id": xgraph.nodes[self.key]["data_id"],
            "uuid": self.id,
            "run": self.run,
        }


class QuantumProvenanceGraph:
    """A set of already-run, merged quantum graphs with provenance
    information.

    Step through all the quantum graphs associated with certain tasks or
    processing steps. For each graph/attempt, the status of each quantum and
    dataset is recorded in `QuantumProvenanceGraph.add_new_graph` and duplicate
    outcomes of dataIds are resolved in
    `QuantumProvenanceGraph.resolve_duplicates`. At the end of this process, we
    can combine all attempts into a final summary graph which can be converted
    into a report on the production over multiple processing and recovery
    attempts in `name functions later`. This serves to answer the question
    "What happened to this data ID?" in a wholistic sense.
    """

    def __init__(self):
        # The graph we annotate as we step through all the graphs associated
        # with the processing to create the `QuantumProvenanceGraph`.
        self._xgraph = networkx.DiGraph()
        # The nodes representing quanta in `_xgraph` grouped by task label.
        self._quanta: dict[str, set[QuantumKey]] = {}
        # The nodes representing datasets in `_xgraph` grouped by dataset type
        # name.
        self._datasets: dict[str, set[DatasetKey]] = {}
        self._published_failures: NonemptyMapping[str, set[ResolvedDatasetKey]] = NonemptyMapping()
        self._ignored_successes: NonemptyMapping[str, set[ResolvedDatasetKey]] = NonemptyMapping()
        self._rejected_successes: NonemptyMapping[str, set[ResolvedDatasetKey]] = NonemptyMapping()
        self._no_work_datasets: NonemptyMapping[str, set[ResolvedDatasetKey]] = NonemptyMapping()
        self._heterogeneous_quanta: set[QuantumKey] = set()

    @property
    def published_failures(self) -> Set[ResolvedDatasetKey]:
        """Datasets that appeared in the final output collection even though
        the quantum that produced them failed.
        """
        return self._published_failures

    @property
    def ignored_successes(self) -> Set[ResolvedDatasetKey]:
        """Dataset types and data ids that were produced by one or more
        successful quanta but not included in the final output collection.
        """
        # Note: we want to make this a set[DatasetKey] instead.
        return self._ignored_successes

    @property
    def rejected_successes(self) -> Set[ResolvedDatasetKey]:
        """Datasets from successful quanta that were not published, where
        another dataset with the same data id was published.
        """
        return self._rejected_successes

    @property
    def heterogeneous_quanta(self) -> Set[QuantumKey]:
        """Quanta whose published outputs came from multiple runs."""
        return self._heterogeneous_quanta

    def add_new_graph(self, butler: Butler, qgraph: QuantumGraph | ResourcePathExpression) -> None:
        """Add a new quantum graph to the `QuantumProvenanceGraph`.

        Step through the quantum graph. Annotate a mirror networkx.DiGraph
        (QuantumProvenanceGraph._xgraph) with all of the relevant information:
        quanta, dataset types and their associated run collection (these unique
        quanta- and dataset type-run collection combinations are encapsulated
        in the dataclasses `DatasetRun` and `QuantumRun`). For each new
        quantum, annotate the status of the `QuantumRun` by inspecting the
        graph. If a DatasetType was produced, annotate this in the run by
        setting `DatasetRun.produced = True`. Then, we can resolve newly-
        successful quanta (failed in previous runs) with
        `QuantumProvenanceGraph.resolve_duplicates`.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.

        qgraph : `QuantumGraph` | `ResourcePathExpression`
            Either the associated quantum graph object or the uri of the
            location of said quantum graph.
        """
        # first we load the quantum graph and associated output run collection
        if not isinstance(qgraph, QuantumGraph):
            qgraph = QuantumGraph.loadUri(qgraph)
        assert qgraph.metadata is not None, "Saved QGs always have metadata."
        output_run = qgraph.metadata["output_run"]
        new_quanta = []
        for node in qgraph:
            # make a key to add to the mirror graph with specific quanta for
            # nodes.
            quantum_key = QuantumKey(node.taskDef.label, node.quantum.dataId.required_values)
            self._xgraph.add_node(quantum_key)
            self._xgraph.nodes[quantum_key]["data_id"] = node.quantum.dataId
            new_quanta.append(quantum_key)
            self._quanta.setdefault(quantum_key.task_label, set()).add(quantum_key)
            # associate run collections with specific quanta. this is important
            # if the same quanta are processed in multiple runs as in recovery
            # workflows.
            quantum_runs = self._xgraph.nodes[quantum_key].setdefault("runs", {})
            # the QuantumRun here is the specific quantum-run collection
            # combination.
            quantum_runs[output_run] = QuantumRun(node.nodeId)
            for ref in itertools.chain.from_iterable(node.quantum.outputs.values()):
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                # add datasets to the nodes of the mirror graph, with edges on
                # the quanta.
                self._xgraph.add_edge(quantum_key, dataset_key)
                self._xgraph.nodes[dataset_key]["data_id"] = ref.dataId
                self._datasets.setdefault(dataset_key.parent_dataset_type_name, set()).add(dataset_key)
                dataset_runs = self._xgraph.nodes[dataset_key].setdefault("runs", {})
                # make a DatasetRun for the specific dataset-run collection
                # combination.
                dataset_runs[output_run] = DatasetRun(ref.id)
                # save metadata and logs for easier status interpretation
                if dataset_key.parent_dataset_type_name.endswith("_metadata"):
                    self._xgraph.nodes[quantum_key]["metadata"] = dataset_key
                if dataset_key.parent_dataset_type_name.endswith("_log"):
                    self._xgraph.nodes[quantum_key]["log"] = dataset_key
            for ref in itertools.chain.from_iterable(node.quantum.inputs.values()):
                dataset_key = DatasetKey(ref.datasetType.nameAndComponent()[0], ref.dataId.required_values)
                if dataset_key in self._xgraph:
                    # add another edge if the input datasetType and quantum are
                    # in the graph
                    self._xgraph.add_edge(dataset_key, quantum_key)
        for dataset_type_name in self._datasets:
            for ref in butler.registry.queryDatasets(dataset_type_name, collections=output_run):
                # find the datasets in the butler
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                dataset_run = self._xgraph.nodes[dataset_key]["runs"][output_run]  # dataset run (singular)
                # if the dataset is in the output run collection, we produced
                # it!
                dataset_run.produced = True
        for quantum_key in new_quanta:
            quantum_run: QuantumRun = self._xgraph.nodes[quantum_key]["runs"][output_run]
            metadata_key = self._xgraph.nodes[quantum_key]["metadata"]
            log_key = self._xgraph.nodes[quantum_key]["log"]
            metadata_dataset_run: DatasetRun = self._xgraph.nodes[metadata_key]["runs"][output_run]
            log_dataset_run: DatasetRun = self._xgraph.nodes[log_key]["runs"][output_run]
            if metadata_dataset_run.produced:  # check with Jim about this condition
                # if we do have metadata:
                if log_dataset_run.produced:
                    # if we also have logs, this is a success
                    # this includes No Work Found (the only things produced
                    # were metadata and logs).
                    quantum_run.status = "successful"
                else:
                    # if we have metadata and no logs, this is a very rare
                    # case. either the task ran successfully and the datastore
                    # died immediately afterwards, or some supporting
                    # infrastructure for transferring the logs to the datastore
                    # failed.
                    quantum_run.status = "logs_missing"
            else:
                # missing metadata means that the task did not finish.
                if log_dataset_run.produced:
                    # if we do have logs, the task not finishing is a failure
                    # in the task itself. This includes all payload errors and
                    # some other errors.
                    quantum_run.status = "failed"
                else:
                    # we are missing metadata and logs. Either the task was not
                    # started, or a hard external environmental error prevented
                    # it from writing logs or metadata.
                    quantum_run.status = "not_attempted"

    # I imagine that the next step is to call `resolve_duplicates` on the
    # self._xgraph.
    # Things that could have happened to a quanta over multiple runs
    # Failed until it suceeded
    # Never been attempted
    # Succeeded immediately
    # Failed and continued to fail
    # Horrible flip-flopping (doesn't happen with skip-existing-in)

    def resolve_duplicates(self, butler: Butler, collections: Sequence[str] | None = None, where: str = ""):
        for dataset_type_name in self._datasets:
            for ref in butler.registry.queryDatasets(
                dataset_type_name,
                collections=collections,
                findFirst=True,
                where=where,
            ):
                # find the datasets in a larger collection. "who won?"
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                self._xgraph.nodes[dataset_key]["winner"] = ref.run
                self._xgraph.nodes[dataset_key]["runs"][ref.run].published = True
        for task_label, task_quanta in self._quanta.items():
            for quantum_key in task_quanta:
                # these are the run collections of the datasets produced by
                # this quantum that were published in the final collection
                winners = {
                    winner
                    for dataset_key in self.iter_outputs_of(quantum_key)
                    if (winner := self._xgraph.nodes[dataset_key].get("winner"))
                }
                # note: we expect len(winners) = 1
                for run, quantum_run in self._xgraph.nodes[quantum_key]["runs"].items():
                    if quantum_run.status != "successful" and run in winners:
                        for dataset_key in self.iter_outputs_of(quantum_key):
                            # the outputs of this quantum in this run may have
                            # been mistakenly published
                            dataset_run = self._xgraph.nodes[dataset_key]["runs"][run]
                            if dataset_run.published:
                                self._published_failures[dataset_key.parent_dataset_type_name].add(
                                    ResolvedDatasetKey(key=dataset_key, run=run, id=dataset_run.id)
                                )
                        break
                    if quantum_run.status == "successful" and run not in winners:
                        if len(winners) == 0:
                            # the quantum succeeded but no outputs were
                            # published
                            for dataset_key in self.iter_outputs_of(quantum_key):
                                dataset_run: DatasetRun = self._xgraph.nodes[dataset_key]["runs"][run]
                                if not dataset_run.published and dataset_run.produced:
                                    self._ignored_successes[dataset_key.parent_dataset_type_name].add(
                                        ResolvedDatasetKey(key=dataset_key, run=run, id=dataset_run.id)
                                    )
                        else:
                            for dataset_key in self.iter_outputs_of(quantum_key):
                                dataset_run = self._xgraph.nodes[dataset_key]["runs"][run]
                                self._rejected_successes[dataset_key.parent_dataset_type_name].add(
                                    ResolvedDatasetKey(key=dataset_key, run=run, id=dataset_run.id)
                                )
                if len(winners) > 1:
                    # some rejected outputs may be in here
                    print("published outputs for this quantum were from multiple runs")
                    self._heterogeneous_quanta.add(quantum_key)
        for dataset_type_name, datasets_for_type in self._datasets.items():
            for dataset_key in datasets_for_type:
                for run, dataset_run in self._xgraph.nodes[dataset_key]["runs"].items():
                    if not dataset_run.produced:
                        quantum_key = self.get_producer_of(dataset_key)
                        quantum_run: QuantumRun = self._xgraph.nodes[quantum_key]["runs"][run]
                        if quantum_run.status == "successful":
                            self._no_work_datasets[dataset_key.parent_dataset_type_name].add(
                                ResolvedDatasetKey(key=dataset_key, run=run, id=dataset_run.id)
                            )
                            # this is a NoWorkFound

    # for each dataset, how many got published of each type? how many were
    # produced and not published? how many were predicted and not produced
    # (for various reasons)

    def to_summary_dict(self, butler: Butler, do_store_logs: bool = True) -> dict[str, Any]:
        """Summarize the results of the TaskExecutionReport in a dictionary.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        do_store_logs : `bool`
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
            - n_successful: The number of quanta which succeeeded.
        """
        result = {
            "tasks": {},
            "datasets": {},
            "published_failures": [
                key.to_summary_dict(self._xgraph)
                for key in sorted(itertools.chain.from_iterable(self._published_failures.values()))
            ],
            "rejected_successes": [
                key.to_summary_dict(self._xgraph)
                for key in sorted(itertools.chain.from_iterable(self._rejected_successes.values()))
            ],
            "ignored_successes": [
                key.to_summary_dict(self._xgraph)
                for key in sorted(itertools.chain.from_iterable(self._ignored_successes.values()))
            ],
            "heterogeneous_quanta": [
                key.to_summary_dict(self._xgraph)
                for key in sorted(itertools.chain.from_iterable(self._heterogeneous_quanta.values()))
            ],
        }
        for task_label, quanta in self._quanta.items():
            n_blocked = 0
            n_successful = 0
            failed_quanta = []
            # every item in this list will correspond to a data_id and be a
            # dict keyed by run
            for quantum_key in quanta:
                failed_quantum_info = {"data_id": {}, "runs": {}}
                for run, quantum_run in self._xgraph.nodes[quantum_key]["runs"].items():
                    if quantum_run.status == "successful":
                        failed_quantum_info["runs"].clear()
                        # if any of the quantum runs successful, we don't worry
                        # about it
                        n_successful += 1
                        break
                    elif quantum_run.status == "blocked":
                        n_blocked += 1
                        continue
                    else:
                        log_key: DatasetKey = self._xgraph.nodes[quantum_key]["log"]
                        quantum_data_id = self._xgraph.nodes[quantum_key]["data_id"]
                        failed_quantum_info["data_id"].update(quantum_data_id.mapping)
                        quantum_info = {"id": quantum_run.id, "status": quantum_run.status}
                        if do_store_logs:
                            try:
                                # should probably upgrade this to use a dataset
                                # ref
                                log = butler.get(
                                    log_key.parent_dataset_type_name, quantum_data_id, collections=run
                                )
                            except LookupError:
                                quantum_info["error"] = []
                            except FileNotFoundError:
                                quantum_info["error"] = None
                            else:
                                quantum_info["error"] = [
                                    record.message for record in log if record.levelno >= logging.ERROR
                                ]
                        failed_quantum_info["runs"][run] = quantum_info
                if failed_quantum_info["runs"]:
                    # if the quantum runs continue to fail, report.
                    failed_quanta.append(failed_quantum_info)
            result["tasks"][task_label] = {
                "failed_quanta": failed_quanta,
                "n_quanta_blocked": n_blocked,
                "n_successful": n_successful,
            }
        for dataset_type_name, datasets in self._datasets.items():
            n_successful = 0
            for dataset_key in datasets:
                producer_quantum_key = self.get_producer_of(dataset_key)
                producer_quantum_run: QuantumRun
                if winning_run := self._xgraph.nodes[dataset_key].get("winner"):
                    # A dataset for this data ID was published.
                    producer_quantum_run = self._xgraph.nodes[producer_quantum_key]["runs"][winning_run]
                    if producer_quantum_run.status == "successful":
                        n_successful += 1
                else:
                    # A dataset for this data ID was not published.
                    # THIS BRANCH VERY MUCH TO DO.  IT MAY BE GARBAGE.
                    dataset_run: DatasetRun
                    final_status: str = "not_attempted"
                    for run, dataset_run in reversed(self._xgraph.nodes[dataset_key]["runs"].items()):
                        if dataset_run.produced:
                            # Published failures handled elsewhere.
                            break
                        producer_quantum_run = self._xgraph.nodes[producer_quantum_key]["runs"][run]
                        match producer_quantum_run.status:
                            case "successful":
                                # No work handled elsewhere.
                                break

            result["datasets"][dataset_type_name] = {
                # This is the total number in the original QG.
                "predicted": len(datasets),
                # These should all add up to 'predicted'...
                "successful": n_successful,  # (and published)
                "no_work": self._no_work_datasets[dataset_type_name],
                "ignored_successes": self._ignored_successes[dataset_type_name],
                "published_failures": self._published_failures[dataset_type_name],
                "failed": ...,
                "blocked": ...,
                # ... these do not sum nicely to anything:
                "rejected_successes": self._rejected_successes[dataset_type_name],
            }
        return result

    def iter_outputs_of(self, quantum_key: QuantumKey) -> Iterator[DatasetKey]:
        metadata_key = self._xgraph.nodes[quantum_key]["metadata"]
        log_key = self._xgraph.nodes[quantum_key]["log"]
        for dataset_key in self._xgraph.successors(quantum_key):
            if dataset_key != metadata_key and dataset_key != log_key:
                yield dataset_key

    def get_producer_of(self, dataset_key: DatasetKey) -> QuantumKey:
        (result,) = self._xgraph.predecessors(dataset_key)
        return result
