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
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple, TypedDict

import networkx
from lsst.daf.butler import Butler, DataCoordinate, DataIdValue
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

    status: Literal["failed", "successful", "logs_missing", "blocked", "metadata_missing"] = (
        "metadata_missing"
    )
    """The status of the quantum in that run.
    """

    def to_summary_dict(
        self,
    ) -> dict[str, Literal["failed", "successful", "logs_missing", "blocked", "metadata_missing"]]:
        return {
            "status": self.status,
        }


class QuantumInfo(TypedDict):
    """Information about a quantum across all run collections.

    Used to annotate the networkx node dictionary.
    """

    data_id: DataCoordinate
    """The data_id of the quantum.
    """

    runs: dict[str, QuantumRun]
    """All run collections associated with the quantum.
    """

    status: Literal["successful", "wonky", "blocked", "not_attempted", "failed"]
    """The overall status of the quantum. Note that it is impossible to exit a
    wonky state.
    """

    recovered: bool
    """The quantum was originally not successful but was ultimately successful.
    """

    messages: list[str]
    """Diagnostic messages to help disambiguate wonky states.
    """


def make_quantum_info_summary_dict(quantum_info: QuantumInfo) -> dict[str, Any]:
    return {
        "data_id": dict(quantum_info["data_id"].required),
        "runs": dict(
            zip(
                quantum_info["runs"].keys(),
                [value.to_summary_dict() for value in quantum_info["runs"].values()],
            )
        ),
        "status": quantum_info["status"],
        "recovered": quantum_info["recovered"],
        "messages": quantum_info["messages"],
    }


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

    def __post_init__(self) -> None:
        assert not (self.published and not self.produced)

    def to_summary_dict(self) -> dict[str, bool]:
        return {
            "produced": self.produced,
            "published": self.published,
        }


class DatasetInfo(TypedDict):
    """Information about a given dataset across all runs.

    Used to annotate the networkx node dictionary.
    """

    parent_task: QuantumKey.task_label
    """The task_label of the task which produced this dataset.
    """

    data_id: DataCoordinate
    """The data_id of the quantum.
    """

    runs: dict[str, DatasetRun]
    """All runs associated with the dataset.
    """

    status: Literal["published", "unpublished", "predicted_only", "unsuccessful", "cursed"]
    """Overall status of the dataset.
    """

    messages: list[str]
    """Diagnostic messages to help disambiguate cursed states.
    """


def make_dataset_info_summary_dict(dataset_info: DatasetInfo) -> dict[str, Any]:
    return {
        "parent_task": str(dataset_info["parent_task"]),
        "data_id": dict(dataset_info["data_id"].required),
        "runs": dict(
            zip(
                dataset_info["runs"].keys(),
                [value.to_summary_dict() for value in dataset_info["runs"].values()],
            )
        ),
        "status": dataset_info["status"],
        "messages": dataset_info["messages"],
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

    def get_quantum_info(self, key: QuantumKey) -> QuantumInfo:
        return self._xgraph.nodes[key]

    def get_dataset_info(self, key: DatasetKey) -> DatasetInfo:
        return self._xgraph.nodes[key]

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
            quantum_info = self.get_quantum_info(quantum_key)
            quantum_info.setdefault("messages", [])
            quantum_info.setdefault("runs", {})
            quantum_info.setdefault("data_id", node.quantum.dataId)
            quantum_info.setdefault("status", "not_attempted")
            quantum_info.setdefault("recovered", False)
            new_quanta.append(quantum_key)
            self._quanta.setdefault(quantum_key.task_label, set()).add(quantum_key)
            # associate run collections with specific quanta. this is important
            # if the same quanta are processed in multiple runs as in recovery
            # workflows.
            quantum_runs = quantum_info.setdefault("runs", {})
            # the QuantumRun here is the specific quantum-run collection
            # combination.
            quantum_runs[output_run] = QuantumRun(node.nodeId)
            for ref in itertools.chain.from_iterable(node.quantum.outputs.values()):
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                # add datasets to the nodes of the mirror graph, with edges on
                # the quanta.
                self._xgraph.add_edge(quantum_key, dataset_key)
                dataset_info = self.get_dataset_info(dataset_key)
                dataset_info.setdefault("parent_task", quantum_key.task_label)
                dataset_info.setdefault("data_id", ref.dataId)
                dataset_info.setdefault("status", "missing")
                dataset_info.setdefault("messages", [])
                self._datasets.setdefault(dataset_key.parent_dataset_type_name, set()).add(dataset_key)
                dataset_runs = dataset_info.setdefault("runs", {})
                # make a DatasetRun for the specific dataset-run collection
                # combination.
                dataset_runs[output_run] = DatasetRun(ref.id)
                # save metadata and logs for easier status interpretation
                if dataset_key.parent_dataset_type_name.endswith("_metadata"):
                    quantum_info["metadata"] = dataset_key
                if dataset_key.parent_dataset_type_name.endswith("_log"):
                    quantum_info["log"] = dataset_key
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
                dataset_run = dataset_info["runs"][output_run]  # dataset run (singular)
                # if the dataset is in the output run collection, we produced
                # it!
                dataset_run.produced = True
        # the outputs of failed or blocked quanta in this run.
        blocked: set[DatasetKey] = set()
        for quantum_key in new_quanta:
            quantum_info = self.get_quantum_info(quantum_key)
            quantum_run = quantum_info["runs"][output_run]
            metadata_key = quantum_info["metadata"]
            log_key = quantum_info["log"]
            metadata_dataset_run = self.get_dataset_info(metadata_key)["runs"][output_run]
            log_dataset_run = self.get_dataset_info(log_key)["runs"][output_run]
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
                    # if a quantum fails, all its successor datasets are
                    # blocked.
                    blocked.update(self._xgraph.successors(quantum_key))
                else:
                    # we are missing metadata and logs. Either the task was not
                    # started, or a hard external environmental error prevented
                    # it from writing logs or metadata.
                    if blocked.isdisjoint(self._xgraph.predecessors(quantum_key)):
                        # None of this quantum's inputs were blocked.
                        quantum_run.status = "metadata_missing"
                    else:
                        quantum_run.status = "blocked"
                        blocked.update(self._xgraph.successors(quantum_key))

            # Now we can start using state transitions to mark overall status.
            if len(quantum_info["runs"]) == 1:
                last_status = "not_attempted"
            else:
                last_run = list(quantum_info["runs"].values())[-1]
                last_status = last_run.status
            match last_status, quantum_run.status:
                case ("not_attempted", new_status):
                    pass
                case ("wonky", _):
                    new_status = "wonky"
                case (_, "successful"):
                    new_status = "successful"
                    if last_status != "successful":
                        quantum_info["recovered"] = True
                case (_, "logs_missing"):
                    new_status = "wonky"
                    quantum_info["messages"].append(f"Logs missing for run {output_run!r}.")
                case ("successful", _):
                    new_status = "wonky"
                    quantum_info["messages"].append(
                        f"Status went from successful in run {last_run!r} "
                        f"to {quantum_run.status!r} in {output_run!r}."
                    )
                case (_, "blocked"):
                    new_status = last_status
                case (_, "metadata_missing"):
                    new_status = "not_attempted"
                case (_, "failed"):
                    new_status = "failed"
            quantum_info["status"] = new_status

    def resolve_duplicates(self, butler: Butler, collections: Sequence[str] | None = None, where: str = ""):
        # could also call "resolve runs"
        for dataset_type_name in self._datasets:
            for ref in butler.registry.queryDatasets(
                dataset_type_name,
                collections=collections,
                findFirst=True,
                where=where,
            ):
                # find the datasets in a larger collection. "who won?"
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                dataset_info = self.get_dataset_info(dataset_key)
                dataset_info["runs"][ref.run].published = True

        for task_label, task_quanta in self._quanta.items():
            for quantum_key in task_quanta:
                # these are the run collections of the datasets produced by
                # this quantum that were published in the final collection
                published_runs = set()
                quantum_info = self.get_quantum_info(quantum_key)
                for dataset_key in self.iter_outputs_of(quantum_key):
                    dataset_info = self.get_dataset_info(dataset_key)
                    dataset_info["parent_task"] = quantum_key.task_label
                    published_runs.update(
                        run for run, dataset_run in dataset_info["runs"].items() if dataset_run.published
                    )
                    if any(dataset_run.published for dataset_run in dataset_info["runs"].values()):
                        publish_state = "published"
                    elif any(dataset_run.produced for dataset_run in dataset_info["runs"].values()):
                        publish_state = "unpublished"
                    else:
                        publish_state = "missing"
                    match (quantum_info["status"], publish_state):
                        case ("successful", "published"):
                            dataset_info["status"] = "published"
                        case ("successful", "missing"):
                            dataset_info["status"] = "predicted_only"
                        case ("successful", "unpublished"):
                            dataset_info["status"] = "unpublished"
                        case (_, "published"):
                            # If anything other than a successful quantum
                            # produces a published dataset, that dataset
                            # is cursed.
                            dataset_info["status"] = "cursed"
                            dataset_info["messages"].append(
                                "Published dataset is from an unsuccessful quantum."
                            )
                        case _:
                            # This should be a regular failure.
                            dataset_info["status"] = "unsuccessful"
                if len(published_runs) > 1:
                    quantum_info["status"] = "wonky"
                    quantum_info["messages"].append(
                        f"Outputs from different runs of the same quanta were published: {published_runs}."
                    )

    def to_summary_dict(self, butler: Butler, do_store_logs: bool = True) -> dict[str, Any]:
        """Summarize the QuantumProvenanceGraph in a dictionary.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        do_store_logs : `bool`
            Store the logs in the summary dictionary.

        Returns
        -------
        summary_dict : `dict`
            A dictionary containing counts of quanta and datasets in each of
            the overall states defined in `QuantumInfo` and `DatasetInfo`,
            as well as diagnostic information and error messages for failed
            quanta and strange edge cases, and a list of recovered quanta.
        """
        result = {
            "tasks": {},
            "datasets": {},
        }
        for task_label, quanta in self._quanta.items():
            n_successful = 0
            n_wonky = 0
            n_blocked = 0
            n_failed = 0
            failed_quanta = {"data_id": {}, "runs": {}, "messages": {}}
            recovered_quanta = []
            wonky_quanta = {"data_id": {}, "runs": {}, "messages": {}}
            for quantum_key in quanta:
                quantum_info = self.get_quantum_info(quantum_key)
                quantum_summary = make_quantum_info_summary_dict(quantum_info)
                if quantum_summary["status"] == "successful":
                    n_successful += 1
                    if quantum_summary["recovered"]:
                        recovered_quanta.append(quantum_summary["data_id"])
                elif quantum_summary["status"] == "wonky":
                    n_wonky += 1
                    wonky_quanta.update({"data_id": quantum_summary["data_id"]})
                    wonky_quanta.update({"runs": quantum_summary["runs"]})
                    wonky_quanta.update({"messages": quantum_summary["messages"]})
                elif quantum_summary["status"] == "blocked":
                    n_blocked += 1
                elif quantum_summary["status"] == "failed":
                    n_failed += 1
                    failed_quanta.update({"data_id": quantum_summary["data_id"]})
                    runs = quantum_summary["runs"]
                    failed_quanta.update({"runs": runs})
                    log_key: DatasetKey = self._xgraph.nodes[quantum_key]["log"]
                    if do_store_logs:
                        for run in runs:
                            try:
                                # should probably upgrade this to use a dataset
                                # ref
                                log = butler.get(
                                    log_key.parent_dataset_type_name, quantum_info["data_id"], collections=run
                                )
                            except LookupError:
                                failed_quanta["messages"] = []
                            except FileNotFoundError:
                                failed_quanta["messages"] = None
                            else:
                                failed_quanta["messages"].extend(
                                    [record.message for record in log if record.levelno >= logging.ERROR]
                                )
            result["tasks"][task_label] = {
                "n_successful": n_successful,
                "n_wonky": n_wonky,
                "n_blocked": n_blocked,
                "n_failed": n_failed,
                "failed_quanta": failed_quanta,
                "recovered_quanta": recovered_quanta,
                "wonky_quanta": wonky_quanta,
            }
        for dataset_type_name, datasets in self._datasets.items():
            n_published = 0
            n_unpublished = 0
            n_predicted_only = 0
            n_unsuccessful = 0
            n_cursed = 0
            unsuccessful_datasets = []
            cursed_datasets = {
                "parent_task_name": "",
                "parent_data_id": {},
                "runs": {},
                "messages": [],
            }
            for dataset_key in datasets:
                dataset_info = self.get_dataset_info(dataset_key)
                dataset_summary = make_dataset_info_summary_dict(dataset_info)
                if dataset_info["status"] == "published":
                    n_published += 1
                elif dataset_info["status"] == "unpublished":
                    n_unpublished += 1
                elif dataset_info["status"] == "predicted_only":
                    n_predicted_only += 1
                elif dataset_info["status"] == "unsuccessful":
                    n_unsuccessful += 1
                    unsuccessful_datasets.append(dataset_summary["data_id"])
                elif dataset_info["status"] == "cursed":
                    n_cursed += 1
                    cursed_datasets.update({"parent_task_name": dataset_summary["parent_task"]})
                    cursed_datasets.update({"parent_data_id": dataset_summary["data_id"]})
                    cursed_datasets.update({"runs": dataset_summary["runs"]})
                    cursed_datasets.update({"messages": dataset_summary["messages"]})

            result["datasets"][dataset_type_name] = {
                # This is the total number in the original QG.
                "n_predicted": len(datasets),
                # These should all add up to 'predicted'...
                "n_published": n_published,
                "n_unpublished": n_unpublished,
                "n_predicted_only": n_predicted_only,
                "n_unsuccessful": n_unsuccessful,
                "n_cursed": n_cursed,
                "unsuccessful_datasets": unsuccessful_datasets,
                "cursed_datasets": cursed_datasets,
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
