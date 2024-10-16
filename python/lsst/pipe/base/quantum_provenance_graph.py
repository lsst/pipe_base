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

import itertools
import logging
import uuid
from collections.abc import Iterator, Sequence
from enum import Enum
from typing import TYPE_CHECKING, ClassVar, Literal, NamedTuple, TypedDict, cast

import networkx
import pydantic
from lsst.daf.butler import Butler, DataCoordinate, DataIdValue
from lsst.resources import ResourcePathExpression
from lsst.utils.logging import getLogger

from .graph import QuantumGraph

if TYPE_CHECKING:
    pass

_LOG = getLogger(__name__)


class QuantumKey(NamedTuple):
    """Identifier type for quantum keys in a `QuantumProvenanceGraph`. These
    keys correspond to a task label and data ID, but can refer to this over
    multiple runs or datasets.
    """

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


class DatasetKey(NamedTuple):
    """Identifier type for dataset keys in a `QuantumProvenanceGraph`."""

    dataset_type_name: str
    """Name of the dataset type (never a component)."""

    data_id_values: tuple[DataIdValue, ...]
    """Data ID values of the dataset.

    Note that keys are fixed given `parent_dataset_type_name`, so using only
    the values here speeds up comparisons.
    """

    is_task: ClassVar[Literal[False]] = False
    """Whether this node represents a quantum rather than a dataset (always
    `False`).
    """

    is_prerequisite: ClassVar[Literal[False]] = False
    """Whether this node is a prerequisite to another node (also always
    `False`).
    """


class PrerequisiteDatasetKey(NamedTuple):
    """Identifier type for prerequisite dataset keys in a
    `QuantumProvenanceGraph`.

    Unlike regular datasets, prerequisites are not actually required to come
    from a find-first search of `input_collections`, so we don't want to
    assume that the same data ID implies the same dataset.  Happily we also
    don't need to search for them by data ID in the graph, so we can use the
    dataset ID (UUID) instead.
    """

    dataset_type_name: str
    """Name of the dataset type (never a component)."""

    dataset_id_bytes: bytes
    """Dataset ID (UUID) as raw bytes."""

    is_task: ClassVar[Literal[False]] = False
    """Whether this node represents a quantum rather
    than a dataset (always `False`).
    """

    is_prerequisite: ClassVar[Literal[True]] = True
    """Whether this node is a prerequisite to another node (always `True`).
    """


class QuantumRunStatus(Enum):
    """Enum describing the status of a quantum-run collection combination.

    Possible Statuses
    -----------------
    METADATA_MISSING = -3: Metadata is missing for this quantum in this run.
        It is impossible to tell whether execution of this quantum was
        attempted due to missing metadata.
    LOGS_MISSING = -2: Logs are missing for this quantum in this run. It was
        attempted, but it is impossible to tell if it succeeded or failed due
        to missing logs.
    FAILED = -1: Attempts to execute the quantum failed in this run.
    BLOCKED = 0: This run does not include an executed version of this
        quantum because an upstream task failed.
    SUCCESSFUL = 1: This quantum was executed successfully in this run.
    """

    METADATA_MISSING = -3
    LOGS_MISSING = -2
    FAILED = -1
    BLOCKED = 0
    SUCCESSFUL = 1


class QuantumRun(pydantic.BaseModel):
    """Information about a quantum in a given run collection."""

    id: uuid.UUID
    """The quantum graph node ID associated with the dataId in a specific run.
    """

    status: QuantumRunStatus = QuantumRunStatus.METADATA_MISSING
    """The status of the quantum in that run.
    """


class QuantumInfoStatus(Enum):
    """The status of a quantum (a particular task run on a particular dataID)
    across all runs.

    Possible Statuses
    -----------------
    WONKY = -3: The overall state of this quantum reflects inconsistencies or
        is difficult to discern. There are a few specific ways to enter a wonky
        state; it is impossible to exit and requires human intervention to
        proceed with processing.
        Currently, a quantum enters a wonky state for one of three reasons:
        - Its overall `QuantumInfoStatus` moves from a successful state (as a
        result of a successful run) to any other state. In other words,
        something that initially succeeded fails on subsequent attempts.
        - A `QuantumRun` is missing logs.
        - There are multiple runs associated with a dataset, and this comes up
          in a findFirst search. This means that a dataset which will be used
          as an input data product for further processing has heterogeneous
          inputs, which may have had different inputs or a different
          data-query.
    FAILED = -2: These quanta were attempted and failed. Failed quanta have
        logs and no metadata.
    UNKNOWN = -1: These are quanta which do not have any metadata associated
        with processing, but for which it is impossible to tell the status due
        to an additional absence of logs. Quanta which had not been processed
        at all would reflect this state, as would quanta which were
        conceptualized in the construction of the quantum graph but later
        identified to be unneccesary or erroneous (deemed NoWorkFound by the
        Science Pipelines).
    BLOCKED = 0: The quantum is not able to execute because its inputs are
        missing due to an upstream failure. Blocked quanta are distinguished
        from failed quanta by being successors of failed quanta in the graph.
        All the successors of blocked quanta are also marked as blocked.
    SUCCESSFUL = 1: Attempts at executing this quantum were successful.
    """

    WONKY = -3
    FAILED = -2
    UNKNOWN = -1
    BLOCKED = 0
    SUCCESSFUL = 1


class QuantumInfo(TypedDict):
    """Information about a quantum (i.e., the combination of a task label and
    data ID) across all attempted runs.

    Used to annotate the networkx node dictionary.
    """

    data_id: DataCoordinate
    """The data_id of the quantum.
    """

    runs: dict[str, QuantumRun]
    """All run collections associated with the quantum.
    """

    status: QuantumInfoStatus
    """The overall status of the quantum. Note that it is impossible to exit a
    wonky state.
    """

    recovered: bool
    """The quantum was originally not successful but was ultimately successful.
    """

    messages: list[str]
    """Diagnostic messages to help disambiguate wonky states.
    """

    log: DatasetKey
    """The `DatasetKey` which can be used to access the log associated with the
    quantum across runs.
    """

    metadata: DatasetKey
    """The `DatasetKey` which can be used to access the metadata for the
    quantum across runs.
    """


class DatasetRun(pydantic.BaseModel):
    """Information about a dataset in a given run collection."""

    id: uuid.UUID
    """The dataset ID associated with the dataset in a specific run.
    """

    produced: bool = False
    """Whether the specific run wrote the dataset.
    """

    visible: bool = False
    """Whether this dataset is visible in the final output collection; in other
    words, whether this dataset is queryable in a find-first search. This
    determines whether it will be used as an input to further processing.
    """

    @pydantic.model_validator(mode="after")
    def _validate(self) -> DatasetRun:
        """Validate the model for `DatasetRun` by asserting that no visible
        `DatasetRun` is also not produced (this should be impossible).

        Returns
        -------
        self : `DatasetRun`
            The `DatasetRun` object, validated.
        """
        assert not (self.visible and not self.produced)
        return self


class DatasetInfoStatus(Enum):
    """Status of the the DatasetType-dataID pair over all runs. This depends
    not only on the presence of the dataset itself, but also on metadata, logs
    and the state of its producer quantum.

    Possible Statuses
    -----------------
    CURSED: The dataset was the result of an unsuccessful quantum and was
        visible in the output collection anyway. These are flagged as
        cursed so that they may be caught before they become inputs to
        further processing.
    UNSUCCESSFUL: The dataset was not produced. These are the results of
        failed or blocked quanta.
    PREDICTED_ONLY: The dataset was predicted, and was not visible in any
        run, but was the successor of a successful quantum. These datasets are
        the result of pipelines NoWorkFound cases, in which a dataset is
        predicted in the graph but found to not be necessary in processing.
    SHADOWED: The dataset exists but is not queryable in a find_first
        search. This could mean that the version of this dataset which is
        passed as an input to further processing is not in the collections
        given. A shadowed dataset will not be used as an input to further
        processing.
    VISIBLE: The dataset is queryable in a find_first search. This means
        that it can be used as an input by subsequent tasks and processing.
    """

    CURSED = -2
    UNSUCCESSFUL = -1
    PREDICTED_ONLY = 0
    SHADOWED = 1
    VISIBLE = 2


class DatasetInfo(TypedDict):
    """Information about a given dataset across all runs.

    Used to annotate the networkx node dictionary.
    """

    data_id: DataCoordinate
    """The data_id of the quantum.
    """

    runs: dict[str, DatasetRun]
    """All runs associated with the dataset.
    """

    status: DatasetInfoStatus
    """Overall status of the dataset.
    """

    messages: list[str]
    """Diagnostic messages to help disambiguate cursed states.
    """


class UnsuccessfulQuantumSummary(pydantic.BaseModel):
    """A summary of all relevant information on an unsuccessful quantum.

    This summarizes all information on a task's output for a particular data ID
    over all runs.
    """

    data_id: dict[str, DataIdValue]
    """The data_id of the unsuccessful quantum.
    """
    runs: dict[str, str]
    """A dictionary (key: output run collection name) with the value of the
    enum name of the `QuantumRunStatus` of each run associated with an attempt
    to process the unsuccessful quantum.
    """
    messages: list[str]
    """Any messages associated with the unsuccessful quantum (any clues as to
    why the quantum may be in a FAILED or WONKY state).
    """

    @classmethod
    def from_info(cls, info: QuantumInfo) -> UnsuccessfulQuantumSummary:
        """Summarize all relevant information from the `QuantumInfo` in an
        `UnsuccessfulQuantumSummary`; return an `UnsuccessfulQuantumSummary`.

        Parameters
        ----------
        info : `QuantumInfo`
            The `QuantumInfo` object for the unsuccessful quantum.

        Returns
        -------
        summary : `UnsuccessfulQuantumSummary`
            A Pydantic model containing the dataID, run collection names (and
            each of their `QuantumRunStatus` enum names) as well as messages
            which may point to any clues about the nature of the problem. For
            failed quanta, these are usually error messages from the butler
            logs. For wonky quanta, these can be messages generated during the
            assembly of the `QuantumProvenanceGraph` that describe why it was
            marked as wonky.
        """
        return cls(
            data_id=dict(info["data_id"].required),
            runs={k: v.status.name for k, v in info["runs"].items()},
            messages=info["messages"],
        )


class TaskSummary(pydantic.BaseModel):
    """A summary of the status of all quanta associated with a single task,
    across all runs.
    """

    n_successful: int = 0
    """A count of successful quanta.
    """
    n_blocked: int = 0
    """A count of blocked quanta.
    """
    n_unknown: int = 0
    """A count of quanta for which there are no metadata or logs.
    """

    n_expected: int = 0
    """The number of quanta expected by the graph.
    """

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def n_wonky(self) -> int:
        """Return a count of `wonky` quanta."""
        return len(self.wonky_quanta)

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def n_failed(self) -> int:
        """Return a count of `failed` quanta."""
        return len(self.failed_quanta)

    failed_quanta: list[UnsuccessfulQuantumSummary] = pydantic.Field(default_factory=list)
    """A list of all `UnsuccessfulQuantumSummary` objects associated with the
    FAILED quanta. This is a report containing their data IDs, the status
    of each run associated with each `failed` quantum, and the error messages
    associated with the failures when applicable.
    """
    recovered_quanta: list[dict[str, DataIdValue]] = pydantic.Field(default_factory=list)
    """A list of dataIDs (key->value) which moved from an unsuccessful to
    successful state.
    """
    wonky_quanta: list[UnsuccessfulQuantumSummary] = pydantic.Field(default_factory=list)
    """A list of all `UnsuccessfulQuantumSummary` objects associated with the
    WONKY quanta. This is a report containing their data_ids, the status of
    each run associated with each `wonky` quantum, and messages (dictated in
    this module) associated with the particular issue identified.
    """

    def add_quantum_info(self, info: QuantumInfo, butler: Butler, do_store_logs: bool = True) -> None:
        """Add a `QuantumInfo` to a `TaskSummary`.

        Unpack the `QuantumInfo` object, sorting quanta of each status into
        the correct place in the `TaskSummary`. If looking for error messages
        in the `Butler` logs is desired, take special care to catch issues
        with missing logs.

        Parameters
        ----------
        info : `QuantumInfo`
            The `QuantumInfo` object to add to the `TaskSummary`.
        butler : `lsst.daf.butler.Butler`
            The butler repo used for the graph being inspected, which can be
            queried for errors and logs.
        do_store_logs : `bool`, optional
            Store error messages from Butler logs associated with failed quanta
            if `True`.
        """
        match info["status"]:
            case QuantumInfoStatus.SUCCESSFUL:
                self.n_successful += 1
                if info["recovered"]:
                    self.recovered_quanta.append(dict(info["data_id"].required))
            case QuantumInfoStatus.WONKY:
                self.wonky_quanta.append(UnsuccessfulQuantumSummary.from_info(info))
            case QuantumInfoStatus.BLOCKED:
                self.n_blocked += 1
            case QuantumInfoStatus.FAILED:
                failed_quantum_summary = UnsuccessfulQuantumSummary.from_info(info)
                log_key = info["log"]
                if do_store_logs:
                    for run in info["runs"]:
                        try:
                            # should probably upgrade this to use a dataset
                            # ref
                            log = butler.get(log_key.dataset_type_name, info["data_id"], collections=run)
                        except LookupError:
                            failed_quantum_summary.messages.append(f"Logs not ingested for {run!r}")
                        except FileNotFoundError:
                            failed_quantum_summary.messages.append(f"Logs missing or corrupt for {run!r}")
                        else:
                            failed_quantum_summary.messages.extend(
                                [record.message for record in log if record.levelno >= logging.ERROR]
                            )
                self.failed_quanta.append(failed_quantum_summary)
            case QuantumInfoStatus.UNKNOWN:
                self.n_unknown += 1
            case unrecognized_state:
                raise AssertionError(f"Unrecognized quantum status {unrecognized_state!r}")

    def add_data_id_group(self, other_summary: TaskSummary) -> None:
        """Add information from a `TaskSummary` over one dataquery-identified
        group to another, as part of aggregating `Summary` reports.

        Parameters
        ----------
        other_summary : `TaskSummary`
            `TaskSummary` to aggregate.
        """
        self.n_successful += other_summary.n_successful
        self.n_blocked += other_summary.n_blocked
        self.n_unknown += other_summary.n_unknown
        self.n_expected += other_summary.n_expected

        self.wonky_quanta.extend(other_summary.wonky_quanta)
        self.recovered_quanta.extend(other_summary.recovered_quanta)
        self.failed_quanta.extend(other_summary.failed_quanta)


class CursedDatasetSummary(pydantic.BaseModel):
    """A summary of all the relevant information on a cursed dataset."""

    producer_data_id: dict[str, DataIdValue]
    """The data_id of the task which produced this dataset. This is mostly
    useful for people wishing to track down the task which produced this
    cursed dataset quickly.
    """
    data_id: dict[str, DataIdValue]
    """The data_id of the cursed dataset.
    """
    runs_produced: dict[str, bool]
    """A dictionary of all the runs associated with the cursed dataset;
    the `bool` is true if the dataset was produced in the associated run.
    """
    run_visible: str | None
    """A dictionary of all `visible` runs containing the cursed dataset.
    """
    messages: list[str]
    """Any diagnostic messages (dictated in this module) which might help in
    understanding why or how the dataset became cursed.
    """

    @classmethod
    def from_info(cls, info: DatasetInfo, producer_info: QuantumInfo) -> CursedDatasetSummary:
        """Summarize all relevant information from the `DatasetInfo` in an
        `CursedDatasetSummary`; return a `CursedDatasetSummary`.

        Parameters
        ----------
        info : `DatasetInfo`
            All relevant information on the dataset.
        producer_info : `QuantumInfo`
            All relevant information on the producer task. This is used to
            report the data_id of the producer task.

        Returns
        -------
        summary : `CursedDatasetSummary`
            A Pydantic model containing the dataID of the task which produced
            this cursed dataset, the dataID associated with the cursed dataset,
            run collection names (and their `DatasetRun` information) as well
            as any messages which may point to any clues about the nature of
            the problem. These are be messages generated during the assembly of
            the `QuantumProvenanceGraph` that describe why it was marked as
            cursed.
        """
        runs_visible = {k for k, v in info["runs"].items() if v.visible}
        return cls(
            producer_data_id=dict(producer_info["data_id"].required),
            data_id=dict(info["data_id"].required),
            runs_produced={k: v.produced for k, v in info["runs"].items()},
            # this has at most one element
            run_visible=runs_visible.pop() if runs_visible else None,
            messages=info["messages"],
        )


class DatasetTypeSummary(pydantic.BaseModel):
    """A summary of the status of all datasets of a particular type across all
    runs.
    """

    producer: str = ""
    """The name of the task which produced this dataset.
    """

    n_visible: int = 0
    """A count of the datasets of this type which were visible in the
    finalized collection(s).
    """
    n_shadowed: int = 0
    """A count of the datasets of this type which were produced but not
    visible. This includes any datasets which do not come up in a butler
    query over their associated collection.
    """
    n_predicted_only: int = 0
    """A count of the datasets of this type which were predicted but
    ultimately not produced. Note that this does not indicate a failure,
    which are accounted for differently. This is commonly referred to as
    a `NoWorkFound` case.
    """
    n_expected: int = 0
    """The number of datasets of this type expected by the graph.
    """

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def n_cursed(self) -> int:
        """Return a count of cursed datasets."""
        return len(self.cursed_datasets)

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def n_unsuccessful(self) -> int:
        """Return a count of unsuccessful datasets."""
        return len(self.unsuccessful_datasets)

    cursed_datasets: list[CursedDatasetSummary] = pydantic.Field(default_factory=list)
    """A list of all `CursedDatasetSummary` objects associated with the
    cursed datasets. This is a report containing their data_ids and the
    data_ids of their producer task, the status of each run associated with
    each `cursed` dataset, and messages (dictated in this module) associated
    with the particular issue identified.
    """
    unsuccessful_datasets: list[dict[str, DataIdValue]] = pydantic.Field(default_factory=list)
    """A list of all unsuccessful datasets by their name and data_id.
    """

    def add_dataset_info(self, info: DatasetInfo, producer_info: QuantumInfo) -> None:
        """Add a `DatasetInfo` to a `DatasetTypeSummary`.

        Unpack the `DatasetInfo` object, sorting datasets of each status into
        the correct place in the `DatasetTypeSummary`. If the status of a
        dataset is not valid, raise an `AssertionError`.

        Parameters
        ----------
        info : `DatasetInfo`
            The `DatasetInfo` object to add to the `DatasetTypeSummary`.
        producer_info : `QuantumInfo`
            The `QuantumInfo` object associated with the producer of the
            dataset. This is used to report the producer task in the
            summaries for cursed datasets, which may help identify
            specific issues.
        """
        match info["status"]:
            case DatasetInfoStatus.VISIBLE:
                self.n_visible += 1
            case DatasetInfoStatus.SHADOWED:
                self.n_shadowed += 1
            case DatasetInfoStatus.UNSUCCESSFUL:
                self.unsuccessful_datasets.append(dict(info["data_id"].mapping))
            case DatasetInfoStatus.CURSED:
                self.cursed_datasets.append(CursedDatasetSummary.from_info(info, producer_info))
            case DatasetInfoStatus.PREDICTED_ONLY:
                self.n_predicted_only += 1
            case unrecognized_state:
                raise AssertionError(f"Unrecognized dataset status {unrecognized_state!r}")

    def add_data_id_group(self, other_summary: DatasetTypeSummary) -> None:
        """Add information from a `DatasetTypeSummary` over one
        dataquery-identified group to another, as part of aggregating `Summary`
        reports.

        Parameters
        ----------
        other_summary : `DatasetTypeSummary`
            `DatasetTypeSummary` to aggregate.
        """
        if self.producer and other_summary.producer:
            # Guard against empty string
            if self.producer != other_summary.producer:
                _LOG.warning(
                    "Producer for dataset type is not consistent: %r != %r.",
                    self.producer,
                    other_summary.producer,
                )
                _LOG.warning("Ignoring %r.", other_summary.producer)
        else:
            if other_summary.producer and not self.producer:
                self.producer = other_summary.producer

        self.n_visible += other_summary.n_visible
        self.n_shadowed += other_summary.n_shadowed
        self.n_predicted_only += other_summary.n_predicted_only
        self.n_expected += other_summary.n_expected

        self.cursed_datasets.extend(other_summary.cursed_datasets)
        self.unsuccessful_datasets.extend(other_summary.unsuccessful_datasets)


class Summary(pydantic.BaseModel):
    """A summary of the contents of the QuantumProvenanceGraph, including
    all information on the quanta for each task and the datasets of each
    `DatasetType`.
    """

    tasks: dict[str, TaskSummary] = pydantic.Field(default_factory=dict)
    """Summaries for the tasks and their quanta.
    """

    datasets: dict[str, DatasetTypeSummary] = pydantic.Field(default_factory=dict)
    """Summaries for the datasets.
    """

    @classmethod
    def aggregate(cls, summaries: Sequence[Summary]) -> Summary:
        """Combine summaries from disjoint data id groups into an overall
        summary of common tasks and datasets. Intended for use when the same
        pipeline has been run over all groups.

        Parameters
        ----------
        summaries : `Sequence[Summary]`
            Sequence of all `Summary` objects to aggregate.
        """
        result = cls()
        for summary in summaries:
            for label, task_summary in summary.tasks.items():
                result_task_summary = result.tasks.setdefault(label, TaskSummary())
                result_task_summary.add_data_id_group(task_summary)
            for dataset_type, dataset_type_summary in summary.datasets.items():
                result_dataset_summary = result.datasets.setdefault(dataset_type, DatasetTypeSummary())
                result_dataset_summary.add_data_id_group(dataset_type_summary)
        return result


class QuantumProvenanceGraph:
    """A set of already-run, merged quantum graphs with provenance
    information.

    Step through all the quantum graphs associated with certain tasks or
    processing steps. For each graph/attempt, the status of each quantum and
    dataset is recorded in `QuantumProvenanceGraph.__add_new_graph` and
    outcomes of quanta over multiple runs are resolved in
    `QuantumProvenanceGraph.__resolve_duplicates`. These can be called outside
    the class in the correct order by
    `QuantumProvenanceGraph.assemble_quantum_provenance_graph`. At the end of
    this process, we can combine all attempts into a summary using the
    `QuantumProvenanceGraph.to_summary` method. This serves to answer the
    question 'What happened to this data ID?' in a wholistic sense.
    """

    def __init__(self) -> None:
        # The graph we annotate as we step through all the graphs associated
        # with the processing to create the `QuantumProvenanceGraph`.
        self._xgraph = networkx.DiGraph()
        # The nodes representing quanta in `_xgraph` grouped by task label.
        self._quanta: dict[str, set[QuantumKey]] = {}
        # The nodes representing datasets in `_xgraph` grouped by dataset type
        # name.
        self._datasets: dict[str, set[DatasetKey]] = {}
        # Bool representing whether the graph has been finalized. This is set
        # to True when resolve_duplicates completes.
        self._finalized: bool = False

    def get_quantum_info(self, key: QuantumKey) -> QuantumInfo:
        """Get a `QuantumInfo` object from the `QuantumProvenanceGraph` using
        a `QuantumKey`.

        Parameters
        ----------
        key : `QuantumKey`
            The key used to refer to the node on the graph.

        Returns
        -------
        quantum_info : `QuantumInfo`
            The `TypedDict` with information on the task label-dataID pair
            across all runs.
        """
        return self._xgraph.nodes[key]

    def get_dataset_info(self, key: DatasetKey) -> DatasetInfo:
        """Get a `DatasetInfo` object from the `QuantumProvenanceGraph` using
        a `DatasetKey`.

        Parameters
        ----------
        key : `DatasetKey`
            The key used to refer to the node on the graph.

        Returns
        -------
        dataset_info : `DatasetInfo`
            The `TypedDict` with information about the `DatasetType`-dataID
            pair across all runs.
        """
        return self._xgraph.nodes[key]

    def __add_new_graph(self, butler: Butler, qgraph: QuantumGraph | ResourcePathExpression) -> None:
        """Add a new quantum graph to the `QuantumProvenanceGraph`.

        Notes
        -----
        The algorithm: step through the quantum graph. Annotate a
        `networkx.DiGraph` (`QuantumProvenanceGraph._xgraph`) with all of the
        relevant information: quanta, dataset types and their associated run
        collections (these unique quanta- and dataset type-run
        collection combinations are encapsulated in the classes
        `DatasetRun` and `QuantumRun`). For each new quantum, annotate
        the status of the `QuantumRun` by inspecting the graph. If a
        DatasetType was produced, annotate this in the run by setting
        `DatasetRun.produced = True`. If a quantum is given BLOCKED
        or FAILED status, annotate all their successors in the graph
        as BLOCKED. For each new quantum, use the transition between
        the current and last `QuantumRun.status` to determine the status
        to assign to the overall `QuantumInfo`. For example, if a
        previous run associated with a quantum had the status FAILED,
        and the status from the new graph reads SUCCESSFUL, we can
        mark the overall quantum status as SUCCESSFUL and list the data_id
        as RECOVERED.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler
            used for the run associated with the executed quantum graph.

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
            # make a key to refer to the quantum and add it to the quantum
            # provenance graph.
            quantum_key = QuantumKey(
                node.taskDef.label, cast(DataCoordinate, node.quantum.dataId).required_values
            )
            self._xgraph.add_node(quantum_key)
            # use the key to get a `QuantumInfo` object for the quantum
            # and set defaults for its values.
            quantum_info = self.get_quantum_info(quantum_key)
            quantum_info.setdefault("messages", [])
            quantum_info.setdefault("runs", {})
            quantum_info.setdefault("data_id", cast(DataCoordinate, node.quantum.dataId))
            quantum_info.setdefault("status", QuantumInfoStatus.UNKNOWN)
            quantum_info.setdefault("recovered", False)
            new_quanta.append(quantum_key)
            self._quanta.setdefault(quantum_key.task_label, set()).add(quantum_key)
            # associate run collections with specific quanta. this is important
            # if the same quanta are processed in multiple runs as in recovery
            # workflows.
            quantum_runs = quantum_info.setdefault("runs", {})
            # the `QuantumRun` here is the specific quantum-run collection
            # combination.
            quantum_runs[output_run] = QuantumRun(id=node.nodeId)
            # For each of the outputs of the quanta (datasets) make a key to
            # refer to the dataset.
            for ref in itertools.chain.from_iterable(node.quantum.outputs.values()):
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                # add datasets to the nodes of the graph, with edges on the
                # quanta.
                self._xgraph.add_edge(quantum_key, dataset_key)
                # use the dataset key to make a `DatasetInfo` object for
                # the dataset and set defaults for its values.
                dataset_info = self.get_dataset_info(dataset_key)
                dataset_info.setdefault("data_id", ref.dataId)
                dataset_info.setdefault("status", DatasetInfoStatus.PREDICTED_ONLY)
                dataset_info.setdefault("messages", [])
                self._datasets.setdefault(dataset_key.dataset_type_name, set()).add(dataset_key)
                dataset_runs = dataset_info.setdefault("runs", {})
                # make a `DatasetRun` for the specific dataset-run
                # collection combination.
                dataset_runs[output_run] = DatasetRun(id=ref.id)
                # save metadata and logs for easier status interpretation later
                if dataset_key.dataset_type_name.endswith("_metadata"):
                    quantum_info["metadata"] = dataset_key
                if dataset_key.dataset_type_name.endswith("_log"):
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
                dataset_info = self.get_dataset_info(dataset_key)
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
            # if we do have metadata, we know that the task finished.
            if metadata_dataset_run.produced:
                # if we also have logs, this is a success.
                if log_dataset_run.produced:
                    quantum_run.status = QuantumRunStatus.SUCCESSFUL
                else:
                    # if we have metadata and no logs, this is a very rare
                    # case. either the task ran successfully and the datastore
                    # died immediately afterwards, or some supporting
                    # infrastructure for transferring the logs to the datastore
                    # failed.
                    quantum_run.status = QuantumRunStatus.LOGS_MISSING
            # missing metadata means that the task did not finish.
            else:
                # if we have logs and no metadata, the task not finishing is
                # a failure in the task itself. This includes all payload
                # errors and some other problems.
                if log_dataset_run.produced:
                    quantum_run.status = QuantumRunStatus.FAILED
                    # if a quantum fails, all its successor datasets are
                    # blocked.
                    blocked.update(self._xgraph.successors(quantum_key))
                # if we are missing metadata and logs, either the task was not
                # started, or a hard external environmental error prevented
                # it from writing logs or metadata.
                else:
                    # if none of this quantum's inputs were blocked, the
                    # metadata must just be missing.
                    if blocked.isdisjoint(self._xgraph.predecessors(quantum_key)):
                        # None of this quantum's inputs were blocked.
                        quantum_run.status = QuantumRunStatus.METADATA_MISSING
                    # otherwise we can assume from no metadata and no logs
                    # that the task was blocked by an upstream failure.
                    else:
                        quantum_run.status = QuantumRunStatus.BLOCKED
                        blocked.update(self._xgraph.successors(quantum_key))

            # Now we can start using state transitions to mark overall status.
            last_status = quantum_info["status"]
            new_status: QuantumInfoStatus
            match last_status, quantum_run.status:
                # A quantum can never escape a WONKY state.
                case (QuantumInfoStatus.WONKY, _):
                    new_status = QuantumInfoStatus.WONKY
                # Any transition to a success (excluding from WONKY) is
                # a success; any transition from a failed state is also a
                # recovery.
                case (_, QuantumRunStatus.SUCCESSFUL):
                    new_status = QuantumInfoStatus.SUCCESSFUL
                    if (
                        last_status != QuantumInfoStatus.SUCCESSFUL
                        and last_status != QuantumInfoStatus.UNKNOWN
                    ):
                        quantum_info["recovered"] = True
                # Missing logs are one of the categories of wonky quanta. They
                # interfere with our ability to discern quantum status and are
                # signs of weird things afoot in processing. Add a message
                # noting why this quantum is being marked as wonky to be stored
                # in its `UnsuccessfulQuantumInfo`.
                case (_, QuantumRunStatus.LOGS_MISSING):
                    new_status = QuantumInfoStatus.WONKY
                    quantum_info["messages"].append(f"Logs missing for run {output_run!r}.")
                # Leaving a successful state is another category of wonky
                # quanta. If a previous success fails on a subsequent run,
                # a human should inspect why. Add a message noting why this
                # quantum is being marked as wonky to be stored in its
                # `UnsuccessfulQuantumInfo`.
                case (QuantumInfoStatus.SUCCESSFUL, _):
                    new_status = QuantumInfoStatus.WONKY
                    quantum_info["messages"].append(
                        f"Status went from successful in run {list(quantum_info['runs'].values())[-1]!r} "
                        f"to {quantum_run.status!r} in {output_run!r}."
                    )
                # If a quantum status is unknown and it moves to blocked, we
                # know for sure that it is a blocked quantum.
                case (QuantumInfoStatus.UNKNOWN, QuantumRunStatus.BLOCKED):
                    new_status = QuantumInfoStatus.BLOCKED
                # A transition into blocked does not change the overall quantum
                # status for a failure.
                case (_, QuantumRunStatus.BLOCKED):
                    new_status = last_status
                # If a quantum transitions from any state into missing
                # metadata, we don't have enough information to diagnose its
                # state.
                case (_, QuantumRunStatus.METADATA_MISSING):
                    new_status = QuantumInfoStatus.UNKNOWN
                # Any transition into failure is a failed state.
                case (_, QuantumRunStatus.FAILED):
                    new_status = QuantumInfoStatus.FAILED
            # Update `QuantumInfo.status` for this quantum.
            quantum_info["status"] = new_status

    def __resolve_duplicates(
        self,
        butler: Butler,
        collections: Sequence[str] | None = None,
        where: str = "",
        curse_failed_logs: bool = False,
    ) -> None:
        """After quantum graphs associated with each run have been added
        to the `QuantumProvenanceGraph, resolve any discrepancies between
        them and use all attempts to finalize overall status.

        Particularly, use the state of each `DatasetRun` in combination with
        overall quantum status to ascertain the status of each dataset.
        Additionally, if there are multiple visible runs associated with a
        dataset, mark the producer quantum as WONKY.

        This method should be called after
        `QuantumProvenanceGraph.__add_new_graph` has been called on every graph
        associated with the data processing.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.

        collections : `Sequence` [`str`] | `None`
            Collections to use in `lsst.daf.butler.registry.queryDatasets` if
            paring down the query would be useful.

        where : `str`
            A "where" string to use to constrain the collections, if passed.

        curse_failed_logs : `bool`
            Mark log datasets as CURSED if they are visible in the final
            output collection. Note that a campaign-level collection must be
            used here for `collections` if `curse_failed_logs` is `True`; if
            `__resolve_duplicates` is run on a list of group-level collections
            then each will only show log datasets from their own failures as
            visible and datasets from others will be marked as cursed.
        """
        # First thing: raise an error if resolve_duplicates has been run
        # before on this qpg.
        if self._finalized:
            raise RuntimeError(
                """resolve_duplicates may only be called on a
                QuantumProvenanceGraph once. Call only after all graphs have
                been added, or make a new graph with all constituent
                attempts."""
            )
        for dataset_type_name in self._datasets:
            # find datasets in a larger collection.
            for ref in butler.registry.queryDatasets(
                dataset_type_name,
                collections=collections,
                findFirst=True,
                where=where,
            ):
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                try:
                    dataset_info = self.get_dataset_info(dataset_key)
                # Ignore if we don't actually have the dataset in any of the
                # graphs given.
                except KeyError:
                    continue
                # queryable datasets are `visible`.
                dataset_info["runs"][ref.run].visible = True

        for task_quanta in self._quanta.values():
            for quantum_key in task_quanta:
                # runs associated with visible datasets.
                visible_runs: set[str] = set()
                quantum_info = self.get_quantum_info(quantum_key)
                # Loop over each dataset in the outputs of a single quantum.
                for dataset_key in self.iter_outputs_of(quantum_key):
                    dataset_info = self.get_dataset_info(dataset_key)
                    visible_runs.update(
                        run for run, dataset_run in dataset_info["runs"].items() if dataset_run.visible
                    )
                    if any(dataset_run.visible for dataset_run in dataset_info["runs"].values()):
                        query_state = "visible"
                    # set the publish state to `shadowed` if the dataset was
                    # produced but not visible (i.e., not queryable from the
                    # final collection(s)).
                    elif any(dataset_run.produced for dataset_run in dataset_info["runs"].values()):
                        query_state = "shadowed"
                    # a dataset which was not produced and not visible is
                    # missing.
                    else:
                        query_state = "missing"
                    # use the quantum status and publish state to ascertain the
                    # status of the dataset.
                    match (quantum_info["status"], query_state):
                        # visible datasets from successful quanta are as
                        # intended.
                        case (QuantumInfoStatus.SUCCESSFUL, "visible"):
                            dataset_info["status"] = DatasetInfoStatus.VISIBLE
                        # missing datasets from successful quanta indicate a
                        # `NoWorkFound` case.
                        case (QuantumInfoStatus.SUCCESSFUL, "missing"):
                            dataset_info["status"] = DatasetInfoStatus.PREDICTED_ONLY
                        case (QuantumInfoStatus.SUCCESSFUL, "shadowed"):
                            dataset_info["status"] = DatasetInfoStatus.SHADOWED
                        # If anything other than a successful quantum produces
                        # a visible dataset, that dataset is cursed. Set the
                        # status for the dataset to cursed and note the reason
                        # for labeling the dataset as cursed.
                        case (_, "visible"):
                            # Avoiding publishing failed logs is difficult
                            # without using tagged collections, so flag them as
                            # merely unsuccessful unless the user requests it.
                            if dataset_type_name.endswith("_log") and not curse_failed_logs:
                                dataset_info["status"] = DatasetInfoStatus.UNSUCCESSFUL
                            else:
                                dataset_info["status"] = DatasetInfoStatus.CURSED
                                dataset_info["messages"].append(
                                    f"Unsuccessful dataset {dataset_type_name} visible in "
                                    "final output collection."
                                )
                        # any other produced dataset (produced but not
                        # visible and not successful) is a regular
                        # failure.
                        case _:
                            dataset_info["status"] = DatasetInfoStatus.UNSUCCESSFUL
                if len(visible_runs) > 1:
                    quantum_info["status"] = QuantumInfoStatus.WONKY
                    quantum_info["messages"].append(
                        f"Outputs from different runs of the same quanta were visible: {visible_runs}."
                    )
                    for dataset_key in self.iter_outputs_of(quantum_key):
                        dataset_info = self.get_dataset_info(dataset_key)
                        quantum_info["messages"].append(
                            f"{dataset_key.dataset_type_name}"
                            + f"from {str(dataset_info['runs'])};"
                            + f"{str(dataset_info['status'])}"
                        )
        # If we make it all the way through resolve_duplicates, set
        # self._finalized = True so that it cannot be run again.
        self._finalized = True

    def assemble_quantum_provenance_graph(
        self,
        butler: Butler,
        qgraphs: Sequence[QuantumGraph | ResourcePathExpression],
        collections: Sequence[str] | None = None,
        where: str = "",
        curse_failed_logs: bool = False,
    ) -> None:
        """Assemble the quantum provenance graph from a list of all graphs
        corresponding to processing attempts.

        This method calls the private method `__add_new_graph` on each of the
        constituent graphs, verifying that the graphs have been passed in
        order. After `__add_new_graph` has been called on all graphs in the
        `Sequence`, the method calls `__resolve_duplicates`.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.
        qgraphs : `Sequence` [`QuantumGraph` | `ResourcePathExpression`]
            A list of either quantum graph objects or their uri's, to be used
            to assemble the `QuantumProvenanceGraph`.
        collections : `Sequence` [`str`] | `None`
            Collections to use in `lsst.daf.butler.registry.queryDatasets` if
            paring down the query would be useful.
        where : `str`
            A "where" string to use to constrain the collections, if passed.
        curse_failed_logs : `bool`
            Mark log datasets as CURSED if they are visible in the final
            output collection. Note that a campaign-level collection must be
            used here for `collections` if `curse_failed_logs` is `True`; if
            `__resolve_duplicates` is run on a list of group-level collections
            then each will only show log datasets from their own failures as
            visible and datasets from others will be marked as cursed.
        """
        output_runs = []
        for graph in qgraphs:
            qgraph = graph if isinstance(graph, QuantumGraph) else QuantumGraph.loadUri(graph)
            assert qgraph.metadata is not None, "Saved QGs always have metadata."
            self.__add_new_graph(butler, qgraph)
            output_runs.append(qgraph.metadata["output_run"])
        # If the user has not passed a `collections` variable
        if not collections:
            # We reverse the order of the associated output runs because the
            # query in __resolve_duplicates must be done most recent-first.
            collections = list(reversed(output_runs))
            assert (
                not curse_failed_logs
            ), "curse_failed_logs option must be used with one campaign-level collection."
        self.__resolve_duplicates(butler, collections, where, curse_failed_logs)

    def to_summary(self, butler: Butler, do_store_logs: bool = True) -> Summary:
        """Summarize the `QuantumProvenanceGraph`.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report.
        do_store_logs : `bool`
            Store the logs in the summary dictionary.

        Returns
        -------
        result : `Summary`
            A struct containing counts of quanta and datasets in each of
            the overall states defined in `QuantumInfo` and `DatasetInfo`,
            as well as diagnostic information and error messages for failed
            quanta and strange edge cases, and a list of recovered quanta.
        """
        if not self._finalized:
            raise RuntimeError(
                """resolve_duplicates must be called to finalize the
                QuantumProvenanceGraph before making a summary."""
            )
        result = Summary()
        for task_label, quanta in self._quanta.items():
            task_summary = TaskSummary()
            task_summary.n_expected = len(quanta)
            for quantum_key in quanta:
                quantum_info = self.get_quantum_info(quantum_key)
                task_summary.add_quantum_info(quantum_info, butler, do_store_logs)
            result.tasks[task_label] = task_summary

        for dataset_type_name, datasets in self._datasets.items():
            dataset_type_summary = DatasetTypeSummary(producer="")
            dataset_type_summary.n_expected = len(datasets)
            for dataset_key in datasets:
                dataset_info = self.get_dataset_info(dataset_key)
                producer_key = self.get_producer_of(dataset_key)
                producer_info = self.get_quantum_info(producer_key)
                # Not ideal, but hard to get out of the graph at the moment.
                # Change after DM-40441
                dataset_type_summary.producer = producer_key.task_label
                dataset_type_summary.add_dataset_info(dataset_info, producer_info)

            result.datasets[dataset_type_name] = dataset_type_summary
        return result

    def iter_outputs_of(self, quantum_key: QuantumKey) -> Iterator[DatasetKey]:
        """Iterate through the outputs of a quantum, yielding all the
        `DatasetKey`s produced by the quantum.

        Parameters
        ----------
        quantum_key : `QuantumKey`
            The key for the quantum whose outputs are needed.
        """
        for dataset_key in self._xgraph.successors(quantum_key):
            yield dataset_key

    def get_producer_of(self, dataset_key: DatasetKey) -> QuantumKey:
        """Unpack the predecessor (producer quantum) of a given dataset key
        from a graph.

        Parameters
        ----------
        dataset_key : `DatasetKey`
            The key for the dataset whose producer quantum is needed.

        Returns
        -------
        result : `QuantumKey`
            The key for the quantum which produced the dataset.
        """
        (result,) = self._xgraph.predecessors(dataset_key)
        return result
