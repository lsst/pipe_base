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
    "DatasetKey",
    "PrerequisiteDatasetKey",
    "QuantumKey",
    "QuantumProvenanceGraph",
)

import concurrent.futures
import dataclasses
import datetime
import itertools
import logging
import textwrap
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from enum import Enum
from typing import Any, ClassVar, Literal, TypedDict, cast

import astropy.table
import networkx
import pydantic

from lsst.daf.butler import (
    Butler,
    ButlerConfig,
    ButlerLogRecords,
    DataCoordinate,
    DataIdValue,
    DatasetId,
    DatasetRef,
    DatasetType,
    DimensionUniverse,
    LimitedButler,
    MissingDatasetTypeError,
    QuantumBackedButler,
)
from lsst.resources import ResourcePathExpression
from lsst.utils.logging import PeriodicLogger, getLogger

from ._status import ExceptionInfo, QuantumSuccessCaveats
from .automatic_connection_constants import (
    LOG_OUTPUT_CONNECTION_NAME,
    LOG_OUTPUT_TEMPLATE,
    METADATA_OUTPUT_CONNECTION_NAME,
    METADATA_OUTPUT_STORAGE_CLASS,
    METADATA_OUTPUT_TEMPLATE,
)
from .graph import QuantumGraph, QuantumNode

_LOG = getLogger(__name__)


@dataclasses.dataclass(slots=True, eq=True, frozen=True)
class QuantumKey:
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


@dataclasses.dataclass(slots=True, eq=True, frozen=True)
class DatasetKey:
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


@dataclasses.dataclass(slots=True, eq=True, frozen=True)
class PrerequisiteDatasetKey:
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

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)  # for DatasetRef attrs.

    id: uuid.UUID
    """The quantum graph node ID associated with the dataId in a specific run.
    """

    status: QuantumRunStatus = QuantumRunStatus.METADATA_MISSING
    """The status of the quantum in that run.
    """

    caveats: QuantumSuccessCaveats | None = None
    """Flags that describe possibly-qualified successes.

    This is `None` when `status` is not `SUCCESSFUL` or `LOGS_MISSING`.  It
    may also be `None` if metadata was not loaded or had no success flags.
    """

    exception: ExceptionInfo | None = None
    """Information about an exception that that was raised during the quantum's
    execution.

    Exception information for failed quanta is not currently stored, so this
    field is actually only populated for quanta that raise
    `AnnotatedPartialOutputsError`, and only when the execution system is
    configured not to consider these partial successes a failure (i.e. when
    `status` is `~QuantumRunStatus.SUCCESSFUL` and `caveats` has
    `~QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR` set.  The error whose
    information is reported here is the exception chained from the
    `AnnotatedPartialOutputsError`.

    In the future, exception information from failures may be available as
    well.
    """

    metadata_ref: DatasetRef
    """Predicted DatasetRef for the metadata dataset."""

    log_ref: DatasetRef
    """Predicted DatasetRef for the log dataset."""

    @staticmethod
    def find_final(info: QuantumInfo) -> tuple[str, QuantumRun]:
        """Return the final RUN collection name and `QuantumRun` structure from
        a `QuantumInfo` dictionary.

        The "final run" is the last RUN collection in the sequence of quantum
        graphs that:

        - actually had a quantum for that task label and data ID;
        - execution seems to have at least been attempted (at least one of
          metadata or logs were produced).

        Parameters
        ----------
        info : `QuantumInfo`
            Quantum information that includes all runs.

        Returns
        -------
        run : `str`
            RUN collection name.
        quantum_run : `QuantumRun`
            Information about a quantum in a RUN collection.

        Raises
        ------
        ValueError
            Raised if this quantum never had a status that suggested execution
            in any run.
        """
        for run, quantum_run in reversed(info["runs"].items()):
            if (
                quantum_run.status is not QuantumRunStatus.METADATA_MISSING
                and quantum_run.status is not QuantumRunStatus.BLOCKED
            ):
                return run, quantum_run
        raise ValueError("Quantum was never executed.")


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
    def _from_info(cls, info: QuantumInfo) -> UnsuccessfulQuantumSummary:
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


class ExceptionInfoSummary(pydantic.BaseModel):
    """A summary of an exception raised by a quantum."""

    quantum_id: uuid.UUID
    """Unique identifier for this quantum in this run."""

    data_id: dict[str, DataIdValue]
    """The data ID of the quantum."""

    run: str
    """Name of the RUN collection in which this exception was (last) raised."""

    exception: ExceptionInfo
    """Information about an exception chained to
    `AnnotatedPartialOutputsError`, if that was raised by this quantum in this
    run.
    """


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

    caveats: dict[str, list[dict[str, DataIdValue]]] = pydantic.Field(default_factory=dict)
    """Quanta that were successful with caveats.

    Keys are 2-character codes returned by `QuantumSuccessCaveats.concise`;
    values are lists of data IDs of quanta with those caveats. Quanta that were
    unqualified successes are not included.

    Quanta for which success flags were not read from metadata will not be
    included.
    """

    exceptions: dict[str, list[ExceptionInfoSummary]] = pydantic.Field(default_factory=dict)
    """Exceptions raised by partially-successful quanta.

    Keys are fully-qualified exception type names and values are lists of
    extra exception information, with each entry corresponding to different
    data ID.  Only the final RUN for each data ID is represented here.

    Every entry in this data structure corresponds to one in `ceveats` with
    the "P" code (for `QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR`).

    In the future, this may be expanded to include exceptions for failed quanta
    as well (at present that information is not retained during execution).
    """

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

    def _add_quantum_info(
        self,
        info: QuantumInfo,
        log_getter: Callable[[DatasetRef], ButlerLogRecords] | None,
        executor: concurrent.futures.Executor,
    ) -> concurrent.futures.Future[None] | None:
        """Add a `QuantumInfo` to a `TaskSummary`.

        Unpack the `QuantumInfo` object, sorting quanta of each status into
        the correct place in the `TaskSummary`. If looking for error messages
        in the `Butler` logs is desired, take special care to catch issues
        with missing logs.

        Parameters
        ----------
        info : `QuantumInfo`
            The `QuantumInfo` object to add to the `TaskSummary`.
        log_getter : `~collections.abc.Callable` or `None`
            A callable that can be passed a `~lsst.daf.butler.DatasetRef` for
            a log dataset to retreive those logs, or `None` to not load any
            logs.
        executor : `concurrent.futures.Executor`
            A possibly-parallel executor that should be used to schedule
            log dataset reads.

        Returns
        -------
        future : `concurrent.futures.Future` or `None`
            A future that represents a parallelized log read and summary
            update.
        """
        try:
            final_run, final_quantum_run = QuantumRun.find_final(info)
        except ValueError:
            final_run = None
            final_quantum_run = None
        match info["status"]:
            case QuantumInfoStatus.SUCCESSFUL:
                self.n_successful += 1
                if info["recovered"]:
                    self.recovered_quanta.append(dict(info["data_id"].required))
                if final_quantum_run is not None and final_quantum_run.caveats:
                    code = final_quantum_run.caveats.concise()
                    self.caveats.setdefault(code, []).append(dict(info["data_id"].required))
                    if final_quantum_run.caveats & QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR:
                        if final_quantum_run.exception is not None:
                            self.exceptions.setdefault(final_quantum_run.exception.type_name, []).append(
                                ExceptionInfoSummary(
                                    quantum_id=final_quantum_run.id,
                                    data_id=dict(info["data_id"].required),
                                    run=final_run,
                                    exception=final_quantum_run.exception,
                                )
                            )
                return None
            case QuantumInfoStatus.WONKY:
                self.wonky_quanta.append(UnsuccessfulQuantumSummary._from_info(info))
                return None
            case QuantumInfoStatus.BLOCKED:
                self.n_blocked += 1
                return None
            case QuantumInfoStatus.FAILED:
                failed_quantum_summary = UnsuccessfulQuantumSummary._from_info(info)
                future: concurrent.futures.Future[None] | None = None
                if log_getter:

                    def callback() -> None:
                        for quantum_run in info["runs"].values():
                            try:
                                log = log_getter(quantum_run.log_ref)
                            except LookupError:
                                failed_quantum_summary.messages.append(
                                    f"Logs not ingested for {quantum_run.log_ref!r}"
                                )
                            except FileNotFoundError:
                                failed_quantum_summary.messages.append(
                                    f"Logs missing or corrupt for {quantum_run.log_ref!r}"
                                )
                            else:
                                failed_quantum_summary.messages.extend(
                                    [record.message for record in log if record.levelno >= logging.ERROR]
                                )

                    future = executor.submit(callback)
                self.failed_quanta.append(failed_quantum_summary)
                return future
            case QuantumInfoStatus.UNKNOWN:
                self.n_unknown += 1
                return None
            case unrecognized_state:
                raise AssertionError(f"Unrecognized quantum status {unrecognized_state!r}")

    def _add_data_id_group(self, other_summary: TaskSummary) -> None:
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
        for code in self.caveats.keys() | other_summary.caveats.keys():
            self.caveats.setdefault(code, []).extend(other_summary.caveats.get(code, []))
        for type_name in self.exceptions.keys() | other_summary.exceptions.keys():
            self.exceptions.setdefault(type_name, []).extend(other_summary.exceptions.get(type_name, []))
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
    """The run collection that holds the dataset that is visible in the final
    output collection.
    """
    messages: list[str]
    """Any diagnostic messages (dictated in this module) which might help in
    understanding why or how the dataset became cursed.
    """

    @classmethod
    def _from_info(cls, info: DatasetInfo, producer_info: QuantumInfo) -> CursedDatasetSummary:
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

    def _add_dataset_info(self, info: DatasetInfo, producer_info: QuantumInfo) -> None:
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
                self.cursed_datasets.append(CursedDatasetSummary._from_info(info, producer_info))
            case DatasetInfoStatus.PREDICTED_ONLY:
                self.n_predicted_only += 1
            case unrecognized_state:
                raise AssertionError(f"Unrecognized dataset status {unrecognized_state!r}")

    def _add_data_id_group(self, other_summary: DatasetTypeSummary) -> None:
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
                result_task_summary._add_data_id_group(task_summary)
            for dataset_type, dataset_type_summary in summary.datasets.items():
                result_dataset_summary = result.datasets.setdefault(dataset_type, DatasetTypeSummary())
                result_dataset_summary._add_data_id_group(dataset_type_summary)
        return result

    def pprint(self, brief: bool = False, datasets: bool = True) -> None:
        """Print this summary to stdout, as a series of tables.

        Parameters
        ----------
        brief : `bool`, optional
            If `True`, only display short (counts-only) tables.  By default,
            per-data ID information for exceptions and failures are printed as
            well.
        datasets : `bool`, optional
            Whether to include tables of datasets as well as quanta.  This
            includes a summary table of dataset counts for various status and
            (if ``brief`` is `True`) a table with per-data ID information for
            each unsuccessful or cursed dataset.
        """
        self.make_quantum_table().pprint_all()
        print("")
        print("Caveat codes:")
        for k, v in QuantumSuccessCaveats.legend().items():
            print(f"{k}: {v}")
        print("")
        if exception_table := self.make_exception_table():
            exception_table.pprint_all()
            print("")
        if datasets:
            self.make_dataset_table().pprint_all()
            print("")
        if not brief:
            for task_label, bad_quantum_table in self.make_bad_quantum_tables().items():
                print(f"{task_label} errors:")
                bad_quantum_table.pprint_all()
                print("")
            if datasets:
                for dataset_type_name, bad_dataset_table in self.make_bad_dataset_tables().items():
                    print(f"{dataset_type_name} errors:")
                    bad_dataset_table.pprint_all()
                    print("")

    def make_quantum_table(self) -> astropy.table.Table:
        """Construct an `astropy.table.Table` with a tabular summary of the
        quanta.

        Returns
        -------
        table : `astropy.table.Table`
            A table view of the quantum information.  This only includes
            counts of status categories and caveats, not any per-data-ID
            detail.

        Notes
        -----
        Success caveats in the table are represented by their
        `~QuantumSuccessCaveats.concise` form, so when pretty-printing this
        table for users, the `~QuantumSuccessCaveats.legend` should generally
        be printed as well.
        """
        rows = []
        for label, task_summary in self.tasks.items():
            if len(task_summary.caveats) > 1:
                caveats = "(multiple)"
            elif len(task_summary.caveats) == 1:
                ((code, data_ids),) = task_summary.caveats.items()
                caveats = f"{code}({len(data_ids)})"
            else:
                caveats = ""
            rows.append(
                {
                    "Task": label,
                    "Unknown": task_summary.n_unknown,
                    "Successful": task_summary.n_successful,
                    "Caveats": caveats,
                    "Blocked": task_summary.n_blocked,
                    "Failed": task_summary.n_failed,
                    "Wonky": task_summary.n_wonky,
                    "TOTAL": sum(
                        [
                            task_summary.n_successful,
                            task_summary.n_unknown,
                            task_summary.n_blocked,
                            task_summary.n_failed,
                            task_summary.n_wonky,
                        ]
                    ),
                    "EXPECTED": task_summary.n_expected,
                }
            )
        return astropy.table.Table(rows)

    def make_dataset_table(self) -> astropy.table.Table:
        """Construct an `astropy.table.Table` with a tabular summary of the
        datasets.

        Returns
        -------
        table : `astropy.table.Table`
            A table view of the dataset information.  This only includes
            counts of status categories, not any per-data-ID detail.
        """
        rows = []
        for dataset_type_name, dataset_type_summary in self.datasets.items():
            rows.append(
                {
                    "Dataset": dataset_type_name,
                    "Visible": dataset_type_summary.n_visible,
                    "Shadowed": dataset_type_summary.n_shadowed,
                    "Predicted Only": dataset_type_summary.n_predicted_only,
                    "Unsuccessful": dataset_type_summary.n_unsuccessful,
                    "Cursed": dataset_type_summary.n_cursed,
                    "TOTAL": sum(
                        [
                            dataset_type_summary.n_visible,
                            dataset_type_summary.n_shadowed,
                            dataset_type_summary.n_predicted_only,
                            dataset_type_summary.n_unsuccessful,
                            dataset_type_summary.n_cursed,
                        ]
                    ),
                    "EXPECTED": dataset_type_summary.n_expected,
                }
            )
        return astropy.table.Table(rows)

    def make_exception_table(self) -> astropy.table.Table:
        """Construct an `astropy.table.Table` with counts for each exception
        type raised by each task.

        At present this only includes information from partial-outputs-error
        successes, since exception information for failures is not tracked.
        This may change in the future.

        Returns
        -------
        table : `astropy.table.Table`
            A table with columns for task label, exception type, and counts.
        """
        rows = []
        for task_label, task_summary in self.tasks.items():
            for type_name, exception_summaries in task_summary.exceptions.items():
                rows.append({"Task": task_label, "Exception": type_name, "Count": len(exception_summaries)})
        return astropy.table.Table(rows)

    def make_bad_quantum_tables(self, max_message_width: int = 80) -> dict[str, astropy.table.Table]:
        """Construct an `astropy.table.Table` with per-data-ID information
        about failed, wonky, and partial-outputs-error quanta.

        Parameters
        ----------
        max_message_width : `int`, optional
            Maximum width for the Message column.  Longer messages are
            truncated.

        Returns
        -------
        tables : `dict` [ `str`, `astropy.table.Table` ]
            A table for each task with status, data IDs, and log messages for
            each unsuccessful quantum.  Keys are task labels.  Only task with
            unsuccessful quanta or partial outputs errors are included.
        """
        result = {}
        for task_label, task_summary in self.tasks.items():
            rows = []
            for status, unsuccessful_quantum_summary in itertools.chain(
                zip(itertools.repeat("FAILED"), task_summary.failed_quanta),
                zip(itertools.repeat("WONKY"), task_summary.wonky_quanta),
            ):
                row = {"Status(Caveats)": status, "Exception": "", **unsuccessful_quantum_summary.data_id}
                row["Message"] = (
                    textwrap.shorten(unsuccessful_quantum_summary.messages[-1], max_message_width)
                    if unsuccessful_quantum_summary.messages
                    else ""
                )
                rows.append(row)
            for exception_summary in itertools.chain.from_iterable(task_summary.exceptions.values()):
                # Trim off the package name from the exception type for
                # brevity.
                short_name: str = exception_summary.exception.type_name.rsplit(".", maxsplit=1)[-1]
                row = {
                    "Status(Caveats)": "SUCCESSFUL(P)",  # we only get exception info for partial outputs
                    "Exception": short_name,
                    **exception_summary.data_id,
                    "Message": textwrap.shorten(exception_summary.exception.message, max_message_width),
                }
                rows.append(row)
            if rows:
                table = astropy.table.Table(rows)
                table.columns["Exception"].format = "<"
                table.columns["Message"].format = "<"
                result[task_label] = table
        return result

    def make_bad_dataset_tables(self, max_message_width: int = 80) -> dict[str, astropy.table.Table]:
        """Construct an `astropy.table.Table` with per-data-ID information
        about unsuccessful and cursed datasets.

        Parameters
        ----------
        max_message_width : `int`, optional
            Maximum width for the Message column.  Longer messages are
            truncated.

        Returns
        -------
        tables : `dict` [ `str`, `astropy.table.Table` ]
            A table for each task with status, data IDs, and log messages for
            each unsuccessful quantum.  Keys are task labels.  Only task with
            unsuccessful quanta are included.
        """
        result = {}
        for dataset_type_name, dataset_type_summary in self.datasets.items():
            rows = []
            for data_id in dataset_type_summary.unsuccessful_datasets:
                row = {"Status": "UNSUCCESSFUL", **data_id, "Message": ""}
            for cursed_dataset_summary in dataset_type_summary.cursed_datasets:
                row = {"Status": "CURSED", **cursed_dataset_summary.data_id}
                row["Message"] = (
                    textwrap.shorten(cursed_dataset_summary.messages[-1], max_message_width)
                    if cursed_dataset_summary.messages
                    else ""
                )
                rows.append(row)
            if rows:
                table = astropy.table.Table(rows)
                table.columns["Message"].format = "<"
                result[dataset_type_name] = table
        return result


class QuantumProvenanceGraph:
    """A set of already-run, merged quantum graphs with provenance
    information.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler used for this report. This should match the Butler used
        for the run associated with the executed quantum graph.
    qgraphs : `~collections.abc.Sequence` [`QuantumGraph` |\
            `~lsst.utils.resources.ResourcePathExpression`]
        A list of either quantum graph objects or their uri's, to be used
        to assemble the `QuantumProvenanceGraph`.
    collections : `~collections.abc.Sequence` [`str`] | `None`
        Collections to use in `lsst.daf.butler.query_datasets` when testing
        which datasets are available at a high level.
    where : `str`
        A "where" string to use to constrain the datasets; should be provided
        if ``collections`` includes many datasets that are not in any graphs,
        to select just those that might be (e.g. when sharding over dimensions
        and using a final collection that spans multiple shards).
    curse_failed_logs : `bool`
        Mark log datasets as CURSED if they are visible in the final output
        collection. Note that a campaign-level collection must be used here for
        `collections` if `curse_failed_logs` is `True`.
    read_caveats : `str` or `None`, optional
        Whether to read metadata files to get flags that describe qualified
        successes.  If `None`, no metadata files will be read and all
        ``caveats`` fields will be `None`.  If "exhaustive", all metadata files
        will be read.  If "lazy", only metadata files where at least one
        predicted output is missing will be read.
    use_qbb : `bool`, optional
        If `True`, use a quantum-backed butler when reading metadata files.
        Note that some butler database queries are still run even if this is
        `True`; this does not avoid database access entirely.
    n_cores : `int`, optional
        Number of threads to use for parallelization.
    """

    def __init__(
        self,
        butler: Butler | None = None,
        qgraphs: Sequence[QuantumGraph | ResourcePathExpression] = (),
        *,
        collections: Sequence[str] | None = None,
        where: str = "",
        curse_failed_logs: bool = False,
        read_caveats: Literal["lazy", "exhaustive"] | None = "lazy",
        use_qbb: bool = True,
        n_cores: int = 1,
    ) -> None:
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
        # In order to both parallelize metadata/log reads and potentially use
        # QBB to do it, we in general need one butler for each output_run and
        # thread combination.  This dict is keyed by the former, and the
        # wrapper type used for the value handles the latter.
        self._butler_wrappers: dict[str, _ThreadLocalButlerWrapper] = {}
        if butler is not None:
            self.assemble_quantum_provenance_graph(
                butler,
                qgraphs,
                collections=collections,
                where=where,
                curse_failed_logs=curse_failed_logs,
                read_caveats=read_caveats,
                use_qbb=use_qbb,
                n_cores=n_cores,
            )
        elif qgraphs:
            raise TypeError("'butler' must be provided if `qgraphs` is.")

    @property
    def quanta(self) -> Mapping[str, Set[QuantumKey]]:
        """A mapping from task label to a set of keys for its quanta."""
        return self._quanta

    @property
    def datasets(self) -> Mapping[str, Set[DatasetKey]]:
        """A mapping from dataset type name to a set of keys for datasets."""
        return self._datasets

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

    def to_summary(
        self, butler: Butler | None = None, do_store_logs: bool = True, n_cores: int = 1
    ) -> Summary:
        """Summarize the `QuantumProvenanceGraph`.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`, optional
            Ignored; accepted for backwards compatibility.
        do_store_logs : `bool`
            Store the logs in the summary dictionary.
        n_cores : `int`, optional

        Returns
        -------
        result : `Summary`
            A struct containing counts of quanta and datasets in each of
            the overall states defined in `QuantumInfo` and `DatasetInfo`,
            as well as diagnostic information and error messages for failed
            quanta and strange edge cases, and a list of recovered quanta.
        """
        status_log = PeriodicLogger(_LOG)
        if not self._finalized:
            raise RuntimeError(
                """resolve_duplicates must be called to finalize the
                QuantumProvenanceGraph before making a summary."""
            )
        result = Summary()
        futures: list[concurrent.futures.Future[None]] = []
        _LOG.verbose("Summarizing %s tasks.", len(self._quanta.keys()))
        with concurrent.futures.ThreadPoolExecutor(n_cores) as executor:
            for m, (task_label, quanta) in enumerate(self._quanta.items()):
                task_summary = TaskSummary()
                task_summary.n_expected = len(quanta)
                for n, quantum_key in enumerate(quanta):
                    quantum_info = self.get_quantum_info(quantum_key)
                    future = task_summary._add_quantum_info(
                        quantum_info,
                        log_getter=self._butler_get if do_store_logs else None,
                        executor=executor,
                    )
                    if future is not None:
                        futures.append(future)
                    status_log.log(
                        "Summarized %s of %s quanta of task %s of %s.",
                        n + 1,
                        len(quanta),
                        m + 1,
                        len(self._quanta.keys()),
                    )
                result.tasks[task_label] = task_summary
            for n, future in enumerate(concurrent.futures.as_completed(futures)):
                if (err := future.exception()) is not None:
                    raise err
                status_log.log("Loaded messages from %s of %s log datasets.", n + 1, len(futures))
        _LOG.verbose("Summarizing %s dataset types.", len(self._datasets.keys()))
        for m, (dataset_type_name, datasets) in enumerate(self._datasets.items()):
            dataset_type_summary = DatasetTypeSummary(producer="")
            dataset_type_summary.n_expected = len(datasets)
            for n, dataset_key in enumerate(datasets):
                dataset_info = self.get_dataset_info(dataset_key)
                producer_key = self.get_producer_of(dataset_key)
                producer_info = self.get_quantum_info(producer_key)
                # Not ideal, but hard to get out of the graph at the moment.
                # Change after DM-40441
                dataset_type_summary.producer = producer_key.task_label
                dataset_type_summary._add_dataset_info(dataset_info, producer_info)
                status_log.log(
                    "Summarized %s of %s datasets of type %s of %s.",
                    n + 1,
                    len(datasets),
                    m + 1,
                    len(self._datasets.keys()),
                )
            result.datasets[dataset_type_name] = dataset_type_summary
        return result

    def iter_outputs_of(self, quantum_key: QuantumKey) -> Iterator[DatasetKey]:
        """Iterate through the outputs of a quantum, yielding the keys of
        all of the datasets produced by the quantum.

        Parameters
        ----------
        quantum_key : `QuantumKey`
            The key for the quantum whose outputs are needed.
        """
        yield from self._xgraph.successors(quantum_key)

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

    def iter_downstream(
        self, key: QuantumKey | DatasetKey
    ) -> Iterator[tuple[QuantumKey, QuantumInfo] | tuple[DatasetKey, DatasetInfo]]:
        """Iterate over the quanta and datasets that are downstream of a
        quantum or dataset.

        Parameters
        ----------
        key : `QuantumKey` or `DatasetKey`
            Starting node.

        Returns
        -------
        iter : `~collections.abc.Iterator` [ `tuple` ]
            An iterator over pairs of (`QuantumKey`, `QuantumInfo`) or
            (`DatasetKey`, `DatasetInfo`).
        """
        for key in networkx.dag.descendants(self._xgraph, key):
            yield (key, self._xgraph.nodes[key])  # type: ignore

    def assemble_quantum_provenance_graph(
        self,
        butler: Butler,
        qgraphs: Sequence[QuantumGraph | ResourcePathExpression],
        collections: Sequence[str] | None = None,
        where: str = "",
        curse_failed_logs: bool = False,
        read_caveats: Literal["lazy", "exhaustive"] | None = "lazy",
        use_qbb: bool = True,
        n_cores: int = 1,
    ) -> None:
        """Assemble the quantum provenance graph from a list of all graphs
        corresponding to processing attempts.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.
        qgraphs : `~collections.abc.Sequence` [`QuantumGraph` |\
            `~lsst.utils.resources.ResourcePathExpression`]
            A list of either quantum graph objects or their uri's, to be used
            to assemble the `QuantumProvenanceGraph`.
        collections : `~collections.abc.Sequence` [`str`] | `None`
            Collections to use in `lsst.daf.butler.query_datasets` when testing
            which datasets are available at a high level.
        where : `str`
            A "where" string to use to constrain the datasets; should be
            provided if ``collections`` includes many datasets that are not in
            any graphs, to select just those that might be (e.g. when sharding
            over dimensions and using a final collection that spans multiple
            shards).
        curse_failed_logs : `bool`
            Mark log datasets as CURSED if they are visible in the final
            output collection. Note that a campaign-level collection must be
            used here for `collections` if `curse_failed_logs` is `True`.
        read_caveats : `str` or `None`, optional
            Whether to read metadata files to get flags that describe qualified
            successes.  If `None`, no metadata files will be read and all
            ``caveats`` fields will be `None`.  If "exhaustive", all
            metadata files will be read.  If "lazy", only metadata files where
            at least one predicted output is missing will be read.
        use_qbb : `bool`, optional
            If `True`, use a quantum-backed butler when reading metadata files.
            Note that some butler database queries are still run even if this
            is `True`; this does not avoid database access entirely.
        n_cores : `int`, optional
            Number of threads to use for parallelization.
        """
        if read_caveats not in ("lazy", "exhaustive", None):
            raise TypeError(
                f"Invalid option {read_caveats!r} for read_caveats; should be 'lazy', 'exhaustive', or None."
            )
        output_runs = []
        last_time: datetime.datetime | None = None
        for graph in qgraphs:
            if not isinstance(graph, QuantumGraph):
                _LOG.verbose("Loading quantum graph %r.", graph)
                qgraph = QuantumGraph.loadUri(graph)
            else:
                qgraph = graph
            assert qgraph.metadata is not None, "Saved QGs always have metadata."
            self._add_new_graph(butler, qgraph, read_caveats=read_caveats, use_qbb=use_qbb, n_cores=n_cores)
            output_runs.append(qgraph.metadata["output_run"])
            if last_time is not None and last_time > qgraph.metadata["time"]:
                raise RuntimeError("Quantum graphs must be passed in chronological order.")
            last_time = qgraph.metadata["time"]
        if not collections:
            # We reverse the order of the associated output runs because the
            # query in _resolve_duplicates must be done most-recent first.
            collections = list(reversed(output_runs))
            assert not curse_failed_logs, (
                "curse_failed_logs option must be used with one campaign-level collection."
            )
        self._resolve_duplicates(butler, collections, where, curse_failed_logs)

    def _add_new_graph(
        self,
        butler: Butler,
        qgraph: QuantumGraph,
        read_caveats: Literal["lazy", "exhaustive"] | None,
        use_qbb: bool = True,
        n_cores: int = 1,
    ) -> None:
        """Add a new quantum graph to the `QuantumProvenanceGraph`.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler
            used for the run associated with the executed quantum graph.
        qgraph : `QuantumGraph`
            The quantum graph object to add.
        read_caveats : `str` or `None`
            Whether to read metadata files to get flags that describe qualified
            successes.  If `None`, no metadata files will be read and all
            ``caveats`` fields will be `None`.  If "exhaustive", all
            metadata files will be read.  If "lazy", only metadata files where
            at least one predicted output is missing will be read.
        use_qbb : `bool`, optional
            If `True`, use a quantum-backed butler when reading metadata files.
            Note that some butler database queries are still run even if this
            is `True`; this does not avoid database access entirely.
        n_cores : `int`, optional
            Number of threads to use for parallelization.
        """
        status_log = PeriodicLogger(_LOG)
        output_run = qgraph.metadata["output_run"]
        # Add QuantumRun and DatasetRun (and nodes/edges, as needed) to the
        # QPG for all quanta in the QG.
        _LOG.verbose("Adding output run to provenance graph.")
        new_quanta: list[QuantumKey] = []
        for n, node in enumerate(qgraph):
            new_quanta.append(self._add_new_quantum(node, output_run))
            status_log.log("Added nodes for %s of %s quanta.", n + 1, len(qgraph))
        # Query for datasets in the output run to see which ones were actually
        # produced.
        _LOG.verbose("Querying for existence for %s dataset types.", len(self._datasets.keys()))
        for m, dataset_type_name in enumerate(self._datasets):
            try:
                refs = butler.query_datasets(
                    dataset_type_name, collections=output_run, explain=False, limit=None
                )
            except MissingDatasetTypeError:
                continue
            for n, ref in enumerate(refs):
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                dataset_info = self.get_dataset_info(dataset_key)
                dataset_run = dataset_info["runs"][output_run]  # dataset run (singular)
                dataset_run.produced = True
                status_log.log(
                    "Updated status for %s of %s datasets of %s of %s types.",
                    n + 1,
                    len(refs),
                    m + 1,
                    len(self._datasets.keys()),
                )
        if use_qbb:
            _LOG.verbose("Using quantum-backed butler for metadata loads.")
            self._butler_wrappers[output_run] = _ThreadLocalButlerWrapper.wrap_qbb(butler, qgraph)
        else:
            _LOG.verbose("Using full butler for metadata loads.")
            self._butler_wrappers[output_run] = _ThreadLocalButlerWrapper.wrap_full(butler)

        _LOG.verbose("Setting quantum status from dataset existence.")
        # Update quantum status information based on which datasets were
        # produced.
        blocked: set[DatasetKey] = set()  # the outputs of failed or blocked quanta in this run.
        with concurrent.futures.ThreadPoolExecutor(n_cores) as executor:
            futures: list[concurrent.futures.Future[None]] = []
            for n, quantum_key in enumerate(new_quanta):
                if (
                    self._update_run_status(quantum_key, output_run, blocked) == QuantumRunStatus.SUCCESSFUL
                    and read_caveats is not None
                ):
                    self._update_caveats(quantum_key, output_run, read_caveats, executor, futures)
                self._update_info_status(quantum_key, output_run)
                status_log.log("Updated status for %s of %s quanta.", n + 1, len(new_quanta))
            for n, future in enumerate(concurrent.futures.as_completed(futures)):
                if (err := future.exception()) is not None:
                    raise err
                status_log.log("Added exception/caveat information for %s of %s quanta.", n + 1, len(futures))

    def _add_new_quantum(self, node: QuantumNode, output_run: str) -> QuantumKey:
        """Add a quantum from a new quantum graph to the provenance graph.

        Parameters
        ----------
        node : `QuantumNode`
            Node in the quantum graph.
        output_run : `str`
            Output run collection.

        Returns
        -------
        quantum_key : `QuantumKey`
            Key for the new or existing node in the provenance graph.

        Notes
        -----
        This method adds new quantum and dataset nodes to the provenance graph
        if they don't already exist, while adding new `QuantumRun` and
        `DatasetRun` objects to both new and existing nodes.  All status
        information on those nodes is set to initial, default values that
        generally reflect quanta that have not been attempted to be run.
        """
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
        self._quanta.setdefault(quantum_key.task_label, set()).add(quantum_key)
        metadata_ref = node.quantum.outputs[METADATA_OUTPUT_TEMPLATE.format(label=node.taskDef.label)][0]
        log_ref = node.quantum.outputs[LOG_OUTPUT_TEMPLATE.format(label=node.taskDef.label)][0]
        # associate run collections with specific quanta. this is important
        # if the same quanta are processed in multiple runs as in recovery
        # workflows.
        quantum_runs = quantum_info.setdefault("runs", {})
        # the `QuantumRun` here is the specific quantum-run collection
        # combination.
        quantum_runs[output_run] = QuantumRun(id=node.nodeId, metadata_ref=metadata_ref, log_ref=log_ref)
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
            if dataset_key.dataset_type_name.endswith(METADATA_OUTPUT_CONNECTION_NAME):
                quantum_info["metadata"] = dataset_key
                quantum_runs[output_run].metadata_ref = ref
            if dataset_key.dataset_type_name.endswith(LOG_OUTPUT_CONNECTION_NAME):
                quantum_info["log"] = dataset_key
                quantum_runs[output_run].log_ref = ref
        for ref in itertools.chain.from_iterable(node.quantum.inputs.values()):
            dataset_key = DatasetKey(ref.datasetType.nameAndComponent()[0], ref.dataId.required_values)
            if dataset_key in self._xgraph:
                # add another edge if the input datasetType and quantum are
                # in the graph
                self._xgraph.add_edge(dataset_key, quantum_key)
        return quantum_key

    def _update_run_status(
        self, quantum_key: QuantumKey, output_run: str, blocked: set[DatasetKey]
    ) -> QuantumRunStatus:
        """Update the status of this quantum in its own output run, using
        information in the graph about which of its output datasets exist.

        Parameters
        ----------
        quantum_key : `QuantumKey`
            Key for the node in the provenance graph.
        output_run : `str`
            Output run collection.
        blocked : `set` [ `DatasetKey` ]
            A set of output datasets (for all quanta, not just this one) that
            were blocked by failures.  Will be modified in place.

        Returns
        -------
        run_status : `QuantumRunStatus`
            Run-specific status for this quantum.
        """
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
        return quantum_run.status

    def _update_info_status(self, quantum_key: QuantumKey, output_run: str) -> QuantumInfoStatus:
        """Update the status of this quantum across all runs with the status
        for its latest run.

        Parameters
        ----------
        quantum_key : `QuantumKey`
            Key for the node in the provenance graph.
        output_run : `str`
            Output run collection.

        Returns
        -------
        info_status : `QuantumRunStatus`
            Run-specific status for this quantum.
        """
        # Now we can start using state transitions to mark overall status.
        quantum_info = self.get_quantum_info(quantum_key)
        quantum_run = quantum_info["runs"][output_run]
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
                if last_status != QuantumInfoStatus.SUCCESSFUL and last_status != QuantumInfoStatus.UNKNOWN:
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
        return new_status

    def _update_caveats(
        self,
        quantum_key: QuantumKey,
        output_run: str,
        read_caveats: Literal["lazy", "exhaustive"],
        executor: concurrent.futures.Executor,
        futures: list[concurrent.futures.Future[None]],
    ) -> None:
        """Read quantum success caveats and exception information from task
        metadata.

        Parameters
        ----------
        quantum_key : `QuantumKey`
            Key for the node in the provenance graph.
        output_run : `str`
            Output run collection.
        read_caveats : `str`
            Whether to read metadata files to get flags that describe qualified
            successes. If "exhaustive", all metadata files will be read.  If
            "lazy", only metadata files where at least one predicted output is
            missing will be read.
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler
            used for the run associated with the executed quantum graph.
        """
        if read_caveats == "lazy" and all(
            self.get_dataset_info(dataset_key)["runs"][output_run].produced
            for dataset_key in self._xgraph.successors(quantum_key)
        ):
            return
        quantum_info = self.get_quantum_info(quantum_key)
        quantum_run = quantum_info["runs"][output_run]

        def read_metadata() -> None:
            md = self._butler_get(quantum_run.metadata_ref, storageClass=METADATA_OUTPUT_STORAGE_CLASS)
            try:
                # Int conversion guards against spurious conversion to
                # float that can apparently sometimes happen in
                # TaskMetadata.
                quantum_run.caveats = QuantumSuccessCaveats(int(md["quantum"]["caveats"]))
            except LookupError:
                pass
            try:
                quantum_run.exception = ExceptionInfo._from_metadata(md[quantum_key.task_label]["failure"])
            except LookupError:
                pass

        futures.append(executor.submit(read_metadata))

    def _resolve_duplicates(
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
        `QuantumProvenanceGraph._add_new_graph` has been called on every graph
        associated with the data processing.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler used for this report. This should match the Butler used
            for the run associated with the executed quantum graph.
        collections : `~collections.abc.Sequence` [`str`] | `None`
            Collections to use in `lsst.daf.butler.query_datasets` when testing
            which datasets are available at a high level.
        where : `str`
            A "where" string to use to constrain the datasets; should be
            provided if ``collections`` includes many datasets that are not in
            any graphs, to select just those that might be (e.g. when sharding
            over dimensions and using a final collection that spans multiple
            shards).
        curse_failed_logs : `bool`
            Mark log datasets as CURSED if they are visible in the final
            output collection. Note that a campaign-level collection must be
            used here for `collections` if `curse_failed_logs` is `True`; if
            `_resolve_duplicates` is run on a list of group-level collections
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
        status_log = PeriodicLogger(_LOG)
        _LOG.verbose("Querying for dataset visibility.")
        for m, dataset_type_name in enumerate(self._datasets):
            # find datasets in a larger collection.
            try:
                refs = butler.query_datasets(
                    dataset_type_name, collections=collections, where=where, limit=None, explain=False
                )
            except MissingDatasetTypeError:
                continue
            for n, ref in enumerate(refs):
                dataset_key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
                try:
                    dataset_info = self.get_dataset_info(dataset_key)
                # Ignore if we don't actually have the dataset in any of the
                # graphs given.
                except KeyError:
                    continue
                # queryable datasets are `visible`.
                dataset_info["runs"][ref.run].visible = True
                status_log.log(
                    "Updated visibility for %s of %s datasets of type %s of %s.",
                    n + 1,
                    len(refs),
                    m + 1,
                    len(self._datasets.keys()),
                )
        _LOG.verbose("Updating task status from dataset visibility.")
        for m, task_quanta in enumerate(self._quanta.values()):
            for n, quantum_key in enumerate(task_quanta):
                # runs associated with visible datasets.
                visible_runs: set[str] = set()
                quantum_info = self.get_quantum_info(quantum_key)
                # Loop over each dataset in the outputs of a single quantum.
                for dataset_key in self.iter_outputs_of(quantum_key):
                    dataset_info = self.get_dataset_info(dataset_key)
                    dataset_type_name = dataset_key.dataset_type_name
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
                            if (
                                dataset_type_name.endswith(LOG_OUTPUT_CONNECTION_NAME)
                                and not curse_failed_logs
                            ):
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
                status_log.log(
                    "Updated task status from visibility for %s of %s quanta of task %s of %s.",
                    n + 1,
                    len(task_quanta),
                    m + 1,
                    len(self._quanta.keys()),
                )
        # If we make it all the way through resolve_duplicates, set
        # self._finalized = True so that it cannot be run again.
        self._finalized = True

    def _butler_get(self, ref: DatasetRef, **kwargs: Any) -> Any:
        return self._butler_wrappers[ref.run].butler.get(ref, **kwargs)


class _ThreadLocalButlerWrapper:
    """A wrapper for a thread-local limited butler.

    Parameter
    ---------
    factory : `~collections.abc.Callable`
        A callable that takes no arguments and returns a limited butler.
    """

    def __init__(self, factory: Callable[[], LimitedButler]):
        self._factory = factory
        self._thread_local = threading.local()

    @classmethod
    def wrap_qbb(cls, full_butler: Butler, qg: QuantumGraph) -> _ThreadLocalButlerWrapper:
        """Wrap a `~lsst.daf.butler.QuantumBackedButler` suitable for reading
        log and metadata files.

        Parameters
        ----------
        full_butler : `~lsst.daf.butler.Butler`
            Full butler to draw datastore and dimension configuration from.
        qg : `QuantumGraph`
            Quantum graph,

        Returns
        -------
        wrapper : `_ThreadLocalButlerWrapper`
            A wrapper that provides access to a thread-local QBB, constructing]
            it on first use.
        """
        dataset_ids = []
        for task_label in qg.pipeline_graph.tasks.keys():
            for quantum in qg.get_task_quanta(task_label).values():
                dataset_ids.append(quantum.outputs[LOG_OUTPUT_TEMPLATE.format(label=task_label)][0].id)
                dataset_ids.append(quantum.outputs[METADATA_OUTPUT_TEMPLATE.format(label=task_label)][0].id)
        try:
            butler_config = full_butler._config  # type: ignore[attr-defined]
        except AttributeError:
            raise RuntimeError("use_qbb=True requires a direct butler.") from None
        factory = _QuantumBackedButlerFactory(
            butler_config,
            dataset_ids,
            full_butler.dimensions,
            dataset_types={dt.name: dt for dt in qg.registryDatasetTypes()},
        )
        return cls(factory)

    @classmethod
    def wrap_full(cls, full_butler: Butler) -> _ThreadLocalButlerWrapper:
        """Wrap a full `~lsst.daf.butler.Butler`.

        Parameters
        ----------
        full_butler : `~lsst.daf.butler.Butler`
            Full butler to clone when making thread-local copies.

        Returns
        -------
        wrapper : `_ThreadLocalButlerWrapper`
            A wrapper that provides access to a thread-local butler,
            constructing it on first use.
        """
        return cls(full_butler.clone)

    @property
    def butler(self) -> LimitedButler:
        """The wrapped butler, constructed on first use within each thread."""
        if (butler := getattr(self._thread_local, "butler", None)) is None:
            self._thread_local.butler = self._factory()
            butler = self._thread_local.butler
        return butler


@dataclasses.dataclass
class _QuantumBackedButlerFactory:
    """A factory for `~lsst.daf.butler.QuantumBackedButler`, for use by
    `_ThreadLocalButlerWrapper`.
    """

    config: ButlerConfig
    dataset_ids: list[DatasetId]
    universe: DimensionUniverse
    dataset_types: dict[str, DatasetType]

    def __call__(self) -> QuantumBackedButler:
        return QuantumBackedButler.from_predicted(
            self.config,
            predicted_inputs=self.dataset_ids,
            predicted_outputs=[],
            dimensions=self.universe,
            # We don't need the datastore records in the QG because we're
            # only going to read metadata and logs, and those are never
            # overall inputs.
            datastore_records={},
            dataset_types=self.dataset_types,
        )


def _cli() -> None:
    import argparse

    from .pipeline_graph.visualization import (
        QuantumProvenanceGraphStatusAnnotator,
        QuantumProvenanceGraphStatusOptions,
        show,
    )

    parser = argparse.ArgumentParser(
        "QuantumProvenanceGraph command-line utilities.",
        description=(
            "This is a small, low-effort debugging utility.  "
            "It may disappear at any time in favor of a public 'pipetask' interface."
        ),
    )
    subparsers = parser.add_subparsers(dest="cmd")

    pprint_parser = subparsers.add_parser("pprint", help="Print a saved summary as a series of tables.")
    pprint_parser.add_argument("file", type=argparse.FileType("r"), help="Saved summary JSON file.")
    pprint_parser.add_argument(
        "--brief",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Whether to print per-data ID information.",
    )
    pprint_parser.add_argument(
        "--datasets",
        action=argparse.BooleanOptionalAction,
        default=True,
    )

    xgraph_parser = subparsers.add_parser("xgraph", help="Print a visual representation of a saved xgraph.")
    xgraph_parser.add_argument("file", type=argparse.FileType("r"), help="Saved summary JSON file.")
    xgraph_parser.add_argument("qgraph", type=str, help="Saved quantum graph file.")

    args = parser.parse_args()

    match args.cmd:
        case "pprint":
            summary = Summary.model_validate_json(args.file.read())
            args.file.close()
            summary.pprint(brief=args.brief, datasets=args.datasets)
        case "xgraph":
            summary = Summary.model_validate_json(args.file.read())
            args.file.close()
            status_annotator = QuantumProvenanceGraphStatusAnnotator(summary)
            status_options = QuantumProvenanceGraphStatusOptions(
                display_percent=True, display_counts=True, abbreviate=True, visualize=True
            )
            qgraph = QuantumGraph.loadUri(args.qgraph)
            pipeline_graph = qgraph.pipeline_graph
            show(
                pipeline_graph,
                dataset_types=True,
                status_annotator=status_annotator,
                status_options=status_options,
            )
        case _:
            raise AssertionError(f"Unhandled subcommand {args.dest}.")


if __name__ == "__main__":
    _cli()
