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

__all__ = ("AggregatorConfig", "ScannerTimeConfigDict", "ScannerTimeOverrideDict")

from collections import ChainMap
from typing import TypedDict

import pydantic

from .._common import TaskLabel


class ScannerTimeConfigDict(TypedDict):
    """Configuration for how long the provenance scanner should wait between
    attempts to read a quantum's metadata and log datasets.

    This waiting logic is only applied if `ScannerConfig.assume_complete` is
    `False`; when that is `True`, all quanta are expected to have either failed
    or succeed, the absence of metadata and log datasets is assumed to indicate
    a hard failure.
    """

    wait: float
    """Wait time (s) between the first attempt to scan for a quantum's
    completion and the second attempt.

    Scanning attempts can start as soon as a quantum is unblocked by upstream
    quanta finishing (the scanner generally has no knowledge of when a quantum
    is actually scheduled for execution by a workflow system), so this wait
    time should account for any expected delay in the start of execution as
    well as the time it takes to start a quantum.
    """

    wait_factor: float
    """Multiplier for `wait` applied on each attempt after the second.
    """

    wait_max: float
    """Maximum wait interval (s) for scans.

    Quanta that have not produced a log dataset are continually scanned until
    this threshold is reached.
    """

    retry_timeout: float
    """Maximum total wait time since the first scan with a log dataset present
    before giving up.

    Quanta with log and no metadata have failed at least once, but may be
    retried and hence turn into successes if we wait.
    """


def make_scanner_time_defaults() -> ScannerTimeConfigDict:
    """Make a `ScannerTimeConfigDict` with default values."""
    return dict(
        wait=60.0,
        wait_factor=1.2,
        wait_max=600.0,
        retry_timeout=600.0,
    )


class ScannerTimeOverrideDict(ScannerTimeConfigDict, total=False):
    """A variance of `ScannerTimeConfigDict` that does not require all keys
    to be present, for use in per-task configurations.

    Missing keys fall back to the task-independent default.
    """


class AggregatorConfig(pydantic.BaseModel):
    """Configuration for the provenance aggregator."""

    # Changes to the defaults in this class are not automatically reflected in
    # the CLI; some defaults unfortunately have to be duplicated there.

    output_path: str | None = None
    """Path for the output provenance quantum graph file.

    At present this option is intended only for debugging.
    """

    worker_log_dir: str | None = None
    """Path to a directory (POSIX only) for parallel worker logs."""

    worker_log_level: str = "VERBOSE"
    """Log level for worker processes/threads.

    Per-quantum messages only appear at ``DEBUG`` level.
    """

    worker_profile_dir: str | None = None
    """Path to a directory (POSIX only) for parallel worker profiling dumps.

    This option is ignored when `n_processes` is `1`.
    """

    n_processes: int = 1
    """Number of processes the scanner should use."""

    assume_complete: bool = True
    """If `True`, the aggregator can assume all quanta have run to completion
    (including any automatic retries).  If `False`, only successes can be
    considered final, and quanta that appear to have failed or to have not been
    are waited on until a timeout is reached.
    """

    defensive_ingest: bool = False
    """If `True`, guard against datasets having already been ingested into the
    central butler repository.

    Defensive ingest mode is automatically turned on (with a warning emitted)
    if an ingest attempt fails due to a database constraint violation. Enabling
    defensive mode up-front avoids this warning and is slightly more efficient
    when it is already known that some datasets have already been ingested.

    Defensive mode does not guard against race conditions from multiple ingest
    processes running simultaneously, as it relies on a one-time query to
    determine what is already present in the central repository.
    """

    default_times: ScannerTimeConfigDict = pydantic.Field(default_factory=make_scanner_time_defaults)
    """Default wait times for all tasks.

    This is ignored if `assume_complete` is `True`.
    """

    task_times: dict[TaskLabel, ScannerTimeOverrideDict] = pydantic.Field(default_factory=dict)
    """Per-task overrides for wait times.

    This is ignored if `assume_complete` is `True`.
    """

    idle_timeout: float = 600.0
    """Minimum time to wait (s) before shutting down the scanner when no
    progress is being made at all.

    This timeout is necessary for ``assume_complete=False`` runs to prevent
    quanta with hard failures that prevent the writing of logs from keeping the
    scanner waiting indefinitely, since these quanta are otherwise
    indistinguishable from quanta that are never started.

    The timeout should not matter when ``assume_complete=True`` unless there
    are major performance problems scanning datasets or writing to the scanner
    database.
    """

    ingest_batch_size: int = 10000
    """Number of butler datasets that must accumulate to trigger an ingest."""

    register_dataset_types: bool = True
    """Whether to register output dataset types in the central butler
    repository before starting ingest.
    """

    update_output_chain: bool = True
    """Whether to prepend the output `~lsst.daf.butler.CollectionType.RUN` to
    the output `~lsst.daf.butler.CollectionType.CHAINED` collection.
    """

    dry_run: bool = False
    """If `True`, do not actually perform any deletions or central butler
    ingests.

    Most log messages concerning deletions and ingests will still be emitted in
    order to provide a better emulation of a real run.
    """

    interactive_status: bool = False
    """Whether to use an interactive status display with progress bars.

    If this is `True`, the `tqdm` module must be available.  If this is
    `False`, a periodic logger will be used to display status at a fixed
    interval instead (see `log_status_interval`).
    """

    log_status_interval: float | None = None
    """Interval (in seconds) between periodic logger status updates."""

    worker_sleep: float = 0.01
    """Time (in seconds) a worker should wait when there are no requests from
    the main aggregator process.
    """

    zstd_level: int = 10
    """ZStandard compression level to use for all compressed-JSON blocks."""

    zstd_dict_size: int = 32768
    """Size (in bytes) of the ZStandard compression dictionary."""

    zstd_dict_n_inputs: int = 512
    """Number of samples of each type (see below) to include in ZStandard
    compression dictionary training.

    Training is run on a random subset of the `PredictedQuantumDatasetsModel`
    objects in the predicted graph, as well as the first provenance quanta,
    logs, and metadata blocks encountered.
    """

    mock_storage_classes: bool = False
    """Enable support for storage classes by created by the
    lsst.pipe.base.tests.mocks package.
    """

    def get_times_for_task(self, task_label: TaskLabel) -> ScannerTimeConfigDict:
        """Apply the time overrides for a task by merging them with the
        defaults.

        Parameters
        ----------
        task_label : `str`
            Label of the task.

        Returns
        -------
        times : `dict`
            Dictionary of times, guaranteed to have all required keys.
        """
        if task_label in self.task_times:
            return ChainMap(self.task_times[task_label], self.default_times)  # type: ignore
        else:
            return self.default_times
