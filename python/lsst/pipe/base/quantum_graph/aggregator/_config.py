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

__all__ = ("AggregatorConfig",)

import sys
from typing import TYPE_CHECKING, Any

import pydantic


class AggregatorConfig(pydantic.BaseModel):
    """Configuration for the provenance aggregator."""

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

    incomplete: bool = False
    """If `True`, do not expect the graph to have been executed to completion
    yet, and only ingest the outputs of successful quanta.

    This disables writing the provenance quantum graph, since this is likely to
    be wasted effort that just complicates a follow-up run with
    ``incomplete=False`` later.
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
    """If `True`, do not actually perform any central butler ingests.

    Most log messages concerning ingests will still be emitted in order to
    provide a better emulation of a real run.
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

    promise_ingest_graph: bool = False
    """If `True`, the aggregator will assume that `~.ingest_graph.ingest_graph`
    will be run later to ingest metadata/log/config datasets, and will not
    ingest them itself.  This means that if `~.ingest_graph.ingest_graph` is
    not run, those files will be abandoned in the butler storage root without
    being present in the butler database, but it will speed up both processes.

    It is *usually* safe to build a quantum graph for downstream processing
    before or while running `~.ingest_graph.ingest_graph`, because
    metadata/log/config datasets are rarely used as inputs.  To check, use
    ``pipetask build ... --show inputs`` to show the overall-inputs to the
    graph and scan for these dataset types.
    """

    worker_check_timeout: float = 5.0
    """Time to wait (s) for reports from subprocesses before running
    process-alive checks.

    These checks are designed to kill the main aggregator process when a
    subprocess has been unexpectedly killed (e.g. for for using too much
    memory).
    """

    @property
    def is_writing_provenance(self) -> bool:
        """Whether the aggregator is configured to write the provenance quantum
        graph.
        """
        return self.output_path is not None and not self.incomplete

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules and not TYPE_CHECKING:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)
