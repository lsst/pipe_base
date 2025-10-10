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


import pydantic

from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionUniverse

from ...pipeline_graph import TaskNode
from .._common import HeaderModel


class AggregatorConfig(pydantic.BaseModel):
    """Configuration for the provenance aggregator."""

    ingest_provenance: bool = False
    """If `True`, ingest the provenance quantum graph into the central butler
    repository use it to back any dataset content it includes (e.g. logs and
    metadata).

    If `False`, the provenance quantum graph is not written at all.
    """

    output_path: str | None = None
    """Path for the output provenance quantum graph file.

    If not `None`, this just sets the path the provenance quantum graph is
    originally written to before it is *copied* into its butler location.

    If `None` (default) the file is written to a temporary location and moved
    (if possible) into its final location.

    This is ignored if `ingest_provenance` is `False`.
    """

    graph_dataset_type_name: str = "run_provenance"
    """The name of the provenance graph dataset type.

    This is ignored if `ingest_provenance` is `False`.
    """

    quantum_dataset_type_template: str = "{}_provenance"
    """A `str.format` template for the dataset type name used to ingest
    per-quantum provenance datasets.

    The template string must exactly one string placeholder for the task label.

    This is ignored if `ingest_provenance` is `False`.
    """

    ingest_logs: bool = True
    """If `True`, ingest "{task_label}_log" datasets.

    Log content is also available via the graph and quantum provenance datasets
    if `ingest_provenance` is `True`, so ``ingest_logs=True`` is mostly for
    backwards compatibility.

    If `ingest_provenance` is `False`, this option must be `True`.
    """

    ingest_configs: bool = True
    """If `True`, ingest "{task_label}_config" datasets.

    Config content is also available via the graph provenance dataset if
    `ingest_provenance` is `True`, so ``ingest_configs=True`` is mostly for
    backwards compatibility.

   If `ingest_provenance` is `False`, this option must be `True`.
    """

    ingest_packages: bool = True
    """If `True`, ingest the "packages" dataset.

    Package version content is also available via the graph provenance dataset
    if `ingest_provenance` is `True`, so ``ingest_packages=True`` is mostly for
    backwards compatibility.

    If `ingest_provenance` is `False`, this option must be `True`.
    """

    remove_dataset_types: list[str] = pydantic.Field(default_factory=list)
    """Names of dataset types whose files should be deleted instead of being
    ingested into the central butler repository.
    """

    worker_log_dir: str | None = None
    """Path to a directory (POSIX only) for parallel worker logs."""

    worker_log_level: str = "VERBOSE"
    """Log level for worker processes/threads.

    Per-quantum messages only appear at ``DEBUG`` level.
    """

    worker_profile_dir: str | None = None
    """Path to a directory (POSIX only) for parallel worker profiling dumps.

    This option is ignored when `n_processes` is ``1``.
    """

    n_processes: int = 1
    """Number of processes the scanner should use."""

    incomplete: bool = False
    """If `True`, do not expect the graph to have been executed to completion
    yet, and only ingest the outputs of successful quanta.

    This disables writing and ingesting the provenance quantum graph, since
    this is likely to be wasted effort that just complicates a follow-up run
    with ``incomplete=False`` later.  But the value of `ingest_provenance` is
    still used in this mode as an indication of whether that follow-up run will
    ingest provenance, and hence whether metadata, log, config, and packages
    datasets should ingested with their original files backing them, or skipped
    until provenance is written later.
    """

    recover: bool = False
    """If `True`, guard against datasets having already been ingested into the
    central butler repository and datasets already having been deleted.

    This mode is automatically turned on (with a warning emitted) if an ingest
    attempt fails due to a database constraint violation and the provenance
    quantum graph has not yet been ingested (and hence no datasets should have
    been deleted).  If the provenance quantum graph has already been ingested,
    this mode must be enabled up front.

    This mode does not guard against race conditions from multiple ingest
    processes running simultaneously, as it relies on a one-time query to
    determine what is already present in the central repository.
    """

    ingest_batch_size: int = 10000
    """Number of butler datasets that must accumulate to trigger an ingest."""

    delete_batch_size: int = 10000
    """Number of butler datasets to delete at once."""

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

    @property
    def actually_ingest_provenance(self) -> bool:
        """Whether the aggregator is configured to write the provenance quantum
        graph.
        """
        return self.ingest_provenance and not self.incomplete

    @pydantic.model_validator(mode="after")
    def _validate_provenance_ingestion(self) -> AggregatorConfig:
        if not self.ingest_provenance:
            if not self.ingest_logs:
                raise ValueError("ingest_logs must be True if ingest_provenance is False.")
            if not self.ingest_configs:
                raise ValueError("ingest_configs must be True if ingest_provenance is False.")
            if not self.ingest_packages:
                raise ValueError("ingest_packages must be True if ingest_provenance is False.")
        return self

    def get_graph_dataset_type(self, universe: DimensionUniverse) -> DatasetType:
        """Return the dataset type that should be used to ingest the
        provenance quantum graph itself.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Definitions of all dimensions.

        Returns
        -------
        dataset_type : `lsst.daf.butler.DatasetType`
            Dataset type definition.
        """
        return DatasetType(
            self.graph_dataset_type_name,
            dimensions=universe.empty,
            storageClass="ProvenanceQuantumGraph",  # TODO[DM-52738]: confirm/update storage class
        )

    def get_graph_dataset_ref(self, universe: DimensionUniverse, header: HeaderModel) -> DatasetRef:
        """Return the dataset reference that should be used to ingest the
        provenance quantum graph itself.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Definitions of all dimensions.
        header : `.HeaderModel`
            Header of the predicted or provenance quantum graph.

        Returns
        -------
        dataset_ref : `lsst.daf.butler.DatasetRef`
            Dataset reference.
        """
        return DatasetRef(
            self.get_graph_dataset_type(universe),
            DataCoordinate.make_empty(universe),
            run=header.output_run,
            id=header.provenance_dataset_id,
        )

    def get_quantum_dataset_type(self, task_node: TaskNode) -> DatasetType:
        """Return the dataset type that should be used to ingest the
        provenance for the quanta of a task.

        Parameters
        ----------
        task_node : `..pipeline_graph.TaskNode`
            Node for this task in the pipeline graph.

        Returns
        -------
        dataset_type : `lsst.daf.butler.DatasetType`
            Dataset type definition.
        """
        return DatasetType(
            self.quantum_dataset_type_template.format(task_node.label),
            dimensions=task_node.dimensions,
            storageClass="ProvenanceQuantumGraph",  # TODO[DM-52738]: confirm/update storage class
        )
