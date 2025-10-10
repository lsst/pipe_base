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

from lsst.daf.butler import Butler, DataCoordinate, DatasetRef, DatasetType, DimensionUniverse
from lsst.resources import ResourcePath

from .._common import HeaderModel


class AggregatorConfig(pydantic.BaseModel):
    """Configuration for the provenance aggregator."""

    # Changes to the defaults in this class are not automatically reflected in
    # the CLI; some defaults unfortunately have to be duplicated there.

    output_path: str | None = None
    """Path for the output provenance quantum graph file.

    If `ingest_provenance` is `False`, this writes the provenance graph file to
    the given location on the assumption that it will *never* be ingested, and
    hence the original dataset files whose content it aggregates (e.g. logs and
    metadata) should back those datasets and should not be deleted.  If
    `output_path` and `ingest_provenance` are both `False`, the provenance QG
    is not written.

    If `ingest_provenance` is `True`, this sets the path to write to prior to
    ingest, and `output_path` defaults to the position expected by the butler
    datastore configuration.
    """

    ingest_provenance: bool = False
    """If `True`, ingest the provenance quantum graph into the central butler
    repository use it to back any dataset content it includes (e.g. logs and
    metadata).
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
    """If `True`, ingest "{task_label}_log" datasets=.

    Log content is also available via the graph and quantum provenance datasets
    if `ingest_provenance` is `True`, so ``ingest_logs=True`` is mostly for
    backwards compatibility.

    If `ingest_provenance` and `ingest_logs` are both `False`, log files are
    deleted and hence lost entirely.
    """

    ingest_configs: bool = True
    """If `True`, ingest "{task_label}_config" datasets.

    Config content is also available via the graph provenance dataset if
    `ingest_provenance` is `True`, so ``ingest_configs=True`` is mostly for
    backwards compatibility.

    If `ingest_provenance` and `ingest_configs` are both `False`, config files
    are deleted and hence lost entirely.
    """

    ingest_packages: bool = True
    """If `True`, ingest the "packages" dataset.

    Package version content is also available via the graph provenance dataset
    if `ingest_provenance` is `True`, so ``ingest_packages=True`` is mostly for
    backwards compatibility.

    If `ingest_provenance` and `ingest_packages` are both `False`, packages
    files are deleted and hence lost entirely.
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

    assume_complete: bool = True
    """If `True`, the aggregator can assume all quanta have run to completion
    (including any automatic retries).  If `False`, only successes can be
    considered final, and quanta that appear to have failed or to have not been
    executed are ignored.
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
    the output `~lsst.daf.butler.CollectionType.CHAINED` collection after
    ingestion is complete.
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
    def writes_provenance(self) -> bool:
        """Whether this configuration involves writing provenance at all."""
        return self.output_path is not None or self.ingest_provenance

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

    def get_actual_output_path(
        self, butler: str | Butler, header: HeaderModel, ref: DatasetRef | None = None
    ) -> ResourcePath:
        """Return the actual output path, selecting between the `output_path`
        field and a butler-generated path as appropriate.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler` or `str`
            Butler client object or path/alias for a butler repository.
        header : `.HeaderModel`
            Header of the predicted or provenance quantum graph.
        ref : `lsst.daf.butler.DatasetRef`, optional
            Dataset reference for the provenance QG dataset.

        Returns
        -------
        path : `lsst.resources.ResourcePath` or `None`
            Path for the quantum provenance graph, if it should not be written.
        """
        if self.output_path is not None:
            return ResourcePath(self.output_path)
        elif self.ingest_provenance:
            if not isinstance(butler, Butler):
                butler = Butler.from_config(butler)
            if ref is None:
                ref = self.get_graph_dataset_ref(butler.dimensions, header)
            return butler.getURI(ref, predict=True).replace(fragment="")
        else:
            raise AssertionError("Provenance quantum graph is not being written.")
