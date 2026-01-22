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

"""A tool for ingesting provenance quantum graphs (written by the `aggregator`
module) and [re-]ingesting other datasets (metadata/logs/configs) backed by the
same file.  This "finalizes" the RUN collection, prohibiting (at least
conceptually) further processing.

This always proceeds in three steps, so we can resume efficiently:

1. First we ask the butler to "forget" any metadata/log/config datasets that
   exist in the output RUN collection, removing any record of them from the
   butler database while preserving their files.

2. Next we ingest the ``run_provenance`` graph dataset itself.

3. Finally, in batches of quanta, we use a
   `~lsst.daf.butler.QuantumBackedButler` to delete the original
   metadata/log/config files and ingest new versions of those datasets into the
   butler.

Thus, at any point, if the ``run_provenance`` dataset has not been ingested,
we know any metadata/log/config datasets that have been ingested are backed by
the original files.

Moreover, if the ``run_provenance`` dataset has been ingested, any existing
metadata/log/config datasets must be backed by the graph file, and the original
files for those datasets will have been deleted.

We also know that at all times the metadata/log/config *content* is safely
present in either the original files in the butler storage or in an
already-ingested ``run_provenance`` dataset.
"""

from __future__ import annotations

__all__ = ("ingest_graph",)

import dataclasses
import itertools
import uuid
from collections.abc import Iterator
from contextlib import contextmanager

from lsst.daf.butler import (
    Butler,
    Config,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    FileDataset,
    QuantumBackedButler,
)
from lsst.daf.butler.registry.sql_registry import SqlRegistry
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.logging import getLogger

from ..automatic_connection_constants import PROVENANCE_DATASET_TYPE_NAME, PROVENANCE_STORAGE_CLASS
from ._provenance import (
    ProvenanceDatasetInfo,
    ProvenanceInitQuantumInfo,
    ProvenanceQuantumGraph,
    ProvenanceQuantumGraphReader,
    ProvenanceQuantumInfo,
)
from .formatter import ProvenanceFormatter

_LOG = getLogger(__name__)


def ingest_graph(
    butler_config: str | Config,
    uri: ResourcePathExpression | None = None,
    *,
    transfer: str | None = "move",
    batch_size: int = 10000,
    output_run: str | None = None,
) -> None:
    """Ingest a provenance graph into a butler repository.

    Parameters
    ----------
    butler_config : `str`
        Path or alias for the butler repository, or a butler repository config
        object.
    uri : convertible to `lsst.resources.ResourcePath` or `None`, optional
        Location of the provenance quantum graph to ingest.  `None` indicates
        that the quantum graph has already been ingested, but other ingests
        and/or deletions failed and need to be resumed.
    batch_size : `int`, optional
        Number of datasets to process in each transaction.
    output_run : `str`, optional
        Output `~lsst.daf.butler.CollectionType.RUN` collection name.  Only
        needs to be provided if ``uri`` is `None`.  If it is provided the
        output run in the graph is checked against it.

    Notes
    -----
    After this operation, no further processing may be done in the
    `~lsst.daf.butler.CollectionType.RUN` collection.

    If this process is interrupted, it can pick up where it left off if run
    again (at the cost of some duplicate work to figure out how much progress
    it had made).
    """
    with _GraphIngester.open(butler_config, uri, output_run) as helper:
        helper.fetch_already_ingested_datasets()
        if not helper.graph_already_ingested:
            assert uri is not None
            helper.forget_ingested_datasets(batch_size=batch_size)
            helper.ingest_graph_dataset(uri, transfer=transfer)
        helper.clean_and_reingest_datasets(batch_size=batch_size)


@dataclasses.dataclass
class _GraphIngester:
    butler_config: str | Config
    butler: Butler
    graph: ProvenanceQuantumGraph
    graph_already_ingested: bool
    n_datasets: int
    datasets_already_ingested: set[uuid.UUID] = dataclasses.field(default_factory=set)

    @property
    def output_run(self) -> str:
        return self.graph.header.output_run

    @classmethod
    @contextmanager
    def open(
        cls,
        butler_config: str | Config,
        uri: ResourcePathExpression | None,
        output_run: str | None,
    ) -> Iterator[_GraphIngester]:
        with Butler.from_config(butler_config, collections=output_run, writeable=True) as butler:
            butler.registry.registerDatasetType(
                DatasetType(PROVENANCE_DATASET_TYPE_NAME, butler.dimensions.empty, PROVENANCE_STORAGE_CLASS)
            )
            graph, graph_already_ingested = cls.read_graph(butler, uri)
            if output_run is not None and graph.header.output_run != output_run:
                raise ValueError(
                    f"Given output run {output_run!r} does not match the graph "
                    f"header {graph.header.output_run!r}."
                )
            n_datasets = 2 * len(graph.quantum_only_xgraph) + len(graph.init_quanta)
            yield cls(
                butler_config=butler_config,
                butler=butler,
                graph=graph,
                graph_already_ingested=graph_already_ingested,
                n_datasets=n_datasets,
            )

    @staticmethod
    def read_graph(
        butler: Butler,
        uri: ResourcePathExpression | None,
    ) -> tuple[ProvenanceQuantumGraph, bool]:
        if uri is not None:
            _LOG.info("Reading the pre-ingest provenance graph.")
            with ProvenanceQuantumGraphReader.open(uri) as reader:
                reader.read_quanta()
                reader.read_init_quanta()
                graph = reader.graph
            already_ingested = (
                butler.find_dataset(PROVENANCE_DATASET_TYPE_NAME, collections=[graph.header.output_run])
                is not None
            )
            return graph, already_ingested
        else:
            _LOG.info("Reading the already-ingested provenance graph.")
            parameters = {"datasets": [], "read_init_quanta": True}
            return butler.get(PROVENANCE_DATASET_TYPE_NAME, parameters=parameters), True

    def fetch_already_ingested_datasets(self) -> None:
        _LOG.info("Querying for existing datasets in %r.", self.output_run)
        self.datasets_already_ingested.update(self.butler.registry._fetch_run_dataset_ids(self.output_run))

    def iter_datasets(self) -> Iterator[tuple[uuid.UUID, ProvenanceDatasetInfo]]:
        xgraph = self.graph.bipartite_xgraph
        for task_label, quanta_for_task in self.graph.quanta_by_task.items():
            _LOG.verbose(
                "Batching up metadata and log datasets from %d %s quanta.", len(quanta_for_task), task_label
            )
            for quantum_id in quanta_for_task.values():
                quantum_info: ProvenanceQuantumInfo = xgraph.nodes[quantum_id]
                metadata_id = quantum_info["metadata_id"]
                yield metadata_id, xgraph.nodes[metadata_id]
                log_id = quantum_info["log_id"]
                yield log_id, xgraph.nodes[log_id]
        _LOG.verbose("Batching up config datasets from %d tasks.", len(self.graph.init_quanta))
        for task_label, quantum_id in self.graph.init_quanta.items():
            init_quantum_info: ProvenanceInitQuantumInfo = xgraph.nodes[quantum_id]
            config_id = init_quantum_info["config_id"]
            yield config_id, xgraph.nodes[config_id]

    def forget_ingested_datasets(self, batch_size: int) -> None:
        _LOG.info(
            "Dropping database records for metadata/log/config datasets backed by their original files."
        )
        to_forget: list[DatasetRef] = []
        n_forgotten: int = 0
        n_skipped: int = 0
        for dataset_id, dataset_info in self.iter_datasets():
            if dataset_info["produced"] and dataset_id in self.datasets_already_ingested:
                to_forget.append(self._make_ref_from_info(dataset_id, dataset_info))
                self.datasets_already_ingested.remove(dataset_id)
                if len(to_forget) >= batch_size:
                    n_forgotten += self._run_forget(to_forget, n_forgotten + n_skipped)
            else:
                n_skipped += 1
        n_forgotten += self._run_forget(to_forget, n_forgotten + n_skipped)
        _LOG.info(
            "Removed database records for %d metadata/log/config datasets, while %d were already absent.",
            n_forgotten,
            n_skipped,
        )

    def _run_forget(self, to_forget: list[DatasetRef], n_current: int) -> int:
        if to_forget:
            _LOG.verbose(
                "Forgetting a %d-dataset batch; %d/%d forgotten so far or already absent.",
                len(to_forget),
                n_current,
                self.n_datasets,
            )
            with self.butler.registry.transaction():
                self.butler._datastore.forget(to_forget)
                self.butler.registry.removeDatasets(to_forget)
        n = len(to_forget)
        to_forget.clear()
        return n

    def ingest_graph_dataset(self, uri: ResourcePathExpression, transfer: str | None) -> None:
        _LOG.info("Ingesting the provenance quantum graph.")
        dataset_type = DatasetType(
            PROVENANCE_DATASET_TYPE_NAME, self.butler.dimensions.empty, PROVENANCE_STORAGE_CLASS
        )
        self.butler.registry.registerDatasetType(dataset_type)
        ref = DatasetRef(dataset_type, DataCoordinate.make_empty(self.butler.dimensions), run=self.output_run)
        uri = ResourcePath(uri)
        self.butler.ingest(
            # We use .abspath() since butler assumes paths are relative to the
            # repo root, while users expects them to be relative to the CWD in
            # this context.
            FileDataset(refs=[ref], path=uri.abspath(), formatter=ProvenanceFormatter),
            transfer=transfer,
        )

    def clean_and_reingest_datasets(self, batch_size: int) -> None:
        _LOG.info(
            "Deleting original metadata/log/config files and re-ingesting them with provenance graph backing."
        )
        direct_uri = self.butler.getURI(PROVENANCE_DATASET_TYPE_NAME, collections=[self.output_run])
        qbb = self.make_qbb()
        to_process: list[DatasetRef] = []
        n_processed: int = 0
        n_skipped: int = 0
        n_not_produced: int = 0
        for dataset_id, dataset_info in self.iter_datasets():
            if not dataset_info["produced"]:
                n_not_produced += 1
            elif dataset_id not in self.datasets_already_ingested:
                to_process.append(self._make_ref_from_info(dataset_id, dataset_info))
                if len(to_process) >= batch_size:
                    n_processed += self._run_clean_and_ingest(
                        qbb, direct_uri, to_process, n_processed + n_skipped
                    )
            else:
                n_skipped += 1
        n_processed += self._run_clean_and_ingest(qbb, direct_uri, to_process, n_processed + n_skipped)
        _LOG.info(
            "Deleted and re-ingested %d metadata/log/config datasets "
            "(%d had already been processed, %d were not produced).",
            n_processed,
            n_skipped,
            n_not_produced,
        )

    def _run_clean_and_ingest(
        self, qbb: QuantumBackedButler, direct_uri: ResourcePath, to_process: list[DatasetRef], n_current: int
    ) -> int:
        if not to_process:
            return 0
        _LOG.verbose(
            "Deleting and deleting a %d-dataset batch; %d/%d complete.",
            len(to_process),
            n_current,
            self.n_datasets,
        )
        sql_registry: SqlRegistry = self.butler._registry  # type: ignore[attr-defined]
        expanded_refs = sql_registry.expand_refs(to_process)
        # We need to pass predict=True to keep QBB/FileDatastore from wasting
        # time doing existence checks, since ResourcePath.mremove will ignore
        # nonexistent files anyway.
        original_uris = list(
            itertools.chain.from_iterable(
                ref_uris.iter_all() for ref_uris in qbb.get_many_uris(expanded_refs, predict=True).values()
            )
        )
        removal_status = ResourcePath.mremove(original_uris, do_raise=False)
        for path, status in removal_status.items():
            if not status.success and not isinstance(status.exception, FileNotFoundError):
                assert status.exception is not None, "Exception should be set if success=False."
                status.exception.add_note(f"Attempting to delete original file at {path}.")
                raise status.exception
        file_dataset = FileDataset(refs=expanded_refs, path=direct_uri, formatter=ProvenanceFormatter)
        self.butler.ingest(file_dataset, transfer=None)
        n = len(to_process)
        to_process.clear()
        return n

    @staticmethod
    def _make_ref_from_info(dataset_id: uuid.UUID, dataset_info: ProvenanceDatasetInfo) -> DatasetRef:
        return DatasetRef(
            dataset_info["pipeline_node"].dataset_type,
            dataset_info["data_id"],
            run=dataset_info["run"],
            id=dataset_id,
        )

    def make_qbb(self) -> QuantumBackedButler:
        dataset_types = {d.name: d.dataset_type for d in self.graph.pipeline_graph.dataset_types.values()}
        return QuantumBackedButler.from_predicted(
            config=self.butler_config,
            predicted_inputs=(),
            predicted_outputs=(),
            dimensions=self.butler.dimensions,
            datastore_records={},
            dataset_types=dataset_types,
        )
