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

import networkx

__all__ = ("ingest_graph",)

import dataclasses
import uuid
from typing import Literal

from lsst.daf.butler import Butler, DataCoordinate, DatasetRef, DatasetType, FileDataset
from lsst.resources import ResourcePathExpression
from lsst.utils.logging import getLogger

from ._provenance import (
    ProvenanceDatasetInfo,
    ProvenanceQuantumGraph,
    ProvenanceQuantumGraphReader,
    ProvenanceQuantumInfo,
)
from .formatter import ProvenanceFormatter

_LOG = getLogger()


def ingest_graph(
    butler: Butler,
    uri: ResourcePathExpression | None = None,
    *,
    graph_dataset: DatasetRef | str = "run_provenance",
    metadata: Literal["ignore", "delete", "replace"] = "replace",
    logs: Literal["ignore", "delete", "replace"] = "delete",
    transfer: str | None = "auto",
    batch_size: int = 10000,
) -> None:
    """Ingest a provenance graph into a butler repository.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        A writable butler client, with its default
        `~lsst.daf.butler.CollectionType.RUN` collection set to the one that
        the provenance graph describes.
    uri : convertible to `lsst.resources.ResourcePath` or `None`, optional
        Location of the provenance quantum graph to ingest.  `None` indicates
        that the quantum graph has already been ingested, but other ingests
        and/or deletions failed and need to be resumed.
    graph_dataset : `str` or `lsst.daf.butler.DatasetRef`, optional
        Name of or reference to the graph dataset.  A
        `~lsst.daf.butler.DatasetRef` must be provided if the dataset type is
        not dimensionless.
    metadata : `str`, optional.
        What to do with ``{task_label}_metadata`` datasets, whose content is
        also available in the provenance quantum graph:

        - ``ignore``: any existing metadata datasets are left unchanged
        - ``delete``: existing metadata datasets are deleted
        - ``replace`` (default): new metadata datasets are ingested with the
          provenance graph file as the backing store; any existing datasets are
          deleted.
    log : `str`, optional
        What to do with ``{task_label}_log`` datasets, whose content is
        also available in the provenance quantum graph.  Options are the same
        as ``metadata``, but the default is different (``delete``).
    transfer : `str` or `None`, optional
        Butler ingest transfer mode.
    batch_size : `int`, optional
        Number of datasets to process in each transaction.
    """
    graph_dataset = _standardize_graph_dataset(butler, graph_dataset)
    if metadata == "ignore" and logs == "ignore":
        if uri is not None:
            _ingest_graph_dataset(butler, uri, graph_dataset, transfer=transfer)
        else:
            _LOG.warning("Graph is already ingested and metadata and logs are to be ignored; nothing to do.")
        return
    graph = _read_quanta(butler, uri, graph_dataset)
    if uri is not None:
        _ingest_graph_dataset(butler, uri, graph_dataset, transfer=transfer)
    direct_uri = butler.getURI(graph_dataset)
    helper = _AuxiliaryHelper(FileDataset(direct_uri, refs=[], formatter=ProvenanceFormatter))
    _LOG.info("Querying for existing datasets in %r.", graph.header.output_run)
    helper.existing_dataset_ids = set(butler.registry._fetch_run_dataset_ids(graph.header.output_run))
    _LOG.info(
        "Deleting %s and ingesting %s.",
    )
    xgraph = graph.bipartite_xgraph
    quantum_info: ProvenanceQuantumInfo
    for quantum_info in graph.quantum_only_xgraph.nodes.values():
        helper.add_dataset(quantum_info["metadata_id"], xgraph, metadata)
        helper.add_dataset(quantum_info["log_id"], xgraph, logs)
        if helper.is_batch_ready(batch_size):
            helper.write(butler)
    helper.write(butler)
    _LOG.info(
        "Deleted %d metadata and/or log datasets and ingested %d.",
        helper.n_deleted_total,
        helper.n_ingested_total,
    )


def _standardize_graph_dataset(
    butler: Butler,
    graph_dataset: DatasetRef | str,
) -> DatasetRef:
    if not isinstance(graph_dataset, DatasetRef):
        if butler.run is None:
            raise ValueError("Butler.run must be set when passed to ingest_graph.")
        graph_dataset_type = DatasetType(graph_dataset, butler.dimensions.empty, "ProvenanceQuantumGraph")
        graph_dataset = DatasetRef(
            graph_dataset_type, DataCoordinate.make_empty(butler.dimensions), run=butler.run
        )
    return graph_dataset


def _read_quanta(
    butler: Butler,
    uri: ResourcePathExpression | None,
    graph_dataset: DatasetRef,
) -> ProvenanceQuantumGraph:
    if uri is not None:
        _LOG.info("Reading quanta from pre-ingest provenance graph.")
        with ProvenanceQuantumGraphReader.open(uri) as reader:
            if reader.header.output_run != butler.run:
                raise ValueError(
                    f"output_run={reader.header.output!r} in the provenance QG header, "
                    f"but {butler.run!r} in the butler client provided."
                )
            reader.read_quanta()
            return reader.graph
    else:
        _LOG.info("Reading quanta from the already-ingested provenance graph.")
        return butler.get(graph_dataset, parameters={"datasets": [], "read_init_quanta": False})


def _ingest_graph_dataset(
    butler: Butler, uri: ResourcePathExpression, graph_dataset: DatasetRef, transfer: str | None
) -> None:
    _LOG.info("Ingesting the provenance quantum graph as %s.", graph_dataset)
    butler.registry.registerDatasetType(graph_dataset.datasetType)
    butler.ingest(FileDataset(uri, refs=[graph_dataset], formatter=ProvenanceFormatter), transfer=transfer)


def _make_ref_from_info(dataset_id: uuid.UUID, dataset_info: ProvenanceDatasetInfo) -> DatasetRef:
    return DatasetRef(
        dataset_info["pipeline_node"].dataset_type,
        dataset_info["data_id"],
        run=dataset_info["run"],
        id=dataset_id,
    )


@dataclasses.dataclass
class _AuxiliaryHelper:
    to_ingest: FileDataset
    to_delete: list[DatasetRef] = dataclasses.field(default_factory=list)
    existing_dataset_ids: set[uuid.UUID] = dataclasses.field(default_factory=set)
    n_ingested_total: int = 0
    n_deleted_total: int = 0

    def is_batch_ready(self, batch_size: int) -> bool:
        return len(self.to_ingest.refs) > batch_size or len(self.to_delete) > batch_size

    def write(self, butler: Butler) -> None:
        if self.to_delete:
            _LOG.verbose("Deleting %d metadata and/or log datasets.", len(self.to_delete))
            butler.pruneDatasets(self.to_delete, disassociate=True, unstore=True, purge=True)
            self.n_deleted_total += len(self.to_delete)
        if self.to_ingest.refs:
            _LOG.verbose("Ingesting %d metadata and/or log datasets.", len(self.to_ingest.refs))
            butler.ingest(self.to_ingest, transfer=None, record_validation_info=False)
            self.n_ingested_total += len(self.to_ingest.refs)
        self.to_delete.clear()
        self.to_ingest.refs.clear()

    def add_dataset(
        self,
        dataset_id: uuid.UUID,
        xgraph: networkx.DiGraph,
        instruction: Literal["ignore", "delete", "replace"],
    ) -> None:
        if instruction == "ignore":
            return
        dataset_info: ProvenanceDatasetInfo = xgraph.nodes[dataset_id]
        ref = _make_ref_from_info(dataset_id, dataset_info)
        if dataset_id in self.existing_dataset_ids:
            self.to_delete.append(ref)
        if instruction == "replace" and dataset_info["produced"]:
            self.to_ingest.refs.append(ref)
