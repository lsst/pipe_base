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

__all__ = ("PrescannedFileTransferSource", "Timer")

import dataclasses
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import cast

from lsst.daf.butler import DatasetRef, QuantumBackedButler
from lsst.daf.butler.datastore import FileTransferMap, FileTransferSource
from lsst.resources import ResourcePath

from ...pipeline_graph import PipelineGraph


class PrescannedFileTransferSource(FileTransferSource):
    """A proxy implementation of `lsst.daf.butler.datastore.FileTransferSource`
    that is initialized with prior knowledge of artifacts existence.

    Parameters
    ----------
    base : `lsst.daf.butler.datastore.FileTransferSource`
        The original file transfer source to proxy.
    artifact_existence : `dict` [`lsst.resources.ResourcePath`, `bool`]
        Mapping from dataset artifact path to whether that artifact exists.
    """

    def __init__(self, base: FileTransferSource, artifact_existence: dict[ResourcePath, bool]):
        self._base = base
        self._artifact_existence = artifact_existence

    @classmethod
    @contextmanager
    def wrap_qbb(cls, qbb: QuantumBackedButler, refs: Iterable[DatasetRef]) -> Iterator[None]:
        """Temporarily install this proxy as the file transfer source for a
        quantum-backed butler.

        Parameters
        ----------
        qbb : `lsst.daf.butler.QuantumBackedButler`
            Quantum-backed butler to temporarily modify in place.
        refs : `~collections.abc.Iterable` [`lsst.daf.butler.DatasetRef`]
            Datasets that are already known to exist, with datastore records
            that can be predicted by the quantum-backed butler.

        Returns
        -------
        context : `AbstractContextManager`
            Context manager that manages the duration of the modification to
            the quantum-backed butler.  Returns `None` when entered.
        """
        artifact_existence: dict[ResourcePath, bool] = {}
        for paths in qbb.get_many_uris(refs, predict=True).values():
            if paths.primaryURI is not None:
                artifact_existence[paths.primaryURI] = True
            for path in paths.componentURIs.values():
                artifact_existence[path] = True
        try:
            qbb._file_transfer_source = PrescannedFileTransferSource(
                qbb._file_transfer_source, artifact_existence
            )
            yield
        finally:
            qbb._file_transfer_source = cast(PrescannedFileTransferSource, qbb._file_transfer_source)._base

    @property
    def name(self) -> str:
        # Docstring inherited.
        return self._base.name

    @name.setter
    def name(self, value: str) -> None:
        # Docstring inherited.
        self._base.name = value

    def get_file_info_for_transfer(self, dataset_ids: Iterable[uuid.UUID]) -> FileTransferMap:
        # Docstring inherited.
        return self._base.get_file_info_for_transfer(dataset_ids)

    def locate_missing_files_for_transfer(
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool]
    ) -> FileTransferMap:
        # Docstring inherited.
        artifact_existence.update(self._artifact_existence)
        return self._base.locate_missing_files_for_transfer(refs, artifact_existence)


def make_qbb(butler_config: str, pipeline_graph: PipelineGraph) -> QuantumBackedButler:
    return QuantumBackedButler.from_predicted(
        butler_config,
        predicted_inputs=[],
        predicted_outputs=[],
        dimensions=pipeline_graph.universe,
        # We don't need the datastore records in the QG because we're
        # only going to read metadata and logs, and those are never
        # overall inputs.
        datastore_records={},
        dataset_types={node.name: node.dataset_type for node in pipeline_graph.dataset_types.values()},
    )


@dataclasses.dataclass(slots=True)
class Timer:
    """A simple timer context manager."""

    total: float = 0.0

    @contextmanager
    def activate(self) -> Iterator[None]:
        start = time.time()
        try:
            yield
        finally:
            self.total += time.time() - start
