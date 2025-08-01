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

__all__ = ["InMemoryLimitedButler"]

import logging
import uuid
from collections.abc import Iterable
from typing import Any

from lsst.daf.butler import (
    ButlerMetrics,
    DatasetProvenance,
    DatasetRef,
    DatasetType,
    DimensionUniverse,
    LimitedButler,
    MissingDatasetTypeError,
    Quantum,
    StorageClass,
    StorageClassFactory,
)
from lsst.daf.butler.registry import ConflictingDefinitionError

from .._dataset_handle import InMemoryDatasetHandle

_LOG = logging.getLogger(__name__)


class InMemoryLimitedButler(LimitedButler):
    """A `LimitedButler` that just stores datasets in an in-memory mapping.

    Parameters
    ----------
    universe : `lsst.daf.butler.DimensionUniverse`
        Definitions for all dimensions.
    dataset_types : `~collections.abc.Iterable` [ \
            `lsst.daf.butler.DatasetType` ]
        Definitions of all dataset types.

    Notes
    -----
    This is an incomplete implementation of the `LimitedButler` interface
    intended only for tests.  It supports all methods required by
    `SingleQuantumExecutor`, but not transfers or URI retrieval.

    While this class supports storage class conversions in `get` and `put`, it
    uses different code paths from real butlers, and should not be used in
    tests in which storage class correctness is part of what is being tested.

    Objects are always copied (via storage class machinery) by `get`.

    Pickling this class will pickle all datasets already `put` (which must be
    pickleable).  This generally allows a central butler to be initialized with
    input datasets in one process and distributed to worker processes that run
    quanta *once*, but it does not allow outputs from a worker process to be
    distributed to others or the originating process.  This can be hard to
    notice because quanta will usually be skipped with
    `lsst.pipe.base.NoWorkFound` (a success!) when all of their inputs are
    missing.
    """

    def __init__(self, universe: DimensionUniverse, dataset_types: Iterable[DatasetType] = ()):
        self.storageClasses = StorageClassFactory()
        self._universe = universe
        self._datasets: dict[uuid.UUID, tuple[DatasetRef, InMemoryDatasetHandle]] = {}
        self._metrics = ButlerMetrics()
        self._dataset_types = {dt.name: dt for dt in dataset_types}
        assert not any(dt.component() for dt in self._dataset_types.values()), (
            "Dataset type definitions must not be components."
        )

    def __getstate__(self) -> dict[str, Any]:
        # Pickle customization is needed because StorageClassFactory is not
        # pickleable.
        return {
            "universe": self._universe,
            "datasets": self._datasets,
            "dataset_types": self._dataset_types,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.storageClasses = StorageClassFactory()
        self._universe = state["universe"]
        self._datasets = state["datasets"]
        self._metrics = ButlerMetrics()
        self._dataset_types = state["dataset_types"]

    def get_datasets(self, dataset_type: str | None = None) -> dict[DatasetRef, object]:
        """Return datasets that have been `put` to this butler.

        Storage classes and corresponding Python types will match the dataset
        type definitions provided at butler construction, which may not be the
        same as what was `put`.

        Parameters
        ----------
        dataset_type : `str`, optional
            Dataset type name used to filter results.

        Returns
        -------
        refs : `dict` [ `lsst.daf.butler.DatasetRef`, `object` ]
            Datasets held by this butler.
        """
        return {
            ref: handle.get()
            for ref, handle in self._datasets.values()
            if dataset_type is None or dataset_type == ref.datasetType.name
        }

    def isWriteable(self) -> bool:
        return True

    def put(self, obj: Any, ref: DatasetRef, /, *, provenance: DatasetProvenance | None = None) -> DatasetRef:
        with self._metrics.instrument_put():
            assert not ref.isComponent(), "Component dataset types cannot be put."
            if ref.id in self._datasets:
                # Some butlers may not raise reliably when a dataset already
                # exists (it's hard to be rigorous in parallel given different
                # guarantees provided by storage), but we don't want code to
                # rely on it not being an error, so we want a test butler to
                # always complain.
                raise ConflictingDefinitionError(f"Dataset {ref} already exists.")
            if (repo_dataset_type := self._dataset_types.get(ref.datasetType.name)) is None:
                raise MissingDatasetTypeError(f"Dataset type {ref.datasetType.name!r} not recognized.")
            repo_dataset_type.storageClass.coerce_type(obj)
            self._datasets[ref.id] = (
                ref.overrideStorageClass(repo_dataset_type.storageClass),
                InMemoryDatasetHandle(
                    obj,
                    storageClass=repo_dataset_type.storageClass,
                    dataId=ref.dataId,
                    copy=True,
                ),
            )
        return ref

    def get(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        with self._metrics.instrument_get():
            if storageClass is None:
                storageClass = ref.datasetType.storageClass
            elif isinstance(storageClass, str):
                storageClass = self.storageClasses.getStorageClass(storageClass)
            if entry := self._datasets.get(ref.id):
                (ref, handle) = entry
                return handle.get(
                    component=ref.datasetType.component(), parameters=parameters, storageClass=storageClass
                )
        raise FileNotFoundError(f"Dataset {ref} does not exist.")

    def stored_many(self, refs: Iterable[DatasetRef]) -> dict[DatasetRef, bool]:
        return {ref: ref.id in self._datasets for ref in refs}

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        for ref in refs:
            self._datasets.pop(ref.id, None)

    @property
    def _datastore(self) -> Any:
        raise NotImplementedError("This test butler does not have a datastore.")

    @_datastore.setter  # demanded by MyPy since we declare it to be an instance attribute in LimitedButler.
    def _datastore(self, value: Any) -> None:
        raise NotImplementedError("This test butler does not have a datastore.")

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._universe

    def factory(self, quantum: Quantum) -> InMemoryLimitedButler:
        """Return ``self``.

        This method can be used as the ``limited_butler_factory`` argument to
        `.single_quantum_executor.SingleQuantumExecutor`.

        Parameters
        ----------
        quantum : `lsst.daf.butler.Quantum`
            Ignored.
        """
        return self
