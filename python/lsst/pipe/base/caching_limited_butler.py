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

__all__ = ["CachingLimitedButler"]

import logging
from collections.abc import Set
from typing import Any, Iterable

from lsst.daf.butler import (
    DatasetId,
    DatasetRef,
    DeferredDatasetHandle,
    DimensionUniverse,
    LimitedButler,
    StorageClass,
)

from ._dataset_handle import InMemoryDatasetHandle

_LOG = logging.getLogger(__name__)


class CachingLimitedButler(LimitedButler):
    """A `LimitedButler` that caches datasets.

    A `CachingLimitedButler` caches on both `.put()` and `.get()`, and holds a
    single instance of the most recently used dataset type for that put/get.

    The dataset types which will be cached on put/get are controlled via the
    `cache_on_put` and `cache_on_get` attributes, respectively.

    By default, copies of the cached items are returned on `get`, so that code
    is free to operate on data in-place. A `no_copy_on_cache` attribute also
    exists to tell the `CachingLimitedButler` not to return copies when it is
    known that the calling code can be trusted not to change values, e.g. when
    passing calibs to `isrTask`.

    Parameters
    ----------
    wrapped : `LimitedButler`
        The butler to wrap.
    cache_on_put : `set` [`str`], optional
        The dataset types to cache on put.
    cache_on_get : `set` [`str`], optional
        The dataset types to cache on get.
    no_copy_on_cache : `set` [`str`], optional
        The dataset types for which to not return copies when cached.
    """

    def __init__(
        self,
        wrapped: LimitedButler,
        cache_on_put: Set[str] = frozenset(),
        cache_on_get: Set[str] = frozenset(),
        no_copy_on_cache: Set[str] = frozenset(),
    ):
        self._wrapped = wrapped
        self._datastore = self._wrapped._datastore
        self.storageClasses = self._wrapped.storageClasses
        self._cache_on_put = cache_on_put
        self._cache_on_get = cache_on_get
        self._cache: dict[str, tuple[DatasetId, InMemoryDatasetHandle]] = {}
        self._no_copy_on_cache = no_copy_on_cache

    def get(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        if storageClass is None:
            storageClass = ref.datasetType.storageClass
        elif isinstance(storageClass, str):
            storageClass = self.storageClasses.getStorageClass(storageClass)

        # check if we have this dataset type in the cache
        if cached := self._cache.get(ref.datasetType.name):
            dataset_id, handle = cached
            if dataset_id == ref.id:  # if we do, check it's the right object
                _LOG.debug("Returning cached dataset %s", ref)
                return handle.get(parameters=parameters, storageClass=storageClass)

        obj = self._wrapped.get(ref, parameters=parameters, storageClass=storageClass)
        if ref.datasetType.name in self._cache_on_get and not parameters:
            handle = InMemoryDatasetHandle(
                obj,
                storageClass=storageClass,
                dataId=ref.dataId,
                copy=ref.datasetType.name not in self._no_copy_on_cache,
            )
            # and not parameters is to make sure we don't cache sub-images etc
            self._cache[ref.datasetType.name] = (ref.id, handle)
            _LOG.debug("Cached dataset %s", ref)
            # make sure copy fires if needed
            return handle.get()
        return obj

    def getDeferred(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: str | StorageClass | None = None,
    ) -> DeferredDatasetHandle:
        # note that this does not use the cache at all
        return self._wrapped.getDeferred(ref, parameters=parameters, storageClass=storageClass)

    def stored(self, ref: DatasetRef) -> bool:
        return self.stored_many([ref])[ref]  # TODO: remove this once DM-43086 is done.

    def stored_many(self, refs: Iterable[DatasetRef]) -> dict[DatasetRef, bool]:
        result = {}
        unknown_refs = []
        for ref in refs:
            if cached := self._cache.get(ref.datasetType.name):
                dataset_id, _ = cached
                if dataset_id == ref.id:
                    result[ref] = True
                    continue
            unknown_refs.append(ref)

        result.update(self._wrapped.stored_many(unknown_refs))
        return result

    def isWriteable(self) -> bool:
        return self._wrapped.isWriteable()

    def put(self, obj: Any, ref: DatasetRef) -> DatasetRef:
        if ref.datasetType.name in self._cache_on_put:
            self._cache[ref.datasetType.name] = (
                ref.id,
                InMemoryDatasetHandle(
                    obj,
                    storageClass=ref.datasetType.storageClass,
                    dataId=ref.dataId,
                    copy=ref.datasetType.name not in self._no_copy_on_cache,
                ),
            )
            _LOG.debug("Cached dataset %s on put", ref)
        return self._wrapped.put(obj, ref)

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        refs = list(refs)
        for ref in refs:
            if cached := self._cache.get(ref.datasetType.name):
                dataset_id, _ = cached
                if dataset_id == ref.id:
                    del self._cache[ref.datasetType.name]

        return self._wrapped.pruneDatasets(
            refs, disassociate=disassociate, unstore=unstore, tags=tags, purge=purge
        )

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._wrapped.dimensions
