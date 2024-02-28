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

from typing import Any, Iterable

from lsst.daf.butler import DatasetRef, DeferredDatasetHandle, DimensionUniverse, LimitedButler, StorageClass


class CachingLimitedButler(LimitedButler):
    def __init__(self, wrapped: LimitedButler):
        self._wrapped = wrapped
        self._datastore = self._wrapped._datastore
        self.storageClasses = self._wrapped.storageClasses

    def get(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        return self._wrapped.get(ref, parameters=parameters, storageClass=storageClass)

    def getDeferred(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: str | StorageClass | None = None,
    ) -> DeferredDatasetHandle:
        return self._wrapped.getDeferred(ref, parameters=parameters, storageClass=storageClass)

    def stored(self, ref: DatasetRef) -> bool:
        return self._wrapped.stored(ref)

    def stored_many(self, refs: Iterable[DatasetRef]) -> dict[DatasetRef, bool]:
        return self._wrapped.stored_many(refs)

    def isWriteable(self) -> bool:
        return self._wrapped.isWriteable()

    def put(self, obj: Any, ref: DatasetRef) -> DatasetRef:
        return self._wrapped.put(obj, ref)

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = ...,
        purge: bool = False,
    ) -> None:
        return self._wrapped.pruneDatasets(
            refs, disassociate=disassociate, unstore=unstore, tags=tags, purge=purge
        )

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._wrapped.dimensions
