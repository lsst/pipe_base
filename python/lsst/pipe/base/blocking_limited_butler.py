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

__all__ = ["BlockingLimitedButler"]

import logging
import time
from collections.abc import Iterable, Mapping
from typing import Any

from lsst.daf.butler import (
    ButlerMetrics,
    DatasetProvenance,
    DatasetRef,
    DeferredDatasetHandle,
    DimensionUniverse,
    LimitedButler,
    StorageClass,
)

_LOG = logging.getLogger(__name__)


class BlockingLimitedButler(LimitedButler):
    """A `LimitedButler` that blocks until certain dataset types exist.

    Parameters
    ----------
    wrapped : `LimitedButler`
        The butler to wrap.
    timeouts : `~collections.abc.Mapping` [ `str`, `float` or `None` ]
        Timeouts in seconds to wait for different dataset types.  Dataset types
        not included not blocked on (i.e. their timeout is ``0.0``).

    Notes
    -----
    When a timeout is exceeded, `get` will raise `FileNotFoundError` (as usual
    for a dataset that does not exist) and `stored_many` will mark the dataset
    as non-existent.  `getDeferred` does not block.
    """

    def __init__(
        self,
        wrapped: LimitedButler,
        timeouts: Mapping[str, float | None],
    ):
        self._wrapped = wrapped
        self._timeouts = timeouts

    def close(self) -> None:
        self._wrapped.close()

    @property
    def _metrics(self) -> ButlerMetrics:
        # Need to always forward from the wrapped metrics object.
        return self._wrapped._metrics

    @_metrics.setter
    def _metrics(self, metrics: ButlerMetrics) -> None:
        # Allow record_metrics() context manager to override the wrapped
        # butler.
        self._wrapped._metrics = metrics

    def get(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        parent_dataset_type_name = ref.datasetType.nameAndComponent()[0]
        timeout = self._timeouts.get(parent_dataset_type_name, 0.0)
        start = time.time()
        warned = False
        while True:
            try:
                return self._wrapped.get(ref, parameters=parameters, storageClass=storageClass)
            except FileNotFoundError as err:
                if timeout is not None:
                    elapsed = time.time() - start
                    if elapsed > timeout:
                        err.add_note(f"Timed out after {elapsed:03f}s.")
                        raise
            if not warned:
                _LOG.warning(f"Dataset {ref.datasetType} not immediately available for {ref.id}.")
                warned = True
            time.sleep(0.5)

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

    def stored_many(self, refs: Iterable[DatasetRef]) -> dict[DatasetRef, bool]:
        start = time.time()
        result = self._wrapped.stored_many(refs)
        timeouts = {ref.id: self._timeouts.get(ref.datasetType.nameAndComponent()[0], 0.0) for ref in result}
        while True:
            elapsed = time.time() - start
            remaining: list[DatasetRef] = []
            for ref, exists in result.items():
                timeout = timeouts[ref.id]
                if not exists and (timeout is None or elapsed <= timeout):
                    remaining.append(ref)
            if not remaining:
                return result
            result.update(self._wrapped.stored_many(remaining))
            time.sleep(0.5)

    def isWriteable(self) -> bool:
        return self._wrapped.isWriteable()

    def put(self, obj: Any, ref: DatasetRef, /, *, provenance: DatasetProvenance | None = None) -> DatasetRef:
        return self._wrapped.put(obj, ref, provenance=provenance)

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        return self._wrapped.pruneDatasets(
            refs, disassociate=disassociate, unstore=unstore, tags=tags, purge=purge
        )

    @property
    def dimensions(self) -> DimensionUniverse:
        return self._wrapped.dimensions

    @property
    def _datastore(self) -> Any:
        return self._wrapped._datastore

    @_datastore.setter  # demanded by MyPy since we declare it to be an instance attribute in LimitedButler.
    def _datastore(self, value: Any) -> None:
        self._wrapped._datastore = value
