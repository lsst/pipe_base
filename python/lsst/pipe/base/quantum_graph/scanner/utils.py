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

__all__ = ("SequentialExecutor",)

import concurrent.futures
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from typing import ParamSpec, TypeVar, cast

from lsst.daf.butler import DatasetRef, QuantumBackedButler
from lsst.daf.butler.datastore import FileTransferMap, FileTransferSource
from lsst.resources import ResourcePath

_T = TypeVar("_T")
_P = ParamSpec("_P")


class SequentialExecutor(concurrent.futures.Executor):
    """An implementation of `concurrent.futures.Executor` that doesn't
    parallelize at all.
    """

    def submit(
        self, fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs
    ) -> concurrent.futures.Future[_T]:
        f = concurrent.futures.Future[_T]()
        try:
            result = fn(*args, **kwargs)
        except BaseException as e:
            f.set_exception(e)
        else:
            f.set_result(result)
        return f

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        pass


class ScannedFileTransferSource(FileTransferSource):
    name: str = "scanned output datasets"

    @classmethod
    @contextmanager
    def wrap(cls, qbb: QuantumBackedButler, refs: Iterable[DatasetRef]) -> Iterator[None]:
        artifact_existence: dict[ResourcePath, bool] = {}
        for paths in qbb.get_many_uris(refs, predict=True).values():
            if paths.primaryURI is not None:
                artifact_existence[paths.primaryURI] = True
            for path in paths.componentURIs.values():
                artifact_existence[path] = True
        try:
            qbb._file_transfer_source = ScannedFileTransferSource(
                qbb._file_transfer_source, artifact_existence
            )
            yield
        finally:
            qbb._file_transfer_source = cast(ScannedFileTransferSource, qbb._file_transfer_source)._base

    def __init__(self, base: FileTransferSource, artifact_existence: dict[ResourcePath, bool]):
        self._base = base
        self._artifact_existence = artifact_existence

    def get_file_info_for_transfer(self, dataset_ids: Iterable[uuid.UUID]) -> FileTransferMap:
        return self._base.get_file_info_for_transfer(dataset_ids)

    def locate_missing_files_for_transfer(
        self, refs: Iterable[DatasetRef], artifact_existence: dict[ResourcePath, bool]
    ) -> FileTransferMap:
        artifact_existence.update(self._artifact_existence)
        return self._base.locate_missing_files_for_transfer(refs, artifact_existence)
