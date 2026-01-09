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

__all__ = ("ProvenanceFormatter",)

import uuid
from typing import Any, ClassVar

import pydantic

from lsst.daf.butler import FormatterV2
from lsst.resources import ResourcePath
from lsst.utils.logging import getLogger

from ..pipeline_graph import TaskImportMode
from ._provenance import ProvenanceQuantumGraphReader

_LOG = getLogger(__file__)


class _ProvenanceFormatterParameters(pydantic.BaseModel):
    """A Pydantic model for validating and applying defaults to the
    read parameters of `ProvenanceFormatter`.
    """

    import_mode: TaskImportMode = TaskImportMode.DO_NOT_IMPORT
    quanta: list[uuid.UUID] | None = None
    datasets: list[uuid.UUID] | None = None
    read_init_quanta: bool = True

    @pydantic.field_validator("quanta", mode="before")
    @classmethod
    def quanta_to_list(cls, v: Any) -> list[uuid.UUID]:
        return list(v)

    @pydantic.field_validator("datasets", mode="before")
    @classmethod
    def datasets_to_list(cls, v: Any) -> list[uuid.UUID]:
        return list(v)

    @property
    def nodes(self) -> list[uuid.UUID]:
        if self.quanta is not None:
            if self.datasets is not None:
                return self.quanta + self.datasets
            else:
                return self.quanta
        elif self.datasets is not None:
            return self.datasets
        raise ValueError("'datasets' and/or 'quanta' parameters are required for this component")


class ProvenanceFormatter(FormatterV2):
    """Butler interface for reading `ProvenanceQuantumGraph` objects."""

    default_extension: ClassVar[str] = ".qg"
    can_read_from_uri: ClassVar[bool] = True

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        parameters = _ProvenanceFormatterParameters.model_validate(self.file_descriptor.parameters or {})
        with ProvenanceQuantumGraphReader.open(uri, import_mode=parameters.import_mode) as reader:
            match component:
                case None:
                    if parameters.read_init_quanta:
                        reader.read_init_quanta()
                    reader.read_quanta(parameters.quanta)
                    reader.read_datasets(parameters.datasets)
                    return reader.graph
                case "metadata":
                    return reader.fetch_metadata(parameters.nodes)
                case "logs":
                    return reader.fetch_logs(parameters.nodes)
                case "packages":
                    return reader.fetch_packages()
        raise AssertionError(f"Unexpected component {component!r}.")
