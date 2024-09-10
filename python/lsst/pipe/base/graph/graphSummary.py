# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

import sys
from collections import Counter
from typing import Any

import pydantic

from .quantumNode import BuildId

__all__ = ("QgraphSummary", "QgraphTaskSummary")


class QgraphTaskSummary(pydantic.BaseModel):
    """Quanta information summarized for single PipelineTask."""

    taskLabel: str | None = None
    """PipelineTask label."""

    numQuanta: int = 0
    """Number of Quanta for this PipelineTask in this QuantumGraph."""

    numInputs: dict[str, int] = Counter()
    """Total number of inputs per dataset type name for this PipelineTask."""

    numOutputs: dict[str, int] = Counter()
    """Total number of outputs per dataset type name for this PipelineTask."""

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules:  # pragma: no cover

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump_json(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)


class QgraphSummary(pydantic.BaseModel):
    """Report for the QuantumGraph creation or reading."""

    graphID: BuildId
    """QuantumGraph ID."""

    cmdLine: str | None = None
    """Command line for creation of original QuantumGraph."""

    creationUTC: str | None = None
    """Time of creation."""

    inputCollection: list[str] | None = None
    """Input collection."""

    outputCollection: str | None = None
    """Output collection."""

    outputRun: str | None = None
    """Output run collection."""

    qgraphTaskSummaries: dict[str, QgraphTaskSummary] = {}
    """Quanta information summarized per PipelineTask."""

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules:  # pragma: no cover

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump_json(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)
