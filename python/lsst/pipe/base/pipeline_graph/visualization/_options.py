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
from __future__ import annotations

__all__ = ("NodeAttributeOptions", "Brevity")

import dataclasses
import enum
from typing import Literal

from .._pipeline_graph import PipelineGraph


class Brevity(enum.Enum):
    FULL = enum.auto()
    CONCISE = enum.auto()

    def __bool__(self) -> Literal[True]:
        return True


@dataclasses.dataclass
class NodeAttributeOptions:
    dimensions: Brevity | None
    task_classes: Brevity | None
    storage_classes: bool

    def __bool__(self) -> bool:
        return bool(self.dimensions or self.storage_classes or self.task_classes)

    def check(self, pipeline_graph: PipelineGraph) -> None:
        is_resolved = hasattr(pipeline_graph, "universe")
        if self.dimensions and not is_resolved:
            raise ValueError("Cannot show dimensions unless they have been resolved.")
        if self.storage_classes and not is_resolved:
            raise ValueError("Cannot show storage classes unless they have been resolved.")
