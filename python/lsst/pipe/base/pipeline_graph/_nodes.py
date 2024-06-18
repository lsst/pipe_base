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

__all__ = (
    "NodeKey",
    "NodeType",
)

import enum
from typing import NamedTuple


class NodeType(enum.Enum):
    """Enumeration of the types of nodes in a PipelineGraph."""

    DATASET_TYPE = 0
    TASK_INIT = 1
    TASK = 2

    @property
    def bipartite(self) -> int:
        """The integer used as the "bipartite" key in networkx exports of a
        `PipelineGraph`.

        This key is used by the `networkx.algorithms.bipartite` module.
        """
        return int(self is not NodeType.DATASET_TYPE)

    def __lt__(self, other: NodeType) -> bool:
        # We define __lt__ only to be able to provide deterministic tiebreaking
        # on top of topological ordering of `PipelineGraph`` and views thereof.
        return self.value < other.value


class NodeKey(NamedTuple):
    """A special key type for nodes in networkx graphs.

    Notes
    -----
    Using a tuple for the key allows tasks labels and dataset type names with
    the same string value to coexist in the graph.  These only rarely appear in
    `PipelineGraph` public interfaces; when the node type is implicit, bare
    `str` task labels or dataset type names are used instead.

    NodeKey objects stringify to just their name, which is used both as a way
    to convert to the `str` objects used in the main public interface and as an
    easy way to usefully stringify containers returned directly by networkx
    algorithms (especially in error messages).  Note that this requires `repr`,
    not just `str`, because Python builtin containers always use `repr` on
    their items, even in their implementations for `str`.
    """

    node_type: NodeType
    """Node type enum for this key."""

    name: str
    """Task label or dataset type name.

    This is always the parent dataset type name for component dataset types.
    """

    def __repr__(self) -> str:
        return self.name

    def __str__(self) -> str:
        return self.name

    @property
    def node_id(self) -> str:
        return f"{self.name}:{self.node_type.value}"
