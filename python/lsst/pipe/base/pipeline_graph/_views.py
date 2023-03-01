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

from collections.abc import Iterable, Iterator, Mapping
from typing import ClassVar, Sequence, TypeVar, cast

import networkx

from ._abcs import Node, NodeKey, NodeType
from ._dataset_types import DatasetTypeNode
from ._tasks import TaskInitNode, TaskNode

_N = TypeVar("_N", bound=Node, covariant=True)
_T = TypeVar("_T", bound=TaskNode, covariant=True)
_D = TypeVar("_D", bound=DatasetTypeNode, covariant=True)


class MappingView(Mapping[str, _N]):
    def __init__(self, parent_xgraph: networkx.DiGraph) -> None:
        self._parent_xgraph = parent_xgraph
        self._keys: list[str] | None = None

    _NODE_TYPE: ClassVar[NodeType]

    def __contains__(self, key: object) -> bool:
        # The given key may not be a str, but if it isn't it'll just fail the
        # check, which is what we want anyway.
        return NodeKey(self._NODE_TYPE, cast(str, key)) in self._parent_xgraph

    def __iter__(self) -> Iterator[str]:
        if self._keys is None:
            self._keys = self._make_keys(self._parent_xgraph)
        return iter(self._keys)

    def __getitem__(self, key: str) -> _N:
        return self._parent_xgraph.nodes[NodeKey(self._NODE_TYPE, key)]["instance"]

    def __len__(self) -> int:
        if self._keys is None:
            self._keys = self._make_keys(self._parent_xgraph)
        return len(self._keys)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self!s})"

    def __str__(self) -> str:
        return f"{{{', '.join(iter(self))}}}"

    def _reorder(self, parent_keys: Sequence[NodeKey]) -> None:
        """Set this view's iteration order according to the given iterable of
        parent keys.

        Parameters
        ----------
        parent_keys : `~collections.abc.Sequence` [ `NodeKey` ]
            Superset of the keys in this view, in the new order.
        """
        self._keys = self._make_keys(parent_keys)

    def _reset(self) -> None:
        """Reset all cached content.

        This should be called by the parent graph after any changes that could
        invalidate the view, causing it to be reconstructed when next
        requested.
        """
        self._keys = None

    def _make_keys(self, parent_keys: Iterable[NodeKey]) -> list[str]:
        """Make a sequence of keys for this view from an iterable of parent
        keys.

        Parameters
        ----------
        parent_keys : `~collections.abc.Iterable` [ `NodeKey` ]
            Superset of the keys in this view.
        """
        return [str(k) for k in parent_keys if k.node_type is self._NODE_TYPE]


class TaskMappingView(MappingView[_T]):
    _NODE_TYPE = NodeType.TASK


class TaskInitMappingView(MappingView[TaskInitNode]):
    _NODE_TYPE = NodeType.TASK_INIT


class DatasetTypeMappingView(MappingView[_D]):
    _NODE_TYPE = NodeType.DATASET_TYPE
