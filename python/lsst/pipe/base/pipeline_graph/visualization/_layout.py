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

__all__ = ("Layout",)

import dataclasses
import itertools
from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import Generic, TextIO, TypeVar

import networkx
import networkx.algorithms.components
import networkx.algorithms.dag
import networkx.algorithms.shortest_paths
import networkx.algorithms.traversal

_K = TypeVar("_K")


@dataclasses.dataclass
class LayoutRow(Generic[_K]):
    node: _K
    x: int
    terminating: list[tuple[int, _K | None]] = dataclasses.field(default_factory=list)
    continuing: list[tuple[int, _K, frozenset[_K]]] = dataclasses.field(default_factory=list)


class Layout(Generic[_K]):
    def __init__(self, graph: networkx.DiGraph):
        self._graph = graph
        self._active_columns: dict[int, set[_K]] = {}
        self._locations: dict[_K, int] = {}
        self._last_x = 0
        self.x_max = 0
        for component in list(networkx.algorithms.components.weakly_connected_components(graph)):
            self._add_connected_graph(graph.subgraph(component))
        del self._active_columns
        del self._last_x

    def _compute_closing_rank(self, node: _K) -> int:
        rank = 2 * self._graph.out_degree(node)
        for destinations in self._active_columns.values():
            if node in destinations:
                rank -= 2
        # Small bonus to rank if this ends an edge in the current column.
        # if node in self._active_columns.get(self._last_x, frozenset()):
        #     rank -= 1
        return rank

    def _add_unblocked_node(self, node: _K) -> set[_K]:
        node_x = None
        for active_column_x, active_column_endpoints in list(self._active_columns.items()):
            if node in active_column_endpoints:
                active_column_endpoints.remove(node)
                if not active_column_endpoints:
                    del self._active_columns[active_column_x]
                    if node_x is None:
                        node_x = active_column_x
        if node_x is None:
            for node_x in itertools.count():
                if node_x not in self._active_columns:
                    break
        outgoing = set(self._graph.successors(node))
        self._locations[node] = node_x
        self.x_max = max(node_x, self.x_max)
        self._last_x = node_x
        if outgoing:
            self._active_columns[node_x] = outgoing
        return outgoing

    def _add_until_successors(self, nodes: Iterable[_K], unblocked: set[_K]) -> bool:
        for node in nodes:
            successors = self._add_unblocked_node(node)
            unblocked.remove(node)
            for successor in successors:
                if all(n in self._locations for n in self._graph.predecessors(successor)):
                    unblocked.add(successor)
            if successors:
                return False
        return True

    def _add_connected_graph(self, xgraph: networkx.DiGraph) -> None:
        unblocked = set(next(networkx.algorithms.dag.topological_generations(xgraph)))
        while unblocked:
            unblocked_by_rank = defaultdict(set)
            for node in unblocked:
                unblocked_by_rank[self._compute_closing_rank(node)].add(node)
            for rank in sorted(unblocked_by_rank):
                if not self._add_until_successors(unblocked_by_rank[rank], unblocked):
                    break

    def print(self, stream: TextIO) -> None:
        for row in self:
            print(f"{' ' * row.x}●{' ' * (self.x_max - row.x)} {row.node}", file=stream)

    def __iter__(self) -> Iterator[LayoutRow]:
        active_edges: dict[_K, set[_K]] = {}
        for node, node_x in self._locations.items():
            row = LayoutRow(node, self.x_max - node_x)
            for origin, destinations in active_edges.items():
                if node in destinations:
                    row.terminating.append((self.x_max - self._locations[origin], origin))
                    destinations.remove(node)
                if destinations:
                    row.continuing.append(
                        (self.x_max - self._locations[origin], origin, frozenset(destinations))
                    )
            row.terminating.sort()
            row.continuing.sort()
            yield row
            active_edges[node] = set(self._graph.successors(node))
