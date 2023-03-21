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
from collections.abc import Iterable, Iterator, Mapping, Set
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
    connecting: list[tuple[int, _K]] = dataclasses.field(default_factory=list)
    continuing: list[tuple[int, _K, frozenset[_K]]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ColumnPenalty:
    interior_column_penalty: int = 1
    crossing_penalty: int = 1
    insertion_penalty: int = 2

    def __call__(
        self,
        connecting_x: list[int],
        node_x: int,
        active_columns: Mapping[int, Set[_K]],
        x_min: int,
        x_max: int,
    ) -> int:
        # Start with a penalty for inserting a new column between two existing
        # columns or on either side.
        penalty = (node_x % 2) * (self.interior_column_penalty + self.insertion_penalty)
        if node_x < x_min:
            penalty += self.insertion_penalty
        elif node_x > x_max:
            penalty += self.insertion_penalty
        # If there are no active edges connecting to this node, we're done.
        if not connecting_x:
            return penalty
        # Find the bounds of the horizontal lines that connect
        horizontal_min_x = min(connecting_x[0], node_x)
        horizontal_max_x = max(connecting_x[-1], node_x)
        # Add the (scaled) number of unrelated continuing (vertical) edges that
        # the (horizontal) input edges for this node would have to "hop".
        penalty += sum(
            self.crossing_penalty
            for x in range(horizontal_min_x, horizontal_max_x + 2)
            if x in active_columns and x not in connecting_x
        )
        return penalty


class Layout(Generic[_K]):
    def __init__(
        self,
        xgraph: networkx.DiGraph,
        column_penalty: ColumnPenalty | None = None,
    ):
        if column_penalty is None:
            column_penalty = ColumnPenalty()
        self._xgraph = xgraph
        self._column_penalty = column_penalty
        self._active_columns: dict[int, set[_K]] = {}
        self._locations: dict[_K, int] = {}
        self._x_min = 0
        self._x_max = 0
        self._add_graph(self._xgraph)
        del self._active_columns

    def _add_single_node(self, node: _K) -> None:
        assert node not in self._locations
        if not self._locations:
            self._locations[node] = 0
            self._active_columns[0] = set(self._xgraph.successors(node))
            return
        # The candidates_x list holds columns where we could insert this node.
        # We start with new columns on the outside and a new column between
        # each pair of existing columns.  These are usually not very good
        # candidates, but it's simpler to always include them and let the
        # penalty system take care of it.
        candidate_x = [self._x_max + 2, self._x_min - 2]
        # candidate_x.extend(range(self._x_min + 1, self._x_max, 2))
        # The columns holding edges that will connect to this node.
        connecting_x = []
        # Iterate over active columns to populate the above.
        for active_column_x, active_column_endpoints in self._active_columns.items():
            if node in active_column_endpoints:
                connecting_x.append(active_column_x)
        # Delete this node from the active columns it is in, and delete any
        # entries that now have empty sets.
        for x in connecting_x:
            destinations = self._active_columns[x]
            destinations.remove(node)
            if not destinations:
                del self._active_columns[x]
        # Add all empty columns between the current min and max as candidates.
        for x in range(self._x_min, self._x_max + 2, 2):
            if x not in self._active_columns:
                candidate_x.append(x)
        # Sort the list of connecting columns so we can easily get min and max.
        connecting_x.sort()
        best_x = min(
            candidate_x,
            key=lambda x: self._column_penalty(
                connecting_x, x, self._active_columns, self._x_min, self._x_max
            ),
        )
        if best_x % 2:
            # We're inserting a new column between two existing ones; shift
            # all existing column values above this one to make room while
            # using only even numbers.
            best_x = self._shift(best_x)
        self._x_min = min(self._x_min, best_x)
        self._x_max = max(self._x_max, best_x)

        self._locations[node] = best_x
        successors = set(self._xgraph.successors(node))
        if successors:
            self._active_columns[best_x] = successors

    def _shift(self, x: int) -> int:
        for node, old_x in self._locations.items():
            if old_x > x:
                self._locations[node] += 2
        self._active_columns = {
            old_x + 2 if old_x > x else old_x: destinations
            for old_x, destinations in self._active_columns.items()
        }
        self._x_max += 2
        return x + 1

    def _add_graph(self, xgraph: networkx.DiGraph) -> None:
        component_xgraphs_and_orders = []
        single_nodes = []
        for component_nodes in networkx.components.weakly_connected_components(xgraph):
            if len(component_nodes) == 1:
                single_nodes.append(component_nodes.pop())
            else:
                component_xgraph = xgraph.subgraph(component_nodes)
                component_order = list(
                    networkx.algorithms.dag.lexicographical_topological_sort(component_xgraph, key=str)
                )
                component_xgraphs_and_orders.append((component_xgraph, component_order))
        # Add all single-node components in lexicographical order.
        single_nodes.sort(key=str)
        for node in single_nodes:
            self._add_single_node(node)
        # Sort component graphs by their size and then str of their first node.
        component_xgraphs_and_orders.sort(key=lambda t: (len(t[1]), str(t[1][0])))
        # Add subgraphs in that order.
        for component_xgraph, component_order in component_xgraphs_and_orders:
            self._add_connected_graph(component_xgraph, component_order)

    def _add_connected_graph(self, xgraph: networkx.DiGraph, order: list[_K] | None = None) -> None:
        if order is None:
            order = list(networkx.algorithms.dag.lexicographical_topological_sort(xgraph, key=str))
        backbone: list[_K] = networkx.algorithms.dag.dag_longest_path(xgraph, topo_order=order)
        current = backbone.pop(0)
        self._add_blockers_of(current)
        self._add_single_node(current)
        descendants = frozenset(networkx.algorithms.dag.descendants(xgraph, current))
        while backbone:
            current = backbone.pop(0)
            # Find descendants of the prevoius node that are:
            # - in this subgraph
            # - not descendants of the current node.
            followers_of_previous = set(descendants)
            descendants = frozenset(networkx.algorithms.dag.descendants(xgraph, current))
            followers_of_previous.remove(current)
            followers_of_previous.difference_update(descendants)
            followers_of_previous.difference_update(self._locations.keys())
            # Add those followers of the previous node.
            self._add_graph(xgraph.subgraph(followers_of_previous))
            # Add the current backbone node and all remaining blockers (which
            # may not even be in this subgraph).
            self._add_blockers_of(current)
            self._add_single_node(current)
        remaining = xgraph.copy()
        remaining.remove_nodes_from(self._locations.keys())
        self._add_graph(remaining)

    def _add_blockers_of(self, node: _K) -> None:
        blockers = set(networkx.algorithms.dag.ancestors(self._xgraph, node))
        blockers.difference_update(self._locations.keys())
        self._add_graph(self._xgraph.subgraph(blockers))

    @property
    def width(self) -> int:
        return (self._x_max - self._x_min) // 2

    @property
    def nodes(self) -> Iterable[_K]:
        return self._locations.keys()

    def print(self, stream: TextIO) -> None:
        for row in self:
            print(f"{' ' * row.x}●{' ' * (self.width - row.x)} {row.node}", file=stream)

    def _external_location(self, x: int) -> int:
        return (self._x_max - x) // 2

    def __iter__(self) -> Iterator[LayoutRow]:
        active_edges: dict[_K, set[_K]] = {}
        for node, node_x in self._locations.items():
            row = LayoutRow(node, self._external_location(node_x))
            for origin, destinations in active_edges.items():
                if node in destinations:
                    row.connecting.append((self._external_location(self._locations[origin]), origin))
                    destinations.remove(node)
                if destinations:
                    row.continuing.append(
                        (self._external_location(self._locations[origin]), origin, frozenset(destinations))
                    )
            row.connecting.sort(key=str)
            row.continuing.sort(key=str)
            yield row
            active_edges[node] = set(self._xgraph.successors(node))
