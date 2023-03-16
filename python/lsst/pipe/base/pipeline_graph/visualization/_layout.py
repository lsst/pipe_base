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
from collections import defaultdict
from collections.abc import Iterable, Iterator
from typing import Generic, Protocol, TextIO, TypeVar

import networkx
import networkx.algorithms.components
import networkx.algorithms.dag
import networkx.algorithms.shortest_paths
import networkx.algorithms.traversal


class KeyProtocol(Protocol):
    def __eq__(self, arg: object) -> bool:
        ...

    def __hash__(self) -> int:
        ...

    def __lt__(self, arg: object) -> bool:
        ...


_K = TypeVar("_K", bound=KeyProtocol)


@dataclasses.dataclass
class LayoutRow(Generic[_K]):
    node: _K
    x: int
    connecting: list[tuple[int, _K | None]] = dataclasses.field(default_factory=list)
    continuing: list[tuple[int, _K, frozenset[_K]]] = dataclasses.field(default_factory=list)


class Layout(Generic[_K]):
    def __init__(
        self,
        graph: networkx.DiGraph,
        *,
        unique_edge_rank_factor: int = 1,
        duplicate_edge_rank_factor: int = 1,
        closing_edge_rank_factor: int = 2,
        closing_node_rank_factor: int = 1,
        interior_column_penalty: int = 2,
        crossing_penalty: int = 1,
        insertion_penalty_threshold: int = 2,
    ):
        self.unique_edge_rank_factor = unique_edge_rank_factor
        self.duplicate_edge_rank_factor = duplicate_edge_rank_factor
        self.closing_edge_rank_factor = closing_edge_rank_factor
        self.closing_node_rank_factor = closing_node_rank_factor
        self.interior_column_penalty = interior_column_penalty
        self.crossing_penalty = crossing_penalty
        self.insertion_penalty_threshold = insertion_penalty_threshold
        self._graph = graph
        self._active_columns: dict[int, set[_K]] = {}
        self._locations: dict[_K, int] = {}
        self._x_min = 0
        self._x_max = 0
        assert networkx.algorithms.components.is_weakly_connected(self._graph)
        self._unblocked = set(next(networkx.algorithms.dag.topological_generations(self._graph)))
        self._add_connected_graph()
        del self._unblocked
        del self._active_columns

    @property
    def width(self) -> int:
        return (self._x_max - self._x_min) // 2

    def _compute_row_rank(self, node: _K) -> int:
        rank = 0
        successors = set(self._graph.successors(node))
        rank += self.duplicate_edge_rank_factor * len(successors)
        for destinations in self._active_columns.values():
            if node in destinations:
                rank -= self.closing_edge_rank_factor
                if len(destinations) == 1:
                    rank -= self.closing_node_rank_factor
            successors.difference_update(destinations)
        rank += (self.unique_edge_rank_factor - self.duplicate_edge_rank_factor) * len(successors)
        return rank

    def _find_candidate_columns(self, node: _K) -> tuple[list[int], list[int], bool]:
        # The columns holding edges that will connect to this node.
        connecting_x = []
        # The columns holding edges that will connect to this node, leaving no
        # remaining edges in that column; if any of these exist, they'll be
        # good candidates for where to put the new node, since they'll minimize
        # crossings without adding a new column.
        candidate_x = []
        # Iterate over active columns to populate the above.
        for active_column_x, active_column_endpoints in self._active_columns.items():
            if node in active_column_endpoints:
                connecting_x.append(active_column_x)
                if len(active_column_endpoints) == 1:
                    candidate_x.append(active_column_x)
        # Sort the list of connecting columns so we can easily get min and max.
        connecting_x.sort()
        # If there are empty columns that were previously filled, those are
        # also candidates.
        for x in range(self._x_min, self._x_max + 2, 2):
            if x not in self._active_columns:
                candidate_x.append(x)
        candidate_x.sort()
        if candidate_x:
            return connecting_x, candidate_x, False
        # If we have to add a new column, consider doing it on either side or
        # between any pair of active columns.
        candidate_x.append(self._x_min - 2)
        candidate_x.extend(range(self._x_min + 1, self._x_max, 2))
        candidate_x.append(self._x_max + 2)
        return connecting_x, candidate_x, True

    def _find_best_column(self, connecting_x: list[int], candidate_x: list[int]) -> tuple[int, int]:
        mid_x = (self._x_max + self._x_min) // 2
        best_x = candidate_x.pop()
        best_penalty = self._compute_column_penalty(connecting_x, best_x)
        while candidate_x:
            next_x = candidate_x.pop()
            next_penalty = self._compute_column_penalty(connecting_x, next_x)
            if next_penalty < best_penalty or (
                next_penalty == best_penalty and abs(next_x - mid_x) < abs(best_x - mid_x)
            ):
                best_x = next_x
                best_penalty = next_penalty
        return best_x, best_penalty

    def _compute_column_penalty(self, connecting_x: list[int], node_x: int) -> int:
        if not connecting_x:
            return 0
        min_x = min(connecting_x[0], node_x)
        max_x = max(connecting_x[-1], node_x)
        return (
            # Penalty is the (scaled) number of unrelated continuing (vertical)
            # edges that the (horizontal) input edges for this node have to
            # "hop"...
            sum(
                self.crossing_penalty
                for x in range(min_x, max_x + 2)
                if x in self._active_columns and x not in connecting_x
            )
            # ... plus an extra something if this is a proposal to insert a
            # new column between two existing columns.
            + (node_x % 2) * self.interior_column_penalty
        )

    def _add_unblocked_node(self, node: _K) -> bool:
        connecting_x, candidate_x, new_column = self._find_candidate_columns(node)
        # Delete this node from the active columns it is in, and delete any
        # entries that now have empty sets.
        for x in connecting_x:
            destinations = self._active_columns[x]
            destinations.remove(node)
            if not destinations:
                del self._active_columns[x]
        node_x, penalty = self._find_best_column(connecting_x, candidate_x)
        if not new_column and penalty > self.insertion_penalty_threshold:
            # We found an empty column, but it's got a pretty high penalty;
            # try an insertion to see if we can do better.
            candidate_x = [node_x]
            candidate_x.append(self._x_min)
            candidate_x.extend(range(self._x_min + 1, self._x_max, 2))
            candidate_x.append(self._x_max)
            node_x, _ = self._find_best_column(connecting_x, candidate_x)
        if node_x % 2:
            # We're inserting a new column between two existing ones; shift
            # all existing column values above this one to make room while
            # using only even numbers.
            node_x = self._shift(node_x)
        successors = set(self._graph.successors(node))
        self._unblocked.remove(node)
        self._locations[node] = node_x
        self._x_min = min(node_x, self._x_min)
        self._x_max = max(node_x, self._x_max)
        if successors:
            self._active_columns[node_x] = successors
            for successor in successors:
                if all(n in self._locations for n in self._graph.predecessors(successor)):
                    self._unblocked.add(successor)
        return bool(successors)

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

    def _add_until_successors(self, nodes: Iterable[_K]) -> bool:
        for node in sorted(nodes):
            if self._add_unblocked_node(node):
                return True
        return False

    def _add_connected_graph(self) -> None:
        while self._unblocked:
            unblocked_by_rank = defaultdict(set)
            for node in self._unblocked:
                unblocked_by_rank[self._compute_row_rank(node)].add(node)
            for rank in sorted(unblocked_by_rank):
                if self._add_until_successors(unblocked_by_rank[rank]):
                    break

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
            row.connecting.sort()
            row.continuing.sort()
            yield row
            active_edges[node] = set(self._graph.successors(node))
