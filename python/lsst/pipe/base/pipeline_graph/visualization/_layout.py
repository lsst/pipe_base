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

import itertools
from collections import deque
from typing import Generic, TextIO, TypeVar

import networkx
import networkx.algorithms.components
import networkx.algorithms.dag
import networkx.algorithms.shortest_paths
import networkx.algorithms.traversal

_K = TypeVar("_K")


class Layout(Generic[_K]):
    def __init__(
        self,
        xgraph: networkx.DiGraph | None = None,
        locations: dict[_K, int] | None = None,
        x_max: int = 0,
    ):
        self.xgraph = xgraph if xgraph is not None else networkx.DiGraph()
        self.locations = locations if locations is not None else {}
        self.x_max = x_max

    def build(self) -> None:
        active_columns: dict[int, set[_K]] = {}

        def add_node(node: _K) -> int:
            for active_column_x, active_column_endpoints in list(active_columns.items()):
                if node in active_column_endpoints:
                    active_column_endpoints.remove(node)
                    if not active_column_endpoints:
                        del active_columns[active_column_x]
            for node_x in itertools.count():
                if node_x not in active_columns:
                    break
            outgoing = set(self.xgraph.successors(node))
            self.locations[node] = node_x
            self.x_max = max(node_x, self.x_max)
            if outgoing:
                active_columns[node_x] = outgoing
            return node_x

        def add_graph(xgraph: networkx.DiGraph) -> None:
            n_components = 0
            for component in networkx.algorithms.components.weakly_connected_components(xgraph):
                if len(component) <= 2:
                    for node in component:
                        add_node(node)
                else:
                    add_connected_graph(xgraph.subgraph(component).copy())
                n_components += 1

        def add_connected_graph(xgraph: networkx.DiGraph) -> None:
            order = list(networkx.algorithms.dag.lexicographical_topological_sort(xgraph))
            backbone = deque(networkx.algorithms.dag.dag_longest_path(xgraph, topo_order=order))
            current_node = backbone.popleft()
            while backbone:
                # Actually add the current node.
                add_node(current_node)
                next_node = backbone.popleft()

                # Add nodes to terminate edges from previously-added nodes, as
                # long as there are no paths from the next_node to them.
                x = 0
                while x <= self.x_max:
                    if (active_column_endpoints := active_columns.get(x)) is not None:
                        to_terminate_now = {
                            node
                            for node in active_column_endpoints
                            if node != next_node
                            and node in xgraph
                            and not networkx.algorithms.shortest_paths.has_path(self.xgraph, next_node, node)
                        }
                        if to_terminate_now:
                            if len(to_terminate_now) <= 2:
                                for node in to_terminate_now:
                                    add_node(node)
                            else:
                                add_graph(xgraph.subgraph(to_terminate_now))
                            # Reset iteration because adding nodes adds new
                            # edges that need to be terminated.
                            x = 0
                            continue
                    x += 1

                # Since next_node follows current_node along the longest path,
                # there cannot be any other paths that connect them.  So we
                # can update current to next and continue the loop.
                current_node = next_node
            # Add the last node.
            add_node(current_node)

        add_graph(self.xgraph)

    def print(self, stream: TextIO) -> None:
        for node, x in self.locations.items():
            print(f"{' ' * x}●{' ' * (self.x_max - x)} {node}", file=stream)
