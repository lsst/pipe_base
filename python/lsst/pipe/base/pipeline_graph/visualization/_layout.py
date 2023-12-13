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

__all__ = ("Layout", "ColumnSelector", "LayoutRow")

import dataclasses
from collections.abc import Iterable, Iterator, Mapping, Set
from typing import Generic, TextIO, TypeVar

import networkx
import networkx.algorithms.components
import networkx.algorithms.dag
import networkx.algorithms.shortest_paths
import networkx.algorithms.traversal

_K = TypeVar("_K")


class Layout(Generic[_K]):
    """A class that positions nodes and edges in text-art graph visualizations.

    Parameters
    ----------
    xgraph : `networkx.DiGraph` or `networkx.MultiDiGraph`
        NetworkX export of a `.PipelineGraph` being visualized.
    column_selector : `ColumnSelector`, optional
        Parameterized helper for selecting which column each node should be
        added to.
    """

    def __init__(
        self,
        xgraph: networkx.DiGraph | networkx.MultiDiGraph,
        column_selector: ColumnSelector | None = None,
    ):
        if column_selector is None:
            column_selector = ColumnSelector()
        self._xgraph = xgraph
        self._column_selector = column_selector
        # Mapping from the column (i.e. 'x') of an already-positioned node to
        # the node keys its outgoing edges terminate at. These and all other
        # column/x variables are multiples of 2 when they refer to
        # already-existing columns, allowing potential insertion of new columns
        # between them to be represented by odd integers. Positions are also
        # inverted from the order they are usually displayed; it's best to
        # think of them as the distance (to the left) from the column where
        # text appears on the right.  This is natural because prefer for nodes
        # to be close to that text when possible (or maybe it's historical, and
        # it's just a lot of work to re-invert the algorithm now that it's
        # written).
        self._active_columns: dict[int, set[_K]] = {}
        # Mapping from node key to its column.
        self._locations: dict[_K, int] = {}
        # Minimum and maximum column (may go negative; will be shifted as
        # needed before actual display).
        self._x_min = 0
        self._x_max = 0
        # Run the algorithm!
        self._add_graph(self._xgraph)
        del self._active_columns

    def _add_graph(self, xgraph: networkx.DiGraph | networkx.MultiDiGraph) -> None:
        """Highest-level routine for the layout algorithm.

        Parameters
        ----------
        xgraph : `networkx.DiGraph` or `networkx.MultiDiGraph`
            Graph or subgraph to add to the layout.
        """
        # Start by identifying unconnected subgraphs ("components"); we'll
        # display these in series, as the first of our many attempts to
        # minimize tangles of edges.
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

    def _add_single_node(self, node: _K) -> None:
        """Add a single node to the layout."""
        assert node not in self._locations
        if not self._locations:
            # Special-case the first node in a component (disconnectd
            # subgraph).
            self._locations[node] = 0
            self._active_columns[0] = set(self._xgraph.successors(node))
            return
        # The candidate_x list holds columns where we could insert this node.
        # We start with new columns on the outside and a new column between
        # each pair of existing columns.  These inner nodes are usually not
        # very good candidates, but it's simplest to always include them and
        # let the penalty system in ColumnSelector take care of it.
        candidate_x = [self._x_max + 2, self._x_min - 2]
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
            key=lambda x: self._column_selector(
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
        """Shift all columns above the given one up by two, allowing a new
        column to be inserted while leaving all columns as even integers.
        """
        for node, old_x in self._locations.items():
            if old_x > x:
                self._locations[node] += 2
        self._active_columns = {
            old_x + 2 if old_x > x else old_x: destinations
            for old_x, destinations in self._active_columns.items()
        }
        self._x_max += 2
        return x + 1

    def _add_connected_graph(
        self, xgraph: networkx.DiGraph | networkx.MultiDiGraph, order: list[_K] | None = None
    ) -> None:
        """Add a subgraph whose nodes are connected.

        Parameters
        ----------
        xgraph : `networkx.DiGraph` or `networkx.MultiDiGraph`
            Graph or subgraph to add to the layout.
        order : `list`, optional
            List providing a lexicographical/topological sort of ``xgraph``.
            Will be computed if not provided.
        """
        if order is None:
            order = list(networkx.algorithms.dag.lexicographical_topological_sort(xgraph, key=str))
        # Find the longest path between two nodes, which we'll call the
        # "backbone" of our layout; we'll step through this path and add
        # recurse via calls to `_add_graph` on the nodes that we think should
        # go between the backbone nodes.
        backbone: list[_K] = networkx.algorithms.dag.dag_longest_path(xgraph, topo_order=order)
        # Add the first backbone node and any ancestors according to the full
        # graph (it can't have ancestors in this _subgraph_ because they'd have
        # been part of the longest path themselves, but the subgraph doesn't
        # have a complete picture).
        current = backbone.pop(0)
        self._add_blockers_of(current)
        self._add_single_node(current)
        # Remember all recursive descendants of the current backbone node for
        # later use.
        descendants = frozenset(networkx.algorithms.dag.descendants(xgraph, current))
        while backbone:
            current = backbone.pop(0)
            # Find descendants of the previous node that are:
            # - in this subgraph
            # - not descendants of the current node.
            followers_of_previous = set(descendants)
            descendants = frozenset(networkx.algorithms.dag.descendants(xgraph, current))
            followers_of_previous.remove(current)
            followers_of_previous.difference_update(descendants)
            followers_of_previous.difference_update(self._locations.keys())
            # Add those followers of the previous node.  We like adding these
            # here because this terminates edges as soon as we can, freeing up
            # those columns for new new nodes.
            self._add_graph(xgraph.subgraph(followers_of_previous))
            # Add the current backbone node and all remaining blockers (which
            # may not even be in this subgraph).
            self._add_blockers_of(current)
            self._add_single_node(current)
        # Any remaining subgraph nodes were not directly connected to the
        # backbone nodes.
        remaining = xgraph.copy()
        remaining.remove_nodes_from(self._locations.keys())
        self._add_graph(remaining)

    def _add_blockers_of(self, node: _K) -> None:
        """Add all nodes that are ancestors of the given node according to the
        full graph.
        """
        blockers = set(networkx.algorithms.dag.ancestors(self._xgraph, node))
        blockers.difference_update(self._locations.keys())
        self._add_graph(self._xgraph.subgraph(blockers))

    @property
    def width(self) -> int:
        """The number of actual (not multiple-of-two) columns in the layout."""
        return (self._x_max - self._x_min) // 2

    @property
    def nodes(self) -> Iterable[_K]:
        """The graph nodes in the order they appear in the layout."""
        return self._locations.keys()

    def print(self, stream: TextIO) -> None:
        """Print the nodes (but not their edges) as symbols in the right
        locations.

        This is intended for use as a debugging diagnostic, not part of a real
        visualization system.

        Parameters
        ----------
        stream : `io.TextIO`
            Output stream to use for printing.
        """
        for row in self:
            print(f"{' ' * row.x}●{' ' * (self.width - row.x)} {row.node}", file=stream)

    def _external_location(self, x: int) -> int:
        """Return the actual (not multiple-of-two, stating from zero) location,
        given the internal multiple-of-two.
        """
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


@dataclasses.dataclass
class LayoutRow(Generic[_K]):
    """Information about a single text-art row in a graph."""

    node: _K
    """Key for the node in the exported NetworkX graph."""

    x: int
    """Column of the node's symbol and its outgoing edges."""

    connecting: list[tuple[int, _K]] = dataclasses.field(default_factory=list)
    """The columns and node keys of edges that terminate at this row.
    """

    continuing: list[tuple[int, _K, frozenset[_K]]] = dataclasses.field(default_factory=list)
    """The columns and node keys of edges that continue through this row.
    """


@dataclasses.dataclass
class ColumnSelector:
    """Helper class that weighs the columns a new node could be added to in a
    text DAG visualization.
    """

    crossing_penalty: int = 1
    """Penalty for each ongoing edge the new node's outgoing edge would have to
    "hop" if it were put at the candidate column.
    """

    interior_penalty: int = 1
    """Penalty for adding a new column to the layout between existing columns
    (in addition to `insertion_penaly`.
    """

    insertion_penalty: int = 2
    """Penalty for adding a new column to the layout instead of reusing an
    empty one.

    This penalty is applied even when there is no empty column; it just cancels
    out in that case because it's applied to all candidate columns.
    """

    def __call__(
        self,
        connecting_x: list[int],
        node_x: int,
        active_columns: Mapping[int, Set[_K]],
        x_min: int,
        x_max: int,
    ) -> int:
        """Compute the penalty score for adding a node in the given column.

        Parameters
        ----------
        connecting_x : `list` [ `int` ]
            The columns of incoming edges for this node.  All values are even.
        node_x : `int`
            The column being considered for the new node.  Will be odd if it
            proposes an insertion between existing columns, or outside the
            bounds of ``x_min`` and ``x_max`` if it proposes an insertion
            on a side.
        active_columns : `~collections.abc.Mapping` [ `int`, \
                `~collections.abc.Set` ]
            The columns of nodes already in the visualization (in previous
            lines) and the nodes at which their edges terminate.  All keys are
            even.
        x_min : `int`
            Current minimum column position (inclusive).  Always even.
        x_max : `int`
            Current maximum column position (exclusive).  Always even.

        Returns
        -------
        penalty : `int`
            Penalty score for this location.  Nodes should be placed at the
            column with the lowest penalty.
        """
        # Start with a penalty for inserting a new column between two existing
        # columns or on either side, if that's what this is (i.e. x is odd).
        penalty = (node_x % 2) * (self.interior_penalty + self.insertion_penalty)
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
