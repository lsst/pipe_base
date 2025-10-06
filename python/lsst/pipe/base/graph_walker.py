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

__all__ = ("GraphWalker",)

from typing import Generic, Self, TypeVar

import networkx

_T = TypeVar("_T")


class GraphWalker(Generic[_T]):
    """A helper for traversing directed acyclic graphs.

    Parameters
    ----------
    xgraph : `networkx.DiGraph` or `networkx.MultiDiGraph`
        Networkx graph to process.  Will be consumed during iteration, so this
        should often be a copy.

    Notes
    -----
    Each iteration yields a `frozenset` of nodes, which may be empty if there
    are no nodes ready for processing.  A node is only considered ready if all
    of its predecessor nodes have been marked as complete with `finish`.
    Iteration only completes when all nodes have been finished or failed.

    `GraphWalker` is not thread-safe; calling one `GraphWalker` method while
    another is in progress is undefined behavior.  It is designed to be used
    in the management thread or process in a parallel traversal.
    """

    def __init__(self, xgraph: networkx.DiGraph | networkx.MultiDiGraph):
        self._xgraph = xgraph
        self._ready: set[_T] = set(next(iter(networkx.dag.topological_generations(self._xgraph)), []))
        self._active: set[_T] = set()
        self._incomplete: set[_T] = set(self._xgraph)

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> frozenset[_T]:
        if not self._incomplete:
            raise StopIteration()
        new_active = frozenset(self._ready)
        self._active.update(new_active)
        self._ready.clear()
        return new_active

    def finish(self, key: _T) -> None:
        """Mark a node as successfully processed, unblocking (at least in part)
        iteration over successor nodes.

        Parameters
        ----------
        key : unspecified
            NetworkX key of the node to mark finished.  Does not need to have
            been returned by the iterator yet.
        """
        self._incomplete.remove(key)
        self._active.discard(key)
        self._ready.discard(key)
        successors = list(self._xgraph.successors(key))
        for successor in successors:
            assert successor not in self._active, (
                "A node downstream of an active one should not have been yielded yet."
            )
            if all(
                predecessor not in self._incomplete for predecessor in self._xgraph.predecessors(successor)
            ):
                self._ready.add(successor)

    def fail(self, key: _T) -> list[_T]:
        """Mark a node as unsuccessfully processed, permanently blocking all
        recursive descendants.

        Parameters
        ----------
        key : unspecified
            NetworkX key of the node to mark as a failure.  Does not need to
            have been returned by the iterator yet.

        Returns
        -------
        blocked : `list`
            NetworkX keys of nodes that were recursive descendants of the
            failed node, and will hence never be yielded by the iterator.
        """
        self._incomplete.remove(key)
        self._active.discard(key)
        self._ready.discard(key)
        descendants = list(networkx.dag.descendants(self._xgraph, key))
        self._xgraph.remove_node(key)
        self._xgraph.remove_nodes_from(descendants)
        self._incomplete.difference_update(descendants)
        return descendants
