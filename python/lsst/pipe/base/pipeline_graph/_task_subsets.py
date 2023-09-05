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

__all__ = ("TaskSubset",)

from collections.abc import Iterator, MutableSet

import networkx
import networkx.algorithms.boundary

from ._exceptions import PipelineGraphError
from ._nodes import NodeKey, NodeType


class TaskSubset(MutableSet[str]):
    """A specialized set that represents a labeles subset of the tasks in a
    pipeline graph.

    Instances of this class should never be constructed directly; they should
    only be accessed via the `PipelineGraph.task_subsets` attribute and created
    by the `PipelineGraph.add_task_subset` method.

    Parameters
    ----------
    parent_xgraph : `networkx.DiGraph`
        Parent networkx graph that this subgraph is part of.
    label : `str`
        Label associated with this subset of the pipeline.
    members : `set` [ `str` ]
        Labels of the tasks that are members of this subset.
    description : `str`, optional
        Description string associated with this labeled subset.

    Notes
    -----
    Iteration order is arbitrary, even when the parent pipeline graph is
    ordered (there is no guarantee that an ordering of the tasks in the graph
    implies a consistent ordering of subsets).
    """

    def __init__(
        self,
        parent_xgraph: networkx.DiGraph,
        label: str,
        members: set[str],
        description: str,
    ):
        self._parent_xgraph = parent_xgraph
        self._label = label
        self._members = members
        self._description = description

    @property
    def label(self) -> str:
        """Label associated with this subset of the pipeline."""
        return self._label

    @property
    def description(self) -> str:
        """Description string associated with this labeled subset."""
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        # Docstring in getter.
        self._description = value

    def __repr__(self) -> str:
        return f"{self.label}: {self.description!r}, tasks={{{', '.join(iter(self))}}}"

    def __contains__(self, key: object) -> bool:
        return key in self._members

    def __len__(self) -> int:
        return len(self._members)

    def __iter__(self) -> Iterator[str]:
        return iter(self._members)

    def add(self, task_label: str) -> None:
        """Add a new task to this subset.

        Parameters
        ----------
        task_label : `str`
            Label for the task.  Must already be present in the parent pipeline
            graph.
        """
        key = NodeKey(NodeType.TASK, task_label)
        if key not in self._parent_xgraph:
            raise PipelineGraphError(f"{task_label!r} is not a task in the parent pipeline.")
        self._members.add(key.name)

    def discard(self, task_label: str) -> None:
        """Remove a task from the subset if it is present.

        Parameters
        ----------
        task_label : `str`
            Label for the task.  Must already be present in the parent pipeline
            graph.
        """
        self._members.discard(task_label)
