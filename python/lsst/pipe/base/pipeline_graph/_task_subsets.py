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

__all__ = ("LabeledSubset", "MutableLabeledSubset", "ResolvedLabeledSubset")

from collections.abc import Iterator, MutableSet, Set
from typing import Any

import networkx
import networkx.algorithms.boundary

from ._abcs import NodeKey, NodeType
from ._exceptions import PipelineGraphError


class LabeledSubset(Set[str]):
    """An abstract base class whose instances represent a labeled subset of the
    tasks in a pipeline.

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

    def __str__(self) -> str:
        return f"{self.label}: {self.description}, tasks={', '.join(iter(self))}"

    def __contains__(self, key: object) -> bool:
        return key in self._members

    def __len__(self) -> int:
        return len(self._members)

    def __iter__(self) -> Iterator[str]:
        return iter(self._members)

    def _resolved(self, parent_xgraph: networkx.DiGraph) -> ResolvedLabeledSubset:
        """Return a version of this view appropriate for a resolved pipeline
        graph.

        Parameters
        ----------
        parent_xgraph : `networkx.DiGraph`
            The new parent networkx graph that will back the new view.

        Returns
        -------
        resolved : `ResolvedTaskSubsetGraph`
            A resolved version of this object.
        """
        return ResolvedLabeledSubset(parent_xgraph, self.label, self._members.copy(), self._description)

    def _mutable_copy(self, parent_xgraph: networkx.DiGraph) -> MutableLabeledSubset:
        """Return a copy of this view appropriate for a mutable pipeline
        graph.

        Parameters
        ----------
        parent_xgraph : `networkx.DiGraph`
            The new parent networkx graph that will back the new view.

        Returns
        -------
        mutable : `MutableTaskSubsetGraph`
            A mutable version of this object.
        """
        return MutableLabeledSubset(parent_xgraph, self.label, self._members.copy(), self._description)

    def _serialize(self) -> dict[str, Any]:
        """Serialize the content of this subset into a dictionary of built-in
        objects suitable for JSON conversion.

        This should not include the label, as it is always serialized in a
        context that already identifies that.
        """
        return {"description": self._description, "tasks": list(sorted(self))}


class MutableLabeledSubset(LabeledSubset, MutableSet[str]):
    @property
    def description(self) -> str:
        # Docstring inherited.
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        # Docstring inherited.
        self._description = value

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
        self._members.discard(task_label)


class ResolvedLabeledSubset(LabeledSubset):
    pass
