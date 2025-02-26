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

__all__ = ("StepDefinitions", "TaskSubset")

from collections.abc import Iterable, Iterator, MutableSet
from contextlib import contextmanager

import networkx
import networkx.algorithms.boundary

from lsst.daf.butler import DimensionGroup, DimensionUniverse

from ._exceptions import InvalidStepsError, PipelineGraphError, UnresolvedGraphError
from ._nodes import NodeKey, NodeType


class TaskSubset(MutableSet[str]):
    """A specialized set that represents a labeled subset of the tasks in a
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
    step_definitions : `StepDefinitions`
        Information about special 'step' subsets that partition the pipeline.

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
        step_definitions: StepDefinitions,
    ):
        self._parent_xgraph = parent_xgraph
        self._label = label
        self._members = members
        self._description = description
        self._step_definitions = step_definitions

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

    @property
    def is_step(self) -> bool:
        """Whether this subset is a step."""
        return self.label in self._step_definitions

    @property
    def dimensions(self) -> DimensionGroup:
        """The dimensions that can be used to split up this subset's quanta
        into independent groups.

        This is only available if `is_step` is `True` and only if the pipeline
        graph has been resolved.
        """
        return self._step_definitions.get_dimensions(self.label)

    @dimensions.setter
    def dimensions(self, dimensions: Iterable[str] | DimensionGroup) -> None:
        # Docstring in getter.
        self._step_definitions.set_dimensions(self.label, dimensions)

    def __repr__(self) -> str:
        return f"{self.label}: {self.description!r}, tasks={{{', '.join(iter(self))}}}"

    def __contains__(self, key: object) -> bool:
        return key in self._members

    def __len__(self) -> int:
        return len(self._members)

    def __iter__(self) -> Iterator[str]:
        return iter(self._members)

    def add(self, value: str) -> None:
        """Add a new task to this subset.

        Parameters
        ----------
        value : `str`
            Label for the task.  Must already be present in the parent pipeline
            graph.
        """
        key = NodeKey(NodeType.TASK, value)
        if key not in self._parent_xgraph:
            raise PipelineGraphError(f"{value!r} is not a task in the parent pipeline.")
        with self._step_definitions._unverified_on_success():
            self._members.add(key.name)

    def discard(self, value: str) -> None:
        """Remove a task from the subset if it is present.

        Parameters
        ----------
        value : `str`
            Label for the task.  Must already be present in the parent pipeline
            graph.
        """
        with self._step_definitions._unverified_on_success():
            self._members.discard(value)

    @classmethod
    def _from_iterable(cls, iterable: Iterable[str]) -> set[str]:
        # This is the hook used by collections.abc.Set when implementing
        # operators that return new sets.  In this case, we want those to be
        # regular `set` (builtin) objects, not `TaskSubset` instances.
        return set(iterable)


class StepDefinitions:
    """A collection of the 'steps' defined in a pipeline graph.

    Steps are special task subsets that must be executed separately.  They may
    also be associated with "sharding dimensions", which are the dimensions of
    data IDs that are independent within the step: splitting up a quantum graph
    along a step's sharding dimensions produces groups that can be safely
    executed independently.

    Parameters
    ----------
    universe : `lsst.daf.butler.DimensionUniverse`, optional
        Definitions for data dimensions.
    dimensions_by_label : `dict` [ `str`, `frozenset` [ `str` ] ], optional
        Sharding dimensions for step subsets, as dictionary with task labels as
        keys and sets of dimension names as values.
    verified : `bool`, optional
        Whether the step definitions have been checked since the last time
        they or some other relevant aspect of the pipeline graph was changed.

    Notes
    -----
    This class only models `collections.abc.Collection` (it is iterable, sized,
    and can be used with ``in`` tests on label names), but it also supports
    `append`, `remove`, and `reset` for modifications.
    """

    def __init__(
        self,
        universe: DimensionUniverse | None = None,
        dimensions_by_label: dict[str, frozenset[str]] | None = None,
        verified: bool = False,
    ):
        self._universe = universe
        self._dimensions_by_label = dimensions_by_label if dimensions_by_label is not None else {}
        self._verified = verified

    @property
    def verified(self) -> bool:
        """Whether the step definitions have been checked since the last time
        they or some other relevant aspect of the pipeline graph was changed.

        This is always `True` if there are no step definitions.
        """
        # If there are no steps, the step definitions are still verified.
        return self._verified or not self._dimensions_by_label

    def __contains__(self, label: object) -> bool:
        return label in self._dimensions_by_label

    def __iter__(self) -> Iterator[str]:
        return iter(self._dimensions_by_label)

    def __len__(self) -> int:
        return len(self._dimensions_by_label)

    def __repr__(self) -> str:
        return str(list(self._dimensions_by_label))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, StepDefinitions):
            return self._dimensions_by_label == other._dimensions_by_label
        return NotImplemented

    def copy(self) -> StepDefinitions:
        """Create a new instance that does not share any mutable state with
        this one.
        """
        return StepDefinitions(
            universe=self._universe,
            dimensions_by_label=self._dimensions_by_label.copy(),
            verified=self._verified,
        )

    def append(self, label: str, dimensions: Iterable[str] | DimensionGroup = ()) -> None:
        """Append a new step.

        Parameters
        ----------
        label : `str`
            Task subset label for the new step.
        dimensions : `~collections.abc.Iterable` [ `str` ] or \
                `~lsst.daf.butler.DimensionGroup`, optional
            Dimensions that can be used to split up the step's quanta
            into independent groups.
        """
        if self._universe is not None:
            dimensions = self._universe.conform(dimensions)
        if isinstance(dimensions, DimensionGroup):
            dimensions = frozenset(dimensions.names)
        else:
            dimensions = frozenset(dimensions)
        with self._unverified_on_success():
            self._dimensions_by_label[label] = dimensions

    def remove(self, label: str) -> None:
        """Remove a named step.

        Parameters
        ----------
        label : `str`
            Task subset label to remove from the list of steps.

        Notes
        -----
        This does not remove the task subset itself; it just "demotes" it to a
        non-step subset.
        """
        with self._unverified_on_success():
            del self._dimensions_by_label[label]

    def assign(self, labels: Iterable[str]) -> None:
        """Set all step definitions to the given labels.

        Parameters
        ----------
        labels : `~collections.abc.Iterable` [ `str` ]
            Subset labels to use as the new steps.

        Notes
        -----
        Sharding dimensions are preserved for any label that was previously a
        step.  If ``labels`` is a `StepDefinitions`` instance, sharding
        dimensions from that instance will be used.
        """
        if isinstance(labels, StepDefinitions):
            with self._unverified_on_success():
                self._dimensions_by_label = labels._dimensions_by_label.copy()
        else:
            with self._unverified_on_success():
                self._dimensions_by_label = {
                    label: self._dimensions_by_label.get(label, frozenset()) for label in labels
                }

    def clear(self) -> None:
        """Remove all step definitions."""
        self.assign(())

    def get_dimensions(self, label: str) -> DimensionGroup:
        """Return the dimensions that can be used to split up a step's quanta
        into independent groups.

        Parameters
        ----------
        label : `str`
            Label for the step.

        Returns
        -------
        dimensions : `lsst.daf.butler.DimensionGroup`
            Dimensions that can be used to split up this step's quanta.
        """
        try:
            raw_dimensions = self._dimensions_by_label[label]
        except KeyError:
            raise InvalidStepsError(f"Task subset {label!r} is not a step.") from None
        if self._universe is not None:
            return self._universe.conform(raw_dimensions)
        else:
            raise UnresolvedGraphError("Step sharding dimensions have not been resolved.")

    def set_dimensions(self, label: str, dimensions: Iterable[str] | DimensionGroup) -> None:
        """Set the dimensions that can be used to split up a step's quanta
        into independent groups.

        Parameters
        ----------
        label : `str`
            Label for the step.
        dimensions : `lsst.daf.butler.DimensionGroup`
            Dimensions that can be used to split up this step's quanta.
        """
        if label not in self._dimensions_by_label:
            raise PipelineGraphError(f"Subset {label!r} is not a step.")
        if self._universe is not None:
            dimensions = self._universe.conform(dimensions)
        if isinstance(dimensions, DimensionGroup):
            dimensions = frozenset(dimensions.names)
        else:
            dimensions = frozenset(dimensions)
        with self._unverified_on_success():
            self._dimensions_by_label[label] = dimensions

    @contextmanager
    def _unverified_on_success(self) -> Iterator[None]:
        """Return the a context manager that marks the step definitions as
        unverified if it is exited without an exception.

        This should be used only for exception-safe modifications for which an
        exception means no changes were made (and hence the verified state can
        remain unchanged as well).
        """
        yield
        self._verified = False
