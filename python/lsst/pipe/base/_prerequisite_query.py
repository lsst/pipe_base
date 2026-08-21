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

__all__ = ("PrerequisiteQuery",)

from collections.abc import Callable, Iterable, Set
from typing import TYPE_CHECKING

from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionGroup,
)
from lsst.daf.butler.queries import Query

if TYPE_CHECKING:
    from .pipeline_graph import TaskNode


class PrerequisiteQuery:
    """A customizable hook for controlling prerequisite input connnection
    queries in a quantum graph build.

    Parameters
    ----------
    constraint_dimensions
        The dimensions that should be used as a data ID constraint for the
        query.  The default is to use the quantum dimensions.  Subclasses
        that override `get_constraint_dimensions` can opt to ignore this
        parameter.
    augment
        A function to augment the query with additional constraints.  This
        is passed a query with the dataset type search and constraint data IDs
        already joined in.  If a subclass overrides `run`, it can opt not to
        delegate to `augment` at all.

    Notes
    -----
    This class is designed to be used directly in simple cases where one or
    more construction parameters can be used to get the desired behavior.
    For more complicated cases this class should be subclassed.
    """

    def __init__(
        self,
        *,
        constraint_dimensions: Iterable[str] | None = None,
        augment: Callable[[Query], Query] | None = None,
    ):
        self._constraint_dimensions = (
            frozenset(constraint_dimensions) if constraint_dimensions is not None else None
        )
        self._augment = augment

    def get_constraint_dimensions(
        self, quantum_dimensions: DimensionGroup, dataset_dimensions: DimensionGroup
    ) -> DimensionGroup:
        """Return the dimensions that should be used as a data ID constraint
        for the query.

        Parameters
        ----------
        quantum_dimensions
            The dimensions of the task's quanta.
        dataset_dimensions
            The dimensions of the prerequisite input dataset.

        Notes
        -----
        The default behavior for prerequisites that don't use a custom finder
        is to constrain the search with the quantum dimensions; for example,
        to find a ``flat`` calibration with ``{detector, physical_filter}``
        dimensions for a task with ``{detector, exposure}`` dimensions, we
        use the ``{detector, exposure}`` quantum data IDs as a constraint on
        the query, which naturally joins on ``detector``, `physical_filter``
        (implied by ``exposure``) and
        ``exposure.timespan OVERLAPS flat.timespan``.

        Overriding these dimensions lets related data IDs play that role
        instead.  For this to work well, the new dimensions need to be related
        to the quantum dimensions somehow, and some other aspect of the build
        needs to constrain it. In the previous example, constraint dimensions
        of ``{visit, detector}`` would be an obvious choice, but would only
        yield a different result if the overall build's data query or
        input-dataset queries limited the set of detectors to something other
        than "all of them".
        """
        if self._constraint_dimensions is not None:
            return quantum_dimensions.universe.conform(self._constraint_dimensions)
        return quantum_dimensions

    @property
    def needs_constraint_dimension_records(self) -> bool:
        """If `True`, the ``constraint_data_ids`` argument `query` will be
        guaranteed to have dimension records attached to all data IDs.
        """
        return False

    @property
    def needs_quantum_dimension_records(self) -> bool:
        """If `True`, the ``quantum_data_ids`` argument `query` will be
        guaranteed to have dimension records attached to all data IDs.
        """
        return False

    def run(
        self,
        butler: Butler,
        dataset_type: DatasetType,
        constraint_data_ids: Set[DataCoordinate],
        quantum_data_ids: Set[DataCoordinate],
        task_node: TaskNode,
    ) -> dict[DataCoordinate, list[DatasetRef]]:
        """Run the query for this prerequisite input connection.

        Parameters
        ----------
        butler
            A read-only butler initialized with the QG build's input
            collections as its default collections.
        dataset_type
            The prerequisite input's dataset type, with any storage class
            overrides requested by the connection.
        constraint_data_ids
            Data IDs to be used as a constraint on the query, with the
            dimensions returned by `get_constraint_dimensions`.
        quantum_data_ids
            The data IDs of all quanta that need prerequisites attached.  May
            be the same object as `constraint_data_ids`.
        task_node
            The node for this task in the pipeline graph.  Use
            ``task_node.config`` for configuration-dependent queries.

        Returns
        -------
        `dict[`lsst.daf.butler.DataCoordinate`, `list` \
                [`lsst.daf.butler.DatasetRef`] ]
            A mapping from quantum data ID to a list of dataset references for
            this connection.  Not including a quantum data ID is the same as
            associating it with an empty list - it does not drop that quantum
            (at least not immediately; if ``minimum > 0``
            `~PipelineTaskConnections.adjustQuantum` would drop it later).
            Datasets must have the given dataset type, with exactly that
            storage class (this is not necessarily checked).

        Notes
        -----
        The default implementation of this method actually just returns
        `NotImplemented`, with the actual default behavior implemented in
        the quantum graph builder itself, because this opens up optimizations
        that are not available to custom implementations.
        """
        return NotImplemented

    def augment(self, query: Query, task_node: TaskNode) -> Query:
        """Augment the default query with additional constraints.

        Parameters
        ----------
        query
            Default query, with the dataset type search and constraint
            data IDs joined in.
        task_node
            The node for this task in the pipeline graph.  Use
            ``task_node.config`` for configuration-dependent queries.

        Notes
        -----
        This primarily exists to allow an additional ``where`` restriction to
        be included, either as a filter or an explicit join clause.  It cannot
        be used to expand the query or control the dimensions or dataset type
        that are returned.  Note that the high-level data-query constraint
        used to constrain the entire quantum graph build is used when
        querying for the constraint data IDs, and does not need to be repeated
        here.
        """
        if self._augment is not None:
            return self._augment(query)
        return query
