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

"""Helper classes for finding prerequisite input datasets during
QuantumGraph generation.
"""

from __future__ import annotations

__all__ = (
    "PrerequisiteBounds",
    "PrerequisiteFinder",
    "PrerequisiteInfo",
    "SkyPixBoundsBuilder",
    "TimespanBuilder",
)

import dataclasses
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import cast

from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionElement,
    Registry,
    SkyPixDimension,
    Timespan,
)
from lsst.daf.butler.registry import MissingDatasetTypeError
from lsst.sphgeom import RangeSet, Region

from .pipeline_graph import DatasetTypeNode, PipelineGraph, ReadEdge, TaskNode


@dataclasses.dataclass
class PrerequisiteInfo:
    """A QuantumGraph-generation helper class that manages the searches for all
    prerequisite input connections for a task.

    Parameters
    ----------
    task_node : `TaskNode`
        The relevant node.
    pipeline_graph : `PipelineGraph`
        The pipeline graph.
    """

    bounds: PrerequisiteBounds
    """Another helper object that manages the spatial/temporal bounds of the
    task's quanta.
    """

    finders: dict[str, PrerequisiteFinder]
    """Mapping of helper objects responsible for a single prerequisite input
    connection.

    Keys are connection names.  Elements of this dictionary should be removed
    by implementations of `QuantumGraphBuilder.process_subgraph` to take
    responsibility for finding them away from the the `QuantumGraphBuilder`
    base class.
    """

    def __init__(self, task_node: TaskNode, pipeline_graph: PipelineGraph):
        self.bounds = PrerequisiteBounds(task_node)
        self.finders = {
            edge.connection_name: PrerequisiteFinder(edge, self.bounds, pipeline_graph)
            for edge in task_node.prerequisite_inputs.values()
        }

    def update_bounds(self) -> None:
        """Inspect the current state of `finders` and update `bounds` to
        reflect the needs of only the finders that remain.
        """
        self.bounds.all_dataset_skypix.clear()
        self.bounds.any_dataset_has_timespan = False
        for finder in self.finders.values():
            self.bounds.all_dataset_skypix.update(finder.dataset_skypix)
            self.bounds.any_dataset_has_timespan = (
                self.bounds.any_dataset_has_timespan or finder.dataset_has_timespan
            )


class PrerequisiteFinder:
    """A QuantumGraph-generation helper class that manages the searches for a
    prerequisite input connection.

    Parameters
    ----------
    edge : `pipeline_graph.ReadEdge`
        A `~pipeline_graph.PipelineGraph` edge that represents a single
        prerequisite input connection.
    bounds : `PrerequisiteBounds`
        Another helper object that manages the spatial/temporal bounds of the
        task's quanta, shared by all prerequisite inputs for that task.
    pipeline_graph : `pipeline_graph.PipelineGraph`
        Graph representation of the pipeline.

    Notes
    -----
    `PrerequisiteFinder` instances are usually constructed by a
    `PrerequisiteInfo` instance, which is in turn constructed by and attached
    to the base `QuantumGraphBuilder` when a new builder is constructed. During
    the `QuantumGraphBuilder.process_subgraph` hook implemented by a builder
    subclass, prerequisite inputs may be found in other ways (e.g. via bulk
    queries), as long as the results are consistent with the finder's
    attributes, and this is indicated to the base `QuantumGraphBuilder` by
    removing those finder instances after those prerequisites have been found
    and added to a `QuantumGraphSkeleton`.  Finder instances that remain in the
    builder are used by calling `PrerequisiteFinder.find` on each quantum
    later in `QuantumGraphBuilder.build`.
    """

    def __init__(
        self,
        edge: ReadEdge,
        bounds: PrerequisiteBounds,
        pipeline_graph: PipelineGraph,
    ):
        self.edge = edge
        self._bounds = bounds
        self.dataset_type_node = pipeline_graph.dataset_types[edge.parent_dataset_type_name]
        self.lookup_function = self.task_node.get_lookup_function(edge.connection_name)
        self.dataset_skypix = {}
        self.dataset_other_spatial = {}
        self.dataset_has_timespan = False
        self.constraint_dimensions = self.task_node.dimensions
        if self.lookup_function is None:
            for family in self.dataset_type_node.dimensions.spatial - self.task_node.dimensions.spatial:
                best_spatial_element = family.choose(self.dataset_type_node.dimensions)
                if isinstance(best_spatial_element, SkyPixDimension):
                    self.dataset_skypix[best_spatial_element.name] = best_spatial_element
                else:
                    self.dataset_other_spatial[best_spatial_element.name] = cast(
                        DimensionElement, best_spatial_element
                    )
            self.dataset_has_timespan = bool(
                # If the task dimensions has a temporal family that isn't in
                # the dataset type (i.e. "observation_timespans", like visit
                # or exposure)...
                self.task_node.dimensions.temporal - self.dataset_type_node.dimensions.temporal
            ) and (
                # ...and the dataset type has a temporal family that isn't in
                # the task dimensions, or is a calibration, the prerequisite
                # search needs a temporal join.  Note that the default
                # dimension universe only has one temporal dimension family, so
                # in practice this just means "calibration lookups when visit
                # or exposure is in the task dimensions".
                self.dataset_type_node.is_calibration
                or bool(self.dataset_type_node.dimensions.temporal - self.task_node.dimensions.temporal)
            )
            new_constraint_dimensions = set()
            universe = self.task_node.dimensions.universe
            for dimension_name in self.task_node.dimensions.names:
                if dimension_name in self.dataset_type_node.dimensions.names:
                    new_constraint_dimensions.add(dimension_name)
                else:
                    dimension = universe[dimension_name]
                    if not (dimension.spatial or dimension.temporal):
                        new_constraint_dimensions.add(dimension_name)
            self.constraint_dimensions = universe.conform(new_constraint_dimensions)

    edge: ReadEdge
    """The `~pipeline_graph.PipelineGraph` edge that represents the
    prerequisite input connection.
    """

    dataset_type_node: DatasetTypeNode
    """The `~pipeline_graph.PipelineGraph` node that represents the dataset
    type of this connection.

    This always uses the registry storage class and is never a component
    dataset type.
    """

    lookup_function: (
        Callable[[DatasetType, Registry, DataCoordinate, Sequence[str]], Iterable[DatasetRef]] | None
    )
    """A task-provided callback for finding these datasets.

    If this is not `None`, it must be used to ensure correct behavior.
    """

    dataset_skypix: dict[str, SkyPixDimension]
    """Dimensions representing a pixelization of the sky used by the dataset
    type for this connection that are also not part of the task's dimensions.

    Keys are dimension names.  It is at least extremely rare for this
    dictionary to have more than one element.
    """

    dataset_other_spatial: dict[str, DimensionElement]
    """Spatial dimensions other than sky pixelizations used by the dataset type
    for this connection that are also not part of the task's dimensions.
    """

    dataset_has_timespan: bool
    """Whether the dataset has a timespan that should be used in the lookup,
    either because it is a calibration dataset or because it has temporal
    dimensions that are not part of the tasks's dimensions.
    """

    @property
    def task_node(self) -> TaskNode:
        """The `~pipeline_graph.PipelineGraph` node that represents the task
        for this connection.
        """
        return self._bounds.task_node

    def find(
        self,
        butler: Butler,
        input_collections: Sequence[str],
        data_id: DataCoordinate,
        skypix_bounds: Mapping[str, RangeSet],
        timespan: Timespan | None,
    ) -> list[DatasetRef]:
        """Find prerequisite input datasets for a single quantum.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Butler client to use for queries.
        input_collections : `~collections.abc.Sequence` [ `str` ]
            Sequence of collections to search, in order.
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for the quantum.
        skypix_bounds : `Mapping` [ `str`, `lsst.sphgeom.RangeSet` ]
            The spatial bounds of this quantum in various skypix dimensions.
            Keys are skypix dimension names (a superset of those in
            `dataset_skypix`) and values are sets of integer pixel ID ranges.
        timespan : `lsst.daf.butler.Timespan` or `None`
            The temporal bounds of this quantum.  Guaranteed to not be `None`
            if `dataset_has_timespan` is `True`.

        Returns
        -------
        refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            Dataset references.  These use
            ``self.dataset_type_node.dataset_type``, which may differ from the
            connection's dataset type in storage class or [lack of] component.

        Raises
        ------
        NotImplementedError
            Raised for certain relationships between task and dataset type
            dimensions that are possible to define but not believed to be
            useful in practice.  These errors occur late rather than early in
            order to allow a `QuantumGraphBuilder` subclass to handle them
            first, in case an unusual task's needs must be met by a custom
            builder class anyway.
        """
        if self.lookup_function:
            # If there is a lookup function, just use it; nothing else matters.
            return [
                self.dataset_type_node.generalize_ref(ref)
                for ref in self.lookup_function(
                    self.edge.adapt_dataset_type(self.dataset_type_node.dataset_type),
                    butler.registry,
                    data_id,
                    input_collections,
                )
                if ref is not None
            ]
        if self.dataset_type_node.dimensions <= self.constraint_dimensions:
            # If this is a calibration dataset and the dataset doesn't have
            # any dimensions that aren't constrained by the quantum data
            # ID, we know there'll only be one result, and that means we
            # can call Butler.find_dataset, which takes a timespan. Note
            # that the AllDimensionsQuantumGraphBuilder subclass will
            # intercept this case in order to optimize it when:
            #
            #  - PipelineTaskConnections.getTemporalBoundsConnections is
            #    empty;
            #
            #  - the quantum data IDs have temporal dimensions;
            #
            # and when that happens PrerequisiteFinder.find never gets
            # called.
            try:
                ref = butler.find_dataset(
                    self.dataset_type_node.dataset_type,
                    data_id.subset(self.constraint_dimensions),
                    collections=input_collections,
                    timespan=timespan,
                )
            except MissingDatasetTypeError:
                ref = None
            return [ref] if ref is not None else []
        elif self.dataset_has_timespan:
            extra_dimensions = self.dataset_type_node.dimensions.names - self.constraint_dimensions.names
            raise NotImplementedError(
                f"No support for calibration lookup {self.task_node.label}.{self.edge.connection_name} "
                f"with dimension(s) {extra_dimensions} not fully constrained by the task. "
                "Please create a feature-request ticket and use a lookup function in the meantime."
            )
        if self.dataset_skypix:
            if not self.dataset_has_timespan and not self.dataset_other_spatial:
                # If the dataset has skypix dimensions but is not otherwise
                # spatial or temporal (this describes reference catalogs and
                # things like them), we can stuff the skypix IDs we want into
                # the query via bind parameters and call queryDatasets.  Once
                # again AllDimensionsQuantumGraphBuilder will often intercept
                # this case in order to optimize it, when:
                #
                #  - PipelineTaskConnections.getSpatialBoundsConnections is
                #    empty;
                #
                #  - the quantum data IDs have spatial dimensions;
                #
                # and when that happens PrerequisiteFinder.find never gets
                # called.
                where_terms: list[str] = []
                bind: dict[str, list[int]] = {}
                for name in self.dataset_skypix:
                    where_terms.append(f"{name} IN ({name}_pixels)")
                    pixels: list[int] = []
                    for begin, end in skypix_bounds[name]:
                        pixels.extend(range(begin, end))
                    bind[f"{name}_pixels"] = pixels
                try:
                    return butler.query_datasets(
                        self.dataset_type_node.dataset_type,
                        collections=input_collections,
                        data_id=data_id.subset(self.constraint_dimensions),
                        where=" AND ".join(where_terms),
                        bind=bind,
                        with_dimension_records=True,
                    )
                except MissingDatasetTypeError:
                    return []
            else:
                raise NotImplementedError(
                    f"No support for skypix lookup {self.task_node.label}.{self.edge.connection_name} "
                    "that requires additional spatial and/or temporal constraints. "
                    "Please create a feature-request ticket and use a lookup function in the meantime."
                )
        if self._bounds.spatial_connections or self._bounds.temporal_connections:
            raise NotImplementedError(
                f"No support for prerequisite lookup {self.task_node.label}.{self.edge.connection_name} "
                "that requires other connections to determine spatial or temporal bounds but does not "
                "fit into one of our standard cases. "
                "Please create a feature-request ticket and use a lookup function in the meantime."
            )
        # If the spatial/temporal bounds are not customized, and the dataset
        # doesn't have any skypix dimensions, a vanilla query_datasets call
        # should work.  This case should always be optimized by
        # AllDimensionsQuantumGraphBuilder as well.  Note that we use the
        # original quantum data ID here, not those with constraint_dimensions
        # that strips out the spatial/temporal stuff, because here we want the
        # butler query system to handle the spatial/temporal stuff like it
        # normally would.
        try:
            return butler.query_datasets(
                self.dataset_type_node.dataset_type,
                collections=input_collections,
                data_id=data_id,
                with_dimension_records=True,
            )
        except MissingDatasetTypeError:
            return []


@dataclasses.dataclass
class PrerequisiteBounds:
    """A QuantumGraph-generation helper class that manages the spatial and
    temporal bounds of a tasks' quanta, for the purpose of finding
    prerequisite inputs.
    """

    task_node: TaskNode
    """The `~pipeline_graph.PipelineGraph` node that represents the task."""

    spatial_connections: frozenset[str] = dataclasses.field(init=False)
    """Regular input or output connections whose (assumed spatial) data IDs
    should be used to define the spatial bounds of this task's quanta.

    See Also
    --------
    PipelineTaskConnections.getSpatialBoundsConnections
    """

    temporal_connections: frozenset[str] = dataclasses.field(init=False)
    """Regular input or output connections whose (assumed temporal) data IDs
    should be used to define the temporal bounds of this task's quanta.

    See Also
    --------
    PipelineTaskConnections.getTemporalBoundsConnections
    """

    all_dataset_skypix: dict[str, SkyPixDimension] = dataclasses.field(default_factory=dict)
    """The union of all `PrerequisiteFinder.dataset_skypix` attributes for all
    (remaining) prerequisite finders for this task.
    """

    any_dataset_has_timespan: bool = dataclasses.field(default=False)
    """Whether any `PrerequisiteFinder.dataset_has_timespan` attribute is true
    for any (remaining) prerequisite finder for this task.
    """

    def __post_init__(self) -> None:
        self.spatial_connections = frozenset(self.task_node.get_spatial_bounds_connections())
        self.temporal_connections = frozenset(self.task_node.get_temporal_bounds_connections())

    def make_skypix_bounds_builder(self, quantum_data_id: DataCoordinate) -> SkyPixBoundsBuilder:
        """Return an object that accumulates the appropriate spatial bounds for
        a quantum.

        Parameters
        ----------
        quantum_data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for this quantum.

        Returns
        -------
        builder : `SkyPixBoundsBuilder`
            Object that accumulates the appropriate spatial bounds for a
            quantum.  If the spatial bounds are not needed, this object will do
            nothing.
        """
        if not self.all_dataset_skypix:
            return _TrivialSkyPixBoundsBuilder()
        if self.spatial_connections:
            return _ConnectionSkyPixBoundsBuilder(
                self.task_node, self.spatial_connections, self.all_dataset_skypix.values(), quantum_data_id
            )
        if self.task_node.dimensions.spatial:
            return _QuantumOnlySkyPixBoundsBuilder(self.all_dataset_skypix.values(), quantum_data_id)
        else:
            return _UnboundedSkyPixBoundsBuilder(self.all_dataset_skypix.values())

    def make_timespan_builder(self, quantum_data_id: DataCoordinate) -> TimespanBuilder:
        """Return an object that accumulates the appropriate timespan for
        a quantum.

        Parameters
        ----------
        quantum_data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for this quantum.

        Returns
        -------
        builder : `TimespanBuilder`
            Object that accumulates the appropriate timespan bounds for a
            quantum.  If a timespan is not needed, this object will do nothing.
        """
        if not self.any_dataset_has_timespan:
            return _TrivialTimespanBuilder()
        if self.temporal_connections:
            return _ConnectionTimespanBuilder(self.task_node, self.temporal_connections, quantum_data_id)
        if self.task_node.dimensions.temporal:
            return _QuantumOnlyTimespanBuilder(quantum_data_id)
        else:
            return _UnboundedTimespanBuilder()


class SkyPixBoundsBuilder(ABC):
    """A base class for objects that accumulate the appropriate spatial bounds
    for a quantum.
    """

    def handle_dataset(self, parent_dataset_type_name: str, data_id: DataCoordinate) -> None:
        """Handle the skeleton graph node for a regular input/output connection
        for this quantum, including its data ID in the bounds if appropriate.

        Parameters
        ----------
        parent_dataset_type_name : `str`
            Name of the dataset type.  Never a component dataset type name.
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for the dataset.
        """
        pass

    @abstractmethod
    def finish(self) -> dict[str, RangeSet]:
        """Finish building the spatial bounds and return them.

        Returns
        -------
        bounds : `dict` [ `str`, `lsst.sphgeom.RangeSet` ]
            The spatial bounds of this quantum in various skypix dimensions.
            Keys are skypix dimension names and values are sets of integer
            pixel ID ranges.
        """
        raise NotImplementedError()


class TimespanBuilder(ABC):
    """A base class for objects that accumulate the appropriate timespan
    for a quantum.
    """

    def handle_dataset(self, parent_dataset_type_name: str, data_id: DataCoordinate) -> None:
        """Handle the skeleton graph node for a regular input/output connection
        for this quantum, including its data ID in the bounds if appropriate.

        Parameters
        ----------
        parent_dataset_type_name : `str`
            Name of the dataset type.  Never a component dataset type name.
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for the dataset.
        """
        pass

    @abstractmethod
    def finish(self) -> Timespan | None:
        """Finish building the timespan and return it.

        Returns
        -------
        timespan : `lsst.daf.butler.Timespan` or `None`
            The timespan of this quantum, or `None` if it is known to not be
            needed.
        """
        raise NotImplementedError()


class _TrivialSkyPixBoundsBuilder(SkyPixBoundsBuilder):
    """Implementation of `SkyPixBoundsBuilder` for when no skypix bounds are
    needed.
    """

    def finish(self) -> dict[str, RangeSet]:
        return {}


class _TrivialTimespanBuilder(TimespanBuilder):
    """Implementation of `TimespanBuilder` for when no timespan is needed."""

    def finish(self) -> None:
        return None


class _QuantumOnlySkyPixBoundsBuilder(SkyPixBoundsBuilder):
    """Implementation of `SkyPixBoundsBuilder` for when the quantum data IDs
    provide the only relevant spatial regions.
    """

    def __init__(self, dimensions: Iterable[SkyPixDimension], quantum_data_id: DataCoordinate) -> None:
        self._region = quantum_data_id.region
        self._dimensions = dimensions

    def finish(self) -> dict[str, RangeSet]:
        return {
            dimension.name: dimension.pixelization.envelope(self._region) for dimension in self._dimensions
        }


class _QuantumOnlyTimespanBuilder(TimespanBuilder):
    """Implementation of `TimespanBuilder` for when the quantum data IDs
    provide the only relevant timespans.
    """

    def __init__(self, quantum_data_id: DataCoordinate) -> None:
        self._timespan = cast(Timespan, quantum_data_id.timespan)

    def finish(self) -> Timespan:
        return self._timespan


class _UnboundedSkyPixBoundsBuilder(SkyPixBoundsBuilder):
    """Implementation of `SkyPixBoundsBuilder` for when the bounds cover the
    full sky.
    """

    def __init__(self, dimensions: Iterable[SkyPixDimension]):
        self._dimensions = dimensions

    def finish(self) -> dict[str, RangeSet]:
        return {dimension.name: dimension.pixelization.universe() for dimension in self._dimensions}


class _UnboundedTimespanBuilder(TimespanBuilder):
    """Implementation of `TimespanBuilder` for when the timespan covers all
    time.
    """

    def finish(self) -> Timespan:
        return Timespan(None, None)


class _ConnectionSkyPixBoundsBuilder(SkyPixBoundsBuilder):
    """Implementation of `SkyPixBoundsBuilder` for when other input or output
    connections contribute to the spatial bounds.
    """

    def __init__(
        self,
        task_node: TaskNode,
        bounds_connections: frozenset[str],
        dimensions: Iterable[SkyPixDimension],
        quantum_data_id: DataCoordinate,
    ) -> None:
        self._dimensions = dimensions
        self._regions: list[Region] = []
        if task_node.dimensions.spatial:
            self._regions.append(quantum_data_id.region)
        self._dataset_type_names: set[str] = set()
        for connection_name in bounds_connections:
            if edge := task_node.inputs.get(connection_name):
                self._dataset_type_names.add(edge.parent_dataset_type_name)
            else:
                self._dataset_type_names.add(task_node.outputs[connection_name].parent_dataset_type_name)
            # Note that we end up raising if the input is a prerequisite (and
            # hence not in task_node.inputs or task_node.outputs); this
            # justifies the cast in `handle_dataset`.

    def handle_dataset(self, parent_dataset_type_name: str, data_id: DataCoordinate) -> None:
        if parent_dataset_type_name in self._dataset_type_names:
            self._regions.append(data_id.region)

    def finish(self) -> dict[str, RangeSet]:
        result = {}
        for dimension in self._dimensions:
            bounds = RangeSet()
            for region in self._regions:
                bounds |= dimension.pixelization.envelope(region)
            result[dimension.name] = bounds
        return result


class _ConnectionTimespanBuilder(TimespanBuilder):
    """Implementation of `TimespanBuilder` for when other input or output
    connections contribute to the timespan.
    """

    def __init__(
        self,
        task_node: TaskNode,
        bounds_connections: frozenset[str],
        quantum_data_id: DataCoordinate,
    ) -> None:
        timespan = (
            cast(Timespan, quantum_data_id.timespan)
            if task_node.dimensions.temporal
            else Timespan.makeEmpty()
        )
        self._begin_nsec = timespan.nsec[0]
        self._end_nsec = timespan.nsec[1]
        self._dataset_type_names = set()
        for connection_name in bounds_connections:
            if edge := task_node.inputs.get(connection_name):
                self._dataset_type_names.add(edge.parent_dataset_type_name)
            else:
                self._dataset_type_names.add(task_node.outputs[connection_name].parent_dataset_type_name)
            # Note that we end up raising if the input is a prerequisite (and
            # hence not in task_node.inputs or task_node.outputs); this
            # justifies the cast in `handle_dataset`.

    def handle_dataset(self, parent_dataset_type_name: str, data_id: DataCoordinate) -> None:
        if parent_dataset_type_name in self._dataset_type_names:
            nsec = cast(Timespan, data_id.timespan).nsec
            self._begin_nsec = min(self._begin_nsec, nsec[0])
            self._end_nsec = max(self._end_nsec, nsec[1])

    def finish(self) -> Timespan:
        return Timespan(None, None, _nsec=(self._begin_nsec, self._end_nsec))
