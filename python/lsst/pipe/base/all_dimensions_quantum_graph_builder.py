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

"""The standard, general-purpose implementation of the QuantumGraph-generation
algorithm.
"""

from __future__ import annotations

__all__ = ("AllDimensionsQuantumGraphBuilder", "DatasetQueryConstraintVariant")

import dataclasses
import itertools
from collections import defaultdict
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeAlias, final

from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DataIdValue,
    DimensionGroup,
    DimensionRecord,
    MissingDatasetTypeError,
)
from lsst.daf.butler.queries import Query
from lsst.utils.logging import LsstLogAdapter
from lsst.utils.timer import timeMethod

from ._datasetQueryConstraints import DatasetQueryConstraintVariant
from .quantum_graph_builder import QuantumGraphBuilder, QuantumGraphBuilderError
from .quantum_graph_skeleton import DatasetKey, Key, PrerequisiteDatasetKey, QuantumGraphSkeleton, QuantumKey

if TYPE_CHECKING:
    from .pipeline_graph import DatasetTypeNode, PipelineGraph, TaskNode


DimensionRecordsMap: TypeAlias = dict[str, dict[tuple[DataIdValue, ...], DimensionRecord]]


@final
class AllDimensionsQuantumGraphBuilder(QuantumGraphBuilder):
    """An implementation of `QuantumGraphBuilder` that uses a single large
    query for data IDs covering all dimensions in the pipeline.

    Parameters
    ----------
    pipeline_graph : `.pipeline_graph.PipelineGraph`
        Pipeline to build a `QuantumGraph` from, as a graph.  Will be resolved
        in-place with the given butler (any existing resolution is ignored).
    butler : `lsst.daf.butler.Butler`
        Client for the data repository.  Should be read-only.
    where : `str`, optional
        Butler expression language constraint to apply to all data IDs.
    dataset_query_constraint : `DatasetQueryConstraintVariant`, optional
        Specification of which overall-input datasets should be used to
        constrain the initial data ID queries.  Not including an important
        constraint can result in catastrophically large query results that take
        too long to process, while including too many makes the query much more
        complex, increasing the chances that the database will choose a bad
        (sometimes catastrophically bad) query plan.
    bind : `~collections.abc.Mapping`, optional
        Variable substitutions for the ``where`` expression.
    **kwargs
        Additional keyword arguments forwarded to `QuantumGraphBuilder`.

    Notes
    -----
    This is a general-purpose algorithm that delegates the problem of
    determining which "end" of the pipeline is more constrained (beginning by
    input collection contents vs. end by the ``where`` string) to the database
    query planner, which *usually* does a good job.

    This algorithm suffers from a serious limitation, which we refer to as the
    "tract slicing" problem from its most common variant: the ``where`` string
    and general data ID intersection rules apply to *all* data IDs in the
    graph.  For example, if a ``tract`` constraint is present in the ``where``
    string or an overall-input dataset, then it is impossible for any data ID
    that does not overlap that tract to be present anywhere in the pipeline,
    such as a ``{visit, detector}`` combination where the ``visit`` overlaps
    the ``tract`` even if the ``detector`` does not.
    """

    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        butler: Butler,
        *,
        where: str = "",
        dataset_query_constraint: DatasetQueryConstraintVariant = DatasetQueryConstraintVariant.ALL,
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(pipeline_graph, butler, **kwargs)
        assert where is not None, "'where' should be an empty str, not None"
        self.where = where
        self.dataset_query_constraint = dataset_query_constraint
        self.bind = bind

    @timeMethod
    def process_subgraph(self, subgraph: PipelineGraph) -> QuantumGraphSkeleton:
        # Docstring inherited.
        # There is some chance that the dimension query for one subgraph would
        # be the same as or a dimension-subset of another.  This is an
        # optimization opportunity we're not currently taking advantage of.
        with _AllDimensionsQuery.from_builder(self, subgraph) as query:
            skeleton = self._make_subgraph_skeleton(query)
            self._find_followup_datasets(query, skeleton)
            dimension_records = self._fetch_most_dimension_records(query)
        leftovers = self._attach_most_dimension_records(skeleton, dimension_records)
        self._fetch_leftover_dimension_records(leftovers, dimension_records)
        self._attach_leftover_dimension_records(skeleton, leftovers, dimension_records)
        return skeleton

    @timeMethod
    def _make_subgraph_skeleton(self, query: _AllDimensionsQuery) -> QuantumGraphSkeleton:
        """Build a `QuantumGraphSkeleton` by iterating over the result rows
        of the initial data ID query.

        Parameters
        ----------
        query : `_AllDimensionsQuery`
            Object representing the full-pipeline data ID query.

        Returns
        -------
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph.
        """
        # First we make containers of empty-dimensions quantum and dataset
        # keys, and add those to the skelton, since empty data IDs are
        # logically subsets of any data ID. We'll copy those to initialize the
        # containers of keys for each result row.  We don't ever explicitly add
        # nodes to the skeleton for these, and that's okay because networkx
        # adds nodes implicitly when an edge to that node is added, and we
        # don't want to add nodes for init datasets here.
        skeleton = QuantumGraphSkeleton(query.subgraph.tasks)
        empty_dimensions_dataset_keys = {}
        for dataset_type_name in query.empty_dimensions_dataset_types.keys():
            dataset_key = skeleton.add_dataset_node(dataset_type_name, self.empty_data_id)
            empty_dimensions_dataset_keys[dataset_type_name] = dataset_key
            if ref := self.empty_dimensions_datasets.inputs.get(dataset_key):
                skeleton.set_dataset_ref(ref, dataset_key)
            if ref := self.empty_dimensions_datasets.outputs_for_skip.get(dataset_key):
                skeleton.set_output_for_skip(ref)
            if ref := self.empty_dimensions_datasets.outputs_in_the_way.get(dataset_key):
                skeleton.set_output_in_the_way(ref)
        empty_dimensions_quantum_keys = []
        for task_label in query.empty_dimensions_tasks.keys():
            empty_dimensions_quantum_keys.append(skeleton.add_quantum_node(task_label, self.empty_data_id))
        self.log.info("Iterating over query results to associate quanta with datasets.")
        # Iterate over query results, populating data IDs for datasets and
        # quanta and then connecting them to each other. This is the slowest
        # client-side part of QG generation, and it's often the slowest part
        # overall, so inside this loop is where it's really critical to avoid
        # expensive things, especially in the nested loops.
        n_rows = 0
        for common_data_id in query.butler_query.data_ids():
            # Create a data ID for each set of dimensions used by one or more
            # tasks or dataset types, and use that to record all quanta and
            # dataset data IDs for this row.
            dataset_keys_for_row: dict[str, DatasetKey] = empty_dimensions_dataset_keys.copy()
            quantum_keys_for_row: list[QuantumKey] = empty_dimensions_quantum_keys.copy()
            for dimensions, (task_nodes, dataset_type_nodes) in query.grouped_by_dimensions.items():
                data_id = common_data_id.subset(dimensions)
                for dataset_type_name in dataset_type_nodes.keys():
                    dataset_keys_for_row[dataset_type_name] = skeleton.add_dataset_node(
                        dataset_type_name, data_id
                    )
                for task_label in task_nodes.keys():
                    quantum_keys_for_row.append(skeleton.add_quantum_node(task_label, data_id))
            # Whether these quanta are new or existing, we can now associate
            # the dataset data IDs for this row with them.  The fact that a
            # quantum data ID and a dataset data ID both came from the same
            # result row is what tells us they should be associated.  Many of
            # these associates will be duplicates (because another query row
            # that differed from this one only in irrelevant dimensions already
            # added them), and our use of sets should take care of that.
            for quantum_key in quantum_keys_for_row:
                for read_edge in self._pipeline_graph.tasks[quantum_key.task_label].inputs.values():
                    skeleton.add_input_edge(
                        quantum_key, dataset_keys_for_row[read_edge.parent_dataset_type_name]
                    )
                for write_edge in self._pipeline_graph.tasks[quantum_key.task_label].iter_all_outputs():
                    skeleton.add_output_edge(
                        quantum_key, dataset_keys_for_row[write_edge.parent_dataset_type_name]
                    )
            n_rows += 1
        if n_rows == 0:
            query.log_failure(self.log)
        else:
            n_quanta = sum(len(skeleton.get_quanta(task_label)) for task_label in query.subgraph.tasks)
            self.log.info(
                "Initial bipartite graph has %d quanta, %d dataset nodes, and %d edges from %d query row(s).",
                n_quanta,
                skeleton.n_nodes - n_quanta,
                skeleton.n_edges,
                n_rows,
            )
        return skeleton

    @timeMethod
    def _find_followup_datasets(self, query: _AllDimensionsQuery, skeleton: QuantumGraphSkeleton) -> None:
        """Populate `existing_datasets` by performing follow-up queries joined
        to column-subsets of the initial data ID query.

        Parameters
        ----------
        query : `_AllDimensionsQuery`
            Object representing the full-pipeline data ID query.
        """
        for dimensions, (tasks_in_group, dataset_types_in_group) in query.grouped_by_dimensions.items():
            # Iterate over regular input/output dataset type nodes with these
            # dimensions to find those datasets using straightforward followup
            # queries.
            for dataset_type_node in dataset_types_in_group.values():
                if dataset_type_node.name in query.overall_inputs:
                    # Dataset type is an overall input; we always need to try
                    # to find these.
                    count = 0
                    try:
                        for ref in query.butler_query.datasets(
                            dataset_type_node.name, self.input_collections
                        ):
                            skeleton.set_dataset_ref(ref)
                            count += 1
                    except MissingDatasetTypeError:
                        pass
                    self.log.verbose(
                        "Found %d overall-input dataset(s) of type %r.", count, dataset_type_node.name
                    )
                    continue
                if self.skip_existing_in:
                    # Dataset type is an intermediate or output; need to find
                    # these if only they're from previously executed quanta
                    # that we might skip...
                    count = 0
                    try:
                        for ref in query.butler_query.datasets(dataset_type_node.name, self.skip_existing_in):
                            skeleton.set_output_for_skip(ref)
                            count += 1
                            if ref.run == self.output_run:
                                skeleton.set_output_in_the_way(ref)
                    except MissingDatasetTypeError:
                        pass
                    self.log.verbose(
                        "Found %d output dataset(s) of type %r in %s.",
                        count,
                        dataset_type_node.name,
                        self.skip_existing_in,
                    )
                if self.output_run_exists and not self.skip_existing_starts_with_output_run:
                    # ...or if they're in the way and would need to be
                    # clobbered (and we haven't already found them in the
                    # previous block).
                    count = 0
                    try:
                        for ref in query.butler_query.datasets(dataset_type_node.name, [self.output_run]):
                            skeleton.set_output_in_the_way(ref)
                            count += 1
                    except MissingDatasetTypeError:
                        pass
                    self.log.verbose(
                        "Found %d output dataset(s) of type %r in %s.",
                        count,
                        dataset_type_node.name,
                        self.output_run,
                    )
            # Iterate over tasks with these dimensions to perform follow-up
            # queries for prerequisite inputs, which may have dimensions that
            # were not in ``query.butler_query.dimensions`` and/or require
            # temporal joins to calibration validity ranges.
            for task_node in tasks_in_group.values():
                task_prerequisite_info = self.prerequisite_info[task_node.label]
                for connection_name, finder in list(task_prerequisite_info.finders.items()):
                    if finder.lookup_function is not None:
                        self.log.verbose(
                            "Deferring prerequisite input %r of task %r to per-quantum processing "
                            "(lookup function provided).",
                            finder.dataset_type_node.name,
                            task_node.label,
                        )
                        continue
                    # We also fall back to the base class if there is a
                    # nontrivial spatial or temporal join in the lookup.
                    if finder.dataset_skypix or finder.dataset_other_spatial:
                        if task_prerequisite_info.bounds.spatial_connections:
                            self.log.verbose(
                                "Deferring prerequisite input %r of task %r to per-quantum processing "
                                "(for spatial-bounds-connections handling).",
                                finder.dataset_type_node.name,
                                task_node.label,
                            )
                            continue
                        if not task_node.dimensions.spatial:
                            self.log.verbose(
                                "Deferring prerequisite input %r of task %r to per-quantum processing "
                                "(dataset has spatial data IDs, but task does not).",
                                finder.dataset_type_node.name,
                                task_node.label,
                            )
                            continue
                    if finder.dataset_has_timespan:
                        if task_prerequisite_info.bounds.spatial_connections:
                            self.log.verbose(
                                "Deferring prerequisite input %r of task %r to per-quantum processing "
                                "(for temporal-bounds-connections handling).",
                                finder.dataset_type_node.name,
                                task_node.label,
                            )
                            continue
                        if not task_node.dimensions.temporal:
                            self.log.verbose(
                                "Deferring prerequisite input %r of task %r to per-quantum processing "
                                "(dataset has temporal data IDs, but task does not).",
                                finder.dataset_type_node.name,
                                task_node.label,
                            )
                            continue
                    # We have a simple case where we can do a single query
                    # that joins the query we already have for the task data
                    # IDs to the datasets we're looking for.
                    count = 0
                    try:
                        query_results = list(
                            # TODO[DM-46042]: We materialize here as a way to
                            # to a SELECT DISTINCT on the main query with a
                            # subset of its dimensions columns.  It'd be better
                            # to have a way to do this that just makes a
                            # subquery or a CTE rather than a temporary table.
                            query.butler_query.materialize(dimensions=dimensions, datasets=())
                            .join_dataset_search(
                                finder.dataset_type_node.dataset_type, self.input_collections
                            )
                            .general(
                                dimensions | finder.dataset_type_node.dataset_type.dimensions,
                                dataset_fields={finder.dataset_type_node.name: ...},
                                find_first=True,
                            )
                            .iter_tuples(finder.dataset_type_node.dataset_type)
                        )
                    except MissingDatasetTypeError:
                        query_results = []
                    for data_id, refs, _ in query_results:
                        ref = refs[0]
                        dataset_key = skeleton.add_prerequisite_node(ref)
                        quantum_key = QuantumKey(task_node.label, data_id.subset(dimensions).required_values)
                        skeleton.add_input_edge(quantum_key, dataset_key)
                        count += 1
                    # Remove this finder from the mapping so the base class
                    # knows it doesn't have to look for these prerequisites.
                    del task_prerequisite_info.finders[connection_name]
                    self.log.verbose(
                        "Added %d prerequisite input edge(s) from dataset type %r to task %r.",
                        count,
                        finder.dataset_type_node.name,
                        task_node.label,
                    )

    @timeMethod
    def _fetch_most_dimension_records(self, query: _AllDimensionsQuery) -> DimensionRecordsMap:
        """Query for dimension records for all non-prerequisite data IDs (and
        possibly some prerequisite data IDs).

        Parameters
        ----------
        query : `_AllDimensionsQuery`
            Object representing the materialized sub-pipeline data ID query.

        Returns
        -------
        dimension_records : `dict`
            Nested dictionary of dimension records, keyed first by dimension
            element name and then by the `DataCoordinate.required_values`
            tuple.

        Notes
        -----
        Because the initial common data ID query is used to generate all
        quantum and regular input/output dataset data IDs, column subsets of it
        can also be used to fetch dimension records for those data IDs.
        """
        self.log.verbose("Performing follow-up queries for dimension records.")
        result: dict[str, dict[tuple[DataIdValue, ...], DimensionRecord]] = {}
        for dimensions in query.grouped_by_dimensions.keys():
            for element in dimensions.elements:
                if element not in result:
                    result[element] = {
                        record.dataId.required_values: record
                        for record in query.butler_query.dimension_records(element)
                    }
        return result

    @timeMethod
    def _attach_most_dimension_records(
        self, skeleton: QuantumGraphSkeleton, dimension_records: DimensionRecordsMap
    ) -> DataIdExpansionLeftovers:
        """Attach dimension records to most data IDs in the in-progress graph,
        and return a data structure that records the rest.

        Parameters
        ----------
        skeleton : `.quantum_graph_skeleton.QuantumGraphSkeleton`
            In-progress quantum graph to modify in place.
        dimension_records : `dict`
            Nested dictionary of dimension records, keyed first by dimension
            element name and then by the `DataCoordinate.required_values`
            tuple.

        Returns
        -------
        leftovers : `DataIdExpansionLeftovers`
            Struct recording data IDs in ``skeleton`` that were not expanded
            and the data IDs of the dimension records that need to be fetched
            in order to do so.
        """
        # Group all nodes by data ID (and dimensions of data ID).
        data_ids_to_expand: defaultdict[DimensionGroup, defaultdict[DataCoordinate, NodeKeysForDataId]] = (
            defaultdict(NodeKeysForDataId.make_defaultdict)
        )
        data_id: DataCoordinate | None
        for node_key in skeleton:
            if data_id := skeleton[node_key].get("data_id"):
                if isinstance(node_key, PrerequisiteDatasetKey):
                    data_ids_to_expand[data_id.dimensions][data_id].prerequisites.append(node_key)
                else:
                    data_ids_to_expand[data_id.dimensions][data_id].others.append(node_key)
        # Expand data IDs and track the records that are missing (which can
        # only come from prerequisite data IDs).
        leftovers = DataIdExpansionLeftovers()
        for dimensions, data_ids in data_ids_to_expand.items():
            attacher = DimensionRecordAttacher(dimensions, dimension_records)
            skipped_data_ids: dict[DataCoordinate, list[PrerequisiteDatasetKey]] = {}
            for data_id, node_keys in data_ids.items():
                expanded_data_id: DataCoordinate | None
                if node_keys.others:
                    # This data ID was used in at least one non-prerequisite
                    # key, so we know we've got the records we need for it
                    # already.
                    expanded_data_id = attacher.apply(data_id)
                else:
                    # This key only appeared in prerequisites, so we might not
                    # have all the records we need.
                    if (expanded_data_id := attacher.maybe_apply(data_id, leftovers)) is None:
                        skipped_data_ids[data_id] = node_keys.prerequisites
                        continue
                for node_key in itertools.chain(node_keys.others, node_keys.prerequisites):
                    skeleton.set_data_id(node_key, expanded_data_id)
            if skipped_data_ids:
                leftovers.data_ids_to_expand[dimensions] = skipped_data_ids
        return leftovers

    @timeMethod
    def _fetch_leftover_dimension_records(
        self, leftovers: DataIdExpansionLeftovers, dimension_records: DimensionRecordsMap
    ) -> None:
        """Fetch additional dimension records whose data IDs were not included
        in the initial common data ID query.

        Parameters
        ----------
        leftovers : `DataIdExpansionLeftovers`
            Struct recording data IDs in ``skeleton`` that were not expanded.
        dimension_records : `dict`
            Nested dictionary of dimension records, keyed first by dimension
            element name and then by the `DataCoordinate.required_values`
            tuple. Will be updated in place.
        """
        for element, data_id_values_set in leftovers.missing_record_data_ids.items():
            dimensions = self.butler.dimensions[element].minimal_group
            data_ids = [DataCoordinate.from_required_values(dimensions, v) for v in data_id_values_set]
            with self.butler.query() as q:
                new_records = {
                    r.dataId.required_values: r
                    for r in q.join_data_coordinates(data_ids).dimension_records(element)
                }
            dimension_records.setdefault(element, {}).update(new_records)

    @timeMethod
    def _attach_leftover_dimension_records(
        self,
        skeleton: QuantumGraphSkeleton,
        leftovers: DataIdExpansionLeftovers,
        dimension_records: DimensionRecordsMap,
    ) -> None:
        """Attach dimension records to any data IDs in the in-progress graph
        that were not handled in the first pass.

        Parameters
        ----------
        skeleton : `.quantum_graph_skeleton.QuantumGraphSkeleton`
            In-progress quantum graph to modify in place.
        leftovers : `DataIdExpansionLeftovers`
            Struct recording data IDs in ``skeleton`` that were not expanded
            and the data IDs of the dimension records that need to be fetched
            in order to do so.
        dimension_records : `dict`
            Nested dictionary of dimension records, keyed first by dimension
            element name and then by the `DataCoordinate.required_values`
            tuple.
        """
        for dimensions, data_ids in leftovers.data_ids_to_expand.items():
            attacher = DimensionRecordAttacher(dimensions, dimension_records)
            for data_id, prerequisite_keys in data_ids.items():
                expanded_data_id = attacher.apply(data_id)
                for node_key in prerequisite_keys:
                    skeleton.set_data_id(node_key, expanded_data_id)


@dataclasses.dataclass(eq=False, repr=False)
class _AllDimensionsQuery:
    """A helper class for `AllDimensionsQuantumGraphBuilder` that holds all
    per-subgraph state.

    This object should always be constructed by `from_builder`, which returns
    an instance wrapped with a context manager. This controls the lifetime of
    the temporary table referenced by `common_data_ids`.
    """

    subgraph: PipelineGraph
    """Graph of this subset of the pipeline."""

    grouped_by_dimensions: dict[DimensionGroup, tuple[dict[str, TaskNode], dict[str, DatasetTypeNode]]] = (
        dataclasses.field(default_factory=dict)
    )
    """The tasks and dataset types of this subset of the pipeline, grouped
    by their dimensions.

    The tasks and dataset types with empty dimensions are not included; they're
    in other attributes since they are usually used differently.  Prerequisite
    dataset types are also not included.
    """

    empty_dimensions_tasks: dict[str, TaskNode] = dataclasses.field(default_factory=dict)
    """The tasks of this subset of this pipeline that have empty dimensions."""

    empty_dimensions_dataset_types: dict[str, DatasetTypeNode] = dataclasses.field(default_factory=dict)
    """The dataset types of this subset of this pipeline that have empty
    dimensions.

    Prerequisite dataset types are not included.
    """

    overall_inputs: dict[str, DatasetTypeNode] = dataclasses.field(default_factory=dict)
    """Pipeline graph nodes for all non-prerequisite, non-init overall-input
    dataset types for this subset of the pipeline.
    """

    query_cmd: list[str] = dataclasses.field(default_factory=list)
    """Python code (split across lines) that could be used to reproduce the
    initial query.
    """

    butler_query: Query = dataclasses.field(init=False)
    """Results of the materialized initial data ID query."""

    @classmethod
    @contextmanager
    def from_builder(
        cls, builder: AllDimensionsQuantumGraphBuilder, subgraph: PipelineGraph
    ) -> Iterator[_AllDimensionsQuery]:
        """Construct and run the query, returning an instance guarded by
        a context manager.

        Parameters
        ----------
        builder : `AllDimensionsQuantumGraphBuilder`
            Builder object this helper is associated with.
        subgraph : `pipeline_graph.PipelineGraph`
            Subset of the pipeline being processed.

        Returns
        -------
        context : `AbstractContextManager` [ `_AllDimensionsQuery` ]
            An instance of this class, inside a context manager that manages
            the lifetime of its temporary database table.
        """
        result = cls(subgraph)
        builder.log.debug("Analyzing subgraph dimensions and overall-inputs.")
        result.grouped_by_dimensions = result.subgraph.group_by_dimensions()
        (
            result.empty_dimensions_tasks,
            result.empty_dimensions_dataset_types,
        ) = result.grouped_by_dimensions.pop(builder.universe.empty)
        result.overall_inputs = {
            name: node  # type: ignore
            for name, node in result.subgraph.iter_overall_inputs()
            if not node.is_prerequisite  # type: ignore
        }
        dimension_names: set[str] = set()
        for dimensions_for_group in result.grouped_by_dimensions.keys():
            dimension_names.update(dimensions_for_group.names)
        dimensions = builder.universe.conform(dimension_names)
        datasets: set[str] = set()
        builder.log.debug("Building query for data IDs.")
        if builder.dataset_query_constraint == DatasetQueryConstraintVariant.ALL:
            builder.log.debug("Constraining graph query using all datasets not marked as deferred.")
            datasets = {
                name
                for name, dataset_type_node in result.overall_inputs.items()
                if (
                    dataset_type_node.is_initial_query_constraint
                    and name not in result.empty_dimensions_dataset_types
                )
            }
        elif builder.dataset_query_constraint == DatasetQueryConstraintVariant.OFF:
            builder.log.debug("Not using dataset existence to constrain query.")
        elif builder.dataset_query_constraint == DatasetQueryConstraintVariant.LIST:
            constraint = set(builder.dataset_query_constraint)
            inputs = result.overall_inputs - result.empty_dimensions_dataset_types.keys()
            if remainder := constraint.difference(inputs):
                builder.log.debug(
                    "Ignoring dataset types %s in dataset query constraint that are not inputs to this "
                    "subgraph, on the assumption that they are relevant for a different subraph.",
                    remainder,
                )
            constraint.intersection_update(inputs)
            builder.log.debug(f"Constraining graph query using {constraint}")
            datasets = constraint
        else:
            raise QuantumGraphBuilderError(
                f"Unable to handle type {builder.dataset_query_constraint} given as datasetQueryConstraint."
            )
        with builder.butler.query() as query:
            result.query_cmd.append("with butler.query() as query:")
            result.query_cmd.append(f"    query = query.join_dimensions({list(dimensions.names)})")
            query = query.join_dimensions(dimensions)
            if datasets:
                result.query_cmd.append(f"    collections = {list(builder.input_collections)}")
            for dataset_type_name in datasets:
                result.query_cmd.append(
                    f"    query = query.join_dataset_search({dataset_type_name!r}, collections)"
                )
                query = query.join_dataset_search(dataset_type_name, builder.input_collections)
            result.query_cmd.append(
                f"    query = query.where({dict(result.subgraph.data_id.mapping)}, "
                f"{builder.where!r}, bind={builder.bind!r})"
            )
            query = query.where(result.subgraph.data_id, builder.where, bind=builder.bind)
            builder.log.verbose(result.format_query_cmd("Querying for data IDs via:"))
            # Allow duplicates from common skypix overlaps to make some queries
            # run faster.
            query._allow_duplicate_overlaps = True
            result.butler_query = query.materialize()
            yield result

    def format_query_cmd(self, *header: str) -> str:
        """Format the butler query call used as a multi-line string.

        Parameters
        ----------
        *header : `str`
            Initial lines the of the returned string, not including newlines.
        """
        lines = list(header)
        lines.extend(self.query_cmd)
        return "\n".join(lines)

    def log_failure(self, log: LsstLogAdapter) -> None:
        """Emit an ERROR-level log message that attempts to explain
        why the initial data ID query returned no rows.

        Parameters
        ----------
        log : `logging.Logger`
            The logger to use to emit log messages.
        """
        # A single multiline log plays better with log aggregators like Loki.
        header = ["Initial data ID query returned no rows, so QuantumGraph will be empty."]
        try:
            header.extend(self.butler_query.explain_no_results())
            header.append("To reproduce this query for debugging purposes, run:")
        finally:
            # If an exception was raised, write a partial.
            log.error(self.format_query_cmd(*header))


class DimensionRecordAttacher:
    """A helper class that expands data IDs by attaching dimension records.

    Parameters
    ----------
    dimensions : `DimensionGroup`
        Dimensions of the data IDs this instance will operate on.
    dimension_records : `dict`
        Nested dictionary of dimension records, keyed first by dimension
        element name and then by the `DataCoordinate.required_values`
        tuple.
    """

    def __init__(self, dimensions: DimensionGroup, dimension_records: DimensionRecordsMap):
        self.dimensions = dimensions
        self.indexers = {element: self._make_indexer(element) for element in dimensions.elements}
        self.dimension_records = dimension_records

    def _make_indexer(self, element: str) -> list[int]:
        """Return a list of indexes into data ID full-values that extract the
        `~DataCoordinate.required_values` for the ``element``
        `DimensionElement.minimal_group` from the `~DataCoordinate.full_values`
        for `dimensions`.
        """
        return [
            self.dimensions._data_coordinate_indices[d]
            for d in self.dimensions.universe[element].minimal_group.required
        ]

    def apply(self, data_id: DataCoordinate) -> DataCoordinate:
        """Attach dimension records to the given data ID.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Input data ID.

        Returns
        -------
        expanded_data_id : `DataCoordinate`
            Output data ID.
        """
        v = data_id.full_values
        records_for_data_id = {}
        for element, indexer in self.indexers.items():
            records_for_element = self.dimension_records[element]
            records_for_data_id[element] = records_for_element[tuple([v[i] for i in indexer])]
        return data_id.expanded(records_for_data_id)

    def maybe_apply(
        self, data_id: DataCoordinate, leftovers: DataIdExpansionLeftovers
    ) -> DataCoordinate | None:
        """Attempt to attach dimension records to the given data ID, and record
        the data IDs of missing dimension records on failure.

        Parameters
        ----------
        data_id : `DataCoordinate`
            Input data ID.
        leftovers : `DataIdExpansionLeftovers`
            Struct recording data IDs that were not expanded and the data IDs
            of the dimension records that need to be fetched in order to do so.
            The latter will be updated in place; callers are responsible for
            updating the former when `None` is returned (since this method
            doesn't have enough information to do so on its own).

        Returns
        -------
        expanded_data_id : `DataCoordinate` or `None`
            Output data ID, or `None` if one or more records were missing.
        """
        v = data_id.full_values
        records_for_data_id = {}
        failed = False
        # Note that we need to process all elements even when we know we're
        # going to fail, since we need to fully populate `leftovers` with what
        # we still need to query for.
        for element, indexer in self.indexers.items():
            k = tuple([v[i] for i in indexer])
            if (records_for_element := self.dimension_records.get(element)) is None:
                leftovers.missing_record_data_ids[element].add(k)
                failed = True
            elif (r := records_for_element.get(k)) is None:
                leftovers.missing_record_data_ids[element].add(k)
                failed = True
            else:
                records_for_data_id[element] = r
        if not failed:
            return data_id.expanded(records_for_data_id)
        else:
            return None


@dataclasses.dataclass
class NodeKeysForDataId:
    """Struct that holds the skeleton-graph node keys for a single data ID.

    This is used when expanding (i.e. attaching dimension records to) data IDs,
    where we group node keys by data ID in order to avoid repeatedly expanding
    the same data ID and cut down on the number of equivalent data ID instances
    alive in memory.  We separate prerequisite nodes from all other nodes
    because they're the only ones whose data IDs are not by construction a
    subset of the data IDs in the big initial query.
    """

    prerequisites: list[PrerequisiteDatasetKey] = dataclasses.field(default_factory=list)
    """Node keys that correspond to prerequisite input dataset types."""

    others: list[Key] = dataclasses.field(default_factory=list)
    """All other node keys."""

    @classmethod
    def make_defaultdict(cls) -> defaultdict[DataCoordinate, NodeKeysForDataId]:
        return defaultdict(cls)


DataIdExpansionToDoMap: TypeAlias = defaultdict[
    DimensionGroup, defaultdict[DataCoordinate, NodeKeysForDataId]
]


@dataclasses.dataclass
class DataIdExpansionLeftovers:
    data_ids_to_expand: dict[DimensionGroup, dict[DataCoordinate, list[PrerequisiteDatasetKey]]] = (
        dataclasses.field(default_factory=dict)
    )
    missing_record_data_ids: defaultdict[str, set[tuple[DataIdValue, ...]]] = dataclasses.field(
        default_factory=lambda: defaultdict(set)
    )
