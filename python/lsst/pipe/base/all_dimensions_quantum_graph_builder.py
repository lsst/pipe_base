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
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, TypeAlias, final

from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DataIdValue,
    DimensionGroup,
    DimensionRecord,
    DimensionUniverse,
    MissingDatasetTypeError,
)
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
        tree = _DimensionGroupTree(subgraph)
        self._query_for_data_ids(tree)
        skeleton = self._make_subgraph_skeleton(tree)
        self._find_followup_datasets(tree, skeleton)
        dimension_records = self._fetch_most_dimension_records(tree)
        leftovers = self._attach_most_dimension_records(skeleton, dimension_records)
        self._fetch_leftover_dimension_records(leftovers, dimension_records)
        self._attach_leftover_dimension_records(skeleton, leftovers, dimension_records)
        return skeleton

    def _query_for_data_ids(self, tree: _DimensionGroupTree) -> None:
        """Query for data IDs and use the result to populate the dimension
        group tree.

        Parameters
        ----------
        tree : `_DimensionGroupTree`
            Tree with dimension group branches that holds subgraph-specific
            state for this builder, to be modified in place.
        """
        self.log.debug("Analyzing subgraph dimensions and overall-inputs.")
        constraint_datasets: set[str] = set()
        self.log.debug("Building query for data IDs.")
        if self.dataset_query_constraint == DatasetQueryConstraintVariant.ALL:
            self.log.debug("Constraining graph query using all datasets not marked as deferred.")
            constraint_datasets = {
                name
                for name, dataset_type_node in tree.overall_inputs.items()
                if (dataset_type_node.is_initial_query_constraint and dataset_type_node.dimensions)
            }
        elif self.dataset_query_constraint == DatasetQueryConstraintVariant.OFF:
            self.log.debug("Not using dataset existence to constrain query.")
        elif self.dataset_query_constraint == DatasetQueryConstraintVariant.LIST:
            constraint = set(self.dataset_query_constraint)
            inputs = tree.overall_inputs - tree.empty_dimensions_branch.dataset_types.keys()
            if remainder := constraint.difference(inputs):
                self.log.debug(
                    "Ignoring dataset types %s in dataset query constraint that are not inputs to this "
                    "subgraph, on the assumption that they are relevant for a different subgraph.",
                    remainder,
                )
            constraint.intersection_update(inputs)
            self.log.debug(f"Constraining graph query using {constraint}")
            constraint_datasets = constraint
        else:
            raise QuantumGraphBuilderError(
                f"Unable to handle type {self.dataset_query_constraint} given as datasetQueryConstraint."
            )
        query_cmd: list[str] = []
        with self.butler.query() as query:
            query_cmd.append("with butler.query() as query:")
            query_cmd.append(f"    query = query.join_dimensions({list(tree.all_dimensions.names)})")
            query = query.join_dimensions(tree.all_dimensions)
            if constraint_datasets:
                query_cmd.append(f"    collections = {list(self.input_collections)}")
            for dataset_type_name in constraint_datasets:
                query_cmd.append(f"    query = query.join_dataset_search({dataset_type_name!r}, collections)")
                query = query.join_dataset_search(dataset_type_name, self.input_collections)
            query_cmd.append(
                f"    query = query.where({dict(tree.subgraph.data_id.mapping)}, "
                f"{self.where!r}, bind={self.bind!r})"
            )
            query = query.where(tree.subgraph.data_id, self.where, bind=self.bind)
            self.log.verbose("Querying for data IDs via: %s", "\n".join(query_cmd))
            # Allow duplicates from common skypix overlaps to make some queries
            # run faster.
            query._allow_duplicate_overlaps = True
            self.log.info("Iterating over query results to associate quanta with datasets.")
            # Iterate over query results, populating data IDs for datasets,
            # quanta, and edges.  We populate only the first level of the tree
            # in the first pass, so we can be done with the query results as
            # quickly as possible in case that holds a connection/cursor open.
            n_rows = 0
            for common_data_id in query.data_ids(tree.all_dimensions):
                for branch_dimensions, branch in tree.trunk_branches.items():
                    data_id = common_data_id.subset(branch_dimensions)
                    branch.data_ids.add(data_id)
                n_rows += 1
            if n_rows == 0:
                # A single multiline log plays better with log aggregators like
                # Loki.
                lines = ["Initial data ID query returned no rows, so QuantumGraph will be empty."]
                try:
                    lines.extend(query.explain_no_results())
                finally:
                    lines.append("To reproduce this query for debugging purposes, run:")
                    lines.extend(query_cmd)
                    # If an exception was raised, write a partial.
                    self.log.error("\n".join(lines))
                return
        self.log.verbose("Processed %s initial data ID query rows.", n_rows)
        # We now recursively populate the data IDs of the rest of the tree.
        tree.project_data_ids(self.log)

    @timeMethod
    def _make_subgraph_skeleton(self, tree: _DimensionGroupTree) -> QuantumGraphSkeleton:
        """Build a `QuantumGraphSkeleton` by processing the data IDs in the
        dimension group tree.

        Parameters
        ----------
        tree : `_DimensionGroupTree`
            Tree with dimension group branches that holds subgraph-specific
            state for this builder.

        Returns
        -------
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph.
        """
        skeleton = QuantumGraphSkeleton(tree.subgraph.tasks)
        for branch_dimensions, branch in tree.trunk_branches.items():
            self.log.verbose(
                "Adding nodes and edges for %s %s data ID(s).",
                len(branch.data_ids),
                branch_dimensions,
            )
            branch.update_skeleton(skeleton, self.log)
        n_quanta = sum(len(skeleton.get_quanta(task_label)) for task_label in tree.subgraph.tasks)
        self.log.info(
            "Initial bipartite graph has %d quanta, %d dataset nodes, and %d edges.",
            n_quanta,
            skeleton.n_nodes - n_quanta,
            skeleton.n_edges,
        )
        return skeleton

    @timeMethod
    def _find_followup_datasets(self, tree: _DimensionGroupTree, skeleton: QuantumGraphSkeleton) -> None:
        """Populate `existing_datasets` by performing follow-up queries with
        the data IDs in the dimension group tree.

        Parameters
        ----------
        tree : `_DimensionGroupTree`
            Tree with dimension group branches that holds subgraph-specific
            state for this builder.
        skeleton : `.quantum_graph_skeleton.QuantumGraphSkeleton`
            In-progress quantum graph to modify in place.
        """
        dataset_key: DatasetKey | PrerequisiteDatasetKey
        for dataset_type_name in tree.empty_dimensions_branch.dataset_types.keys():
            dataset_key = DatasetKey(dataset_type_name, self.empty_data_id.required_values)
            if ref := self.empty_dimensions_datasets.inputs.get(dataset_key):
                skeleton.set_dataset_ref(ref, dataset_key)
            if ref := self.empty_dimensions_datasets.outputs_for_skip.get(dataset_key):
                skeleton.set_output_for_skip(ref)
            if ref := self.empty_dimensions_datasets.outputs_in_the_way.get(dataset_key):
                skeleton.set_output_in_the_way(ref)
        for dimensions, branch in tree.branches_by_dimensions.items():
            if not branch.has_followup_queries:
                continue
            if not branch.data_ids:
                continue
            # Iterate over regular input/output dataset type nodes with these
            # dimensions to find those datasets using followup queries.
            with self.butler.query() as butler_query:
                butler_query = butler_query.join_data_coordinates(branch.data_ids)
                for dataset_type_node in branch.dataset_types.values():
                    if dataset_type_node.name in tree.overall_inputs:
                        # Dataset type is an overall input; we always need to
                        # try to find these.
                        count = 0
                        try:
                            for ref in butler_query.datasets(dataset_type_node.name, self.input_collections):
                                skeleton.set_dataset_ref(ref)
                                count += 1
                        except MissingDatasetTypeError:
                            pass
                        self.log.verbose(
                            "Found %d overall-input dataset(s) of type %r.", count, dataset_type_node.name
                        )
                        continue
                    if self.skip_existing_in:
                        # Dataset type is an intermediate or output; need to
                        # find these if only they're from previously executed
                        # quanta that we might skip...
                        count = 0
                        try:
                            for ref in butler_query.datasets(dataset_type_node.name, self.skip_existing_in):
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
                            for ref in butler_query.datasets(dataset_type_node.name, [self.output_run]):
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
                # queries for prerequisite inputs, which may have dimensions
                # that were not in ``tree.all_dimensions`` and/or require
                # temporal joins to calibration validity ranges.
                for task_node in branch.tasks.values():
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
                        # that joins the query we already have for the task
                        # data IDs to the datasets we're looking for.
                        count = 0
                        try:
                            query_results = list(
                                butler_query.join_dataset_search(
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
                            quantum_key = QuantumKey(
                                task_node.label, data_id.subset(dimensions).required_values
                            )
                            skeleton.add_input_edge(quantum_key, dataset_key)
                            count += 1
                        # Remove this finder from the mapping so the base class
                        # knows it doesn't have to look for these
                        # prerequisites.
                        del task_prerequisite_info.finders[connection_name]
                        self.log.verbose(
                            "Added %d prerequisite input edge(s) from dataset type %r to task %r.",
                            count,
                            finder.dataset_type_node.name,
                            task_node.label,
                        )
                if not branch.record_elements:
                    # Delete data ID sets we don't need anymore.
                    del branch.data_ids

    @timeMethod
    def _fetch_most_dimension_records(self, tree: _DimensionGroupTree) -> DimensionRecordsMap:
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
        for branch in tree.branches_by_dimensions.values():
            if not branch.record_elements:
                continue
            if not branch.data_ids:
                continue
            with self.butler.query() as butler_query:
                butler_query = butler_query.join_data_coordinates(branch.data_ids)
                for element in branch.record_elements:
                    result[element] = {
                        record.dataId.required_values: record
                        for record in butler_query.dimension_records(element)
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


@dataclasses.dataclass(eq=False, repr=False, slots=True)
class _DimensionGroupTwig:
    """A small side-branch of the tree of dimensions groups that tracks the
    tasks and dataset types with a particular set of dimensions that appear in
    the edges populated by its parent branch.

    See `_DimensionGroupTree` for more details.
    """

    parent_edge_tasks: set[str] = dataclasses.field(default_factory=set)
    """Task labels for tasks whose quanta have the dimensions of this twig and
    are endpoints of edges that have the combined dimensions of this twig's
    parent branch.
    """

    parent_edge_dataset_types: set[str] = dataclasses.field(default_factory=set)
    """Dataset type names for datasets whose quanta have the dimensions of this
    twig and are endpoints of edges that have the combined dimensions of this
    twig's parent branch.
    """


@dataclasses.dataclass(eq=False, repr=False, slots=True)
class _DimensionGroupBranch:
    """A node in the tree of dimension groups that are used to recursively
    process query data IDs into a quantum graph.
    """

    tasks: dict[str, TaskNode] = dataclasses.field(default_factory=dict)
    """The task nodes whose quanta have these dimensions, keyed by task label.
    """

    dataset_types: dict[str, DatasetTypeNode] = dataclasses.field(default_factory=dict)
    """The dataset type nodes whose datasets have these dimensions, keyed by
    dataset type name.
    """

    record_elements: list[str] = dataclasses.field(default_factory=list)
    """The names of dimension elements whose records should be looked up via
    these dimensions.
    """

    data_ids: set[DataCoordinate] = dataclasses.field(default_factory=set)
    """All data IDs with these dimensions seen in the QuantumGraph."""

    input_edges: list[tuple[str, str]] = dataclasses.field(default_factory=list)
    """Dataset type -> task edges that are populated by this set of dimensions.

    These are cases where `dimensions` is the union of the task and dataset
    type dimensions.
    """

    output_edges: list[tuple[str, str]] = dataclasses.field(default_factory=list)
    """Task -> dataset type edges that are populated by this set of dimensions.

    These are cases where `dimensions` is the union of the task and dataset
    type dimensions.
    """

    branches: dict[DimensionGroup, _DimensionGroupBranch] = dataclasses.field(default_factory=dict)
    """Child branches whose dimensions are strict subsets of this branch's
    dimensions.
    """

    twigs: defaultdict[DimensionGroup, _DimensionGroupTwig] = dataclasses.field(
        default_factory=lambda: defaultdict(_DimensionGroupTwig)
    )
    """Small branches for all of the dimensions that appear on one side of any
    edge in `input_edges` or `output_edges`.
    """

    @property
    def has_followup_queries(self) -> bool:
        """Whether we will need to perform follow-up queries with these
        dimensions.
        """
        return bool(self.tasks or self.dataset_types or self.record_elements)

    @staticmethod
    def populate_record_elements(
        all_dimensions: DimensionGroup, branches: dict[DimensionGroup, _DimensionGroupBranch]
    ) -> None:
        """Ensure we have branches for all dimension elements we'll need to
        fetch dimension records for.

        Parameters
        ----------
        all_dimensions : `~lsst.daf.butler.DimensionGroup`
            All dimensions that appear in the quantum graph.
        branches : `dict` [ `~lsst.daf.butler.DimensionGroup`,\
                `_DimensionGroupBranch` ]
            Flat mapping of all branches to update in-place.  New branches may
            be added and existing branches may have their `record_element`
            attributes updated.
        """
        for element_name in all_dimensions.elements:
            element = all_dimensions.universe[element_name]
            if element.minimal_group in branches:
                branches[element.minimal_group].record_elements.append(element_name)
            else:
                branches[element.minimal_group] = _DimensionGroupBranch(record_elements=[element_name])

    @staticmethod
    def populate_edges(
        pipeline_graph: PipelineGraph, branches: dict[DimensionGroup, _DimensionGroupBranch]
    ) -> None:
        """Ensure we have branches for all edges in the graph.

        Parameters
        ----------
        pipeline_graph : `~..pipeline_graph.PipelineGraph``
            Graph of tasks and dataset types.
        branches : `dict` [ `~lsst.daf.butler.DimensionGroup`,\
                `_DimensionGroupBranch` ]
            Flat mapping of all branches to update in-place.  New branches may
            be added and existing branches may have their `input_edges`,
            `output_edges`, and `twigs` attributes updated.
        """

        def update_edge_branch(
            task_node: TaskNode, dataset_type_node: DatasetTypeNode
        ) -> _DimensionGroupBranch:
            union_dimensions = task_node.dimensions.union(dataset_type_node.dimensions)
            if (branch := branches.get(union_dimensions)) is None:
                branch = _DimensionGroupBranch()
                branches[union_dimensions] = branch
            branch.twigs[dataset_type_node.dimensions].parent_edge_dataset_types.add(dataset_type_node.name)
            branch.twigs[task_node.dimensions].parent_edge_tasks.add(task_node.label)
            return branch

        for task_node in pipeline_graph.tasks.values():
            for dataset_type_node in pipeline_graph.inputs_of(task_node.label).values():
                assert dataset_type_node is not None, "Pipeline graph is resolved."
                if dataset_type_node.is_prerequisite:
                    continue
                branch = update_edge_branch(task_node, dataset_type_node)
                branch.input_edges.append((dataset_type_node.name, task_node.label))
            for dataset_type_node in pipeline_graph.outputs_of(task_node.label).values():
                assert dataset_type_node is not None, "Pipeline graph is resolved."
                branch = update_edge_branch(task_node, dataset_type_node)
                branch.output_edges.append((task_node.label, dataset_type_node.name))

    @staticmethod
    def find_next_uncontained_dimensions(
        parent_dimensions: DimensionGroup | None, candidates: Iterable[DimensionGroup]
    ) -> list[DimensionGroup]:
        """Find dimension groups that are not a subset of any other dimension
        groups in a set.

        Parameters
        ----------
        parent_dimensions : `~lsst.daf.butler.DimensionGroup` or `None`
            If not `None`, first filter out any candidates that are not strict
            subsets of these dimensions.
        candidates : `~collections.abc.Iterable` [\
                `~lsst.daf.butler.DimensionGroup` ]
            Iterable of dimension groups to consider.

        Returns
        -------
        uncontained : `list` [ `~lsst.daf.butler.DimensionGroup` ]
            Dimension groups that are not contained by any other dimension
            group in the set of filtered candidates.
        """
        if parent_dimensions is None:
            refined_candidates = candidates
        else:
            refined_candidates = [dimensions for dimensions in candidates if dimensions < parent_dimensions]
        return [
            dimensions
            for dimensions in refined_candidates
            if not any(dimensions < other for other in refined_candidates)
        ]

    @classmethod
    def populate_branches(
        cls,
        parent_dimensions: DimensionGroup | None,
        branches: dict[DimensionGroup, _DimensionGroupBranch],
    ) -> dict[DimensionGroup, _DimensionGroupBranch]:
        """Transform a flat mapping of dimension group branches into a tree.

        Parameters
        ----------
        parent_dimensions : `~lsst.daf.butler.DimensionGroup` or `None`
            If not `None`, ignore any candidates in `branches` that are not
            strict subsets of these dimensions.
        branches : `dict` [ `~lsst.daf.butler.DimensionGroup`,\
                `_DimensionGroupBranch` ]
            Flat mapping of all branches to update in-place, by populating
            the `branches` attributes to form a tree and removing entries that
            have been put into the tree.

        Returns
        -------
        uncontained_branches : `dict` [ `~lsst.daf.butler.DimensionGroup`,\
                `_DimensionGroupBranch` ]
            Branches whose dimensions were not subsets of any others in the
            mapping except those that were supersets of ``parent_dimensions``.
        """
        result: dict[DimensionGroup, _DimensionGroupBranch] = {}
        for parent_branch_dimensions in cls.find_next_uncontained_dimensions(
            parent_dimensions, branches.keys()
        ):
            parent_branch = branches.pop(parent_branch_dimensions)
            result[parent_branch_dimensions] = parent_branch
            for child_branch_dimensions, child_branch in cls.populate_branches(
                parent_branch_dimensions, branches
            ).items():
                parent_branch.branches[child_branch_dimensions] = child_branch
        return result

    def project_data_ids(self, log: LsstLogAdapter, log_indent: str = "  ") -> None:
        """Populate the data ID sets of child branches from the data IDs in
        this branch, recursively.

        Parameters
        ----------
        log : `lsst.logging.LsstLogAdapter`
            Logger to use for status reporting.
        log_indent : `str`, optional
            Indentation to prefix the log message.  This is used when recursing
            to make the branch structure clear.
        """
        for data_id in self.data_ids:
            for branch_dimensions, branch in self.branches.items():
                branch.data_ids.add(data_id.subset(branch_dimensions))
        for branch_dimensions, branch in self.branches.items():
            log.debug("%sProjecting query data IDs to %s.", log_indent, branch_dimensions)
            branch.project_data_ids(log, log_indent + "  ")

    def update_skeleton(
        self, skeleton: QuantumGraphSkeleton, log: LsstLogAdapter, log_indent: str = "  "
    ) -> None:
        """Process the data ID sets of this branch and its children recursively
        to add nodes and edges to the under-construction quantum graph.

        Parameters
        ----------
        skeleton : `QuantumGraphSkeleton`
            Under-construction quantum graph to modify in place.
        log : `lsst.logging.LsstLogAdapter`
            Logger to use for status reporting.
        log_indent : `str`, optional
            Indentation to prefix the log message.  This is used when recursing
            to make the branch structure clear.
        """
        for branch_dimensions, branch in self.branches.items():
            log.verbose(
                "%sAdding nodes and edges for %s %s data ID(s).",
                log_indent,
                len(branch.data_ids),
                branch_dimensions,
            )
            branch.update_skeleton(skeleton, log, log_indent + "  ")
        for data_id in self.data_ids:
            for task_label in self.tasks:
                skeleton.add_quantum_node(task_label, data_id)
            for dataset_type_name in self.dataset_types:
                skeleton.add_dataset_node(dataset_type_name, data_id)
            quantum_keys: dict[str, QuantumKey] = {}
            dataset_keys: dict[str, DatasetKey] = {}
            for twig_dimensions, twig in self.twigs.items():
                twig_data_id = data_id.subset(twig_dimensions)
                for task_label in twig.parent_edge_tasks:
                    quantum_keys[task_label] = QuantumKey(task_label, twig_data_id.required_values)
                for dataset_type_name in twig.parent_edge_dataset_types:
                    dataset_keys[dataset_type_name] = DatasetKey(
                        dataset_type_name, twig_data_id.required_values
                    )
            for dataset_type_name, task_label in self.input_edges:
                skeleton.add_input_edge(quantum_keys[task_label], dataset_keys[dataset_type_name])
            for task_label, dataset_type_name in self.output_edges:
                skeleton.add_output_edge(quantum_keys[task_label], dataset_keys[dataset_type_name])
        if not self.has_followup_queries:
            # Delete data IDs we don't need anymore to save memory.
            del self.data_ids


@dataclasses.dataclass(eq=False, repr=False)
class _DimensionGroupTree:
    """A tree of dimension groups in which branches are subsets of their
    parents.

    This class holds all of the per-subgraph state for this QG builder
    subclass.

    Notes
    -----
    The full set of dimensions referenced by any task or dataset type (except
    prerequisite inputs) forms the conceptual "trunk" of this tree.  Each
    branch has a subset of the dimensions of its parent branch, and each set
    of dimensions appears exactly once in a tree (so there is some flexibility
    in where certain dimension subsets may appear; right now this is resolved
    somewhat arbitrarily).
    We do not add branches for every possible dimension subset; a branch is
    created for a `~lsst.daf.butler.DimensionGroup` if:

     - if there is a task whose quanta have those dimensions;
     - if there is a non-prerequisite dataset type with those dimensions;
     - if there is an edge for which the union of the task and dataset type
       dimensions are those dimensions;
     - if there is a dimension element in any task or non-prerequisite dataset
       type dimensions whose `~lsst.daf.butler.DimensionElement.minimal_group`
       is those dimensions.

    We process the initial data query by recursing through this tree structure
    to populate a data ID set for each branch
    (`_DimensionGroupBranch.project_data_ids`), and then process those sets
    recursively (`_DimensionGroupBranch.update_skeleton`). This can be far
    faster than the non-recursive processing the QG builder used to use because
    the set of data IDs is smaller (sometimes dramatically smaller) as we move
    to smaller sets of dimensions.

    In addition to their child branches, a branch that is used to define graph
    edges also has "twigs", which are a flatter set of dimension subsets for
    each of the tasks and dataset types that appear in that branch's edges.
    The same twig dimensions can appear in multiple branches, and twig
    dimensions can be the same as their parent branch's (but not a superset).
    """

    subgraph: PipelineGraph
    """Graph of this subset of the pipeline."""

    all_dimensions: DimensionGroup = dataclasses.field(init=False)
    """The union of all dimensions that appear in any task or
    (non-prerequisite) dataset type in this subgraph.
    """

    empty_dimensions_branch: _DimensionGroupBranch = dataclasses.field(init=False)
    """The tasks and dataset types of this subset of this pipeline that have
    empty dimensions.

    Prerequisite dataset types are not included.
    """

    trunk_branches: dict[DimensionGroup, _DimensionGroupBranch] = dataclasses.field(init=False)
    """The top-level branches in the tree of dimension groups.
    """

    branches_by_dimensions: dict[DimensionGroup, _DimensionGroupBranch] = dataclasses.field(init=False)
    """The tasks and dataset types of this subset of the pipeline, grouped
    by their dimensions.

    The tasks and dataset types with empty dimensions are not included; they're
    in `empty_dimensions_tree` since they are usually used differently.
    Prerequisite dataset types are also not included.

    This is a flatter view of the objects in `trunk_branches`.
    """

    overall_inputs: dict[str, DatasetTypeNode] = dataclasses.field(init=False)
    """Pipeline graph nodes for all non-prerequisite, non-init overall-input
    dataset types for this subset of the pipeline.
    """

    def __post_init__(self) -> None:
        universe = self.subgraph.universe
        assert universe is not None, "Pipeline graph is resolved."
        self.branches_by_dimensions = {
            dimensions: _DimensionGroupBranch(tasks, dataset_types)
            for dimensions, (tasks, dataset_types) in self.subgraph.group_by_dimensions().items()
        }
        self.all_dimensions = _union_dimensions(self.branches_by_dimensions.keys(), universe)
        _DimensionGroupBranch.populate_record_elements(self.all_dimensions, self.branches_by_dimensions)
        _DimensionGroupBranch.populate_edges(self.subgraph, self.branches_by_dimensions)
        self.trunk_branches = _DimensionGroupBranch.populate_branches(
            None, self.branches_by_dimensions.copy()
        )
        self.empty_dimensions_branch = self.branches_by_dimensions.pop(
            universe.empty, _DimensionGroupBranch()
        )
        self.overall_inputs = {
            name: node  # type: ignore
            for name, node in self.subgraph.iter_overall_inputs()
            if not node.is_prerequisite  # type: ignore
        }

    def project_data_ids(self, log: LsstLogAdapter) -> None:
        """Recursively populate the data ID sets of the dimension group tree
        from the data ID sets of the trunk branches.

        Parameters
        ----------
        log : `lsst.logging.LsstLogAdapter`
            Logger to use for status reporting.
        """
        for branch_dimensions, branch in self.trunk_branches.items():
            log.debug("Projecting query data IDs to %s.", branch_dimensions)
            branch.project_data_ids(log)


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


def _union_dimensions(groups: Iterable[DimensionGroup], universe: DimensionUniverse) -> DimensionGroup:
    dimension_names: set[str] = set()
    for dimensions_for_group in groups:
        dimension_names.update(dimensions_for_group.names)
    return universe.conform(dimension_names)
