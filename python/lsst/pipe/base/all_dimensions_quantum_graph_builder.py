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
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, final

import astropy.table

from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DimensionElement,
    DimensionGroup,
    DimensionRecordSet,
    MissingDatasetTypeError,
    SkyPixDimension,
)
from lsst.sphgeom import RangeSet
from lsst.utils.logging import LsstLogAdapter, PeriodicLogger
from lsst.utils.timer import timeMethod

from ._datasetQueryConstraints import DatasetQueryConstraintVariant
from .quantum_graph_builder import QuantumGraphBuilder, QuantumGraphBuilderError
from .quantum_graph_skeleton import DatasetKey, PrerequisiteDatasetKey, QuantumGraphSkeleton, QuantumKey

if TYPE_CHECKING:
    from .pipeline_graph import DatasetTypeNode, PipelineGraph, TaskNode


@final
class AllDimensionsQuantumGraphBuilder(QuantumGraphBuilder):
    """An implementation of `.quantum_graph_builder.QuantumGraphBuilder` that
    uses a single large query for data IDs covering all dimensions in the
    pipeline.

    Parameters
    ----------
    pipeline_graph : `.pipeline_graph.PipelineGraph`
        Pipeline to build a `.QuantumGraph` from, as a graph.  Will be resolved
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
    data_id_tables : `~collections.abc.Iterable` [ `astropy.table.Table` ],\
            optional
        Tables of data IDs to join in as constraints.  Missing dimensions that
        are constrained by the ``where`` argument or pipeline data ID will be
        filled in automatically.
    **kwargs
        Additional keyword arguments forwarded to
        `.quantum_graph_builder.QuantumGraphBuilder`.

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
        data_id_tables: Iterable[astropy.table.Table] = (),
        **kwargs: Any,
    ):
        super().__init__(pipeline_graph, butler, **kwargs)
        assert where is not None, "'where' should be an empty str, not None"
        self.where = where
        self.dataset_query_constraint = dataset_query_constraint
        self.bind = bind
        self.data_id_tables = list(data_id_tables)

    @timeMethod
    def process_subgraph(self, subgraph: PipelineGraph) -> QuantumGraphSkeleton:
        # Docstring inherited.
        # There is some chance that the dimension query for one subgraph would
        # be the same as or a dimension-subset of another.  This is an
        # optimization opportunity we're not currently taking advantage of.
        tree = _DimensionGroupTree(subgraph)
        tree.build(self.dataset_query_constraint, self.data_id_tables, log=self.log)
        tree.pprint(printer=self.log.debug)
        self._query_for_data_ids(tree)
        dimension_records = self._fetch_most_dimension_records(tree)
        tree.generate_data_ids(self.log)
        skeleton: QuantumGraphSkeleton = self._make_subgraph_skeleton(tree)
        if not skeleton.has_any_quanta:
            # QG is going to be empty; exit early not just for efficiency, but
            # also so downstream code doesn't have to guard against this case.
            return skeleton
        self._find_followup_datasets(tree, skeleton)
        all_data_id_dimensions = subgraph.get_all_dimensions()
        skeleton.attach_dimension_records(self.butler, all_data_id_dimensions, dimension_records)
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
        query_cmd: list[str] = []
        with self.butler.query() as query:
            query_cmd.append("with butler.query() as query:")
            query_cmd.append(f"    query = query.join_dimensions({list(tree.queryable_dimensions.names)})")
            query = query.join_dimensions(tree.queryable_dimensions)
            if tree.dataset_constraint:
                query_cmd.append(f"    collections = {list(self.input_collections)}")
            for dataset_type_name in tree.dataset_constraint:
                query_cmd.append(f"    query = query.join_dataset_search({dataset_type_name!r}, collections)")
                try:
                    query = query.join_dataset_search(dataset_type_name, self.input_collections)
                except MissingDatasetTypeError:
                    raise QuantumGraphBuilderError(
                        f"No datasets for overall-input {dataset_type_name!r} found (the dataset type is "
                        "not even registered).  This is probably a bug in either the pipeline definition or "
                        "the dataset constraints passed to the quantum graph builder."
                    ) from None
            query_cmd.append(
                f"    query = query.where({dict(tree.subgraph.data_id.mapping)}, "
                f"{self.where!r}, bind={self.bind!r})"
            )
            query = query.where(tree.subgraph.data_id, self.where, bind=self.bind)
            # It's important for tables to be joined in last, so data IDs from
            # pipeline and where can be used to fill in missing columns.
            for table in self.data_id_tables:
                # If this is from ctrl_mpexec's pipetask, it'll have added
                # a filename to the metadata for us.
                table_name = table.meta.get("filename", "unknown")
                query_cmd.append(f"    query = query.join_data_coordinate_table(<{table_name}>)")
                query = query.join_data_coordinate_table(table)
            self.log.verbose("Querying for data IDs via: %s", "\n".join(query_cmd))
            # Allow duplicates from common skypix overlaps to make some queries
            # run faster.
            query._allow_duplicate_overlaps = True
            # Iterate over query results, populating data IDs for datasets,
            # quanta, and edges.  We populate only the first level of the tree
            # in the first pass, so we can be done with the query results as
            # quickly as possible in case that holds a connection/cursor open.
            n_rows = 0
            progress_logger: PeriodicLogger | None = None
            for common_data_id in query.data_ids(tree.queryable_dimensions):
                if progress_logger is None:
                    # There can be a long wait between submitting the query and
                    # returning the first row, so we want to make sure we log
                    # when we get it; note that PeriodicLogger is not going to
                    # do that for us, as it waits for its interval _after_ the
                    # first log is seen.
                    self.log.info("Iterating over data ID query results.")
                    progress_logger = PeriodicLogger(self.log)
                for branch_dimensions, branch in tree.queryable_branches.items():
                    data_id = common_data_id.subset(branch_dimensions)
                    branch.data_ids.add(data_id)
                n_rows += 1
                progress_logger.log("Iterating over data ID query results: %d rows processed so far.", n_rows)
            if n_rows == 0:
                # A single multiline log plays better with log aggregators like
                # Loki.
                lines = ["Initial data ID query returned no rows, so QuantumGraph will be empty."]
                try:
                    lines.extend(query.explain_no_results())
                finally:
                    lines.append("To reproduce this query for debugging purposes, run:")
                    lines.append("")
                    lines.extend(query_cmd)
                    lines.append("    print(query.any())")
                    lines.append("")
                    lines.append("And then try removing various constraints until query.any() returns True.")
                    # If an exception was raised, write a partial.
                    self.log.error("\n".join(lines))
                return
        self.log.verbose("Done iterating over query results: %d rows processed in total.", n_rows)
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
        for branch_dimensions, branch in tree.branches_by_dimensions.items():
            self.log.verbose(
                "Adding nodes for %s %s data ID(s).",
                len(branch.data_ids),
                branch_dimensions,
            )
            branch.update_skeleton_nodes(skeleton)
        for branch_dimensions, branch in tree.branches_by_dimensions.items():
            self.log.verbose(
                "Adding edges for %s %s data ID(s).",
                len(branch.data_ids),
                branch_dimensions,
            )
            branch.update_skeleton_edges(skeleton)
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
        for dimensions, branch in tree.branches_by_dimensions.items():
            if not dimensions:
                for dataset_type_name in branch.dataset_types.keys():
                    dataset_key = DatasetKey(dataset_type_name, self.empty_data_id.required_values)
                    if ref := self.empty_dimensions_datasets.inputs.get(dataset_key):
                        skeleton.set_dataset_ref(ref, dataset_key)
                    if ref := self.empty_dimensions_datasets.outputs_for_skip.get(dataset_key):
                        skeleton.set_output_for_skip(ref)
                    if ref := self.empty_dimensions_datasets.outputs_in_the_way.get(dataset_key):
                        skeleton.set_output_in_the_way(ref)
                continue
            if not branch.dataset_types and not branch.tasks:
                continue
            if not branch.data_ids:
                continue
            # Iterate over regular input/output dataset type nodes with these
            # dimensions to find those datasets using followup queries.
            with self.butler.query() as butler_query:
                butler_query = butler_query.join_data_coordinates(branch.data_ids)
                for dataset_type_node in branch.dataset_types.values():
                    if tree.subgraph.producer_of(dataset_type_node.name) is None:
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
                # Delete data ID sets we don't need anymore to save memory.
                del branch.data_ids

    @timeMethod
    def _fetch_most_dimension_records(self, tree: _DimensionGroupTree) -> list[DimensionRecordSet]:
        """Query for dimension records for all non-prerequisite data IDs (and
        possibly some prerequisite data IDs).

        Parameters
        ----------
        tree : `_DimensionGroupTree`
            Tree with dimension group branches that holds subgraph-specific
            state for this builder.

        Returns
        -------
        dimension_records : `list` [ `lsst.daf.butler.DimensionRecordSet` ]
            List of sets of dimension records.

        Notes
        -----
        Because the initial common data ID query is used to generate all
        quantum and regular input/output dataset data IDs, column subsets of it
        can also be used to fetch dimension records for those data IDs.
        """
        self.log.verbose("Performing follow-up queries for dimension records.")
        result: list[DimensionRecordSet] = []
        for branch in tree.branches_by_dimensions.values():
            if not branch.dimension_records:
                continue
            if not branch.data_ids:
                continue
            with self.butler.query() as butler_query:
                butler_query = butler_query.join_data_coordinates(branch.data_ids)
                for record_set in branch.dimension_records:
                    record_set.update(butler_query.dimension_records(record_set.element.name))
                    result.append(record_set)
        return result


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

    dimension_records: list[DimensionRecordSet] = dataclasses.field(default_factory=list)
    """Sets of dimension records looked up with these dimensions."""

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
    dimensions, populated by projecting this branch's set of data IDs (i.e.
    remove a dimension, then deduplicate).
    """

    twigs: defaultdict[DimensionGroup, _DimensionGroupTwig] = dataclasses.field(
        default_factory=lambda: defaultdict(_DimensionGroupTwig)
    )
    """Small branches for all of the dimensions that appear on one side of any
    edge in `input_edges` or `output_edges`.
    """

    def pprint(
        self,
        dimensions: DimensionGroup,
        indent: str = "  ",
        suffix: str = "",
        printer: Callable[[str], None] = print,
    ) -> None:
        printer(f"{indent}{dimensions}{suffix}")
        for branch_dimensions, branch in self.branches.items():
            branch.pprint(branch_dimensions, indent + "  ", printer=printer)

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
            log.verbose("%sProjecting query data ID(s) to %s.", log_indent, branch_dimensions)
            branch.project_data_ids(log, log_indent + "  ")

    def update_skeleton_nodes(self, skeleton: QuantumGraphSkeleton) -> None:
        """Process the data ID sets of this branch and its children recursively
        to add nodes and edges to the under-construction quantum graph.

        Parameters
        ----------
        skeleton : `QuantumGraphSkeleton`
            Under-construction quantum graph to modify in place.
        """
        for data_id in self.data_ids:
            for task_label in self.tasks:
                skeleton.add_quantum_node(task_label, data_id)
            for dataset_type_name in self.dataset_types:
                skeleton.add_dataset_node(dataset_type_name, data_id)

    def update_skeleton_edges(self, skeleton: QuantumGraphSkeleton) -> None:
        """Process the data ID sets of this branch and its children recursively
        to add nodes and edges to the under-construction quantum graph.

        Parameters
        ----------
        skeleton : `QuantumGraphSkeleton`
            Under-construction quantum graph to modify in place.
        """
        for data_id in self.data_ids:
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
        if not self.dataset_types and not self.tasks:
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
       is those dimensions (allowing us to look up dimension records).

    In addition, for any dimension group that has unqueryable dimensions (e.g.
    non-common skypix dimensions, like healpix), we create a branch for the
    subset of the group with only queryable dimensions.

    We process the initial data query by recursing through this tree structure
    to populate a data ID set for each branch
    (`_DimensionGroupBranch.project_data_ids`), and then process those sets.
    This can be far faster than the non-recursive processing the QG builder
    used to use because the set of data IDs is smaller (sometimes dramatically
    smaller) as we move to smaller sets of dimensions.

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

    queryable_dimensions: DimensionGroup = dataclasses.field(init=False)
    """All dimensions except those that cannot be queried for directly via the
    butler (e.g. skypix systems other than the common one).
    """

    branches_by_dimensions: dict[DimensionGroup, _DimensionGroupBranch] = dataclasses.field(init=False)
    """The tasks and dataset types of this subset of the pipeline, grouped
    by their dimensions.
    """

    dataset_constraint: set[str] = dataclasses.field(default_factory=set)
    """The names of dataset types used as query constraints."""

    queryable_branches: dict[DimensionGroup, _DimensionGroupBranch] = dataclasses.field(default_factory=dict)
    """The top-level branches in the tree of dimension groups populated by the
    butler query.

    Data IDs in these branches are populated from the top down, with each
    branch a projection ("remove dimension, then deduplicate") of its parent,
    starting with the query result rows.
    """

    generators: list[DataIdGenerator] = dataclasses.field(default_factory=list)
    """Branches for dimensions groups that are populated by algorithmically
    generating data IDs from those in one or more other branches.

    These are typically variants on the theme of adding a skypix dimension to
    another set of dimensions by identifying the sky pixels that overlap the
    region of the original dimensions.
    """

    def __post_init__(self) -> None:
        universe = self.subgraph.universe
        assert universe is not None, "Pipeline graph is resolved."
        self.branches_by_dimensions = {
            dimensions: _DimensionGroupBranch(tasks, dataset_types)
            for dimensions, (tasks, dataset_types) in self.subgraph.group_by_dimensions().items()
        }
        self.all_dimensions = DimensionGroup.union(*self.branches_by_dimensions.keys(), universe=universe)

    def build(
        self,
        requested: DatasetQueryConstraintVariant,
        data_id_tables: Iterable[astropy.table.Table],
        *,
        log: LsstLogAdapter,
    ) -> None:
        """Organize the branches into a tree.

        Parameters
        ----------
        requested : `DatasetQueryConstraintVariant`
            Query constraint specified by the user.
        data_id_tables : `~collections.abc.Iterable` [ `astropy.table.Table` ]
            Data ID tables being joined into the query.
        log : `lsst.log.LsstLogAdapter`
            Logger that supports ``verbose`` output.
        """
        universe = self.all_dimensions.universe
        self._make_dimension_record_branches()
        self._make_edge_branches()
        self._set_dataset_constraint(requested, log)
        # Work out which dimensions we can potentially query the database for.
        # We start out by dropping all skypix dimensions other than the common
        # one, and then we add them back in if a constraint dataset type or
        # data ID table provides them.
        unqueryable_skypix = universe.conform(self.all_dimensions.skypix - {universe.commonSkyPix.name})
        self.queryable_dimensions = self.all_dimensions.difference(unqueryable_skypix)
        for dataset_type_name in sorted(self.dataset_constraint):
            dataset_type_dimensions = self.subgraph.dataset_types[dataset_type_name].dimensions
            dataset_type_skypix = dataset_type_dimensions.intersection(unqueryable_skypix)
            if dataset_type_skypix:
                log.info(
                    f"Including {dataset_type_skypix} in the set of dimensions to query via "
                    f"{dataset_type_name}.  If this query fails, exclude those dataset type "
                    "from the constraint or provide a data ID table for missing spatial joins."
                )
            self.queryable_dimensions = self.queryable_dimensions.union(dataset_type_dimensions)
        for data_id_table in data_id_tables:
            table_dimensions = universe.conform(data_id_table.colnames)
            if table_dimensions.skypix:
                self.queryable_dimensions = self.queryable_dimensions.union(table_dimensions)
        # Set up the tree to generate most data IDs by querying for them from
        # the database and then projecting to subset dimensions.
        branches_not_in_tree = set(self.branches_by_dimensions.keys())
        self._make_queryable_branch_tree(branches_not_in_tree)
        # Try to find ways to generate other data IDs directly from the
        # queryable branches.
        self._make_queryable_overlap_branch_generators(branches_not_in_tree)
        # As long as there are still branches that haven't been inserted into
        # the tree, try to add them as projections of generated branches or
        # generators on generated branches.
        while branches_not_in_tree:
            # Look for projections first, since those are more efficient, and
            # some may be available after we've added some generators.
            # We intentionally add the same branch as a projection of multiple
            # parents since (unlike queryable dimensions) there's no guarantee
            # that each parent branch's data IDs would project to the same set
            # (e.g. a visit-healpix overlap may yield different healpixels than
            # a patch-healpix overlap, even if the visits and patches overlap).
            for target_dimensions in sorted(branches_not_in_tree):
                for generator in self.generators:
                    if self._maybe_insert_projection_branch(
                        target_dimensions, generator.dimensions, generator.branch.branches
                    ):
                        branches_not_in_tree.discard(target_dimensions)
            if not self._make_general_overlap_branch_generator(branches_not_in_tree):
                break
        # After we've exhausted overlap generation, try generation via joins
        # of dimensions we can already query for or generate.
        while branches_not_in_tree:
            if not self._make_join_branch_generator(branches_not_in_tree):
                raise QuantumGraphBuilderError(f"Could not generate data IDs for {branches_not_in_tree}.")

    def _set_dataset_constraint(self, requested: DatasetQueryConstraintVariant, log: LsstLogAdapter) -> None:
        """Set the dataset query constraint.

        Parameters
        ----------
        requested : `DatasetQueryConstraintVariant`
            Query constraint specified by the user.
        log : `lsst.log.LsstLogAdapter`
            Logger that supports ``verbose`` output.
        """
        overall_inputs: dict[str, DatasetTypeNode] = {
            name: node  # type: ignore
            for name, node in self.subgraph.iter_overall_inputs()
            if not node.is_prerequisite  # type: ignore
        }
        match requested:
            case DatasetQueryConstraintVariant.ALL:
                self.dataset_constraint = {
                    name
                    for name, dataset_type_node in overall_inputs.items()
                    if (dataset_type_node.is_initial_query_constraint and dataset_type_node.dimensions)
                }
            case DatasetQueryConstraintVariant.OFF:
                pass
            case DatasetQueryConstraintVariant.LIST:
                self.dataset_constraint = set(requested)
                inputs = {
                    name for name, dataset_type_node in overall_inputs.items() if dataset_type_node.dimensions
                }
                if remainder := self.dataset_constraint.difference(inputs):
                    log.verbose(
                        "Ignoring dataset types %s in dataset query constraint that are not inputs to this "
                        "subgraph, on the assumption that they are relevant for a different subgraph.",
                        remainder,
                    )
                self.dataset_constraint.intersection_update(inputs)
            case _:
                raise QuantumGraphBuilderError(
                    f"Unable to handle type {requested} given as dataset query constraint."
                )

    def _make_dimension_record_branches(self) -> None:
        """Ensure we have branches for all dimension elements we'll need to
        fetch dimension records for.
        """
        for element_name in self.all_dimensions.elements:
            element = self.all_dimensions.universe[element_name]
            record_set = DimensionRecordSet(element_name, universe=self.all_dimensions.universe)
            if element.minimal_group in self.branches_by_dimensions:
                self.branches_by_dimensions[element.minimal_group].dimension_records.append(record_set)
            else:
                self.branches_by_dimensions[element.minimal_group] = _DimensionGroupBranch(
                    dimension_records=[record_set]
                )

    def _make_edge_branches(self) -> None:
        """Ensure we have branches for all edges in the graph."""

        def update_edge_branch(
            task_node: TaskNode, dataset_type_node: DatasetTypeNode
        ) -> _DimensionGroupBranch:
            union_dimensions = task_node.dimensions.union(dataset_type_node.dimensions)
            if (branch := self.branches_by_dimensions.get(union_dimensions)) is None:
                branch = _DimensionGroupBranch()
                self.branches_by_dimensions[union_dimensions] = branch
            branch.twigs[dataset_type_node.dimensions].parent_edge_dataset_types.add(dataset_type_node.name)
            branch.twigs[task_node.dimensions].parent_edge_tasks.add(task_node.label)
            return branch

        for task_node in self.subgraph.tasks.values():
            for dataset_type_node in self.subgraph.inputs_of(task_node.label).values():
                assert dataset_type_node is not None, "Pipeline graph is resolved."
                if dataset_type_node.is_prerequisite:
                    continue
                branch = update_edge_branch(task_node, dataset_type_node)
                branch.input_edges.append((dataset_type_node.name, task_node.label))
            for dataset_type_node in self.subgraph.outputs_of(task_node.label).values():
                assert dataset_type_node is not None, "Pipeline graph is resolved."
                branch = update_edge_branch(task_node, dataset_type_node)
                branch.output_edges.append((task_node.label, dataset_type_node.name))

    def _make_queryable_branch_tree(self, branches_not_in_tree: set[DimensionGroup]) -> None:
        """Assemble the branches with queryable dimensions into a tree, in
        which each branch has a subset of the dimensions of its parent.

        Parameters
        ----------
        branches_not_in_tree : `set` [ `lsst.daf.butler.DimensionGroup` ]
            Dimensions that have not yet been inserted into the tree.  Updated
            in place.
        """
        for target_dimensions in sorted(branches_not_in_tree):
            if target_dimensions.issubset(self.queryable_dimensions):
                if self._maybe_insert_projection_branch(
                    target_dimensions, self.queryable_dimensions, self.queryable_branches
                ):
                    branches_not_in_tree.remove(target_dimensions)
                else:
                    raise AssertionError(
                        "Projection-branch insertion should not fail for queryable dimensions."
                    )

    def _maybe_insert_projection_branch(
        self,
        target_dimensions: DimensionGroup,
        candidate_dimensions: DimensionGroup,
        candidate_projection_branches: dict[DimensionGroup, _DimensionGroupBranch],
    ) -> bool:
        """Insert a branch at the appropriate location in a [sub]tree.

        Branches are inserted below the first parent branch whose dimensions
        are a superset of their own.

        Parameters
        ----------
        target_dimensions : `lsst.daf.butler.DimensionGroup`
            Dimensions of the branch to be inserted.
        candidate_dimensions : `lsst.daf.butler.DimensionGroup`
            Dimensions of the subtree the branch might be inserted under.  If
            this is not a superset of ``target_dimensions``, this method
            returns `False` and nothing is done.
        candidate_projection_branches : `dict` [ \
                `lsst.daf.butler.DimensionGroup`, `_DimensionGroupBranch` ]
            Subtree branches to be updated directly or indirectly (i.e. in a
            nested branch).

        Returns
        -------
        inserted : `bool`
            Whether the branch was actually inserted.
        """
        if candidate_dimensions >= target_dimensions:
            target_branch = self.branches_by_dimensions[target_dimensions]
            for child_dimensions in list(candidate_projection_branches.keys()):
                if self._maybe_insert_projection_branch(
                    child_dimensions, target_dimensions, target_branch.branches
                ):
                    del candidate_projection_branches[child_dimensions]
            for child_dimensions, child_branch in candidate_projection_branches.items():
                if self._maybe_insert_projection_branch(
                    target_dimensions, child_dimensions, child_branch.branches
                ):
                    return True
            candidate_projection_branches[target_dimensions] = target_branch
            return True
        return False

    def _make_queryable_overlap_branch_generators(self, branches_not_in_tree: set[DimensionGroup]) -> None:
        """Add data ID generators for sets of dimensions that can only
        partially queried for, with the rest needing to be generated by
        manipulating the data IDs of the queryable subset.

        Parameters
        ----------
        branches_not_in_tree : `set` [ `lsst.daf.butler.DimensionGroup` ]
            Dimensions that have not yet been inserted into the tree.  Updated
            in place.
        """
        for target_dimensions in sorted(branches_not_in_tree):
            queryable_subset_dimensions = target_dimensions.intersection(self.queryable_dimensions)
            # Make sure we actually have a branch to capture the queryable
            # subset data IDs (i.e. in case we didn't already have one for some
            # dataset type or task, etc).
            if queryable_subset_dimensions not in self.branches_by_dimensions:
                # If we have to make a new queryable branch, we also have to
                # insert it into the tree so its data IDs get populated.
                self.branches_by_dimensions[queryable_subset_dimensions] = _DimensionGroupBranch()
                if not self._maybe_insert_projection_branch(
                    queryable_subset_dimensions,
                    self.queryable_dimensions,
                    self.queryable_branches,
                ):
                    raise AssertionError(
                        "Projection-branch insertion should not fail for queryable dimensions."
                    )
            if queryable_region_name := queryable_subset_dimensions.region_dimension:
                # If there is a single well-defined region for the queryable
                # subset, we can potentially generate skypix IDs from it.
                # Do the target dimensions just add a single skypix dimension
                # to the queryable subset?
                remainder_dimensions = target_dimensions - queryable_subset_dimensions
                if (remainder_skypix := get_single_skypix(remainder_dimensions)) is not None:
                    queryable_region_element = target_dimensions.universe[queryable_region_name]
                    self._append_data_id_generator(
                        queryable_subset_dimensions,
                        queryable_region_element,
                        target_dimensions,
                        remainder_skypix,
                        branches_not_in_tree,
                    )

    def _append_data_id_generator(
        self,
        source_dimensions: DimensionGroup,
        source_region_element: DimensionElement,
        target_dimensions: DimensionGroup,
        remainder_skypix: SkyPixDimension,
        branches_not_in_tree: set[DimensionGroup],
    ) -> None:
        """Append an appropriate `DataIdGenerator` instance for generating
        data IDs with the given characteristics.

        Parameters
        ----------
        source_dimensions : `lsst.daf.butler.DimensionGroup`
            Dimensions whose data IDs can already populated, to use as a
            starting point.
        source_region_element : `lsst.daf.butler.DimensionElement`
            Dimension element associated with the region for the source
            dimensions.  It is guaranteed that there is exactly one such
            region.
        target_dimensions : `lsst.daf.butler.DimensionGroup`
            Dimensions of the data IDs to be generated.
        remainder_skypix : `lsst.daf.butler.SkyPixDimension`
            The single skypix dimension that is being added to
            ``source_dimensions`` to yield ``target_dimensions``.
        branches_not_in_tree : `set` [ `lsst.daf.butler.DimensionGroup` ]
            Dimensions that have not yet been inserted into the tree.  Updated
            in place.
        """
        target_branch = self.branches_by_dimensions[target_dimensions]
        # We want to do the overlap calculation without any extra dimensions
        # beyond the two spatial dimensions, which may or may not be what we
        # already have.
        overlap_dimensions = source_region_element.minimal_group | remainder_skypix.minimal_group
        generator: DataIdGenerator
        if overlap_dimensions == target_dimensions:
            if isinstance(source_region_element, SkyPixDimension):
                if source_region_element.system == remainder_skypix.system:
                    if source_region_element.level > remainder_skypix.level:
                        generator = SkyPixGatherDataIdGenerator(
                            target_branch,
                            target_dimensions,
                            source_dimensions,
                            remainder_skypix,
                            source_region_element,
                        )
                    else:
                        generator = SkyPixScatterDataIdGenerator(
                            target_branch,
                            target_dimensions,
                            source_dimensions,
                            remainder_skypix,
                            source_region_element,
                        )
                else:
                    generator = CrossSystemDataIdGenerator(
                        target_branch,
                        target_dimensions,
                        source_dimensions,
                        remainder_skypix,
                        source_region_element,
                    )
            else:
                generator = DatabaseSourceDataIdGenerator(
                    target_branch,
                    target_dimensions,
                    source_dimensions,
                    remainder_skypix,
                    source_region_element,
                )
            # We know we can populate the data IDs in remainder_skypix_branch
            # from the target branch by projection.  Even if it's already
            # populated by some other generated branch, we want to populate it
            # again in case that picks up additional sky pixels.
            target_branch.branches[remainder_skypix.minimal_group] = self.branches_by_dimensions[
                remainder_skypix.minimal_group
            ]
            branches_not_in_tree.discard(remainder_skypix.minimal_group)
        else:
            if overlap_dimensions not in self.branches_by_dimensions:
                self.branches_by_dimensions[overlap_dimensions] = _DimensionGroupBranch()
                branches_not_in_tree.add(overlap_dimensions)
                self._append_data_id_generator(
                    source_region_element.minimal_group,
                    source_region_element,
                    overlap_dimensions,
                    remainder_skypix,
                    branches_not_in_tree,
                )
            generator = JoinDataIdGenerator(
                target_branch,
                target_dimensions,
                source_dimensions,
                overlap_dimensions,
            )
        self.generators.append(generator)
        branches_not_in_tree.remove(target_dimensions)

    def _make_general_overlap_branch_generator(self, branches_not_in_tree: set[DimensionGroup]) -> bool:
        """Add data ID generators for sets of dimensions that can be generated
        via skypix envelopes of other generated data IDs.

        This method should be called in a loop until it returns `False`
        (indicating no progress was made) or ``branches_not_in_tree`` is empty
        (indicating no more work to be done).

        Parameters
        ----------
        branches_not_in_tree : `set` [ `lsst.daf.butler.DimensionGroup` ]
            Dimensions that have not yet been inserted into the tree.  Updated
            in place.

        Returns
        -------
        appended : `bool`
            Whether a new data ID generator was successfully appended.
        """
        dimensions_done = sorted(self.branches_by_dimensions.keys() - branches_not_in_tree)
        for source_dimensions in dimensions_done:
            for target_dimensions in sorted(branches_not_in_tree):
                if not source_dimensions <= target_dimensions:
                    continue
                remainder_dimensions = target_dimensions - source_dimensions
                if (remainder_skypix := get_single_skypix(remainder_dimensions)) is not None:
                    if source_region_name := source_dimensions.region_dimension:
                        # If the target dimensions are just adding a single
                        # skypix to the source dimensions and the source
                        # dimensions have a single region column, we can
                        # generate the skypix indices from the envelopes of
                        # those regions.
                        source_region_element = source_dimensions.universe[source_region_name]
                        self._append_data_id_generator(
                            source_dimensions,
                            source_region_element,
                            target_dimensions,
                            remainder_skypix,
                            branches_not_in_tree,
                        )
                        return True
        return not branches_not_in_tree

    def _make_join_branch_generator(self, branches_not_in_tree: set[DimensionGroup]) -> bool:
        """Add data ID generators for sets of dimensions that can be generated
        via inner joints of other generated data IDs.

        This method should be called in a loop until it returns `False`
        (indicating no progress was made) or ``branches_not_in_tree`` is empty
        (indicating no more work to be done).

        Parameters
        ----------
        branches_not_in_tree : `set` [ `lsst.daf.butler.DimensionGroup` ]
            Dimensions that have not yet been inserted into the tree.  Updated
            in place.

        Returns
        -------
        appended : `bool`
            Whether a new data ID generator was successfully appended.
        """
        for target_dimensions in sorted(branches_not_in_tree):
            dimensions_done = sorted(self.branches_by_dimensions.keys() - branches_not_in_tree)
            candidates_by_common: dict[DimensionGroup, tuple[DimensionGroup, DimensionGroup]] = {}
            for operand1, operand2 in itertools.combinations(dimensions_done, 2):
                if operand1.union(operand2) == target_dimensions:
                    candidates_by_common[operand1.intersection(operand2)] = (operand1, operand2)
            if candidates_by_common:
                # Because DimensionGroup defines a set-like inequality
                # operator, 'max' returns the set of dimensions that contains
                # as many of the other sets of dimensions as possible, which is
                # a reasonable guess at the most-constrained join.
                operand1, operand2 = candidates_by_common[max(candidates_by_common)]
                generator = JoinDataIdGenerator(
                    self.branches_by_dimensions[target_dimensions],
                    target_dimensions,
                    operand1,
                    operand2,
                )
                self.generators.append(generator)
                branches_not_in_tree.remove(target_dimensions)
                return True
        return not branches_not_in_tree

    def project_data_ids(self, log: LsstLogAdapter) -> None:
        """Recursively populate the data ID sets of the dimension group tree
        from the data ID sets of the queryable branches.

        Parameters
        ----------
        log : `lsst.logging.LsstLogAdapter`
            Logger to use for status reporting.
        """
        for branch_dimensions, branch in self.queryable_branches.items():
            log.verbose("Projecting query data ID(s) to %s.", branch_dimensions)
            branch.project_data_ids(log)

    def generate_data_ids(self, log: LsstLogAdapter) -> None:
        """Run all data ID generators.

        This runs data ID generators and projects data IDs to their subset
        dimensions.  It can only be called after queryable data IDs have been
        populated and dimension records fetched.

        Parameters
        ----------
        log : `lsst.logging.LsstLogAdapter`
            Logger to use for status reporting.
        """
        for generator in self.generators:
            generator.run(log, self.branches_by_dimensions)
            generator.branch.project_data_ids(log, log_indent="  ")

    def pprint(self, printer: Callable[[str], None] = print) -> None:
        """Print a human-readable representation of the dimensions tree.

        Parameters
        ----------
        printer : `~collections.abc.Callable`, optional
            A function that takes a single string argument and prints a single
            line (including a newline). Default is the built-in `print`
            function.
        """
        printer("Queryable:")
        for branch_dimensions, branch in self.queryable_branches.items():
            branch.pprint(branch_dimensions, "  ", printer=printer)
        printer("Generator:")
        for generator in self.generators:
            generator.pprint("  ", printer=printer)


def get_single_skypix(dimensions: DimensionGroup) -> SkyPixDimension | None:
    """Try to coerce a dimension group a single skypix dimenison.

    Parameters
    ----------
    dimensions : `lsst.daf.butler.DimensionGroup`
        Input dimensions.

    Returns
    -------
    skypix : `lsst.daf.butler.SkyPixDimension` or `None`
        A skypix dimension that is the only dimension in the given group, or
        `None` in all other cases.
    """
    if len(dimensions) == 1:
        (name,) = dimensions.names
        return dimensions.universe.skypix_dimensions.get(name)
    return None


@dataclasses.dataclass
class DataIdGenerator:
    """A base class for generators for quantum and dataset data IDs that cannot
    be directly queried for.
    """

    branch: _DimensionGroupBranch
    """Branch of the dimensions tree that this generator populates."""

    dimensions: DimensionGroup
    """Dimensions of the data IDs generated."""

    source: DimensionGroup
    """Dimensions of another set of data IDs that this generator uses as a
    starting point.
    """

    def pprint(self, indent: str = "  ", printer: Callable[[str], None] = print) -> None:
        """Print a human-readable representation of this generator.

        Parameters
        ----------
        indent : `str`
            Blank spaces to prefix the output with (useful when this is nested
            in hierarchical object being printed).
        printer : `~collections.abc.Callable`, optional
            A function that takes a single string argument and prints a single
            line (including a newline). Default is the built-in `print`
            function.
        """
        self.branch.pprint(
            self.dimensions,
            indent,
            f" <- {self.source} ({self.__class__.__name__})",
            printer=printer,
        )

    def run(self, log: LsstLogAdapter, branches: Mapping[DimensionGroup, _DimensionGroupBranch]) -> None:
        """Run the generator, populating its branch's data IDs.

        Parameters
        ----------
        log : `lsst.log.LsstLogAdapter`
            Logger with a ``verbose`` method as well as the built-in ones.
        branches : `~collections.abc.Mapping`
            Mapping of other dimension branches, keyed by their dimensions.
        """
        raise NotImplementedError()


@dataclasses.dataclass
class DatabaseSourceDataIdGenerator(DataIdGenerator):
    """A data ID generator that generates skypix indices from the envelope of
    regions stored in the database.
    """

    remainder_skypix: SkyPixDimension
    """A single additional skypix dimension to be added to the source
    dimensions.
    """

    source_element: DimensionElement
    """Dimension element that the database-stored regions are associated with.
    """

    def run(self, log: LsstLogAdapter, branches: Mapping[DimensionGroup, _DimensionGroupBranch]) -> None:
        # Docstring inherited.
        source_branch = branches[self.source]
        log.verbose(
            "Generating %s data IDs via %s envelope of %s %s region(s).",
            self.dimensions,
            self.remainder_skypix,
            len(source_branch.data_ids),
            self.source_element,
        )
        pixelization = self.remainder_skypix.pixelization
        (source_records,) = [
            record_set
            for record_set in source_branch.dimension_records
            if record_set.element == self.source_element
        ]
        for source_data_id in source_branch.data_ids:
            source_record = source_records.find(source_data_id)
            for begin, end in pixelization.envelope(source_record.region):
                for index in range(begin, end):
                    target_data_id = DataCoordinate.standardize(
                        source_data_id,
                        **{self.remainder_skypix.name: index},  # type: ignore[arg-type]
                    )
                    self.branch.data_ids.add(target_data_id)


@dataclasses.dataclass
class CrossSystemDataIdGenerator(DataIdGenerator):
    """A data ID generator that generates skypix indices from the envelope of
    skypix regions from some other system (e.g. healpix from HTM).
    """

    remainder_skypix: SkyPixDimension
    """A single additional skypix dimension to be added to the source
    dimensions.
    """

    source_skypix: SkyPixDimension
    """Dimension element for the already-known skypix indices."""

    def run(self, log: LsstLogAdapter, branches: Mapping[DimensionGroup, _DimensionGroupBranch]) -> None:
        # Docstring inherited.
        source_branch = branches[self.source]
        log.verbose(
            "Generating %s data IDs via %s envelope of %s %s region(s).",
            self.dimensions,
            self.remainder_skypix,
            len(source_branch.data_ids),
            self.source_skypix,
        )
        source_pixelization = self.source_skypix.pixelization
        remainder_pixelization = self.remainder_skypix.pixelization
        for source_data_id in source_branch.data_ids:
            source_region = source_pixelization.pixel(source_data_id[self.source_skypix.name])
            for begin, end in remainder_pixelization.envelope(source_region):
                for index in range(begin, end):
                    target_data_id = DataCoordinate.standardize(
                        source_data_id,
                        **{self.remainder_skypix.name: index},  # type: ignore[arg-type]
                    )
                    self.branch.data_ids.add(target_data_id)


@dataclasses.dataclass
class SkyPixScatterDataIdGenerator(DataIdGenerator):
    """A data ID generator that generates skypix indices at a high (fine) level
    from low-level (coarse) indices in the same system.
    """

    remainder_skypix: SkyPixDimension
    """A single additional skypix dimension to be added to the source
    dimensions.
    """

    source_skypix: SkyPixDimension
    """Dimension element for the already-known skypix indices."""

    def run(self, log: LsstLogAdapter, branches: Mapping[DimensionGroup, _DimensionGroupBranch]) -> None:
        # Docstring inherited.
        factor = 4 ** (self.remainder_skypix.level - self.source_skypix.level)
        source_branch = branches[self.source]
        log.verbose(
            "Generating %s data IDs by scaling %s %s IDs in %s by %s.",
            self.dimensions,
            len(source_branch.data_ids),
            self.remainder_skypix,
            self.source,
            factor,
        )
        for source_data_id in source_branch.data_ids:
            ranges = RangeSet(source_data_id[self.source_skypix.name])
            ranges.scale(factor)
            for begin, end in ranges:
                for index in range(begin, end):
                    target_data_id = DataCoordinate.standardize(
                        source_data_id,
                        **{self.remainder_skypix.name: index},  # type: ignore[arg-type]
                    )
                    self.branch.data_ids.add(target_data_id)


@dataclasses.dataclass
class SkyPixGatherDataIdGenerator(DataIdGenerator):
    """A data ID generator that generates skypix indices at a low (coarse)
    level from high-level (fine) indices in the same system.
    """

    remainder_skypix: SkyPixDimension
    """A single additional skypix dimension to be added to the source
    dimensions.
    """

    source_skypix: SkyPixDimension
    """Dimension element for the already-known skypix indices."""

    def run(self, log: LsstLogAdapter, branches: Mapping[DimensionGroup, _DimensionGroupBranch]) -> None:
        # Docstring inherited.
        factor = 4 ** (self.source_skypix.level - self.remainder_skypix.level)
        source_branch = branches[self.source]
        log.verbose(
            "Generating %s data IDs by dividing %s %s IDs in %s by %s.",
            self.dimensions,
            len(source_branch.data_ids),
            self.remainder_skypix,
            self.source,
            factor,
        )
        for source_data_id in source_branch.data_ids:
            index = source_data_id[self.source_skypix.name] // factor
            target_data_id = DataCoordinate.standardize(source_data_id, **{self.remainder_skypix.name: index})
            self.branch.data_ids.add(target_data_id)


@dataclasses.dataclass
class JoinDataIdGenerator(DataIdGenerator):
    """A data ID that does an inner join between two already-populated
    sets of data IDs.
    """

    other: DimensionGroup
    """Dimensions of the other data ID branches to join to those of ``source``.
    """

    def run(self, log: LsstLogAdapter, branches: Mapping[DimensionGroup, _DimensionGroupBranch]) -> None:
        # Docstring inherited.
        source_branch = branches[self.source]
        other_branch = branches[self.other]
        log.verbose(
            "Generating %s data IDs by joining %s (%s) to %s (%s).",
            self.dimensions,
            self.source,
            len(source_branch.data_ids),
            self.other,
            len(other_branch.data_ids),
        )
        common = self.source & self.other
        other_by_common: defaultdict[DataCoordinate, list[DataCoordinate]] = defaultdict(list)
        for other_data_id in other_branch.data_ids:
            other_by_common[other_data_id.subset(common)].append(other_data_id)
        source_by_common: defaultdict[DataCoordinate, list[DataCoordinate]] = defaultdict(list)
        for source_data_id in source_branch.data_ids:
            source_by_common[source_data_id.subset(common)].append(source_data_id)
        for common_data_id in other_by_common.keys() & source_by_common.keys():
            for other_data_id in other_by_common[common_data_id]:
                for source_data_id in source_by_common[common_data_id]:
                    self.branch.data_ids.add(other_data_id.union(source_data_id))
