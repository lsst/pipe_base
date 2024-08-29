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
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from io import StringIO
from typing import TYPE_CHECKING, Any, final

from lsst.daf.butler.registry import MissingDatasetTypeError
from lsst.utils.timer import timeMethod

from ._datasetQueryConstraints import DatasetQueryConstraintVariant
from .quantum_graph_builder import (
    DatasetKey,
    PrerequisiteDatasetKey,
    QuantumGraphBuilder,
    QuantumGraphBuilderError,
    QuantumGraphSkeleton,
    QuantumKey,
)

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DimensionGroup
    from lsst.daf.butler.registry.queries import DataCoordinateQueryResults
    from lsst.utils.logging import LsstLogAdapter

    from .pipeline_graph import DatasetTypeNode, PipelineGraph, TaskNode


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
            empty_dimensions_dataset_keys[dataset_type_name] = skeleton.add_dataset_node(
                dataset_type_name, self.empty_data_id
            )
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
        for common_data_id in query.common_data_ids:
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
            data_ids = query.common_data_ids.subset(dimensions, unique=True)
            # Iterate over regular input/output dataset type nodes with these
            # dimensions to find those datasets using straightforward followup
            # queries.
            for dataset_type_node in dataset_types_in_group.values():
                if dataset_type_node.name in query.overall_inputs:
                    # Dataset type is an overall input; we always need to try
                    # to find these.
                    count = 0
                    try:
                        for ref in data_ids.findDatasets(dataset_type_node.name, self.input_collections):
                            self.existing_datasets.inputs[
                                DatasetKey(dataset_type_node.name, ref.dataId.required_values)
                            ] = ref
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
                        for ref in data_ids.findDatasets(dataset_type_node.name, self.skip_existing_in):
                            key = DatasetKey(dataset_type_node.name, ref.dataId.required_values)
                            self.existing_datasets.outputs_for_skip[key] = ref
                            count += 1
                            if ref.run == self.output_run:
                                self.existing_datasets.outputs_in_the_way[key] = ref
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
                        for ref in data_ids.findDatasets(dataset_type_node.name, [self.output_run]):
                            self.existing_datasets.outputs_in_the_way[
                                DatasetKey(dataset_type_node.name, ref.dataId.required_values)
                            ] = ref
                            count += 1
                    except MissingDatasetTypeError:
                        pass
                    self.log.verbose(
                        "Found %d output dataset(s) of type %r in %s.",
                        count,
                        dataset_type_node.name,
                        self.output_run,
                    )
            del dataset_type_node
            # Iterate over tasks with these dimensions to perform follow-up
            # queries for prerequisite inputs, which may have dimensions that
            # were not in ``common_data_ids`` and/or require temporal joins to
            # calibration validity ranges.
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
                        query_results = data_ids.findRelatedDatasets(
                            finder.dataset_type_node.dataset_type, self.input_collections
                        )
                    except MissingDatasetTypeError:
                        query_results = []
                    for data_id, ref in query_results:
                        dataset_key = PrerequisiteDatasetKey(finder.dataset_type_node.name, ref.id.bytes)
                        quantum_key = QuantumKey(task_node.label, data_id.required_values)
                        # The column-subset operation used to make `data_ids`
                        # from `common_data_ids` can strip away post-query
                        # filtering; e.g. if we starts with a {visit, patch}
                        # query but subset down to just {visit}, we can't keep
                        # the patch.region column we need for that filtering.
                        # This means we can get some data IDs that weren't in
                        # the original query (e.g. visits that don't overlap
                        # the same patch, but do overlap the some common skypix
                        # ID).  We don't want to add quanta with those data ID
                        # here, which is why we pass
                        # ignore_unrecognized_quanta=True here.
                        if skeleton.add_input_edge(quantum_key, dataset_key, ignore_unrecognized_quanta=True):
                            self.existing_datasets.inputs[dataset_key] = ref
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

    query_args: dict[str, Any] = dataclasses.field(default_factory=dict)
    """All keyword arguments passed to `lsst.daf.butler.Registry.queryDataIds`.
    """

    common_data_ids: DataCoordinateQueryResults = dataclasses.field(init=False)
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
        builder.log.debug("Building query for data IDs.")
        result.query_args = {
            "dimensions": dimensions,
            "where": builder.where,
            "dataId": result.subgraph.data_id,
            "bind": builder.bind,
        }
        if builder.dataset_query_constraint == DatasetQueryConstraintVariant.ALL:
            builder.log.debug("Constraining graph query using all datasets not marked as deferred.")
            result.query_args["datasets"] = {
                name
                for name, dataset_type_node in result.overall_inputs.items()
                if (
                    dataset_type_node.is_initial_query_constraint
                    and name not in result.empty_dimensions_dataset_types
                )
            }
            result.query_args["collections"] = builder.input_collections
        elif builder.dataset_query_constraint == DatasetQueryConstraintVariant.OFF:
            builder.log.debug("Not using dataset existence to constrain query.")
        elif builder.dataset_query_constraint == DatasetQueryConstraintVariant.LIST:
            constraint = set(builder.dataset_query_constraint)
            inputs = result.overall_inputs - result.empty_dimensions_dataset_types.keys()
            if remainder := constraint.difference(inputs):
                raise QuantumGraphBuilderError(
                    f"{remainder} dataset type(s) specified as a graph constraint, but"
                    f" do not appear as an overall input to the specified pipeline: {inputs}."
                    " Note that component datasets are not permitted as constraints."
                )
            builder.log.debug(f"Constraining graph query using {constraint}")
            result.query_args["datasets"] = constraint
            result.query_args["collections"] = builder.input_collections
        else:
            raise QuantumGraphBuilderError(
                f"Unable to handle type {builder.dataset_query_constraint} "
                "given as datasetQueryConstraint."
            )
        builder.log.verbose("Querying for data IDs with arguments:")
        builder.log.verbose("  dimensions=%s,", list(result.query_args["dimensions"].names))
        builder.log.verbose("  dataId=%s,", dict(result.query_args["dataId"].required))
        if result.query_args["where"]:
            builder.log.verbose("  where=%s,", repr(result.query_args["where"]))
        if "datasets" in result.query_args:
            builder.log.verbose("  datasets=%s,", list(result.query_args["datasets"]))
        if "collections" in result.query_args:
            builder.log.verbose("  collections=%s,", list(result.query_args["collections"]))
        with builder.butler.registry.caching_context():
            with builder.butler.registry.queryDataIds(**result.query_args).materialize() as common_data_ids:
                builder.log.debug("Expanding data IDs.")
                result.common_data_ids = common_data_ids.expanded()
                yield result

    def log_failure(self, log: LsstLogAdapter) -> None:
        """Emit an ERROR-level log message that attempts to explain
        why the initial data ID query returned no rows.

        Parameters
        ----------
        log : `logging.Logger`
            The logger to use to emit log messages.
        """
        # A single multiline log plays better with log aggregators like Loki.
        buffer = StringIO()
        try:
            buffer.write("Initial data ID query returned no rows, so QuantumGraph will be empty.\n")
            for message in self.common_data_ids.explain_no_results():
                buffer.write(message)
                buffer.write("\n")
            buffer.write(
                "To reproduce this query for debugging purposes, run "
                "Registry.queryDataIds with these arguments:\n"
            )
            # We could just repr() the queryArgs dict to get something
            # the user could make sense of, but it's friendlier to
            # put these args in an easier-to-reconstruct equivalent form
            # so they can read it more easily and copy and paste into
            # a Python terminal.
            buffer.write(f"  dimensions={list(self.query_args['dimensions'].names)},\n")
            buffer.write(f"  dataId={dict(self.query_args['dataId'].required)},\n")
            if self.query_args["where"]:
                buffer.write(f"  where={repr(self.query_args['where'])},\n")
            if "datasets" in self.query_args:
                buffer.write(f"  datasets={list(self.query_args['datasets'])},\n")
            if "collections" in self.query_args:
                buffer.write(f"  collections={list(self.query_args['collections'])},\n")
        finally:
            # If an exception was raised, write a partial.
            log.error(buffer.getvalue())
            buffer.close()
