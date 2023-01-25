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

__all__ = ("AllDimensionsQuantumGraphBuilder",)

import dataclasses
from contextlib import contextmanager
from collections.abc import Mapping, Iterator
import logging
from typing import Any, final

from lsst.daf.butler import Butler, DimensionGraph
from lsst.daf.butler.registry import MissingDatasetTypeError
from lsst.daf.butler.registry.queries import DataCoordinateQueryResults
from .pipeline_graph import PipelineGraph, DatasetTypeNode, TaskNode
from ._datasetQueryConstraints import DatasetQueryConstraintVariant
from .quantum_graph_builder import (
    QuantumGraphBuilder,
    QuantumGraphSkeleton,
    DatasetKey,
    QuantumKey,
)


_LOG = logging.getLogger(__name__)


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
    where : `str`
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
    the ``tract`` even if the ``detector`` does not.  As a result, tasks that
    require full visits cannot have any per-detector inputs or outputs, and may
    not be included in the same quantum graph as any tasks that do have
    per-detector inputs or outputs.
    """

    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        butler: Butler,
        *,
        where: str,
        dataset_query_constraint: DatasetQueryConstraintVariant = DatasetQueryConstraintVariant.ALL,
        bind: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(pipeline_graph, butler, **kwargs)
        self.where = where
        self.dataset_query_constraint = dataset_query_constraint
        self.bind = bind

    def process_subgraph(self, subgraph: PipelineGraph) -> QuantumGraphSkeleton:
        # Docstring inherited.
        # There is some chance that the dimension query for one subgraph
        # would be the same as or a dimension-subset of another.  This is
        # an optimization opportunity we're not currently taking advantage
        # of.
        with _AllDimensionsQuery.from_builder(self, subgraph) as query:
            skeleton = self._make_subgraph_skeleton(query)
            self._find_followup_datasets(query)
        return skeleton

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
        # keys, since empty data IDs are logically subsets of any data ID.
        # We'll copy those to initialize the containers of keys for each result
        # row.
        skeleton = QuantumGraphSkeleton()
        empty_dimensions_dataset_keys = {
            dataset_type_name: DatasetKey(dataset_type_name, self.empty_data_id)
            for dataset_type_name in query.empty_dimensions_dataset_types.keys()
        }
        empty_dimensions_quantum_keys = [
            QuantumKey(task_label, self.empty_data_id) for task_label in query.empty_dimensions_tasks.keys()
        ]
        # Empty dicts-of-sets to be filled with the keys of quanta and
        # datasets, grouped by task label and parent dataset type name
        # (respectively).  Start with the tasks and datasets with empty data
        # IDs.  This will lead to some init datasets that aren't relevant to
        # any quanta appearing in the datasets dict and ultimately the
        # run_xgraph attribute, but we don't care as long as they don't have an
        # edge to a quantum.
        skeleton.quanta.update({key.task_label: {key} for key in empty_dimensions_quantum_keys})
        datasets: dict[str, set[DatasetKey]] = {
            key.parent_dataset_type_name: {key} for key in empty_dimensions_dataset_keys.values()
        }
        # Prepare dicts of edges we'll also populate from the query rows.
        # Outer `str` keys are task labels, inner `str` keys are parent
        # dataset type names.
        read_edges: dict[str, dict[str, set[tuple[DatasetKey, QuantumKey]]]] = {}
        write_edges: dict[str, dict[str, set[tuple[QuantumKey, DatasetKey]]]] = {}
        # Populate the keys of the above dicts using empty-set values.
        for (
            task_nodes_in_group,
            dataset_type_nodes_in_group,
        ) in query.grouped_by_dimensions.values():
            for task_node in task_nodes_in_group.values():
                skeleton.quanta[task_node.label] = set()
                read_edges[task_node.label] = {
                    edge.parent_dataset_type_name: set() for edge in task_node.inputs.values()
                }
                write_edges[task_node.label] = {
                    edge.parent_dataset_type_name: set() for edge in task_node.iter_all_outputs()
                }
            for dataset_type_name in dataset_type_nodes_in_group.keys():
                datasets[dataset_type_name] = set()
        _LOG.info("Iterating over query results to associate quanta with datasets.")
        # Iterate over query results, populating data IDs for datasets and
        # quanta and then connecting them to each other. This is the slowest
        # client-side part of QG generation, and it's often the slowest part
        # overall, so inside this loop is where it's really critical to avoid
        # expensive things, especially in the nested loops.
        n = -1
        for n, common_data_id in enumerate(query.common_data_ids):
            _LOG.debug("Next data ID = %s", common_data_id)
            # Create a data ID for each set of dimensions used by one or more
            # tasks or dataset types, and use that to record all quanta and
            # dataset data IDs for this row.
            dataset_keys_for_row: dict[str, DatasetKey] = empty_dimensions_dataset_keys.copy()
            quantum_keys_for_row: list[QuantumKey] = empty_dimensions_quantum_keys.copy()
            for dimensions, (
                task_nodes,
                dataset_type_nodes,
            ) in query.grouped_by_dimensions.items():
                data_id = common_data_id.subset(dimensions)
                for dataset_type_name in dataset_type_nodes.keys():
                    dataset_key = DatasetKey(dataset_type_name, data_id)
                    dataset_keys_for_row[dataset_type_name] = dataset_key
                    datasets[dataset_type_name].add(dataset_key)
                for task_label in task_nodes.keys():
                    quantum_key = QuantumKey(task_label, data_id)
                    quantum_keys_for_row.append(quantum_key)
                    skeleton.quanta[task_label].add(quantum_key)
            # Whether these quanta are new or existing, we can now associate
            # the dataset data IDs for this row with them.  The fact that a
            # quantum data ID and a dataset data ID both came from the same
            # result row is what tells us they should be associated.  Many of
            # these associates will be duplicates (because another query row
            # that differed from this one only in irrelevant dimensions already
            # added them), and our use of sets should take care of that.
            for quantum_key in quantum_keys_for_row:
                for parent_dataset_type_name, read_edges_for_quantum in read_edges[
                    quantum_key.task_label
                ].items():
                    read_edges_for_quantum.add((dataset_keys_for_row[parent_dataset_type_name], quantum_key))
                for parent_dataset_type_name, write_edges_for_quantum in write_edges[
                    quantum_key.task_label
                ].items():
                    write_edges_for_quantum.add((quantum_key, dataset_keys_for_row[parent_dataset_type_name]))
        if n < 0:
            query.log_failure()
        else:
            _LOG.info("Finished processing %d rows from data ID query.", n)
        _LOG.debug("Updating initial bipartite graph.")
        for datasets_for_type in datasets.values():
            skeleton.xgraph.add_nodes_from(datasets_for_type)
        _LOG.debug("Added %d dataset nodes to initial bipartite graph.", len(skeleton.xgraph))
        for task_label, quanta_for_task in skeleton.quanta.items():
            skeleton.xgraph.add_nodes_from(quanta_for_task)
            for parent_dataset_type_name, read_edge_data in read_edges[task_label].items():
                skeleton.xgraph.add_edges_from(read_edge_data)
            for parent_dataset_type_name, write_edge_data in write_edges[task_label].items():
                skeleton.xgraph.add_edges_from(write_edge_data)
        _LOG.info(
            "Done updating initial bipartite graph: %d new task and dataset nodes, %d edges.",
            len(skeleton.xgraph.nodes),
            len(skeleton.xgraph.edges),
        )
        return skeleton

    def _find_followup_datasets(self, query: _AllDimensionsQuery) -> None:
        """Populate `existing_datasets` by performing follow-up queries joined
        to column-subsets of the initial data ID query.

        Parameters
        ----------
        query : `_AllDimensionsQuery`
            Object representing the full-pipeline data ID query.
        """
        for dimensions, (_, dataset_types_in_group) in query.grouped_by_dimensions.items():
            data_ids = query.common_data_ids.subset(dimensions, unique=True)
            for dataset_type_node in dataset_types_in_group.values():
                if dataset_type_node.name in query.overall_inputs:
                    # Dataset type is an overall input; we always need to try
                    # to find these.
                    count = 0
                    try:
                        for ref in data_ids.findDatasets(dataset_type_node.name, self.input_collections):
                            self.existing_datasets.inputs[
                                DatasetKey(dataset_type_node.name, ref.dataId)
                            ] = ref
                            count += 1
                    except MissingDatasetTypeError:
                        pass
                    _LOG.info("Found %d overall-input datasets of type %r.", count, dataset_type_node.name)
                    continue
                if self.skip_existing_in:
                    # Dataset type is an intermediate or output; need to find
                    # these if only they're from previously executed quanta
                    # that we might skip...
                    count = 0
                    try:
                        for ref in data_ids.findDatasets(dataset_type_node.name, self.skip_existing_in):
                            key = DatasetKey(dataset_type_node.name, ref.dataId)
                            self.existing_datasets.outputs_for_skip[key] = ref
                            count += 1
                            if ref.run == self.output_run:
                                self.existing_datasets.outputs_in_the_way[key] = ref
                    except MissingDatasetTypeError:
                        pass
                    _LOG.info(
                        "Found %d output datasets of type %r in %s.",
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
                                DatasetKey(dataset_type_node.name, ref.dataId)
                            ] = ref
                            count += 1
                    except MissingDatasetTypeError:
                        pass
                    _LOG.info(
                        "Found %d output datasets of type %r in %s.",
                        count,
                        dataset_type_node.name,
                        self.output_run,
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

    grouped_by_dimensions: dict[
        DimensionGraph, tuple[dict[str, TaskNode], dict[str, DatasetTypeNode]]
    ] = dataclasses.field(default_factory=dict)
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
        _LOG.debug("Analyzing subgraph dimensions and overall-inputs.")
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
        dimensions = builder.universe.extract(dimension_names)
        _LOG.debug("Building query for data IDs.")
        result.query_args = {
            "dimensions": dimensions,
            "where": builder.where,
            "dataId": result.subgraph.data_id,
            "bind": builder.bind,
        }
        if builder.dataset_query_constraint == DatasetQueryConstraintVariant.ALL:
            _LOG.debug("Constraining graph query using all datasets not marked as deferred.")
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
            _LOG.debug("Not using dataset existence to constrain query.")
        elif builder.dataset_query_constraint == DatasetQueryConstraintVariant.LIST:
            constraint = set(builder.dataset_query_constraint)
            inputs = result.overall_inputs - result.empty_dimensions_dataset_types.keys()
            if remainder := constraint.difference(inputs):
                raise ValueError(
                    f"{remainder} dataset type(s) specified as a graph constraint, but"
                    f" do not appear as an overall input to the specified pipeline: {inputs}."
                    " Note that component datasets are not permitted as constraints."
                )
            _LOG.debug(f"Constraining graph query using {constraint}")
            result.query_args["datasets"] = constraint
            result.query_args["collections"] = builder.input_collections
        else:
            raise ValueError(
                f"Unable to handle type {builder.dataset_query_constraint} "
                "given as datasetQueryConstraint."
            )
        _LOG.info("Querying for data IDs with arguments:")
        _LOG.info("  dimensions=%s,", list(result.query_args["dimensions"].names))
        _LOG.info("  dataId=%s,", result.query_args["dataId"].byName())
        if result.query_args["where"]:
            _LOG.info("  where=%s,", repr(result.query_args["where"]))
        if "datasets" in result.query_args:
            _LOG.info("  datasets=%s,", list(result.query_args["datasets"]))
        if "collections" in result.query_args:
            _LOG.info("  collections=%s,", list(result.query_args["collections"]))
        with builder.butler.registry.queryDataIds(**result.query_args).materialize() as common_data_ids:
            _LOG.debug("Expanding data IDs.")
            result.common_data_ids = common_data_ids.expanded()
            yield result

    def log_failure(self) -> None:
        """Emit a series of CRITICAL-level log message that attempts to explain
        why the initial data ID query returned no rows.
        """
        _LOG.critical("Initial data ID query returned no rows, so QuantumGraph will be empty.")
        for message in self.common_data_ids.explain_no_results():
            _LOG.critical(message)
        _LOG.critical(
            "To reproduce this query for debugging purposes, run "
            "Registry.queryDataIds with these arguments:"
        )
        # We could just repr() the queryArgs dict to get something
        # the user could make sense of, but it's friendlier to
        # put these args in an easier-to-reconstruct equivalent form
        # so they can read it more easily and copy and paste into
        # a Python terminal.
        _LOG.critical("  dimensions=%s,", list(self.query_args["dimensions"].names))
        _LOG.critical("  dataId=%s,", self.query_args["dataId"].byName())
        if self.query_args["where"]:
            _LOG.critical("  where=%s,", repr(self.query_args["where"]))
        if "datasets" in self.query_args:
            _LOG.critical("  datasets=%s,", list(self.query_args["datasets"]))
        if "collections" in self.query_args:
            _LOG.critical("  collections=%s,", list(self.query_args["collections"]))
