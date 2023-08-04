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

__all__ = (
    "QuantumGraphBuilder",
    "DatasetKey",
    "QuantumKey",
    "ExistingDatasets",
    "QuantumGraphSkeleton",
)

import dataclasses
import itertools
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, cast, final

import networkx
from lsst.daf.butler import (
    Butler,
    CollectionType,
    DataCoordinate,
    DatasetId,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    DimensionUniverse,
    Quantum,
    Registry,
    SkyPixDimension,
)
from lsst.daf.butler.core.named import NamedKeyDict, NamedKeyMapping
from lsst.daf.butler.registry import MissingCollectionError, MissingDatasetTypeError
from lsst.sphgeom import RangeSet

from . import automatic_connection_constants as acc
from ._status import NoWorkFound
from .connections import AdjustQuantumHelper
from .graph import QuantumGraph
from .pipeline_graph import DatasetTypeNode, PipelineGraph, ReadEdge, TaskNode, WriteEdge

if TYPE_CHECKING:
    from .pipeline import TaskDef

_LOG = logging.getLogger(__name__)


class QuantumGraphBuilder(ABC):
    """An abstract base class for building `QuantumGraph` objects from a
    pipeline.

    Parameters
    ----------
    pipeline_graph : `.pipeline_graph.PipelineGraph`
        Pipeline to build a `QuantumGraph` from, as a graph.  Will be resolved
        in-place with the given butler (any existing resolution is ignored).
    butler : `lsst.daf.butler.Butler`
        Client for the data repository.  Should be read-only.
    input_collections : `~collections.abc.Sequence` [ `str` ], optional
        Collections to search for overall-input datasets.  If not provided,
        ``butler.collections`` is used (and must not be empty).
    output_run : `str`, optional
        Output `~lsst.daf.butler.CollectionType.RUN` collection.  If not
        provided, ``butler.run`` is used (and must not be `None`).
    skip_existing_in : `~collections.abc.Sequence` [ `str` ], optional
        Collections to search for outputs that already exist for the purpose of
        skipping quanta that have already been run.
    clobber : `bool`, optional
        Whether to raise if predicted outputs already exist in ``output_run``
        (not including those of quanta that would be skipped because they've
        already been run).  This never actually clobbers outputs; it just
        informs the graph generation algorithm whether execution will run with
        clobbering enabled.  This is ignored if ``output_run`` does not exist.

    Notes
    -----
    Constructing a `QuantumGraphBuilder` will run queries for existing datasets
    with empty data IDs (including but not limited to init inputs and outputs),
    in addition to resolving the given pipeline graph and testing for existence
    of the ``output`` run collection.

    The `build` method splits the pipeline graph into independent subgraphs,
    then calls the abstract method hook (`process_subgraph`) on each, to allow
    concrete implementations to populate the rough graph structure (the
    `QuantumGraphSkeleton` class) and search for existing datasets (further
    populating the builder's `existing_datasets` struct).  The `build` method
    then:

    - assembles `lsst.daf.butler.Quantum` instances from all data IDs in the
      skeleton;
    - looks for existing outputs found in ``skip_existing_in`` to see if any
      quanta should be skipped;
    - calls `PipelineTaskConnections.adjustQuantum` on all quanta, adjusting
      downstream quanta appropriately when preliminary predicted outputs are
      rejected (pruning nodes that will not have the inputs they need to run);
    - attaches datastore records and registry dataset types to the graph.

    In addition to implementing `process_subgraph`, derived classes are
    generally expected to add new construction keyword-only arguments to
    control the data IDs of the quantum graph, while forwarding all of the
    arguments defined in the base class to `super`.
    """

    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        butler: Butler,
        *,
        input_collections: Sequence[str] | None = None,
        output_run: str | None = None,
        skip_existing_in: Sequence[str] = (),
        clobber: bool = False,
    ):
        self._pipeline_graph = pipeline_graph
        self.butler = butler
        self._pipeline_graph.resolve(self.butler.registry)
        if input_collections is None:
            input_collections = butler.collections
        if not input_collections:
            raise ValueError("No input collections provided.")
        self.input_collections = input_collections
        if output_run is None:
            output_run = butler.run
        if not output_run:
            raise ValueError("No output RUN collection provided.")
        self.output_run = output_run
        self.skip_existing_in = skip_existing_in
        self.empty_data_id = DataCoordinate.makeEmpty(butler.dimensions)
        self.clobber = clobber
        # See whether the output run already exists.
        self.output_run_exists = False
        try:
            if self.butler.registry.getCollectionType(self.output_run) is not CollectionType.RUN:
                raise RuntimeError(f"{self.output_run!r} is not a RUN collection.")
            self.output_run_exists = True
        except MissingCollectionError:
            # If the run doesn't exist we never need to clobber.  This is not
            # an error so you can run with clobber=True the first time you
            # attempt some processing as well as all subsequent times, instead
            # of forcing the user to make the first attempt different.
            self.clobber = False
        # We need to know whether the skip_existing_in collection sequence
        # starts with the output run collection, as an optimization to avoid
        # queries later.
        if self.skip_existing_in and self.output_run_exists:
            first, *_ = self.butler.registry.queryCollections(self.skip_existing_in, flattenChains=True)
            self.skip_existing_starts_with_output_run = self.output_run == first
        else:
            self.skip_existing_starts_with_output_run = False
        self.existing_datasets = ExistingDatasets()
        try:
            packages_storage_class = butler.registry.getDatasetType(
                acc.PACKAGES_INIT_OUTPUT_NAME
            ).storageClass_name
        except MissingDatasetTypeError:
            packages_storage_class = acc.PACKAGES_INIT_OUTPUT_STORAGE_CLASS
        global_init_output_types = {
            acc.PACKAGES_INIT_OUTPUT_NAME: DatasetType(
                acc.PACKAGES_INIT_OUTPUT_NAME,
                self.universe.empty,
                packages_storage_class,
            )
        }
        self._find_empty_dimension_datasets(global_init_output_types)
        self._init_info = _InitInfo()
        self._init_info.populate(self, global_init_output_types)

    butler: Butler
    """Client for the data repository.

    Should be read-only.
    """

    input_collections: Sequence[str]
    """Collections to search for overall-input datasets.
    """

    output_run: str
    """Output `~lsst.daf.butler.CollectionType.RUN` collection.
    """

    skip_existing_in: Sequence[str]
    """Collections to search for outputs that already exist for the purpose
    of skipping quanta that have already been run.
    """

    clobber: bool
    """Whether to raise if predicted outputs already exist in ``output_run``

    This never actually clobbers outputs; it just informs the graph generation
    algorithm whether execution will run with clobbering enabled.  This is
    always `False` if `output_run_exists` is `False`.
    """

    empty_data_id: DataCoordinate
    """An empty data ID in the data repository's dimension universe.
    """

    output_run_exists: bool
    """Whether the output run exists in the data repository already.
    """

    skip_existing_starts_with_output_run: bool
    """Whether the `skip_existing_in` sequence begins with `output_run`.

    If this is true, any dataset found in `output_run` can be used to
    short-circuit queries in `skip_existing_in`.
    """

    existing_datasets: ExistingDatasets
    """Struct holding datasets that have already been found in the data
    repository.

    This is updated in-place as the `QuantumGraph` generation algorithm
    proceeds.
    """

    @property
    def universe(self) -> DimensionUniverse:
        """Definitions of all data dimensions."""
        return self.butler.dimensions

    @final
    def build(self, metadata: Mapping[str, Any] | None = None) -> QuantumGraph:
        """Build the quantum graph.

        Parameters
        ----------
        metadata : `~collections.abc.Mapping`, optional
            Flexible metadata to add to the quantum graph.

        Returns
        -------
        quantum_graph : `QuantumGraph`
            DAG describing processing to be performed.

        Notes
        -----
        External code is expected to construct a `QuantumGraphBuilder` and then
        call this method exactly once.  See class documentation for details on
        what it does.
        """
        full_skeleton = QuantumGraphSkeleton()
        subgraphs = list(self._pipeline_graph.split_independent())
        for i, subgraph in enumerate(subgraphs):
            _LOG.info(
                "Processing pipeline subgraph %d of %d with tasks %s.",
                i,
                len(subgraphs),
                ", ".join(str(t) for t in subgraph.tasks.keys()),
            )
            subgraph_skeleton = self.process_subgraph(subgraph)
            full_skeleton.quanta.update(subgraph_skeleton.quanta)
            full_skeleton.xgraph.update(subgraph_skeleton.xgraph)
        # Loop over tasks.  The pipeline graph must be topologically sorted,
        # so a quantum is only processed after any quantum that provides its
        # inputs has been processed.
        for task_node in self._pipeline_graph.tasks.values():
            self._resolve_task_quanta(task_node, full_skeleton)
        # Remove nodes with no edges, which could include:
        # - potential input datasets that are no longer being consumed by
        #   anything.
        # - init input/output datasets that aren't used in the runtime graph.
        full_skeleton.xgraph.remove_nodes_from(list(networkx.isolates(full_skeleton.xgraph)))
        self._attach_datastore_records(full_skeleton)
        # TODO initialize most metadata here instead of in ctrl_mpexec.
        if metadata is None:
            metadata = {}
        return self._construct_quantum_graph(full_skeleton, metadata)

    @abstractmethod
    def process_subgraph(self, subgraph: PipelineGraph) -> QuantumGraphSkeleton:
        """Build the rough structure for an independent subset of the
        `QuantumGraph` and query for relevant existing datasets.

        Parameters
        ----------
        subgraph : `.pipeline_graph.PipelineGraph`
            Subset of the pipeline graph that should be processed by this call.
            This is always resolved and topologically sorted.  It should not be
            modified.

        Returns
        -------
        skeleton : `QuantumGraphSkeleton`
            Simple data structure representing an initial quantum graph. See
            `QuantumGraphSkeleton` docs for details.  After this is returned,
            the object may be modified in-place in unspecified ways.

        Notes
        -----
        In addition to returning a `QuantumGraphSkeleton`, this method should
        populate the `existing_datasets` structure by querying for all relevant
        datasets with non-empty data IDs (those with empty data IDs will
        already be present).  In particular:

        - `~ExistingDatasets.inputs` must always be populated with all
          overall-input datasets (but not prerequisites), by querying
          `input_collections`;
        - `~ExistingDatasets.outputs_for_skip` must be populated with any
          intermediate our output datasets present in `skip_existing_in` (it
          can be ignored if `skip_existing_in` is empty);
        - `~ExistingDatasets.outputs_in_the_way` must be populated with any
          intermediate or ouptut datasets present in `output_run`, if
          `output_run_exists` (it can be ignored if `output_run_exists` is
          `False`).  Note that the presence of such datasets is not
          automatically an error, even if `clobber is `False`, as these may be
          quanta that will be skipped.

        Dataset types should never be components and should always use the
        "common" storage class definition in `pipeline_graph.DatasetTypeNode`
        (which is the data repository definition when the dataset type is
        registered).
        """
        raise NotImplementedError()

    def compute_skypix_bounds(
        self, dimension: SkyPixDimension, task_node: TaskNode, skeleton: QuantumGraphSkeleton
    ) -> _SkyPixBounds:
        result = _SkyPixBounds(dimension)
        spatial_bounds_connections = task_node.get_spatial_bounds_connections()
        if not spatial_bounds_connections:
            if task_node.dimensions.spatial:
                # Simple, usual case: task data ID is spatial and is all that's
                # needed.
                for quantum_key in skeleton.quanta[task_node.label]:
                    result.add_quantum_data_id(quantum_key.data_id)
            else:
                # Simple but possibly problematic case: task data ID is not
                # spatial, but it's all we've got.
                _LOG.warning(
                    "Task %r has no spatial bounds connections and does not have spatial dimensions, so "
                    "lookups of prerequisite input datasets with %s data IDs will be limited only by the "
                    "input collection contents.",
                    task_node.label,
                    dimension.name,
                )
                for quantum_key in skeleton.quanta[task_node.label]:
                    result.add_unbounded(quantum_key.data_id)
            return result
        # Advanced case, where the task has provided additional connection
        # names that should be used to compute the spatial bounds. We start by
        # converting from a set of connection names to a set of parent dataset
        # type names.
        spatial_bounds_inputs = {
            task_node.inputs[connection_name].parent_dataset_type_name
            for connection_name in task_node.inputs.keys() & spatial_bounds_connections
        }
        spatial_bounds_outputs = {
            task_node.inputs[connection_name].parent_dataset_type_name
            for connection_name in task_node.outputs.keys() & spatial_bounds_connections
        }
        for quantum_key in skeleton.quanta[task_node.label]:
            related_data_ids = set()
            dataset_key: DatasetKey
            if spatial_bounds_inputs:
                for dataset_key in skeleton.xgraph.predecessors(quantum_key):
                    if dataset_key.parent_dataset_type_name in spatial_bounds_inputs:
                        related_data_ids.add(dataset_key.data_id)
            if spatial_bounds_outputs:
                for dataset_key in skeleton.xgraph.successors(quantum_key):
                    if dataset_key.parent_dataset_type_name in spatial_bounds_outputs:
                        related_data_ids.add(dataset_key.data_id)
            result.add_quantum_data_id(quantum_key.data_id, related_data_ids)
        return result

    @final
    def _resolve_task_quanta(self, task_node: TaskNode, skeleton: QuantumGraphSkeleton) -> None:
        """Process the quanta for one task in a skeleton graph to skip those
        that have already completed and adjust those that request it.

        Parameters
        ----------
        task_node : `pipeline_graph.TaskNode`
            Node for this task in the pipeline graph.
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph, to be modified in-place.

        Notes
        -----
        This method modifies ``skeleton`` in-place in several ways:

        - It adds a "ref" attribute to dataset nodes, using the contents of
          `existing_datasets`.  This ensures producing and consuming tasks
          start from the same `DatasetRef`.
        - It adds "inputs", "outputs", and "init_inputs" attributes to the
          quantum nodes, holding the same `NamedValueMapping` objects needed to
          construct an actual `Quantum` instances.
        - It removes quantum nodes that are to skipped because their outputs
          already exist in `skip_existing_in`.  It also removes their outputs
          from `ExistingDatasets.outputs_in_the_way`.
        - It adds prerequisite dataset nodes and edges that connect them to the
          quanta that consume them.
        - It removes quantum nodes whose
          `~PipelineTaskConnections.adjustQuantum` calls raise `NoWorkFound` or
          predict no outputs;
        - It removes the nodes of output datasets that are "adjusted away".
        - It removes the edges of input datasets that are "adjusted away".

        The difference between how adjusted inputs and outputs are handled
        reflects the fact that many quanta can share the same input, but only
        one produces each output.  This can lead to the graph having
        superfluous isolated nodes after processing is complete, but these
        should only be removed after all the quanta from all tasks have been
        processed.
        """
        # Gather the init inputs for this task, since we'll need the same
        # ones for each each quantum.
        task_init_info = self._init_info.tasks[task_node.label]
        # Add init-input nodes (with refs) to the runtime graph,
        # since we need to read those when executing quanta even though
        # we don't write the init-outputs then.  If multiple tasks share
        # an init_input we'll attempt to add that node twice, but that's
        # fine since networkx will deduplicate on add.
        for init_dataset_key, init_ref in task_init_info.inputs.items():
            skeleton.xgraph.add_node(init_dataset_key, ref=init_ref)
        # Prepare finder objects for finding prerequisite inputs.  This
        # can include executing queries to begin that search, depending on the
        # finder that's matched.
        prereq_finders: dict[str, _PrerequisiteFinder] = {}
        skypix_bounds: dict[str, _SkyPixBounds] = {}
        for read_edge in task_node.prerequisite_inputs.values():
            for cls in _PrerequisiteFinder.implementations:
                if matched := cls.match(self._pipeline_graph, read_edge):
                    for skypix_dimension in matched.get_needed_skypix_bounds():
                        if skypix_dimension.name not in skypix_bounds:
                            skypix_bounds[skypix_dimension.name] = self.compute_skypix_bounds(
                                skypix_dimension, task_node, skeleton
                            )
                    if matched.needs_temporal_bounds():
                        raise NotImplementedError("TODO")
                    matched.begin(
                        self.butler,
                        self.input_collections,
                        (quantum_key.data_id for quantum_key in skeleton.quanta[task_node.label]),
                        skypix_bounds,
                    )
                    prereq_finders[read_edge.connection_name] = matched
        # Loop over all quanta for this task, remembering the ones we've
        # gotten rid of.
        skipped_quanta = []
        no_work_quanta = []
        for quantum_key in skeleton.quanta[task_node.label]:
            if self._skip_quantum_if_metadata_exists(task_node, quantum_key, skeleton):
                skipped_quanta.append(quantum_key)
                continue
            # Give the task's Connections class an opportunity to remove
            # some inputs, or complain if they are unacceptable.  This will
            # raise if one of the check conditions is not met, which is the
            # intended behavior.
            helper = AdjustQuantumHelper(
                inputs=self._gather_quantum_inputs(task_node, quantum_key, skeleton, prereq_finders),
                outputs=self._gather_quantum_outputs(task_node, quantum_key, skeleton),
            )
            try:
                helper.adjust_in_place(
                    task_node._get_imported_data().connections, task_node.label, quantum_key.data_id
                )
            except NoWorkFound:
                # Do not generate this quantum; it would not produce any
                # outputs.  Remove it and all of the outputs it might have
                # produced from the skeleton.
                no_work_quanta.append(quantum_key)
                skeleton.xgraph.remove_nodes_from(list(skeleton.xgraph.successors(quantum_key)))
                skeleton.xgraph.remove_node(quantum_key)
                continue
            if helper.outputs_adjusted:
                if not any(adjusted_refs for adjusted_refs in helper.outputs.values()):
                    # No outputs also means we don't generate this quantum.
                    no_work_quanta.append(quantum_key)
                    skeleton.xgraph.remove_nodes_from(list(skeleton.xgraph.successors(quantum_key)))
                    skeleton.xgraph.remove_node(quantum_key)
                    continue
                # Remove output nodes that were not retained by
                # adjustQuantum.
                skeleton.xgraph.remove_nodes_from(
                    self._find_removed(skeleton.xgraph.successors(quantum_key), helper.outputs)
                )
            if helper.inputs_adjusted:
                if not any(bool(adjusted_refs) for adjusted_refs in helper.inputs.values()):
                    raise RuntimeError(
                        f"adjustQuantum implementation for {task_node.label}@{quantum_key.data_id} "
                        "returned outputs but no inputs."
                    )
                # Remove input dataset edges that were not retained by
                # adjustQuantum.  We can't remove the input dataset nodes
                # because some other quantum might still want them.
                for dataset_key in self._find_removed(
                    skeleton.xgraph.predecessors(quantum_key), helper.inputs
                ):
                    skeleton.xgraph.remove_edge(dataset_key, quantum_key)
            # Save the adjusted inputs and outputs to the quantum node's
            # state so we don't have to regenerate those data structures
            # from the graph.
            skeleton.xgraph.nodes[quantum_key]["inputs"] = helper.inputs
            skeleton.xgraph.nodes[quantum_key]["outputs"] = helper.outputs
            # Also save the quantum-adapted init inputs, and add edges
            # connecting the init inputs to this quantum.
            skeleton.xgraph.nodes[quantum_key]["init_inputs"] = task_init_info.adapted_inputs
            skeleton.xgraph.add_edges_from(
                [(init_dataset_key, quantum_key) for init_dataset_key in task_init_info.inputs.keys()]
            )
        skeleton.quanta[task_node.label].difference_update(no_work_quanta)
        skeleton.quanta[task_node.label].difference_update(skipped_quanta)
        _LOG.info(
            "Generated %d quanta for task %s, skipping %d due to existing outputs "
            "and %d due to adjustQuantum pruning.",
            len(skeleton.quanta[task_node.label]),
            task_node.label,
            len(skipped_quanta),
            len(no_work_quanta),
        )

    @final
    def _skip_quantum_if_metadata_exists(
        self, task_node: TaskNode, quantum_key: QuantumKey, skeleton: QuantumGraphSkeleton
    ) -> bool:
        """Identify and drop quanta that should be skipped because their
        metadata datasets already exist.

        Parameters
        ----------
        task_node : `pipeline_graph.TaskNode`
            Node for this task in the pipeline graph.
        quantum_key : `QuantumKey`
            Identifier for this quantum in the graph.
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph, to be modified in-place.

        Returns
        -------
        skipped : `bool`
            `True` if the quantum is being skipped and has been removed from
            the graph, `False` otherwise.

        Notes
        -----
        If the metadata dataset for this quantum exists in
        `ExistingDatasets.outputs_for_skip`, the quantum will be skipped.
        This causes the quantum node to be removed from the graph.  Dataset
        nodes that were previously the outputs of this quantum will have their
        "ref"` output set from `ExistingDatasets.outputs_for_skip`, or will be
        removed if there is no such dataset there.  Any output dataset in
        `ExistingDatasets.outputs_in_the_way` will be removed.
        """
        metadata_dataset_key = DatasetKey(
            task_node.metadata_output.parent_dataset_type_name, quantum_key.data_id
        )
        if metadata_dataset_key in self.existing_datasets.outputs_for_skip:
            # This quantum's metadata is already present in the the
            # skip_existing_in collections; we'll skip it.  But the presence of
            # the metadata dataset doesn't guarantee that all of the other
            # outputs we predicted are present; we have to check.
            for output_dataset_key in list(skeleton.xgraph.successors(quantum_key)):
                if (
                    output_ref := self.existing_datasets.outputs_for_skip.get(output_dataset_key)
                ) is not None:
                    # Populate the skeleton graph's node attributes
                    # with the existing DatasetRef, just like a
                    # predicted output of a non-skipped quantum.
                    skeleton.xgraph.nodes[output_dataset_key]["ref"] = output_ref
                else:
                    # Remove this dataset from the skeleton graph,
                    # because the quantum that would have produced it
                    # is being skipped and it doesn't already exist.
                    skeleton.xgraph.remove_node(output_dataset_key)
                # If this dataset was "in the way" (i.e. already in the
                # output run), it isn't anymore.
                self.existing_datasets.outputs_in_the_way.pop(output_dataset_key, None)
            # Remove the quantum from the skeleton graph.
            # We dont'
            skeleton.xgraph.remove_node(quantum_key)
            return True
        return False

    @final
    def _gather_quantum_inputs(
        self,
        task_node: TaskNode,
        quantum_key: QuantumKey,
        skeleton: QuantumGraphSkeleton,
        prereq_finders: Mapping[str, _PrerequisiteFinder],
    ) -> NamedKeyDict[DatasetType, list[DatasetRef]]:
        """Collect input datasets for a preliminary quantum and put them in the
        form used by `~lsst.daf.butler.Quantum` and
        `~PipelineTaskConnections.adjustQuantum`.

        Parameters
        ----------
        task_node : `pipeline_graph.TaskNode`
            Node for this task in the pipeline graph.
        quantum_key : `QuantumKey`
            Identifier for this quantum in the graph.
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph, to be modified in-place.
        prereq_finders : `~collections.abc.Mapping` [ `str`, \
                `_PrerequisiteFinder` ]
            Helper objects for finding prerequisites.

        Returns
        -------
        inputs : `~lsst.daf.butler.NamedKeyDict` [ \
                `~lsst.daf.butler.DatasetType`, `list` [
                `~lsst.daf.butler.DatasetRef` ] ]
            All regular and prerequisite inputs to the task, using the storage
            class and components defined by the task's own connections.

        Notes
        -----
        On return, the dataset nodes that represent inputs to this quantum will
        either have their "ref" attribute set (using the common dataset type,
        not the task-specific one) or will be removed from the graph.

        For regular inputs, usually an existing "ref" (corresponding to an
        output of another quantum) will be found and left unchanged.  When
        there is no existing "ref" attribute, `ExistingDatasets.inputs` is
        searched next; if there is nothing there, the input will be removed.

        Prerequisite inputs are always queried for directly here (delegating to
        `_find_prerequisite_inputs`).  They are never produced by other tasks,
        and cannot in general be queried for in advance when
        `ExistingDatasets.inputs` is populated.
        """
        inputs_by_type: dict[str, list[DatasetRef]] = {}
        dataset_key: DatasetKey
        for dataset_key in list(skeleton.xgraph.predecessors(quantum_key)):
            if (ref := skeleton.xgraph.nodes[dataset_key].get("ref")) is None:
                # This dataset is an overall input - if it was an intermediate,
                # we would have already either removed the node or set the
                # "ref" attribute when processing its producing quantum - and
                # this is the first time we're trying to resolve it.
                if (ref := self.existing_datasets.inputs.get(dataset_key)) is None:
                    # It also doesn't exist in the input
                    # collections, so we remove its node in the
                    # skeleton graph (so other consumers won't
                    # have to check for it).
                    skeleton.xgraph.remove_node(dataset_key)
                    continue
                skeleton.xgraph.nodes[dataset_key]["ref"] = ref
            inputs_by_type.setdefault(dataset_key.parent_dataset_type_name, []).append(ref)
        adapted_inputs: NamedKeyDict[DatasetType, list[DatasetRef]] = NamedKeyDict()
        for read_edge in task_node.inputs.values():
            dataset_type_node = self._pipeline_graph.dataset_types[read_edge.parent_dataset_type_name]
            edge_dataset_type = read_edge.adapt_dataset_type(dataset_type_node.dataset_type)
            if (current_dataset_type := adapted_inputs.keys().get(edge_dataset_type.name)) is None:
                adapted_inputs[edge_dataset_type] = [
                    read_edge.adapt_dataset_ref(ref)
                    for ref in inputs_by_type.get(read_edge.parent_dataset_type_name, [])
                ]
            elif current_dataset_type != edge_dataset_type:
                raise NotImplementedError(
                    f"Task {task_node.label!r} has {edge_dataset_type.name!r} as an input via "
                    "two different connections, with two different storage class overrides. "
                    "This is not yet supported due to limitations in the Quantum data structure."
                )
            # If neither the `if` nor the `elif` above match, it means
            # multiple input connections have exactly the same dataset
            # type, and hence nothing to do after the first one.
        # Query for prerequisites, since we need to have those in order
        # to check whether the quantum has work to do, and we have to
        # do that quantum-by-quantum.  Note that these were not already
        # in the skeleton graph, so we add them now.
        for read_edge in task_node.prerequisite_inputs.values():
            dataset_type_node = self._pipeline_graph.dataset_types[read_edge.parent_dataset_type_name]
            prerequisite_refs = prereq_finders[read_edge.connection_name].finish(
                self.butler, self.input_collections, quantum_key.data_id
            )
            prerequisite_node_data = {
                _PrerequisiteDatasetKey(ref.datasetType.name, ref.id): {"ref": ref}
                for ref in prerequisite_refs
            }
            skeleton.xgraph.add_nodes_from(prerequisite_node_data.items())
            skeleton.xgraph.add_edges_from(
                (dataset_key, quantum_key) for dataset_key in prerequisite_node_data
            )
            adapted_inputs[read_edge.adapt_dataset_type(dataset_type_node.dataset_type)] = [
                read_edge.adapt_dataset_ref(ref) for ref in prerequisite_refs
            ]
        return adapted_inputs

    @final
    def _gather_quantum_outputs(
        self, task_node: TaskNode, quantum_key: QuantumKey, skeleton: QuantumGraphSkeleton
    ) -> NamedKeyDict[DatasetType, list[DatasetRef]]:
        """Collect outputs or generate datasets for a preliminary quantum and
        put them in the form used by `~lsst.daf.butler.Quantum` and
        `~PipelineTaskConnections.adjustQuantum`.

        Parameters
        ----------
        task_node : `pipeline_graph.TaskNode`
            Node for this task in the pipeline graph.
        quantum_key : `QuantumKey`
            Identifier for this quantum in the graph.
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph, to be modified in-place.

        Returns
        -------
        outputs : `~lsst.daf.butler.NamedKeyDict` [ \
                `~lsst.daf.butler.DatasetType`, `list` [
                `~lsst.daf.butler.DatasetRef` ] ]
            All outputs to the task, using the storage class and components
            defined by the task's own connections.

        Notes
        -----
        This first looks for outputs already present in the `output_run` by
        looking in `ExistingDatasets.outputs_in_the_way`; if it finds something
        and `clobber` is `True`, it uses that ref (it's not ideal that both the
        original dataset and its replacement will have the same UUID, but we
        don't have space in the quantum graph for two UUIDs, and we need the
        datastore records of the original there).  If `clobber` is `False`,
        `RuntimeError` is raised.  If there is no output already present,
        a new one with a random UUID is generated.  In all cases the "ref"
        attribute of the dataset node in the skeleton is set.
        """
        outputs_by_type: dict[str, list[DatasetRef]] = {}
        dataset_key: DatasetKey
        for dataset_key in skeleton.xgraph.successors(quantum_key):
            dataset_type_node = self._pipeline_graph.dataset_types[dataset_key.parent_dataset_type_name]
            if (ref := self.existing_datasets.outputs_in_the_way.get(dataset_key)) is None:
                ref = DatasetRef(dataset_type_node.dataset_type, dataset_key.data_id, run=self.output_run)
            elif not self.clobber:
                # We intentionally raise here, before running
                # adjustQuantum, because it'd be weird if we left
                # an old potential output of a task sitting there
                # in the output collection, just because the task
                # happened not to not actually produce it.
                raise RuntimeError(
                    f"Potential output dataset {ref} already exists in the output run "
                    f"{self.output_run}, but clobbering outputs was not expected to be necessary."
                )
            skeleton.xgraph.nodes[dataset_key]["ref"] = ref
            outputs_by_type.setdefault(dataset_key.parent_dataset_type_name, []).append(ref)
        adapted_outputs: NamedKeyDict[DatasetType, list[DatasetRef]] = NamedKeyDict()
        for write_edge in task_node.iter_all_outputs():
            dataset_type_node = self._pipeline_graph.dataset_types[write_edge.parent_dataset_type_name]
            edge_dataset_type = write_edge.adapt_dataset_type(dataset_type_node.dataset_type)
            adapted_outputs[edge_dataset_type] = [
                write_edge.adapt_dataset_ref(ref)
                for ref in outputs_by_type.get(write_edge.parent_dataset_type_name, [])
            ]
        return adapted_outputs

    @final
    def _find_empty_dimension_datasets(self, global_init_outputs: dict[str, DatasetType]) -> None:
        """Query for all dataset types with no dimensions, updating
        `existing_datasets` in-place.

        This includes but is not limited to init inputs and init outputs.
        """
        _, dataset_type_nodes = self._pipeline_graph.group_by_dimensions()[self.universe.empty]
        dataset_types = [node.dataset_type for node in dataset_type_nodes.values()]
        dataset_types.extend(global_init_outputs.values())
        for dataset_type in dataset_types:
            key = DatasetKey(dataset_type.name, self.empty_data_id)
            if self._pipeline_graph.producer_of(dataset_type.name) is None:
                # Dataset type is an overall input; we always need to try to
                # find these.
                try:
                    ref = self.butler.registry.findDataset(
                        dataset_type.name, collections=self.input_collections
                    )
                except MissingDatasetTypeError:
                    ref = None
                if ref is not None:
                    self.existing_datasets.inputs[key] = ref
            elif self.skip_existing_in:
                # Dataset type is an intermediate or output; need to find these
                # if only they're from previously executed quanta that we might
                # skip...
                try:
                    ref = self.butler.registry.findDataset(
                        dataset_type.name, collections=self.skip_existing_in
                    )
                except MissingDatasetTypeError:
                    ref = None
                if ref is not None:
                    self.existing_datasets.outputs_for_skip[key] = ref
                    if ref.run == self.output_run:
                        self.existing_datasets.outputs_in_the_way[key] = ref
            if self.output_run_exists and not self.skip_existing_starts_with_output_run:
                # ...or if they're in the way and would need to be clobbered
                # (and we haven't already found them in the previous block).
                try:
                    ref = self.butler.registry.findDataset(dataset_type.name, collections=[self.output_run])
                except MissingDatasetTypeError:
                    ref = None
                if ref is not None:
                    self.existing_datasets.outputs_in_the_way[key] = ref

    @final
    def _attach_datastore_records(self, skeleton: QuantumGraphSkeleton) -> None:
        """Add datastore records for all overall inputs to a preliminary
        quantum graph.

        Parameters
        ----------
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph to update in place.

        Notes
        -----
        On return, all quantum nodes in the skeleton graph will have a
        "datastore_records" attribute that is a mapping from datastore name
        to `lsst.daf.butler.DatastoreRecordData`, as used by
        `lsst.daf.butler.Quantum`.
        """
        overall_inputs = self._extract_overall_inputs(skeleton)
        exported_records = self.butler._datastore.export_records(overall_inputs.values())
        for quantum_key in itertools.chain.from_iterable(skeleton.quanta.values()):
            quantum_records = {}
            input_ids = {
                ref.id
                for dataset_key in skeleton.xgraph.predecessors(quantum_key)
                if (ref := overall_inputs.get(dataset_key)) is not None
            }
            if input_ids:
                for datastore_name, records in exported_records.items():
                    matching_records = records.subset(input_ids)
                    if matching_records is not None:
                        quantum_records[datastore_name] = matching_records
            skeleton.xgraph.nodes[quantum_key]["datastore_records"] = quantum_records

    @final
    def _construct_quantum_graph(
        self, skeleton: QuantumGraphSkeleton, metadata: Mapping[str, Any]
    ) -> QuantumGraph:
        """Construct a `QuantumGraph` object from the contents of a
        fully-processed `QuantumGraphSkeleton`.

        Parameters
        ----------
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph.  Must have "init_inputs", "inputs", and
            "outputs" attributes on all quantum nodes, as added by
            `_resolve_task_quanta`, as well as a "datastore_records" attribute
            as added by `_attach_datastore_records`.
        metadata : `Mapping`
            Flexible metadata to add to the graph.

        Returns
        -------
        quantum_graph : `QuantumGraph`
            DAG describing processing to be performed.
        """
        quanta: dict[TaskDef, set[Quantum]] = {}
        init_inputs: dict[TaskDef, Iterable[DatasetRef]] = {}
        init_outputs: dict[TaskDef, Iterable[DatasetRef]] = {}
        for task_def in self._pipeline_graph._iter_task_defs():
            task_node = self._pipeline_graph.tasks[task_def.label]
            init_inputs[task_def] = self._init_info.tasks[task_node.label].adapted_inputs.values()
            init_outputs[task_def] = self._init_info.tasks[task_node.label].adapted_outputs.values()
            quanta_for_task: set[Quantum] = set()
            for quantum_key in skeleton.quanta.get(task_node.label, []):
                node_state = skeleton.xgraph.nodes[quantum_key]
                quanta_for_task.add(
                    Quantum(
                        taskName=task_node.task_class_name,
                        taskClass=task_node.task_class,
                        dataId=quantum_key.data_id,
                        initInputs=node_state["init_inputs"],
                        inputs=node_state["inputs"],
                        outputs=node_state["outputs"],
                        datastore_records=node_state.get("datastore_records"),
                    )
                )
            quanta[task_def] = quanta_for_task
        registry_dataset_types: list[DatasetType] = [
            node.dataset_type for node in self._pipeline_graph.dataset_types.values()
        ]
        return QuantumGraph(
            quanta,
            metadata=metadata,
            universe=self.universe,
            initInputs=init_inputs,
            initOutputs=init_outputs,
            globalInitOutputs=self._init_info.global_outputs,
            registryDatasetTypes=registry_dataset_types,
        )

    @staticmethod
    @final
    def _find_removed(
        original: Iterable[DatasetKey | _PrerequisiteDatasetKey],
        adjusted: NamedKeyMapping[DatasetType, Sequence[DatasetRef]],
    ) -> set[DatasetKey | _PrerequisiteDatasetKey]:
        """Identify skeleton-graph dataset nodes that have been removed by
        `~PipelineTaskConnections.adjustQuantum`.

        Parameters
        ----------
        original : `~collections.abc.Iterable` [ `DatasetKey` or \
                `PrerequisiteDatasetKey` ]
            Identifiers for the dataset nodes that were the original neighbors
            (inputs or outputs) of a quantum.
        adjusted : `~lsst.daf.butler.NamedKeyMapping` [ \
                `~lsst.daf.butler.DatasetType`, \
                `~collections.abc.Sequence` [ `lsst.daf.butler.DatasetType` ] ]
            Adjusted neighbors, in the form used by `lsst.daf.butler.Quantum`.

        Returns
        -------
        removed : `set` [ `DatasetKey` ]
            Datasets in ``original`` that have no counterpart in ``adjusted``.
        """
        result = set(original)
        for dataset_type, kept_refs in adjusted.items():
            parent_dataset_type_name, _ = DatasetType.splitDatasetTypeName(dataset_type.name)
            for kept_ref in kept_refs:
                result.remove(DatasetKey(parent_dataset_type_name, kept_ref.dataId))
        return result

    @staticmethod
    @final
    def _extract_overall_inputs(
        skeleton: QuantumGraphSkeleton,
    ) -> dict[DatasetKey | _PrerequisiteDatasetKey, DatasetRef]:
        """Find overall input datasets in a preliminary quantum graph.

        Parameters
        ----------
        skeleton : `QuantumGraphSkeleton`
            Preliminary quantum graph.  Dataset nodes must have a "ref"
            attribute.

        Returns
        -------
        datasets : `dict` [ `DatasetKey` or `PrerequisiteDatasetKey`,
                `~lsst.daf.butler.DatasetRef` ]
            Overall-input datasets.
        """
        result = {}
        for generation in networkx.algorithms.topological_generations(skeleton.xgraph):
            for dataset_key in generation:
                assert (
                    type(dataset_key) is not QuantumKey
                ), "Should be impossible to have a quantum in the first topological generation."
                result[dataset_key] = skeleton.xgraph.nodes[dataset_key]["ref"]
            break
        return result


class QuantumKey(NamedTuple):
    """Identifier type for quantum keys in a `QuantumGraphSkeleton`."""

    task_label: str
    """Label of the task in the pipeline."""

    data_id: DataCoordinate
    """Data ID of the quantum."""


class DatasetKey(NamedTuple):
    """Identifier type for dataset keys in a `QuantumGraphSkeleton`."""

    parent_dataset_type_name: str
    """Name of the dataset type (never a component)."""

    data_id: DataCoordinate
    """Data ID for the dataset."""


class _PrerequisiteDatasetKey(NamedTuple):
    """Identifier type for prerequisite dataset keys in a
    `QuantumGraphSkeleton`.

    Unlike regular datasets, prerequisites are not actually required to come
    from a find-first search of `input_collections`, so we don't want to
    assume that the same data ID implies the same dataset.  Happily we also
    don't need to search for them by data ID in the graph, so we can use the
    dataset ID (UUID) instead.
    """

    parent_dataset_type_name: str
    """Name of the dataset type (never a component)."""

    dataset_id: DatasetId
    """Dataset ID (UUID)."""


@dataclasses.dataclass(eq=False, order=False)
class QuantumGraphSkeleton:
    """Struct representing a quantum graph under construction."""

    quanta: dict[str, set[QuantumKey]] = dataclasses.field(default_factory=dict)
    """All nodes in `xgraph` that represent quanta, grouped by task label.
    """

    xgraph: networkx.DiGraph = dataclasses.field(default_factory=networkx.DiGraph)
    """A bipartite graph with `DatasetKey` and `QuantumKey` nodes that captures
    the structure of an under-construction quantum graph.

    A bipartite graph is one in which there are two kinds of nodes, and each
    node has edges only to the other kind.  This form of the quantum graph maps
    each dataset (not just each task) to a unique node, while the task-only
    projection used in `QuantumGraph` associates each dataset with potentially
    many edges, and during construction it is important to be able to operate
    on datasets directly.

    When originally constructed by `QuantumGraphBuilder.process_subgraph`, no
    node or edge attributes should be present; only the graph structure is
    considered, and no nodes for prerequisite inputs should be present.  After
    this, `QuantumGraphBuilder.build` will make further changes (including
    adding node attributes); these are considered implementation details, but
    are documented for maintainers in the private methods that
    `QuantumGraphBuilder.build` delegates xto.
    """


@dataclasses.dataclass(eq=False, order=False)
class ExistingDatasets:
    """Struct that holds the results of dataset queries for
    `QuantumGraphBuilder`.
    """

    inputs: dict[DatasetKey, DatasetRef] = dataclasses.field(default_factory=dict)
    """Overall-input datasets found in `QuantumGraphBuilder.input_collections`.

    This does not include prerequisite inputs or intermediates.  It does
    include init-inputs.
    """

    outputs_for_skip: dict[DatasetKey, DatasetRef] = dataclasses.field(default_factory=dict)
    """Output datasets found in `QuantumGraphBuilder.skip_existing_in.

    It is unspecified whether this contains include init-outputs; there is
    no concept of skipping at the init stage, so this is not expected to
    matter.
    """

    outputs_in_the_way: dict[DatasetKey, DatasetRef] = dataclasses.field(default_factory=dict)
    """Output datasets found in `QuantumGraphBuilder.output_run`.

    This includes regular outputs and init-outputs.
    """


@dataclasses.dataclass(eq=False, order=False)
class _InitInfo:
    """Struct that holds information about init-input and init-output
    datasets for a `QuantumGraphBuilder`.
    """

    tasks: dict[str, _TaskInitInfo] = dataclasses.field(default_factory=dict)
    """Nested structs containing information about task init-inputs and
    init-output datasets.

    Keys are task labels.
    """

    global_outputs: list[DatasetRef] = dataclasses.field(default_factory=list)
    """List of pipeline-wide init-outputs (not specific to any task)."""

    overall_inputs: dict[DatasetKey, DatasetRef] = dataclasses.field(default_factory=dict)
    """Init-input datasets that are not also init-output datasets.

    This uses `DatasetKey` as the key type for compatibility with
    `ExistingDatasets` and `QuantumGraphSkeleton`.
    """

    def populate(self, builder: QuantumGraphBuilder, global_output_types: dict[str, DatasetType]) -> None:
        """Populate this struct using the contents of [the rest of] a
        `QuantumGraphBuilder`.

        Parameters
        ----------
        builder : `QuantumGraphBuilder`
            Builder object with `QuantumGraphBuild.existing_datasets` already
            populated with all empty-dimension datasets.
        global_output_types : `dict` [ `str`, `~lsst.daf.butler.DatasetType` ]
            Dataset types for global init-outputs not associated with any
            specific task.
        """
        predicted_outputs: dict[str, DatasetRef] = {}
        for task_node in builder._pipeline_graph.tasks.values():
            task_info = _TaskInitInfo()
            for read_edge in task_node.init.iter_all_inputs():
                task_info.process_read_edge(
                    read_edge,
                    builder,
                    predicted_outputs=predicted_outputs,
                    overall_inputs=self.overall_inputs,
                )
            for write_edge in task_node.init.iter_all_outputs():
                task_info.process_write_edge(write_edge, builder, predicted_outputs)
            self.tasks[task_node.label] = task_info
        # Add global init outputs not associated with any task.  These only
        # go in the init graph, because they're never inputs to quanta.
        for dataset_type in global_output_types.values():
            dataset_key = DatasetKey(dataset_type.name, builder.empty_data_id)
            ref = builder.existing_datasets.outputs_in_the_way.get(dataset_key)
            if ref is None:
                ref = DatasetRef(dataset_type, builder.empty_data_id, run=builder.output_run)
            self.global_outputs.append(ref)


@dataclasses.dataclass(eq=False, order=False)
class _TaskInitInfo:
    """Struct that holds information about init-input and init-output
    datasets for a single task.
    """

    adapted_inputs: NamedKeyDict[DatasetType, DatasetRef] = dataclasses.field(default_factory=NamedKeyDict)
    """Init-input datasets for the task, using the task's specialization of the
    dataset type.
    """

    adapted_outputs: NamedKeyDict[DatasetType, DatasetRef] = dataclasses.field(default_factory=NamedKeyDict)
    """Init-output datasets for the task, using the task's specialization of
    the dataset type.
    """

    inputs: dict[DatasetKey, DatasetRef] = dataclasses.field(default_factory=dict)
    """Init-input datasets for the task, using the common definition of the
    dataset type.

    This uses `DatasetKey` as the key type for compatibility with
    `ExistingDatasets` and `QuantumGraphSkeleton`.
    """

    def process_read_edge(
        self,
        edge: ReadEdge,
        builder: QuantumGraphBuilder,
        predicted_outputs: dict[str, DatasetRef],
        overall_inputs: dict[DatasetKey, DatasetRef],
    ) -> None:
        """Add a dataset to this object by processing a single init-input read
        edge of its task.

        Parameters
        ----------
        edge : `pipeline_graph.ReadEdge`
            Pipeline graph edge describing an init-input connection.
        builder : `QuantumGraphBuilder`
            Builder with `~QuantumGraphBuilder.existing_datasets` populated
            with all empty-dimensions datasets.
        predicted_outputs : `dict` [ `str`, `~lsst.daf.butler.DatasetRef` ]
            Init-outputs that will be produced by other tasks.  Keys are
            parent dataset type names.
        overall_inputs : `dict` [ `DatasetKey`, `~lsst.daf.butler.DatasetRef` ]
            Dictionary of overall init-inputs; modified in-place whenever an
            init-input is not already present in ``predicted_outputs``.
        """
        dataset_key = DatasetKey(edge.parent_dataset_type_name, builder.empty_data_id)
        if (ref := predicted_outputs.get(edge.parent_dataset_type_name)) is None:
            try:
                ref = builder.existing_datasets.inputs[dataset_key]
            except KeyError:
                raise RuntimeError(
                    f"Overall init-input dataset {edge.parent_dataset_type_name!r} "
                    f"not found in input collections {builder.input_collections}."
                ) from None
            overall_inputs[dataset_key] = ref
            predicted_outputs[edge.parent_dataset_type_name] = ref
        self.inputs[dataset_key] = ref
        adapted_ref = edge.adapt_dataset_ref(ref)
        self.adapted_inputs[adapted_ref.datasetType] = adapted_ref

    def process_write_edge(
        self,
        edge: WriteEdge,
        builder: QuantumGraphBuilder,
        predicted_outputs: dict[str, DatasetRef],
    ) -> None:
        """Add a dataset to this object by processing a single init-output
        write edge of its task.

        Parameters
        ----------
        edge : `pipeline_graph.WriteEdge`
            Pipeline graph edge describing an init-output connection.
        builder : `QuantumGraphBuilder`
            Builder with `~QuantumGraphBuilder.existing_datasets` populated
            with all empty-dimensions datasets.
        predicted_outputs : `dict` [ `str`, `~lsst.daf.butler.DatasetRef` ]
            Dictionary of all init-outputs processed so far; modified in place.
        """
        assert (
            edge.parent_dataset_type_name not in predicted_outputs
        ), "Multiple producers prohibited by PipelineGraph."
        dataset_key = DatasetKey(edge.parent_dataset_type_name, builder.empty_data_id)
        ref = builder.existing_datasets.outputs_in_the_way.get(dataset_key)
        if ref is None:
            ref = DatasetRef(
                builder._pipeline_graph.dataset_types[edge.parent_dataset_type_name].dataset_type,
                builder.empty_data_id,
                run=builder.output_run,
            )
        predicted_outputs[edge.parent_dataset_type_name] = ref
        adapted_ref = edge.adapt_dataset_ref(ref)
        self.adapted_outputs[adapted_ref.datasetType] = adapted_ref


class _PrerequisiteFinder(ABC):
    """Base class for helper objects that search for prerequisite inputs.

    Parameters
    ----------
    pipeline_graph : `pipeline_graph.PipelineGraph`
        Graph form of the pipeline.
    edge : `pipeline_graph.ReadEdge`
        Edge object for this prerequisite input connection.

    Notes
    -----
    Concrete implementations of this ABC are added to the `implementations`
    class variable as they are declared, *in that order*, and are then matched
    against each prerequisite input edge until a match is found.  This is a
    convenient pattern as long as all concrete implementations are in this
    file, but if this ABC is every made public it should be revisited so that
    the order is more explicit.
    """

    def __init__(self, pipeline_graph: PipelineGraph, edge: ReadEdge):
        self.pipeline_graph = pipeline_graph
        self.edge = edge

    implementations: ClassVar[list[type[_PrerequisiteFinder]]] = []
    """Ordered list of all implementations of this task, which are assumed
    to be concrete.
    """

    def __init_subclass__(cls) -> None:
        cls.implementations.append(cls)

    @classmethod
    @abstractmethod
    def match(cls, pipeline_graph: PipelineGraph, edge: ReadEdge) -> _PrerequisiteFinder | None:
        """Test whether this finder type matches an edge, returning an instance
        if it does.

        Parameters
        ----------
        pipeline_graph : `pipeline_graph.PipelineGraph`
            Graph form of the pipeline.
        edge : `pipeline_graph.ReadEdge`
            Edge object for this prerequisite input connection.

        Returns
        -------
        finder : `_PrerequisiteInputFinder` or `None`
            Finder object if this type can handle this edge; `None` otherwise.
        """
        raise NotImplementedError()

    def get_needed_skypix_bounds(self) -> Iterable[SkyPixDimension]:
        """Return an iterable of skypix dimensions over which the spatial
        bounds of all data IDs need to be computed for this finder.

        Dimensions returned here will be present as keys in the
        ``skypix_bounds`` argument to `begin`.
        """
        return ()

    def needs_temporal_bounds(self) -> bool:
        """Whether this finder needs the temporal bounds of all data IDs to be
        computed.

        If `True`, the ``temporal_bounds`` argument to `begin` will not be
        `None`.
        """
        return False

    def begin(
        self,
        butler: Butler,
        input_collections: Sequence[str],
        data_ids: Iterable[DataCoordinate],
        skypix_bounds: dict[str, _SkyPixBounds],
    ) -> None:
        """Perform any preliminary queries for all quanta.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Data repository client to use for queries.
        input_collections : `~collections.abc.Sequence`
            Ordered sequence of collections to search in.
        data_ids : `~collections.abc.Iterable` \
                [ `lsst.daf.butler.DataCoordinate` ]
            Superset of all quantum data IDs that will later be passed to
            `finish`.
        skypix_bounds : `dict` [ `str`, `_SkyPixBounds` ]
            The spatial bounds of all data IDs for this task (values) for
            all skypix dimensions (keys) for which those bounds have been
            computed.  This should be updated in-place when these bounds are
            needed so they can be reused by other prerequisite connections.

        Notes
        -----
        Query results should be saved within ``self`` for later use by
        `finish`.  This method is guaranteed to be called exactly once on
        each instance, before any calls to `finish`.
        """
        return None

    @abstractmethod
    def finish(
        self, butler: Butler, input_collections: Sequence[str], data_id: DataCoordinate
    ) -> list[DatasetRef]:
        """Perform any final queries and gather results from preliminary
        queries.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Data repository client to use for queries.
        input_collections : `~collections.abc.Sequence`
            Ordered sequence of collections to search in.
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for this quantum.

        Returns
        -------
        refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            Found datasets.  These must use the
            ``self.dataset_type_node.dataset_type``, not the version specific
            to this task.
        """
        raise NotImplementedError()


class _LookupFunctionFinder(_PrerequisiteFinder):
    """A prerequisite finder implementation that just delegates to a
    lookupFunction defined in the connection object.

    Parameters
    ----------
    pipeline_graph : `pipeline_graph.PipelineGraph`
        Graph form of the pipeline.
    edge : `pipeline_graph.ReadEdge`
        Edge object for this prerequisite input connection.
    lookupFunction : `~collections.abc.Callable`
        Callable that performs the lookup, one data ID at a time.
    """

    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        edge: ReadEdge,
        lookup_function: Callable[
            [DatasetType, Registry, DataCoordinate, Sequence[str]], Iterable[DatasetRef]
        ],
    ):
        super().__init__(pipeline_graph, edge)
        self.lookup_function = lookup_function
        self.dataset_type_node = self.pipeline_graph.dataset_types[edge.parent_dataset_type_name]

    @classmethod
    def match(cls, pipeline_graph: PipelineGraph, edge: ReadEdge) -> _PrerequisiteFinder | None:
        # Docstring inherited.
        task_node = pipeline_graph.tasks[edge.task_label]
        if (lookup_function := task_node.get_lookup_function(edge.connection_name)) is not None:
            return cls(pipeline_graph, edge, lookup_function)
        return None

    def finish(
        self, butler: Butler, input_collections: Sequence[str], data_id: DataCoordinate
    ) -> list[DatasetRef]:
        # Docstring inherited.
        return [
            self.dataset_type_node.generalize_ref(ref)
            for ref in self.lookup_function(
                self.edge.adapt_dataset_type(self.dataset_type_node.dataset_type),
                butler.registry,
                data_id,
                input_collections,
            )
        ]


class _CalibrationFinder(_PrerequisiteFinder):
    """A prerequisite finder implementation that performs a standard
    calibration-dataset temporal lookup.
    """

    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        edge: ReadEdge,
        dataset_type_node: DatasetTypeNode,
    ):
        super().__init__(pipeline_graph, edge)
        self.dataset_type_node = dataset_type_node

    @classmethod
    def match(cls, pipeline_graph: PipelineGraph, edge: ReadEdge) -> _PrerequisiteFinder | None:
        # Docstring inherited.
        dataset_type_node = pipeline_graph.dataset_types[edge.parent_dataset_type_name]
        task_node = pipeline_graph.tasks[edge.task_label]
        if (
            dataset_type_node.is_calibration
            and dataset_type_node.dataset_type.dimensions <= task_node.dimensions
            and task_node.dimensions.temporal
        ):
            return cls(pipeline_graph, edge, dataset_type_node)
        return None

    def finish(
        self, butler: Butler, input_collections: Sequence[str], data_id: DataCoordinate
    ) -> list[DatasetRef]:
        # Docstring inherited.
        try:
            prereq_ref = butler.registry.findDataset(
                self.dataset_type_node.dataset_type,
                data_id,
                collections=input_collections,
                timespan=data_id.timespan,
            )
            if prereq_ref is not None:
                return [prereq_ref]
            else:
                return []
        except (KeyError, MissingDatasetTypeError):
            # This dataset type is not present in the registry,
            # which just means there are no datasets here.
            return []


class _SkyPixPrerequisiteFinder(_PrerequisiteFinder):
    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        edge: ReadEdge,
        skypix: SkyPixDimension,
    ):
        super().__init__(pipeline_graph, edge)
        self.skypix = skypix
        self.dataset_type = self.pipeline_graph.dataset_types[edge.parent_dataset_type_name].dataset_type

    @classmethod
    def match(cls, pipeline_graph: PipelineGraph, edge: ReadEdge) -> _PrerequisiteFinder | None:
        task_node = pipeline_graph.tasks[edge.task_label]
        if not task_node.dimensions.spatial:
            return None
        dataset_type_node = pipeline_graph.dataset_types[edge.parent_dataset_type_name]
        skypix: SkyPixDimension | None = None
        for dimension in dataset_type_node.dimensions:
            if isinstance(dimension, SkyPixDimension):
                skypix = dimension
            else:
                # Only datasets whose _only_ dimension is a skypix dimension
                # are currently supported.
                return None
        if skypix is None:
            # Datasets with empty dimensions are also not supported
            return None
        return cls(pipeline_graph, edge, skypix)

    def get_needed_skypix_bounds(self) -> Iterable[SkyPixDimension]:
        return (self.skypix,)

    def begin(
        self,
        butler: Butler,
        input_collections: Sequence[str],
        data_ids: Iterable[DataCoordinate],
        skypix_bounds: dict[str, _SkyPixBounds],
    ) -> None:
        self.bounds = skypix_bounds[self.skypix.name]
        self.refs = self.bounds.find_datasets(self.dataset_type, butler, input_collections)

    def finish(
        self, butler: Butler, input_collections: Sequence[str], data_id: DataCoordinate
    ) -> list[DatasetRef]:
        return self.bounds.index_datasets(self.refs, data_id)


class _GeneralPrerequisiteFinder(_PrerequisiteFinder):
    """A prerequisite finder implementation that runs queryDatasets with the
    quantum data ID as a constraint.
    """

    def __init__(
        self,
        pipeline_graph: PipelineGraph,
        edge: ReadEdge,
    ):
        super().__init__(pipeline_graph, edge)
        self.dataset_type = self.pipeline_graph.dataset_types[edge.parent_dataset_type_name].dataset_type

    @classmethod
    def match(cls, pipeline_graph: PipelineGraph, edge: ReadEdge) -> _PrerequisiteFinder:
        # Docstring inherited.
        return cls(pipeline_graph, edge)

    def finish(
        self, butler: Butler, input_collections: Sequence[str], data_id: DataCoordinate
    ) -> list[DatasetRef]:
        # Docstring inherited.
        return list(
            butler.registry.queryDatasets(
                self.dataset_type,
                collections=input_collections,
                dataId=data_id,
                findFirst=True,
            ).expanded()
        )


class _SkyPixBounds:
    def __init__(self, dimension: SkyPixDimension) -> None:
        self._dimension = dimension
        self._contributors: dict[DimensionGraph, dict[DataCoordinate, RangeSet | None]] = {}
        self._envelope: RangeSet | None = RangeSet()

    def add_quantum_data_id(
        self, quantum_data_id: DataCoordinate, related_data_ids: Iterable[DataCoordinate] = ()
    ) -> None:
        nested = self._contributors.setdefault(quantum_data_id.graph, {})
        if quantum_data_id not in self._contributors:
            ranges = self._dimension.pixelization.envelope(quantum_data_id.region)
            for related_data_id in related_data_ids:
                ranges |= self._dimension.pixelization.envelope(related_data_id.region)
            nested[quantum_data_id] = ranges
            if self._envelope is not None:
                self._envelope |= ranges

    def add_unbounded(self, quantum_data_id: DataCoordinate) -> None:
        nested = self._contributors.setdefault(quantum_data_id.graph, {})
        if quantum_data_id not in self._contributors:
            nested[quantum_data_id] = None
        self._envelope = None

    def find_datasets(
        self, dataset_type: DatasetType, butler: Butler, collections: Sequence[str]
    ) -> dict[int, DatasetRef]:
        if self._envelope is None:
            return {
                cast(int, ref.dataId[self._dimension]): ref
                for ref in butler.registry.queryDatasets(
                    dataset_type,
                    collections=collections,
                    findFirst=True,
                )
            }
        pixels: list[int] = []
        for begin, end in self._envelope:
            pixels.extend(range(begin, end))
        return {
            cast(int, ref.dataId[self._dimension]): ref
            for ref in butler.registry.queryDatasets(
                dataset_type,
                collections=collections,
                where=f"{self._dimension} IN (pixels)",
                bind={"pixels": pixels},
                findFirst=True,
            )
        }

    def index_datasets(self, refs: Mapping[int, DatasetRef], data_id: DataCoordinate) -> list[DatasetRef]:
        result: list[DatasetRef] = []
        ranges = self._contributors[data_id.graph][data_id]
        if ranges is None:
            return list(refs.values())
        for begin, end in ranges:
            for pixel in range(begin, end):
                result.append(refs[pixel])
        return result
