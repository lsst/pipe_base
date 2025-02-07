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

"""An under-construction version of QuantumGraph and various helper
classes.
"""

from __future__ import annotations

__all__ = (
    "DatasetKey",
    "PrerequisiteDatasetKey",
    "QuantumGraphSkeleton",
    "QuantumKey",
    "TaskInitKey",
)

import dataclasses
from collections.abc import Iterable, Iterator, MutableMapping, Set
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeAlias

import networkx

from lsst.daf.butler import DataCoordinate, DataIdValue, DatasetRef
from lsst.utils.logging import getLogger

if TYPE_CHECKING:
    pass

_LOG = getLogger(__name__)


@dataclasses.dataclass(slots=True, eq=True, frozen=True)
class QuantumKey:
    """Identifier type for quantum keys in a `QuantumGraphSkeleton`."""

    task_label: str
    """Label of the task in the pipeline."""

    data_id_values: tuple[DataIdValue, ...]
    """Data ID values of the quantum.

    Note that keys are fixed given `task_label`, so using only the values here
    speeds up comparisons.
    """

    is_task: ClassVar[Literal[True]] = True
    """Whether this node represents a quantum or task initialization rather
    than a dataset (always `True`).
    """


@dataclasses.dataclass(slots=True, eq=True, frozen=True)
class TaskInitKey:
    """Identifier type for task init keys in a `QuantumGraphSkeleton`."""

    task_label: str
    """Label of the task in the pipeline."""

    is_task: ClassVar[Literal[True]] = True
    """Whether this node represents a quantum or task initialization rather
    than a dataset (always `True`).
    """


@dataclasses.dataclass(slots=True, eq=True, frozen=True)
class DatasetKey:
    """Identifier type for dataset keys in a `QuantumGraphSkeleton`."""

    parent_dataset_type_name: str
    """Name of the dataset type (never a component)."""

    data_id_values: tuple[DataIdValue, ...]
    """Data ID values of the dataset.

    Note that keys are fixed given `parent_dataset_type_name`, so using only
    the values here speeds up comparisons.
    """

    is_task: ClassVar[Literal[False]] = False
    """Whether this node represents a quantum or task initialization rather
    than a dataset (always `False`).
    """

    is_prerequisite: ClassVar[Literal[False]] = False


@dataclasses.dataclass(slots=True, eq=True, frozen=True)
class PrerequisiteDatasetKey:
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

    dataset_id_bytes: bytes
    """Dataset ID (UUID) as raw bytes."""

    is_task: ClassVar[Literal[False]] = False
    """Whether this node represents a quantum or task initialization rather
    than a dataset (always `False`).
    """

    is_prerequisite: ClassVar[Literal[True]] = True


Key: TypeAlias = QuantumKey | TaskInitKey | DatasetKey | PrerequisiteDatasetKey


class QuantumGraphSkeleton:
    """An under-construction quantum graph.

    QuantumGraphSkeleton is intended for use inside `QuantumGraphBuilder` and
    its subclasses.

    Parameters
    ----------
    task_labels : `~collections.abc.Iterable` [ `str` ]
        The labels of all tasks whose quanta may be included in the graph, in
        topological order.

    Notes
    -----
    QuantumGraphSkeleton models a bipartite version of the quantum graph, in
    which both quanta and datasets are represented as nodes and each type of
    node only has edges to the other type.

    Square-bracket (`getitem`) indexing returns a mutable mapping of a node's
    flexible attributes.

    The details of the `QuantumGraphSkeleton` API (e.g. which operations
    operate on multiple nodes vs. a single node) are set by what's actually
    needed by current quantum graph generation algorithms.  New variants can be
    added as needed, but adding all operations that *might* be useful for some
    future algorithm seems premature.
    """

    def __init__(self, task_labels: Iterable[str]):
        self._tasks: dict[str, tuple[TaskInitKey, set[QuantumKey]]] = {}
        self._xgraph: networkx.DiGraph = networkx.DiGraph()
        self._global_init_outputs: set[DatasetKey] = set()
        for task_label in task_labels:
            task_init_key = TaskInitKey(task_label)
            self._tasks[task_label] = (task_init_key, set())
            self._xgraph.add_node(task_init_key)

    def __contains__(self, key: Key) -> bool:
        return key in self._xgraph.nodes

    def __getitem__(self, key: Key) -> MutableMapping[str, Any]:
        return self._xgraph.nodes[key]

    def __iter__(self) -> Iterator[Key]:
        return iter(self._xgraph.nodes)

    @property
    def n_nodes(self) -> int:
        """The total number of nodes of all types."""
        return len(self._xgraph.nodes)

    @property
    def n_edges(self) -> int:
        """The total number of edges."""
        return len(self._xgraph.edges)

    def has_task(self, task_label: str) -> bool:
        """Test whether the given task is in this skeleton.

        Tasks are only added to the skeleton at initialization, but may be
        removed by `remove_task` if they end up having no quanta.

        Parameters
        ----------
        task_label : `str`
            Task to check for.

        Returns
        -------
        has : `bool`
            `True` if the task is in this skeleton.
        """
        return task_label in self._tasks

    def get_task_init_node(self, task_label: str) -> TaskInitKey:
        """Return the graph node that represents a task's initialization.

        Parameters
        ----------
        task_label : `str`
            The task label to use.

        Returns
        -------
        node : `TaskInitKey`
            The graph node representing this task's initialization.
        """
        return self._tasks[task_label][0]

    def get_quanta(self, task_label: str) -> Set[QuantumKey]:
        """Return the quanta for the given task label.

        Parameters
        ----------
        task_label : `str`
            Label for the task.

        Returns
        -------
        quanta : `~collections.abc.Set` [ `QuantumKey` ]
            A set-like object with the identifiers of all quanta for the given
            task.  *The skeleton object's set of quanta must not be modified
            while iterating over this container; make a copy if mutation during
            iteration is necessary*.
        """
        return self._tasks[task_label][1]

    @property
    def global_init_outputs(self) -> Set[DatasetKey]:
        """The set of dataset nodes that are not associated with any task."""
        return self._global_init_outputs

    def iter_all_quanta(self) -> Iterator[QuantumKey]:
        """Iterate over all quanta from any task, in topological (but otherwise
        unspecified) order.
        """
        for _, quanta in self._tasks.values():
            yield from quanta

    def iter_outputs_of(self, quantum_key: QuantumKey | TaskInitKey) -> Iterator[DatasetKey]:
        """Iterate over the datasets produced by the given quantum.

        Parameters
        ----------
        quantum_key : `QuantumKey` or `TaskInitKey`
            Quantum to iterate over.

        Returns
        -------
        datasets : `~collections.abc.Iterator` of `DatasetKey`
            Datasets produced by the given quanta.
        """
        return self._xgraph.successors(quantum_key)

    def iter_inputs_of(
        self, quantum_key: QuantumKey | TaskInitKey
    ) -> Iterator[DatasetKey | PrerequisiteDatasetKey]:
        """Iterate over the datasets consumed by the given quantum.

        Parameters
        ----------
        quantum_key : `QuantumKey` or `TaskInitKey`
            Quantum to iterate over.

        Returns
        -------
        datasets : `~collections.abc.Iterator` of `DatasetKey` \
                or `PrequisiteDatasetKey`
            Datasets consumed by the given quanta.
        """
        return self._xgraph.predecessors(quantum_key)

    def update(self, other: QuantumGraphSkeleton) -> None:
        """Copy all nodes from ``other`` to ``self``.

        Parameters
        ----------
        other : `QuantumGraphSkeleton`
            Source of nodes. The tasks in ``other`` must be a subset of the
            tasks in ``self`` (this method is expected to be used to populate
            a skeleton for a full from independent-subgraph skeletons).
        """
        for task_label, (_, quanta) in other._tasks.items():
            self._tasks[task_label][1].update(quanta)
        self._xgraph.update(other._xgraph)

    def add_quantum_node(self, task_label: str, data_id: DataCoordinate, **attrs: Any) -> QuantumKey:
        """Add a new node representing a quantum.

        Parameters
        ----------
        task_label : `str`
            Name of task.
        data_id : `~lsst.daf.butler.DataCoordinate`
            The data ID of the quantum.
        **attrs : `~typing.Any`
            Additional attributes.
        """
        key = QuantumKey(task_label, data_id.required_values)
        self._xgraph.add_node(key, data_id=data_id, **attrs)
        self._tasks[key.task_label][1].add(key)
        return key

    def add_dataset_node(
        self,
        parent_dataset_type_name: str,
        data_id: DataCoordinate,
        is_global_init_output: bool = False,
        **attrs: Any,
    ) -> DatasetKey:
        """Add a new node representing a dataset.

        Parameters
        ----------
        parent_dataset_type_name : `str`
            Name of the parent dataset type.
        data_id : `~lsst.daf.butler.DataCoordinate`
            The dataset data ID.
        is_global_init_output : `bool`, optional
            Whether this dataset is a global init output.
        **attrs : `~typing.Any`
            Additional attributes for the node.
        """
        key = DatasetKey(parent_dataset_type_name, data_id.required_values)
        self._xgraph.add_node(key, data_id=data_id, **attrs)
        if is_global_init_output:
            assert isinstance(key, DatasetKey)
            self._global_init_outputs.add(key)
        return key

    def add_prerequisite_node(
        self,
        ref: DatasetRef,
        **attrs: Any,
    ) -> PrerequisiteDatasetKey:
        """Add a new node representing a prerequisite input dataset.

        Parameters
        ----------
        ref : `~lsst.daf.butler.DatasetRef`
            The dataset ref of the prerequisite.
        **attrs : `~typing.Any`
            Additional attributes for the node.

        Notes
        -----
        This automatically sets the 'existing_input' ref attribute (see
        `set_existing_input_ref`), since prerequisites are always overall
        inputs.
        """
        key = PrerequisiteDatasetKey(ref.datasetType.name, ref.id.bytes)
        self._xgraph.add_node(key, data_id=ref.dataId, ref=ref, **attrs)
        return key

    def remove_quantum_node(self, key: QuantumKey, remove_outputs: bool) -> None:
        """Remove a node representing a quantum.

        Parameters
        ----------
        key : `QuantumKey`
            Identifier for the node.
        remove_outputs : `bool`
            If `True`, also remove all dataset nodes produced by this quantum.
            If `False`, any such dataset nodes will become overall inputs.
        """
        _, quanta = self._tasks[key.task_label]
        quanta.remove(key)
        if remove_outputs:
            to_remove = list(self._xgraph.successors(key))
            to_remove.append(key)
            self._xgraph.remove_nodes_from(to_remove)
        else:
            self._xgraph.remove_node(key)

    def remove_dataset_nodes(self, keys: Iterable[DatasetKey | PrerequisiteDatasetKey]) -> None:
        """Remove nodes representing datasets.

        Parameters
        ----------
        keys : `~collections.abc.Iterable` of `DatasetKey`\
                or `PrerequisiteDatasetKey`
            Nodes to remove.
        """
        self._xgraph.remove_nodes_from(keys)

    def remove_task(self, task_label: str) -> None:
        """Fully remove a task from the skeleton.

        All init-output datasets and quanta for the task must already have been
        removed.

        Parameters
        ----------
        task_label : `str`
            Name of task to remove.
        """
        task_init_key, quanta = self._tasks.pop(task_label)
        assert not quanta, "Cannot remove task unless all quanta have already been removed."
        assert not list(self._xgraph.successors(task_init_key))
        self._xgraph.remove_node(task_init_key)

    def add_input_edges(
        self,
        task_key: QuantumKey | TaskInitKey,
        dataset_keys: Iterable[DatasetKey | PrerequisiteDatasetKey],
    ) -> None:
        """Add edges connecting datasets to a quantum that consumes them.

        Parameters
        ----------
        task_key : `QuantumKey` or `TaskInitKey`
            Quantum to connect.
        dataset_keys : `~collections.abc.Iterable` of `DatasetKey`\
                or `PrequisiteDatasetKey`
            Datasets to join to the quantum.

        Notes
        -----
        This must only be called if the task node has already been added.
        Use `add_input_edge` if this cannot be assumed.

        Dataset nodes that are not already present will be created.
        """
        assert task_key in self._xgraph
        self._xgraph.add_edges_from((dataset_key, task_key) for dataset_key in dataset_keys)

    def remove_input_edges(
        self,
        task_key: QuantumKey | TaskInitKey,
        dataset_keys: Iterable[DatasetKey | PrerequisiteDatasetKey],
    ) -> None:
        """Remove edges connecting datasets to a quantum that consumes them.

        Parameters
        ----------
        task_key : `QuantumKey` or `TaskInitKey`
            Quantum to disconnect.
        dataset_keys : `~collections.abc.Iterable` of `DatasetKey`\
                or `PrequisiteDatasetKey`
            Datasets to remove from the quantum.
        """
        self._xgraph.remove_edges_from((dataset_key, task_key) for dataset_key in dataset_keys)

    def add_input_edge(
        self,
        task_key: QuantumKey | TaskInitKey,
        dataset_key: DatasetKey | PrerequisiteDatasetKey,
        ignore_unrecognized_quanta: bool = False,
    ) -> bool:
        """Add an edge connecting a dataset to a quantum that consumes it.

        Parameters
        ----------
        task_key : `QuantumKey` or `TaskInitKey`
            Identifier for the quantum node.
        dataset_key : `DatasetKey` or `PrerequisiteKey`
            Identifier for the dataset node.
        ignore_unrecognized_quanta : `bool`, optional
            If `False`, do nothing if the quantum node is not already present.
            If `True`, the quantum node is assumed to be present.

        Returns
        -------
        added : `bool`
            `True` if an edge was actually added, `False` if the quantum was
            not recognized and the edge was not added as a result.

        Notes
        -----
        Dataset nodes that are not already present will be created.
        """
        if ignore_unrecognized_quanta and task_key not in self._xgraph:
            return False
        self._xgraph.add_edge(dataset_key, task_key)
        return True

    def add_output_edge(self, task_key: QuantumKey | TaskInitKey, dataset_key: DatasetKey) -> None:
        """Add an edge connecting a dataset to the quantum that produces it.

        Parameters
        ----------
        task_key : `QuantumKey` or `TaskInitKey`
            Identifier for the quantum node.  Must identify a node already
            present in the graph.
        dataset_key : `DatasetKey`
            Identifier for the dataset node.  Must identify a node already
            present in the graph.
        """
        assert task_key in self._xgraph
        assert dataset_key in self._xgraph
        self._xgraph.add_edge(task_key, dataset_key)

    def remove_orphan_datasets(self) -> None:
        """Remove any dataset nodes that do not have any edges."""
        for orphan in list(networkx.isolates(self._xgraph)):
            if not orphan.is_task and orphan not in self._global_init_outputs:
                self._xgraph.remove_node(orphan)

    def extract_overall_inputs(self) -> dict[DatasetKey | PrerequisiteDatasetKey, DatasetRef]:
        """Find overall input datasets.

        Returns
        -------
        datasets : `dict` [ `DatasetKey` or `PrerequisiteDatasetKey`, \
                `~lsst.daf.butler.DatasetRef` ]
            Overall-input datasets, including prerequisites and init-inputs.
        """
        result = {}
        for generation in networkx.algorithms.topological_generations(self._xgraph):
            for dataset_key in generation:
                if dataset_key.is_task:
                    continue
                if (ref := self.get_dataset_ref(dataset_key)) is None:
                    raise AssertionError(
                        f"Logic bug in QG generation: dataset {dataset_key} was never resolved."
                    )
                result[dataset_key] = ref
            break
        return result

    def set_dataset_ref(
        self, ref: DatasetRef, key: DatasetKey | PrerequisiteDatasetKey | None = None
    ) -> None:
        """Associate a dataset node with a `DatasetRef` instance.

        Parameters
        ----------
        ref : `DatasetRef`
            `DatasetRef` to associate with the node.
        key : `DatasetKey` or `PrerequisiteDatasetKey`, optional
            Identifier for the graph node.  If not provided, a `DatasetKey`
            is constructed from the dataset type name and data ID of ``ref``.
        """
        if key is None:
            key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
        self._xgraph.nodes[key]["ref"] = ref

    def set_output_for_skip(self, ref: DatasetRef) -> None:
        """Associate a dataset node with a `DatasetRef` that represents an
        existing output in a collection where such outputs can cause a quantum
        to be skipped.

        Parameters
        ----------
        ref : `DatasetRef`
            `DatasetRef` to associate with the node.
        """
        key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
        self._xgraph.nodes[key]["output_for_skip"] = ref

    def set_output_in_the_way(self, ref: DatasetRef) -> None:
        """Associate a dataset node with a `DatasetRef` that represents an
        existing output in the output RUN collectoin.

        Parameters
        ----------
        ref : `DatasetRef`
            `DatasetRef` to associate with the node.
        """
        key = DatasetKey(ref.datasetType.name, ref.dataId.required_values)
        self._xgraph.nodes[key]["output_in_the_way"] = ref

    def get_dataset_ref(self, key: DatasetKey | PrerequisiteDatasetKey) -> DatasetRef | None:
        """Return the `DatasetRef` associated with the given node.

        This does not return "output for skip" and "output in the way"
        datasets.

        Parameters
        ----------
        key : `DatasetKey` or `PrerequisiteDatasetKey`
            Identifier for the graph node.

        Returns
        -------
        ref : `DatasetRef` or `None`
            Dataset reference associated with the node.
        """
        return self._xgraph.nodes[key].get("ref")

    def get_output_for_skip(self, key: DatasetKey) -> DatasetRef | None:
        """Return the `DatasetRef` associated with the given node in a
        collection where it could lead to a quantum being skipped.

        Parameters
        ----------
        key : `DatasetKey`
            Identifier for the graph node.

        Returns
        -------
        ref : `DatasetRef` or `None`
            Dataset reference associated with the node.
        """
        return self._xgraph.nodes[key].get("output_for_skip")

    def get_output_in_the_way(self, key: DatasetKey) -> DatasetRef | None:
        """Return the `DatasetRef` associated with the given node in the
        output RUN collection.

        Parameters
        ----------
        key : `DatasetKey`
            Identifier for the graph node.

        Returns
        -------
        ref : `DatasetRef` or `None`
            Dataset reference associated with the node.
        """
        return self._xgraph.nodes[key].get("output_in_the_way")

    def discard_output_in_the_way(self, key: DatasetKey) -> None:
        """Drop any `DatasetRef` associated with this node in the output RUN
        collection.

        Does nothing if there is no such `DatasetRef`.

        Parameters
        ----------
        key : `DatasetKey`
            Identifier for the graph node.
        """
        self._xgraph.nodes[key].pop("output_in_the_way", None)

    def set_data_id(self, key: Key, data_id: DataCoordinate) -> None:
        """Set the data ID associated with a node.

        This updates the data ID in any `DatasetRef` objects associated with
        the node via `set_ref`, `set_output_for_skip`, or
        `set_output_in_the_way` as well, assuming it is an expanded version
        of the original data ID.

        Parameters
        ----------
        key : `Key`
            Identifier for the graph node.
        data_id : `DataCoordinate`
            Data ID for the node.
        """
        state: MutableMapping[str, Any] = self._xgraph.nodes[key]
        state["data_id"] = data_id
        ref: DatasetRef | None
        if (ref := state.get("ref")) is not None:
            state["ref"] = ref.expanded(data_id)
        output_for_skip: DatasetRef | None
        if (output_for_skip := state.get("output_for_skip")) is not None:
            state["output_for_skip"] = output_for_skip.expanded(data_id)
        output_in_the_way: DatasetRef | None
        if (output_in_the_way := state.get("output_in_the_way")) is not None:
            state["output_in_the_way"] = output_in_the_way.expanded(data_id)
