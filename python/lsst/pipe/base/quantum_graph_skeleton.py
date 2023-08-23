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

"""An under-construction version of QuantumGraph and various helper
classes.
"""

from __future__ import annotations

__all__ = (
    "QuantumGraphSkeleton",
    "QuantumKey",
    "TaskInitKey",
    "DatasetKey",
    "PrerequisiteDatasetKey",
)

from collections.abc import Iterable, Iterator, MutableMapping, Set
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NamedTuple

import networkx
from lsst.daf.butler import DataCoordinate, DataIdValue, DatasetRef
from lsst.utils.logging import getLogger

if TYPE_CHECKING:
    pass

_LOG = getLogger(__name__)


class QuantumKey(NamedTuple):
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


class TaskInitKey(NamedTuple):
    """Identifier type for task init keys in a `QuantumGraphSkeleton`."""

    task_label: str
    """Label of the task in the pipeline."""

    is_task: ClassVar[Literal[True]] = True
    """Whether this node represents a quantum or task initialization rather
    than a dataset (always `True`).
    """


class DatasetKey(NamedTuple):
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


class PrerequisiteDatasetKey(NamedTuple):
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

    def __contains__(self, key: QuantumKey | TaskInitKey | DatasetKey | PrerequisiteDatasetKey) -> bool:
        return key in self._xgraph.nodes

    def __getitem__(
        self, key: QuantumKey | TaskInitKey | DatasetKey | PrerequisiteDatasetKey
    ) -> MutableMapping[str, Any]:
        return self._xgraph.nodes[key]

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
        """
        return task_label in self._tasks

    def get_task_init_node(self, task_label: str) -> TaskInitKey:
        """Return the graph node that represents a task's initialization."""
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
            iteration is necessary.*
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
        """Iterate over the datasets produced by the given quantum."""
        return self._xgraph.successors(quantum_key)

    def iter_inputs_of(
        self, quantum_key: QuantumKey | TaskInitKey
    ) -> Iterator[DatasetKey | PrerequisiteDatasetKey]:
        """Iterate over the datasets consumed by the given quantum."""
        return self._xgraph.predecessors(quantum_key)

    def update(self, other: QuantumGraphSkeleton) -> None:
        """Copy all nodes from ``other`` to ``self``.

        The tasks in ``other`` must be a subset of the tasks in ``self`` (this
        method is expected to be used to populate a skeleton for a full
        from independent-subgraph skeletons).
        """
        for task_label, (_, quanta) in other._tasks.items():
            self._tasks[task_label][1].update(quanta)
        self._xgraph.update(other._xgraph)

    def add_quantum_node(self, task_label: str, data_id: DataCoordinate, **attrs: Any) -> QuantumKey:
        """Add a new node representing a quantum."""
        key = QuantumKey(task_label, data_id.values_tuple())
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
        """Add a new node representing a dataset."""
        key = DatasetKey(parent_dataset_type_name, data_id.values_tuple())
        self._xgraph.add_node(key, data_id=data_id, **attrs)
        if is_global_init_output:
            assert isinstance(key, DatasetKey)
            self._global_init_outputs.add(key)
        return key

    def add_prerequisite_node(
        self,
        parent_dataset_type_name: str,
        ref: DatasetRef,
        **attrs: Any,
    ) -> PrerequisiteDatasetKey:
        """Add a new node representing a prerequisite input dataset."""
        key = PrerequisiteDatasetKey(parent_dataset_type_name, ref.id.bytes)
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
        """Remove nodes representing datasets."""
        self._xgraph.remove_nodes_from(keys)

    def remove_task(self, task_label: str) -> None:
        task_init_key, quanta = self._tasks.pop(task_label)
        assert not quanta, "Cannot remove task unless all quanta have already been removed."
        self._xgraph.remove_node(task_init_key)

    def add_input_edges(
        self,
        task_key: QuantumKey | TaskInitKey,
        dataset_keys: Iterable[DatasetKey | PrerequisiteDatasetKey],
    ) -> None:
        """Add edges connecting datasets to a quantum that consumes them.

        *This must only be called if the task node has already been added.*
        Use `add_input_edge` if this cannot be assumed.
        """
        assert task_key in self._xgraph
        self._xgraph.add_edges_from((dataset_key, task_key) for dataset_key in dataset_keys)

    def remove_input_edges(
        self,
        task_key: QuantumKey | TaskInitKey,
        dataset_keys: Iterable[DatasetKey | PrerequisiteDatasetKey],
    ) -> None:
        """Remove edges connecting datasets to a quantum that consumes them."""
        self._xgraph.remove_edges_from((dataset_key, task_key) for dataset_key in dataset_keys)

    def add_input_edge(
        self,
        task_key: QuantumKey | TaskInitKey,
        dataset_key: DatasetKey | PrerequisiteDatasetKey,
        add_unrecognized_quanta: bool = False,
    ) -> bool:
        """Add an edge connecting a dataset to a quantum that consumes it.

        Parameters
        ----------
        task_key : `QuantumKey` or `TaskInitKey`
            Identifier for the quantum node.
        dataset_key : `DatasetKey` or `PrerequisiteKey`
            Identifier for the dataset node.
        add_unrecognized_quanta : `bool`, optional
            If `True`, automatically add quantum nodes that are not already
            present.  If `False`, do nothing if the quantum node is not already
            present.

        Returns
        -------
        added : `bool`
            `True` if an edge was actually added, `False` if the quantum was
            not recognized and the edge was not added as a result.
        """
        if task_key not in self._xgraph:
            assert isinstance(task_key, QuantumKey)
            if add_unrecognized_quanta:
                self._tasks[task_key.task_label][1].add(task_key)
            else:
                return False
        self._xgraph.add_edge(dataset_key, task_key)
        return True

    def add_output_edge(
        self, task_key: QuantumKey | TaskInitKey, dataset_key: DatasetKey | PrerequisiteDatasetKey
    ) -> None:
        """Add an edge connecting a dataset to the quantum that produces it.

        Parameters
        ----------
        task_key : `QuantumKey` or `TaskInitKey`
            Identifier for the quantum node.
        dataset_key : `DatasetKey` or `PrerequisiteKey`
            Identifier for the dataset node.
        """
        assert task_key in self._xgraph
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
                try:
                    result[dataset_key] = self[dataset_key]["ref"]
                except KeyError:
                    raise AssertionError(
                        f"Logic bug in QG generation: dataset {dataset_key} was never resolved."
                    )
            break
        return result
