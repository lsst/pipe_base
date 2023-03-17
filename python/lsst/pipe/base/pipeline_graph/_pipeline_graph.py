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

__all__ = ("PipelineGraph", "MutablePipelineGraph", "ResolvedPipelineGraph")

import io
import json
import os
import tarfile
from abc import abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime
from types import EllipsisType
from typing import TYPE_CHECKING, Any, BinaryIO, Generic, TypeVar, cast, final

import networkx
import networkx.algorithms.bipartite
import networkx.algorithms.dag
from lsst.daf.butler import DimensionGraph, DimensionUniverse, Registry
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.iteration import ensure_iterable

from ._abcs import Edge, Node, NodeKey, NodeType
from ._dataset_types import DatasetTypeNode, ResolvedDatasetTypeNode
from ._edges import ReadEdge, WriteEdge
from ._exceptions import PipelineDataCycleError
from ._io import MutablePipelineGraphReader, ResolvedPipelineGraphReader
from ._task_subsets import LabeledSubset, MutableLabeledSubset, ResolvedLabeledSubset
from ._tasks import ResolvedTaskNode, TaskNode, _TaskNodeImportedData
from ._views import _D, _T, DatasetTypeMappingView, TaskMappingView

if TYPE_CHECKING:
    from ..config import PipelineTaskConfig
    from ..connections import PipelineTaskConnections
    from ..pipeline import TaskDef
    from ..pipelineTask import PipelineTask


_S = TypeVar("_S", bound="LabeledSubset", covariant=True)
_P = TypeVar("_P", bound="PipelineGraph", covariant=True)


_IO_VERSION_INFO = (0, 0, 1)


class ExtractHelper(Generic[_P]):
    def __init__(self, parent: _P) -> None:
        self._parent = parent
        self._run_xgraph: networkx.DiGraph | None = None
        self._task_keys: set[NodeKey] = set()

    def include_tasks(self, labels: str | Iterable[str] | EllipsisType = ...) -> None:
        if labels is ...:
            self._task_keys.update(key for key in self._parent._xgraph if key.node_type is NodeType.TASK)
        else:
            self._task_keys.update(
                NodeKey(NodeType.TASK, task_label) for task_label in ensure_iterable(labels)
            )

    def exclude_tasks(self, labels: str | Iterable[str]) -> None:
        self._task_keys.difference_update(
            NodeKey(NodeType.TASK, task_label) for task_label in ensure_iterable(labels)
        )

    def include_subset(self, label: str) -> None:
        self._task_keys.update(node.key for node in self._parent.labeled_subsets[label].values())

    def exclude_subset(self, label: str) -> None:
        self._task_keys.difference_update(node.key for node in self._parent.labeled_subsets[label].values())

    def start_after(self, names: str | Iterable[str], node_type: NodeType) -> None:
        to_exclude: set[NodeKey] = set()
        for name in ensure_iterable(names):
            key = NodeKey(node_type, name)
            to_exclude.update(networkx.algorithms.dag.ancestors(self._get_run_xgraph(), key))
            to_exclude.add(key)
        self._task_keys.difference_update(to_exclude)

    def stop_at(self, names: str | Iterable[str], node_type: NodeType) -> None:
        to_exclude: set[NodeKey] = set()
        for name in ensure_iterable(names):
            key = NodeKey(node_type, name)
            to_exclude.update(networkx.algorithms.dag.descendants(self._get_run_xgraph(), key))
        self._task_keys.difference_update(to_exclude)

    def finish(self, description: str | None = None) -> MutablePipelineGraph:
        if description is None:
            description = self._parent._description
        # Combine the task_keys we're starting with and the keys for their init
        # nodes.
        keys = self._task_keys | {NodeKey(NodeType.TASK_INIT, key.name) for key in self._task_keys}
        # Also add the keys for the adjacent dataset type nodes.
        keys.update(networkx.node_boundary(self._parent._xgraph.to_undirected(as_view=True), keys))
        # Make the new backing networkx graph.
        xgraph: networkx.DiGraph = self._parent._xgraph.subgraph(keys).copy()
        for state in xgraph.nodes.values():
            node: Node = state["instance"]
            state["instance"] = node._unresolved()
        result = MutablePipelineGraph.__new__(MutablePipelineGraph)
        result._init_from_args(xgraph, None, description=description)
        return result

    def _get_run_xgraph(self) -> networkx.DiGraph:
        if self._run_xgraph is None:
            self._run_xgraph = self._parent.make_bipartite_xgraph(init=False)
        return self._run_xgraph


class PipelineGraph(Generic[_T, _D, _S]):
    """A base class for directed acyclic graph of `PipelineTask` definitions.

    This abstract base class should not be inherited from outside its package;
    it exists to share code and interfaces between `MutablePipelineGraph` and
    `ResolvedPipelineGraph`.
    """

    def __init__(self) -> None:
        self._init_from_args()

    def _init_from_args(
        self,
        xgraph: networkx.DiGraph | None = None,
        sorted_keys: Sequence[NodeKey] | None = None,
        labeled_subsets: dict[str, _S] | None = None,
        description: str = "",
    ) -> None:
        """Initialize the graph with possibly-nontrivial arguments.

        Parameters
        ----------
        xgraph : `networkx.DiGraph` or `None`, optional
            The backing networkx graph, or `None` to create an empty one.
        sorted_keys : `Sequence` [ `NodeKey` ] or `None`, optional
            Topologically sorted sequence of node keys, or `None` if the graph
            is not sorted.
        labeled_subsets : `dict` [ `str`, `LabeledSubsetMapping` ], optional
            Labeled subsets of tasks.  Values must be constructed with
            ``xgraph`` as their parent graph.
        description : `str`, optional
            String description for this pipeline.

        Notes
        -----
        Only empty `PipelineGraph` [subclass] instances should be constructed
        directly by users, which sets the signature of ``__init__`` itself, but
        methods on `PipelineGraph` and its helper classes need to be able to
        create them with state.  Those methods can call this after calling
        ``__new__`` manually.
        """
        self._xgraph = xgraph if xgraph is not None else networkx.DiGraph()
        self._sorted_keys: Sequence[NodeKey] | None = None
        self._labeled_subsets = labeled_subsets if labeled_subsets is not None else {}
        self._description = description
        self._tasks = TaskMappingView[_T](self._xgraph)
        self._dataset_types = DatasetTypeMappingView[_D](self._xgraph)
        if sorted_keys is not None:
            self._reorder(sorted_keys)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.description!r}, tasks={self.tasks!s})"

    @property
    def description(self) -> str:
        """String description for this pipeline."""
        return self._description

    @property
    def tasks(self) -> TaskMappingView[_T]:
        return self._tasks

    @property
    def dataset_types(self) -> DatasetTypeMappingView[_D]:
        return self._dataset_types

    @property
    def labeled_subsets(self) -> Mapping[str, _S]:
        """Mapping of all labeled subsets of tasks.

        Keys are subset labels, values are Task-only graphs (subgraphs of
        `tasks`).  See `TaskSubsetGraph` and its subclasses for more
        information.
        """
        return self._labeled_subsets

    def iter_edges(self, init: bool = False) -> Iterator[Edge]:
        for _, _, edge in self._xgraph.edges(data="instance"):
            if edge is not None and edge.is_init == init:
                yield edge

    def iter_nodes(self) -> Iterator[Node]:
        if self._sorted_keys is not None:
            for key in self._sorted_keys:
                yield self._xgraph.nodes[key]["instance"]
        else:
            for _, node in self._xgraph.nodes(data="instance"):
                yield node

    def iter_overall_inputs(self) -> Iterator[_D]:
        if self._sorted_keys is not None:
            # If the graph is sorted, we know the overall inputs (init and
            # runtime) come before any task node or task init node, so we
            # just iterate over those until we see a non-dataset type node.
            for key in self._sorted_keys:
                if key.node_type is NodeType.DATASET_TYPE:
                    yield self._xgraph.nodes[key]["instance"]
                else:
                    return
        else:
            for generation in networkx.algorithms.dag.topological_generations():
                for key in generation:
                    yield self._xgraph.nodes[key]["instance"]

    def make_xgraph(self, *, import_tasks: bool = False) -> networkx.DiGraph:
        return self._transform_xgraph_state(self._xgraph.copy(), import_tasks)

    def make_bipartite_xgraph(self, init: bool = False, *, import_tasks: bool = False) -> networkx.DiGraph:
        return self._transform_xgraph_state(self._make_bipartite_xgraph_internal(init).copy(), import_tasks)

    def make_task_xgraph(self, init: bool = False, *, import_tasks: bool = False) -> networkx.DiGraph:
        bipartite_xgraph = self._make_bipartite_xgraph_internal(init)
        task_keys = [
            key
            for key, bipartite in bipartite_xgraph.nodes(data="bipartite")
            if bipartite == NodeType.TASK.bipartite
        ]
        return self._transform_xgraph_state(
            networkx.algorithms.bipartite.projected_graph(bipartite_xgraph, task_keys), import_tasks
        )

    def make_dataset_type_xgraph(self, init: bool = False) -> networkx.DiGraph:
        bipartite_xgraph = self._make_bipartite_xgraph_internal(init)
        dataset_type_keys = [
            key
            for key, bipartite in bipartite_xgraph.nodes(data="bipartite")
            if bipartite == NodeType.DATASET_TYPE.bipartite
        ]
        return self._transform_xgraph_state(
            networkx.algorithms.bipartite.projected_graph(bipartite_xgraph, dataset_type_keys), False
        )

    def _make_bipartite_xgraph_internal(self, init: bool) -> networkx.DiGraph:
        return self._xgraph.edge_subgraph([edge.key for edge in self.iter_edges(init)])

    def _transform_xgraph_state(
        self, xgraph: networkx.DiGraph, import_tasks: bool = False
    ) -> networkx.DiGraph:
        state: dict[str, Any]
        for state in xgraph.nodes.values():
            try:
                node: Node = state.pop("instance")
            except KeyError:
                breakpoint()
                raise
            state.update(node._to_xgraph_state(import_tasks))
        for _, _, state in xgraph.edges(data=True):
            edge: Edge | None = state.pop("instance", None)
            if edge is not None:
                state.update(edge._to_xgraph_state())
        return xgraph

    @property
    def is_sorted(self) -> bool:
        """Whether this graph's tasks and dataset types are topologically
        sorted (with lexicographical tiebreakers).

        This may perform (and then discard) a full sort if `has_been_sorted` is
        `False`.  If the goal is to obtain a sorted graph, it is better to just
        call `sort` without guarding that with an ``if not graph.is_sorted``
        check.
        """
        if self._sorted_keys is not None:
            return True
        return all(
            sorted == unsorted
            for sorted, unsorted in zip(networkx.lexicographical_topological_sort(self._xgraph), self._xgraph)
        )

    @property
    def has_been_sorted(self) -> bool:
        """Whether this graph's tasks and dataset types have been
        topologically sorted (with lexicographical tiebreakers) since the last
        modification to the graph.

        This may return `False` if the graph *happens* to be sorted but `sort`
        was never called, but it is potentially much faster than `is_sorted`,z
        which may attempt (and then discard) a full sort if `has_been_sorted`
        is `False`.
        """
        return self._sorted_keys is not None

    def sort(self) -> None:
        """Sort this graph's nodes topologically with lexicographical
        tiebreakers.

        This does nothing if the graph is already definitely sorted.
        """
        if self._sorted_keys is None:
            try:
                sorted_keys: Sequence[NodeKey] = list(networkx.lexicographical_topological_sort(self._xgraph))
            except networkx.NetworkXUnfeasible as err:
                cycle = networkx.find_cycle(self._xgraph)
                raise PipelineDataCycleError(
                    f"Cycle detected while attempting to sort graph: {cycle}."
                ) from err
            self._reorder(sorted_keys)

    def producer_of(self, dataset_type_name: str) -> WriteEdge | None:
        """Return the `WriteEdge` that links the producing task to the named
        dataset type.

        Parameters
        ----------
        dataset_type_name : `str`
            Dataset type name.  Must not be a component.

        Returns
        -------
        edge : `WriteEdge` or `None`
            Producing edge or `None` if there isn't one in this graph.
        """
        for _, _, edge in self._xgraph.in_edges(
            NodeKey(NodeType.DATASET_TYPE, dataset_type_name), "instance"
        ):
            return edge
        return None

    def consumers_of(self, dataset_type_name: str) -> dict[str, ReadEdge]:
        """Return the `ReadEdge` objects that link the named dataset type to
        the tasks that consume it.

        Parameters
        ----------
        dataset_type_name : `str`
            Dataset type name.  Must not be a component.

        Returns
        -------
        edges : `dict` [ `str`, `ReadEdge` ]
            Edges to tasks that consume this object, with task labels as keys.
        """
        return {
            task_label: edge
            for task_label, _, edge in self._xgraph.out_edges(
                NodeKey(NodeType.DATASET_TYPE, dataset_type_name), "instance"
            )
        }

    def extract(self) -> ExtractHelper:
        """Create a new `MutablePipelineGraph` containing just the tasks that
        match the given criteria.
        """
        return ExtractHelper(self)

    def _reorder(self, sorted_keys: Sequence[NodeKey]) -> None:
        """Set the order of all views of this graph from the given sorted
        sequence of task labels and dataset type names.
        """
        self._sorted_keys = sorted_keys
        self._tasks._reorder(sorted_keys)
        self._dataset_types._reorder(sorted_keys)

    def _reset(self) -> None:
        """Reset the all views of this graph following a modification that
        might invalidate them.
        """
        self._sorted_keys = None
        self._tasks._reset()
        self._dataset_types._reset()

    @abstractmethod
    def copy(self: _P) -> _P:
        """Return a copy of this graph that copies all mutable state."""
        raise NotImplementedError()

    def __copy__(self: _P) -> _P:
        # Fully shallow copies are dangerous; we don't want shared mutable
        # state to lead to broken class invariants.
        return self.copy()

    def __deepcopy__(self: _P, memo: dict) -> _P:
        # Genuine deep copies are sometimes unnecessary, since we should only
        # ever care that mutable state is copied.
        return self.copy()

    @abstractmethod
    def resolved(self, registry: Registry, *, redo: bool = False) -> ResolvedPipelineGraph:
        """Return a version of this graph with all dimensions and dataset types
        resolved according to the given butler registry.

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Client for the data repository to resolve against.
        redo : `bool`, optional
            If `True`, re-do the resolution even if the graph has already been
            resolved to pick up changes in the registry.  If `False` (default)
            and the graph is already resolved, this method returns ``self``.

        Returns
        -------
        resolved : `ResolvedPipelineGraph`
            A resolved version of this graph.  Always sorted and immutable.

        Raises
        ------
        IncompatibleDatasetTypeError
            Raised if different tasks have different definitions of a dataset
            type.
        MissingDatasetTypeError
            Raised if a dataset type definition is required to exist in the
            data repository but none was found.  This should only occur for
            dataset types that are not produced by a task in the pipeline and
            are consumed with different storage classes or as components by
            tasks in the pipeline.
        """
        raise NotImplementedError()

    @abstractmethod
    def mutable_copy(self) -> MutablePipelineGraph:
        """Return a mutable copy of this graph.

        Returns
        -------
        mutable : `MutablePipelineGraph`
            A mutable copy of this graph.  This drops all dimension and
            dataset type resolutions that may be present in ``self``.  See
            docs for `MutablePipelineGraph` for details.
        """
        raise NotImplementedError()

    def _iter_task_defs(self) -> Iterator[TaskDef]:
        """Iterate over this pipeline as a sequence of `TaskDef` instances.

        Notes
        -----
        This is a package-private method intended to aid in the transition to a
        codebase more fully integrated with the `PipelineGraph` class, in which
        both `TaskDef` and `PipelineDatasetTypes` are expected to go away, and
        much of the functionality on the `Pipeline` class will be moved to
        `PipelineGraph` as well.
        """
        from ..pipeline import TaskDef

        for node in self._tasks.values():
            yield TaskDef(
                config=node.config,
                taskClass=node.task_class,
                label=node.label,
                connections=node._get_imported_data().connections,
            )

    def write_to_stream(self, stream: BinaryIO, basename: str = "pipeline", compression: str = "gz") -> None:
        """Write the pipeline to a file-like object.

        Parameters
        ----------
        stream
            File-like object opened for binary writing.
        basename : `str`, optional
            If provided, the name of the directory embedded in the saved tar
            file that holds all other embedded files.  Defaults to ``pipeline``
            (note that `write_to_uri` provides what is often a much better
            default for this parameter, since it has access to a filename).
        compression : `str`, optional
            How to compress output; defaults to ``"gz"``.  Any string supported
            as a ``mode`` suffix to to `tarfile.open` (including ``""`` for no
            compression), but by convention on-disk pipelines should use
            ``"gz"``.

        Notes
        -----
        See `write_to_uri` for a description of the file format.
        """
        # Can't get umask without also setting it, so call twice to reset.
        mask = os.umask(0o755)
        os.umask(mask)
        uid = os.getuid()
        gid = os.getgid()
        timestamp = int(datetime.now().timestamp())
        graph_bytes = json.dumps(self._serialize(), ensure_ascii=False, indent=2).encode("utf-8")
        with tarfile.open(fileobj=stream, mode=f"w|{compression}") as archive:
            dir_tar_info = tarfile.TarInfo(basename)
            dir_tar_info.type = tarfile.DIRTYPE
            dir_tar_info.uid = uid
            dir_tar_info.gid = gid
            dir_tar_info.mode = 0o777 & ~mask
            dir_tar_info.mtime = timestamp
            archive.addfile(dir_tar_info)
            graph_tar_info = tarfile.TarInfo(os.path.join(basename, "graph.json"))
            graph_tar_info.size = len(graph_bytes)
            graph_tar_info.uid = uid
            graph_tar_info.gid = gid
            graph_tar_info.mode = 0o666 & ~mask
            graph_tar_info.mtime = timestamp
            archive.addfile(graph_tar_info, io.BytesIO(graph_bytes))
            for label, task_node in self.tasks.items():
                config_bytes = task_node.config.saveToString().encode("utf-8")
                config_tar_info = tarfile.TarInfo(os.path.join(basename, f"{label}.py"))
                config_tar_info.size = len(config_bytes)
                config_tar_info.uid = uid
                config_tar_info.gid = gid
                config_tar_info.mode = 0o666 & ~mask
                config_tar_info.mtime = timestamp
                archive.addfile(config_tar_info, io.BytesIO(config_bytes))

    def write_to_uri(self, uri: ResourcePathExpression, basename: str | None = None) -> None:
        """Write the pipeline to a file given a URI.

        Parameters
        ----------
        uri
            Filename to write to; must be convertible to
            `lsst.resources.ResourcePath`.  May have ``.tar.gz`` or no
            extension (which will cause a ``.tar.gz`` extension to be added).
        basename : `str`, optional
            If provided, the name of the directory embedded in the saved tar
            file that holds all other embedded files.  Defaults to the last
            component in ``uri``, minus any extension: if
            ``uri="/a/b/c.tar.gz"``, ``basename="c"``.

        Notes
        -----
        The file format is intended to be human-readable.  It can be untarred
        to yield a directory with a straightforward ``graph.json`` file
        describing the graph itself along with one config ``.py`` file for each
        labeled task, e.g.::

            $ tar -xvzf pipeline.tar.gz
            pipeline/
            pipeline/graph.json
            pipeline/firstTask.py
            pipeline/anotherTask.py
            ...

        The file format should not be considered a stable public interface for
        outside code, which should always use `PipelineGraph` methods to read
        these files.
        """
        uri = ResourcePath(uri)
        extension = uri.getExtension()
        if not extension:
            uri = uri.updatedExtension(".tar.gz")
        elif extension != ".tar.gz":
            raise ValueError("Expanded pipeline files should always have a .tar.gz extension.")
        basename = uri.basename().removesuffix(".tar.gz")
        with uri.open(mode="wb") as stream:
            self.write_to_stream(cast(BinaryIO, stream), basename)

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        """Serialize the content of this graph into a dictionary of built-in
        objects suitable for JSON conversion.

        This should not include config files, as those are always serialized
        separately.
        """
        tasks = {}
        dataset_types = {}
        if self._sorted_keys:
            init_indices: dict[str, int] = {}
            for index, node_key in enumerate(self._sorted_keys):
                node: Node = self._xgraph.nodes[node_key]["instance"]
                match node_key.node_type:
                    case NodeType.TASK:
                        tasks[node_key.name] = node._serialize(
                            index=index, init_index=init_indices[node_key.name]
                        )
                    case NodeType.DATASET_TYPE:
                        dataset_types[node_key.name] = node._serialize(index=index)
                    case NodeType.TASK_INIT:
                        init_indices[node_key.name] = index
        else:
            tasks = {label: node._serialize() for label, node in self.tasks.items()}
            dataset_types = {name: node._serialize() for name, node in self.dataset_types.items()}
        return {
            "version": ".".join(str(item) for item in _IO_VERSION_INFO),
            "description": self.description,
            "tasks": tasks,
            "dataset_types": dataset_types,
            "labeled_subsets": {label: subset._serialize() for label, subset in self.labeled_subsets.items()},
            **kwargs,
        }


@final
class MutablePipelineGraph(PipelineGraph[TaskNode, DatasetTypeNode, MutableLabeledSubset]):
    """A pipeline graph that can be modified in place.

    Notes
    -----
    Mutable pipeline graphs are not automatically sorted and are not checked
    for cycles until they are sorted, but they do remember when they've been
    sorted so repeated calls to `sort` with no modifications in between are
    fast.

    Mutable pipeline graphs never carry around resolved dimensions and dataset
    types, since the process of resolving dataset types in particular depends
    in subtle ways on having the full graph available.  In other words, a graph
    that has its dataset types resolved as tasks are added to it could end up
    with different dataset types from a complete graph that is resolved all at
    once, and we don't want to deal with that kind of inconsistency.
    """

    @classmethod
    def read_stream(cls, stream: BinaryIO) -> MutablePipelineGraph:
        reader = MutablePipelineGraphReader()
        reader.read_stream(stream)
        result = MutablePipelineGraph.__new__(MutablePipelineGraph)
        result._init_from_args(reader.xgraph, reader.sort_keys, reader.labeled_subsets, reader.description)
        return result

    @classmethod
    def read_uri(cls, uri: ResourcePathExpression) -> MutablePipelineGraph:
        uri = ResourcePath(uri)
        with uri.open("rb") as stream:
            return cls.read_stream(cast(BinaryIO, stream))

    @property
    def description(self) -> str:
        # Docstring inherited.
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        # Docstring inherited.
        self._description = value

    def copy(self) -> MutablePipelineGraph:
        # Docstring inherited.
        xgraph = self._xgraph.copy()
        result = MutablePipelineGraph.__new__(MutablePipelineGraph)
        result._init_from_args(
            xgraph,
            self._sorted_keys,
            labeled_subsets={k: v._mutable_copy(xgraph) for k, v in self._labeled_subsets.items()},
            description=self._description,
        )
        return result

    def resolved(self, registry: Registry, *, redo: bool = False) -> ResolvedPipelineGraph:
        # Docstring inherited.
        xgraph = self._xgraph.copy()
        for state in xgraph.nodes.values():
            node: Node = state["instance"]
            state["instance"] = node._resolved(xgraph, registry)
        result = ResolvedPipelineGraph.__new__(ResolvedPipelineGraph)
        result._init_from_args(
            xgraph,
            self._sorted_keys,
            labeled_subsets={k: v._resolved(xgraph) for k, v in self._labeled_subsets.items()},
            description=self._description,
        )
        result.universe = registry.dimensions
        return result

    def mutable_copy(self) -> MutablePipelineGraph:
        # Docstring inherited.
        return self.copy()

    def add_task(
        self,
        label: str,
        task_class: type[PipelineTask],
        config: PipelineTaskConfig,
        connections: PipelineTaskConnections | None = None,
    ) -> None:
        """Add a new task to the graph.

        Parameters
        ----------
        label : `str`
            Label for the task in the pipeline.
        task_class : `type` [ `PipelineTask` ]
            Class object for the task.
        config : `PipelineTaskConfig`
            Configuration for the task.
        connections : `PipelineTaskConnections`, optional
            Object that describes the dataset types used by the task.  If not
            provided, one will be constructed from the given configuration.  If
            provided, it is assumed that ``config`` has already been validated
            and frozen.

        Raises
        ------
        ConnectionTypeConsistencyError
            Raised if the task defines a dataset type's ``is_init`` or
            ``is_prequisite`` flags in a way that is inconsistent with some
            other task in the graph.
        IncompatibleDatasetTypeError
            Raised if the task defines a dataset type differently from some
            other task in the graph.  Note that checks for dataset type
            dimension consistency do not occur until the graph is resolved.
        ValueError
            Raised if configuration validation failed when constructing.
            ``connections``.
        PipelineDataCycleError
            Raised if the graph is cyclic after this addition.
        RuntimeError
            Raised if an unexpected exception (which will be chained) occurred
            at a stage that may have left the graph in an inconsistent state.
            Other exceptions should leave the graph unchanged.
        """
        # Make the task node, the corresponding state dict that will be held
        # by the networkx graph (which includes the node instance), and the
        # state dicts for the edges
        task_node = TaskNode._from_imported_data(
            label, _TaskNodeImportedData.configure(label, task_class, config, connections)
        )
        node_data: list[tuple[NodeKey, dict[str, Any]]] = [
            (task_node.key, {"instance": task_node}),
            (task_node.init.key, {"instance": task_node.init}),
        ]
        # Convert the edge objects attached to the task node to networkx form.
        edge_data: list[tuple[NodeKey, NodeKey, dict[str, Any]]] = []
        for read_edge in task_node.init.iter_all_inputs():
            self._append_graph_from_edge(node_data, edge_data, read_edge)
        for write_edge in task_node.init.iter_all_outputs():
            self._append_graph_from_edge(node_data, edge_data, write_edge)
        for read_edge in task_node.prerequisite_inputs:
            self._append_graph_from_edge(node_data, edge_data, read_edge)
        for read_edge in task_node.inputs:
            self._append_graph_from_edge(node_data, edge_data, read_edge)
        for write_edge in task_node.iter_all_outputs():
            self._append_graph_from_edge(node_data, edge_data, write_edge)
        # Add a special edge (with no Edge instance) that connects the
        # TaskInitNode to the runtime TaskNode.
        edge_data.append((task_node.init.key, task_node.key, {"instance": None}))
        # Checks complete; time to start the actual modification, during which
        # it's hard to provide strong exception safety.
        self._reset()
        try:
            self._xgraph.add_nodes_from(node_data)
            self._xgraph.add_edges_from(edge_data)
            if not networkx.algorithms.dag.is_directed_acyclic_graph(self._xgraph):
                cycle = networkx.find_cycle(self._xgraph)
                raise PipelineDataCycleError(f"Cycle detected while adding task {label} graph: {cycle}.")
        except Exception:
            # First try to roll back our changes.
            try:
                self._xgraph.remove_edges_from(edge_data)
                self._xgraph.remove_nodes_from(key for key, _ in node_data)
            except Exception as err:
                raise RuntimeError(
                    "Error while attempting to revert PipelineGraph modification has left the graph in "
                    "an inconsistent state."
                ) from err
            # Successfully rolled back; raise the original exception.
            raise

    def add_task_subset(self, subset_label: str, task_labels: Iterable[str], description: str = "") -> None:
        """Add a label for a set of tasks that are already in the pipeline.

        Parameters
        ----------
        subset_label : `str`
            Label for this set of tasks.
        task_labels : `~collections.abc.Iterable` [ `str` ]
            Labels of the tasks to include in the set.  All must already be
            included in the graph.
        description : `str`, optional
            String description to associate with this label.
        """
        subset = MutableLabeledSubset(self._xgraph, subset_label, set(task_labels), description)
        self._labeled_subsets[subset_label] = subset

    def _append_graph_from_edge(
        self,
        node_data: list[tuple[NodeKey, dict[str, Any]]],
        edge_data: list[tuple[NodeKey, NodeKey, dict[str, Any]]],
        edge: Edge,
    ) -> None:
        """Append networkx state dictionaries for an edge and the corresponding
        dataset type node.

        Parameters
        ----------
        node_data : `list`
            List of node keys and state dictionaries.  A node is appended if
            one does not already exist for this dataset type.
        edge_data : `list`
            List of node key pairs and state dictionaries for edges.
        edge : `Edge`
            New edge being processed.
        """
        if (existing_dataset_type_state := self._xgraph.nodes.get(edge.dataset_type_key)) is not None:
            dataset_type_node = existing_dataset_type_state["instance"]
            edge._check_dataset_type(self._xgraph, dataset_type_node)
        else:
            dataset_type_node = DatasetTypeNode(edge.dataset_type_key)
            node_data.append((edge.dataset_type_key, {"instance": dataset_type_node}))
        edge_data.append(edge.key + ({"instance": edge},))


@final
class ResolvedPipelineGraph(PipelineGraph[ResolvedTaskNode, ResolvedDatasetTypeNode, ResolvedLabeledSubset]):
    """An immutable pipeline graph with resolved dimensions and dataset types.

    Resolved pipeline graphs are sorted at construction and cannot be modified,
    so calling `sort` on them does nothing.
    """

    def __init__(self, universe: DimensionUniverse) -> None:
        super().__init__()
        self.universe = universe

    def _init_from_args(
        self,
        xgraph: networkx.DiGraph | None = None,
        sorted_keys: Sequence[NodeKey] | None = None,
        labeled_subsets: dict[str, ResolvedLabeledSubset] | None = None,
        description: str = "",
    ) -> None:
        super()._init_from_args(xgraph, sorted_keys, labeled_subsets, description)
        super().sort()

    def sort(self) -> None:
        # Docstring inherited.
        assert self.is_sorted, "Sorted at construction and immutable."

    def copy(self) -> ResolvedPipelineGraph:
        # Docstring inherited.
        # Immutable types shouldn't actually be copied, since there's nothing
        # one could do with the copy that couldn't be done with the original.
        return self

    def resolved(self, registry: Registry, *, redo: bool = False) -> ResolvedPipelineGraph:
        # Docstring inherited.
        if redo:
            return self.mutable_copy().resolved(registry)
        return self

    def mutable_copy(self) -> MutablePipelineGraph:
        # Docstring inherited.
        xgraph = self._xgraph.copy()
        for state in xgraph.nodes.values():
            node: Node = state["instance"]
            state["instance"] = node._unresolved()
        result = MutablePipelineGraph.__new__(MutablePipelineGraph)
        result._init_from_args(
            xgraph,
            self._sorted_keys,
            labeled_subsets={k: v._mutable_copy(xgraph) for k, v in self._labeled_subsets.items()},
            description=self._description,
        )
        return result

    def group_by_dimensions(
        self, prerequisites: bool = False
    ) -> dict[DimensionGraph, tuple[list[ResolvedTaskNode], list[ResolvedDatasetTypeNode]]]:
        """Group this graph's tasks and runtime dataset types by their
        dimensions.

        Parameters
        ----------
        prerequisites : `bool`, optional
            If `True`, include prerequisite dataset types as well as regular
            input and output datasets (including intermediates).

        Returns
        -------
        groups : `dict` [ `DimensionGraph`, `tuple` ]
            A dictionary of groups keyed by `DimensionGraph`, which each value
            a tuple of:

            - a `list` of `ResolvedTaskNode` instances
            - a `list` of `ResolvedDatasetTypeNode` instances

            that have those dimensions.

        Notes
        -----
        Init inputs and outputs are always included, but always have empty
        dimensions and are hence easily filtered out.
        """
        result: dict[DimensionGraph, tuple[list[ResolvedTaskNode], list[ResolvedDatasetTypeNode]]] = {}
        next_new_value: tuple[list[ResolvedTaskNode], list[ResolvedDatasetTypeNode]] = ([], [])
        for task_node in self.tasks.values():
            if (group := result.setdefault(task_node.dimensions, next_new_value)) is next_new_value:
                next_new_value = ([], [])  # make new lists for next time
            group[0].append(task_node)
        for dataset_type_node in self.dataset_types.values():
            if not dataset_type_node.is_prerequisite or prerequisites:
                if (
                    group := result.setdefault(dataset_type_node.dataset_type.dimensions, next_new_value)
                ) is next_new_value:
                    next_new_value = ([], [])  # make new lists for next time
                group[1].append(dataset_type_node)
        return result

    def _serialize(self, **kwargs: Any) -> dict[str, Any]:
        # Docstring inherited.
        return super()._serialize(dimensions=self.universe.dimensionConfig.toDict(), **kwargs)

    @classmethod
    def read_stream(cls, stream: BinaryIO) -> ResolvedPipelineGraph:
        reader = ResolvedPipelineGraphReader()
        reader.read_stream(stream)
        result = ResolvedPipelineGraph.__new__(ResolvedPipelineGraph)
        result._init_from_args(reader.xgraph, reader.sort_keys, reader.labeled_subsets, reader.description)
        result.universe = reader.universe
        return result

    @classmethod
    def read_uri(cls, uri: ResourcePathExpression) -> ResolvedPipelineGraph:
        uri = ResourcePath(uri)
        with uri.open("rb") as stream:
            return cls.read_stream(cast(BinaryIO, stream))
