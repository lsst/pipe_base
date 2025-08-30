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

from __future__ import annotations

__all__ = (
    "BipartiteEdgeInfo",
    "DatasetInfo",
    "HeaderModel",
    "QuantumInfo",
)

import datetime
import getpass
import sys
import uuid
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypeAlias, TypedDict

import networkx
import networkx.algorithms.bipartite
import pydantic

from lsst.daf.butler import (
    DataCoordinate,
    DataIdValue,
    DatasetType,
)

from ..pipeline_graph import (
    Edge,
    NodeBipartite,
    PipelineGraph,
    TaskNode,
)

if TYPE_CHECKING:
    from ..graph import QuantumGraph


if TYPE_CHECKING:
    from ..graph import QuantumGraph


# These aliases make it a lot easier how the various pydantic models are
# structured, but they're too verbose to be worth exporting to code outside the
# quantum_graph subpackage.
TaskLabel: TypeAlias = str
DatasetTypeName: TypeAlias = str
ConnectionName: TypeAlias = str
QuantumIndex: TypeAlias = int
DatastoreName: TypeAlias = str
DimensionElementName: TypeAlias = str
DataCoordinateValues: TypeAlias = list[DataIdValue]


class IncompleteQuantumGraphError(RuntimeError):
    pass


class HeaderModel(pydantic.BaseModel):
    """Data model for the header of a predicted quantum graph file."""

    version: int = 0
    """File format / data model version number."""

    graph_type: str = "predicted"
    """Type of quantum graph stored in this file."""

    inputs: list[str] = pydantic.Field(default_factory=list)
    """List of input collections used to build the quantum graph."""

    output: str | None = ""
    """Output CHAINED collection provided when building the quantum graph."""

    output_run: str = ""
    """Output RUN collection for all output datasets in this graph."""

    user: str = pydantic.Field(default_factory=getpass.getuser)
    """Username of the process that built this quantum graph."""

    timestamp: datetime.datetime = pydantic.Field(default_factory=datetime.datetime.now)
    """Timestamp for when this quantum graph was built.

    It is unspecified exactly which point during quantum-graph generation this
    timestamp is recorded.
    """

    command: str = pydantic.Field(default_factory=lambda: " ".join(sys.argv))
    """Command-line invocation that created this graph."""

    metadata: dict[str, Any] = pydantic.Field(default_factory=dict)
    """Free-form metadata associated with this quantum graph at build time."""

    int_size: int = 8
    """Number of bytes in the integers used in this file's multi-block and
    address files.
    """

    n_quanta: int = 0
    """Total number of quanta in this graph.

    This does not include special "init" quanta, but it does include quanta
    that were not loaded in a partial read (except when reading from an old
    quantum graph file).
    """

    n_datasets: int = 0
    """Total number of distinct datasets in the full graph.  This includes
    datasets whose related quanta were not loaded in a partial read (except
    when reading from an old quantum graph file).
    """

    n_task_quanta: dict[TaskLabel, int] = pydantic.Field(default_factory=dict)
    """Number of quanta for each task label.

    This does not include special "init" quanta, but it does include quanta
    that were not loaded in a partial read (except when reading from an old
    quantum graph file).
    """

    @classmethod
    def from_old_quantum_graph(cls, old_quantum_graph: QuantumGraph) -> HeaderModel:
        """Extract a header from an old `QuantumGraph` instance.

        Parameters
        ----------
        old_quantum_graph : `QuantumGraph`
            Quantum graph to extract a header from.

        Returns
        -------
        header : `PredictedHeaderModel`
            Header for a new predicted quantum graph.
        """
        metadata = dict(old_quantum_graph.metadata)
        metadata.pop("packages", None)
        if (time_str := metadata.pop("time", None)) is not None:
            timestamp = datetime.datetime.fromisoformat(time_str)
        else:
            timestamp = datetime.datetime.now()
        return cls(
            inputs=list(metadata.pop("input", []) or []),  # Guard against explicit None and missing key.
            output=metadata.pop("output", None),
            output_run=metadata.pop("output_run", ""),
            user=metadata.pop("user", ""),
            command=metadata.pop("full_command", ""),
            timestamp=timestamp,
            metadata=metadata,
        )

    def to_old_metadata(self) -> dict[str, Any]:
        """Return a dictionary using the key conventions used in old quantum
        graph files.
        """
        result = self.metadata.copy()
        result["input"] = self.inputs
        result["output"] = self.output
        result["output_run"] = self.output_run
        result["full_command"] = self.command
        result["user"] = self.user
        result["time"] = str(self.timestamp)
        return result

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc, override]
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)


class QuantumInfo(TypedDict):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    node data for a quantum.

    Since NetworkX types are not generic over their node mapping type, this has
    to be used explicitly, e.g.::

        node_data: QuantumInfo = xgraph.nodes[quantum_id]

    where ``xgraph`` can be either `BaseQuantumGraph.quantum_only_xgraph`
    or `BaseQuantumGraph.bipartite_xgraph`.
    """

    data_id: DataCoordinate
    """Data ID of the quantum."""

    task_label: str
    """Label of the task for this quantum."""

    task_node: TaskNode
    """Node in the pipeline graph for this quantum's task."""

    bipartite: NotRequired[Literal[NodeBipartite.TASK_OR_QUANTUM]]
    """Whether this is a quantum node or a dataset type node/

    This attribute is only present on the nodes of bipartite graphs.
    """


class DatasetInfo(TypedDict):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    node data for a dataset.

    Since NetworkX types are not generic over their node mapping type, this has
    to be used explicitly, e.g.::

        node_data: DatasetInfo = xgraph.nodes[dataset_id]

    where ``xgraph`` is from the `BaseQuantumGraph.bipartite_xgraph` property.
    """

    data_id: DataCoordinate
    """Data ID of the dataset."""

    dataset_type: DatasetType
    """Type of this dataset.

    This is always the general dataset type that matches the data repository
    storage class, which may differ from any particular task-adapted dataset
    type whose storage class has been overridden to match the task connections.
    This means is it never a component.
    """

    run: str
    """Name of the `~lsst.daf.butler.CollectionType.RUN` collection that holds
    or will hold this dataset.
    """

    bipartite: NotRequired[Literal[NodeBipartite.DATASET_OR_TYPE]]
    """Whether this is a quantum node or a dataset type node/

    This attribute is only present on the nodes of bipartite graphs.
    """


class BipartiteEdgeInfo(TypedDict):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    edge data in a bipartite graph.
    """

    is_read: bool
    """`True` if this is a dataset -> quantum edge; `False` if it is a
    quantum -> dataset edge.
    """

    pipeline_edge: Edge
    """Corresponding edge in the pipeline graph."""


class BaseQuantumGraph(ABC):
    """An abstract base for quantum graphs.

    Parameters
    ----------
    header : `HeaderModel`
        Structured metadata for the graph.
    pipeline_graph : `..pipeline_graph.PipelineGraph`
        Graph of tasks and dataset types.  May contain a superset of the tasks
        and dataset types that actually have quanta and datasets in the quantum
        graph.
    """

    def __init__(self, header: HeaderModel, pipeline_graph: PipelineGraph):
        self.header = header
        self.pipeline_graph = pipeline_graph

    @property
    @abstractmethod
    def quanta_by_task(self) -> Mapping[str, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested mapping of all quanta, keyed first by task name and then by
        data ID.

        Notes
        -----
        Partial loads may not fully populate this mapping, but it can always
        be accessed.

        The returned object may be an internal dictionary; as the type
        annotation indicates, it should not be modified in place.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def datasets_by_type(self) -> Mapping[str, Mapping[DataCoordinate, uuid.UUID]]:
        """A nested mapping of all datasets, keyed first by dataset type name
        and then by data ID.

        Notes
        -----
        Partial loads may not fully populate this mapping, but it can always
        be accessed.

        The returned object may be an internal dictionary; as the type
        annotation indicates, it should not be modified in place.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def quantum_only_xgraph(self) -> networkx.DiGraph:
        """A directed acyclic graph with quanta as nodes and datasets elided.

        Notes
        -----
        Partial loads may not fully populate this graph, but it can always be
        accessed.

        Node state dictionaries are described by the `QuantumInfo` type
        (or a subtype thereof).

        The returned object is a read-only view of an internal one.
        """
        raise NotImplementedError()

    @property
    @abstractmethod
    def bipartite_xgraph(self) -> networkx.MultiDiGraph:
        """A directed acyclic graph with quantum and dataset nodes.

        This graph never includes init-input and init-output datasets.

        Notes
        -----
        Partial loads may not fully populate this graph, but it can always be
        accessed.

        Node state dictionaries are described by the `QuantumInfo` and
        `DatasetInfo` types (or a subtypes thereof).  Edges are keyed by their
        connection name, and have state dictionaries described by
        `BipartiteEdgeInfo`.

        The returned object is a read-only view of an internal one.
        """
        raise NotImplementedError()
