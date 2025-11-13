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
    "FORMAT_VERSION",
    "BaseQuantumGraph",
    "BaseQuantumGraphReader",
    "BipartiteEdgeInfo",
    "DatasetInfo",
    "HeaderModel",
    "QuantumInfo",
)
import dataclasses
import datetime
import getpass
import os
import sys
import uuid
import zipfile
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Self,
    TypeAlias,
    TypedDict,
    TypeVar,
)

import networkx
import networkx.algorithms.bipartite
import pydantic
import zstandard

from lsst.daf.butler import DataCoordinate, DataIdValue
from lsst.daf.butler._rubin import generate_uuidv7
from lsst.resources import ResourcePath, ResourcePathExpression

from ..pipeline_graph import DatasetTypeNode, Edge, PipelineGraph, TaskImportMode, TaskNode
from ..pipeline_graph.io import SerializedPipelineGraph
from ._multiblock import (
    DEFAULT_PAGE_SIZE,
    AddressReader,
    AddressWriter,
    Compressor,
    Decompressor,
)

if TYPE_CHECKING:
    from ..graph import QuantumGraph


# These aliases make it a lot easier how the various pydantic models are
# structured, but they're too verbose to be worth exporting to code outside the
# quantum_graph subpackage.
TaskLabel: TypeAlias = str
DatasetTypeName: TypeAlias = str
ConnectionName: TypeAlias = str
DatasetIndex: TypeAlias = int
QuantumIndex: TypeAlias = int
DatastoreName: TypeAlias = str
DimensionElementName: TypeAlias = str
DataCoordinateValues: TypeAlias = list[DataIdValue]


_T = TypeVar("_T", bound=pydantic.BaseModel)

FORMAT_VERSION: int = 1
"""
File format version number for new files.

This applies to both predicted and provenance QGs, since they usually change
in concert.

CHANGELOG:

- 0: Initial version.
- 1: Switched from internal integer IDs to UUIDs in all models.
"""


class IncompleteQuantumGraphError(RuntimeError):
    pass


class HeaderModel(pydantic.BaseModel):
    """Data model for the header of a quantum graph file."""

    version: int = FORMAT_VERSION
    """File format / data model version number."""

    graph_type: str = ""
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

    provenance_dataset_id: uuid.UUID = pydantic.Field(default_factory=generate_uuidv7)
    """The dataset ID for provenance quantum graph when it is ingested into
    a butler repository.
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
    if "sphinx" in sys.modules and not TYPE_CHECKING:

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

        @classmethod
        def model_validate(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate`."""
            return super().model_validate(*args, **kwargs)

        @classmethod
        def model_validate_json(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_json`."""
            return super().model_validate_json(*args, **kwargs)

        @classmethod
        def model_validate_strings(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_validate_strings`."""
            return super().model_validate_strings(*args, **kwargs)


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

    pipeline_node: TaskNode
    """Node in the pipeline graph for this quantum's task."""


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

    dataset_type_name: DatasetTypeName
    """Name of the type of this dataset.

    This is always the general dataset type that matches the data repository
    storage class, which may differ from any particular task-adapted dataset
    type whose storage class has been overridden to match the task connections.
    This means is it never a component.
    """

    run: str
    """Name of the `~lsst.daf.butler.CollectionType.RUN` collection that holds
    or will hold this dataset.
    """

    pipeline_node: DatasetTypeNode
    """Node in the pipeline graph for this dataset's type."""


class BipartiteEdgeInfo(TypedDict):
    """A typed dictionary that annotates the attributes of the NetworkX graph
    edge data in a bipartite graph.
    """

    is_read: bool
    """`True` if this is a dataset -> quantum edge; `False` if it is a
    quantum -> dataset edge.
    """

    pipeline_edges: list[Edge]
    """Corresponding edges in the pipeline graph.

    Note that there may be more than one pipeline edge since a quantum can
    consume a particular dataset via multiple connections.
    """


class BaseQuantumGraph(ABC):
    """An abstract base for quantum graphs.

    Parameters
    ----------
    header : `HeaderModel`
        Structured metadata for the graph.
    pipeline_graph : `.pipeline_graph.PipelineGraph`
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
    def bipartite_xgraph(self) -> networkx.DiGraph:
        """A directed acyclic graph with quantum and dataset nodes.

        Notes
        -----
        Partial loads may not fully populate this graph, but it can always be
        accessed.

        Node state dictionaries are described by the `QuantumInfo` and
        `DatasetInfo` types (or a subtypes thereof).  Edges have state
        dictionaries described by `BipartiteEdgeInfo`.

        The returned object is a read-only view of an internal one.
        """
        raise NotImplementedError()


@dataclasses.dataclass
class BaseQuantumGraphWriter:
    """A helper class for writing quantum graphs."""

    zf: zipfile.ZipFile
    """The zip archive that represents the quantum graph on disk."""

    compressor: Compressor
    """A compressor for all compressed JSON blocks."""

    address_writer: AddressWriter
    """A helper object for reading addresses into the multi-block files."""

    int_size: int
    """Size (in bytes) used to write integers to binary files."""

    @classmethod
    @contextmanager
    def open(
        cls,
        uri: ResourcePathExpression,
        header: HeaderModel,
        pipeline_graph: PipelineGraph,
        indices: dict[uuid.UUID, int],
        *,
        address_filename: str,
        compressor: Compressor,
        cdict_data: bytes | None = None,
    ) -> Iterator[Self]:
        uri = ResourcePath(uri)
        address_writer = AddressWriter(indices)
        with uri.open(mode="wb") as stream:
            with zipfile.ZipFile(stream, mode="w", compression=zipfile.ZIP_STORED) as zf:
                self = cls(zf, compressor, address_writer, header.int_size)
                self.write_single_model("header", header)
                if cdict_data is not None:
                    zf.writestr("compression_dict", cdict_data)
                self.write_single_model("pipeline_graph", SerializedPipelineGraph.serialize(pipeline_graph))
                yield self
                address_writer.write_to_zip(zf, address_filename, int_size=self.int_size)

    def write_single_model(self, name: str, model: pydantic.BaseModel) -> None:
        """Write a single compressed JSON block as a 'file' in a zip archive.

        Parameters
        ----------
        name : `str`
            Base name of the file.  An extension will be added.
        model : `pydantic.BaseModel`
            Pydantic model to convert to JSON.
        """
        json_data = model.model_dump_json().encode()
        self.write_single_block(name, json_data)

    def write_single_block(self, name: str, json_data: bytes) -> None:
        """Write a single compressed JSON block as a 'file' in a zip archive.

        Parameters
        ----------
        name : `str`
            Base name of the file.  An extension will be added.
        json_data : `bytes`
            Raw JSON to compress and write.
        """
        json_data = self.compressor.compress(json_data)
        self.zf.writestr(f"{name}.json.zst", json_data)


@dataclasses.dataclass
class BaseQuantumGraphReader:
    """A helper class for reading quantum graphs."""

    header: HeaderModel
    """Header metadata for the quantum graph."""

    pipeline_graph: PipelineGraph
    """Graph of tasks and dataset type names that appear in the quantum
    graph.
    """

    zf: zipfile.ZipFile
    """The zip archive that represents the quantum graph on disk."""

    decompressor: Decompressor
    """A decompressor for all compressed JSON blocks."""

    address_reader: AddressReader
    """A helper object for reading addresses into the multi-block files."""

    page_size: int
    """Approximate number of bytes to read at a time.

    Note that this does not set a page size for *all* reads, but it
    does affect the smallest, most numerous reads.
    """

    @classmethod
    @contextmanager
    def _open(
        cls,
        uri: ResourcePathExpression,
        *,
        address_filename: str,
        graph_type: str,
        n_addresses: int,
        page_size: int | None = None,
        import_mode: TaskImportMode = TaskImportMode.ASSUME_CONSISTENT_EDGES,
    ) -> Iterator[Self]:
        """Construct a reader from a URI.

        Parameters
        ----------
        uri : convertible to `lsst.resources.ResourcePath`
            URI to open.  Should have a ``.qg`` extension.
        address_filename : `str`
            Base filename for the address file.
        graph_type : `str`
            Value to expect for `HeaderModel.graph_type`.
        n_addresses : `int`
            Number of addresses to expect per row in the address file.
        page_size : `int`, optional
            Approximate number of bytes to read at once from address files.
            Note that this does not set a page size for *all* reads, but it
            does affect the smallest, most numerous reads.  When `None`, the
            ``LSST_QG_PAGE_SIZE`` environment variable is checked before
            falling back to a default of 5MB.
        import_mode : `..pipeline_graph.TaskImportMode`, optional
            How to handle importing the task classes referenced in the pipeline
            graph.

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ \
                `PredictedQuantumGraphReader` ]
            A context manager that returns the reader when entered.
        """
        if page_size is None:
            page_size = int(os.environ.get("LSST_QG_PAGE_SIZE", DEFAULT_PAGE_SIZE))
        uri = ResourcePath(uri)
        cdict: zstandard.ZstdCompressionDict | None = None
        with uri.open(mode="rb") as zf_stream:
            with zipfile.ZipFile(zf_stream, "r") as zf:
                if (cdict_path := zipfile.Path(zf, "compression_dict")).exists():
                    cdict = zstandard.ZstdCompressionDict(cdict_path.read_bytes())
                decompressor = zstandard.ZstdDecompressor(cdict)
                header = cls._read_single_block_static("header", HeaderModel, zf, decompressor)
                if not header.graph_type == graph_type:
                    raise TypeError(f"Header is for a {header.graph_type!r} graph, not {graph_type!r} graph.")
                serialized_pipeline_graph = cls._read_single_block_static(
                    "pipeline_graph", SerializedPipelineGraph, zf, decompressor
                )
                pipeline_graph = serialized_pipeline_graph.deserialize(import_mode)
                with AddressReader.open_in_zip(
                    zf,
                    address_filename,
                    page_size=page_size,
                    int_size=header.int_size,
                    n_addresses=n_addresses,
                ) as address_reader:
                    yield cls(
                        header=header,
                        pipeline_graph=pipeline_graph,
                        zf=zf,
                        decompressor=decompressor,
                        address_reader=address_reader,
                        page_size=page_size,
                    )

    @staticmethod
    def _read_single_block_static(
        name: str, model_type: type[_T], zf: zipfile.ZipFile, decompressor: Decompressor
    ) -> _T:
        """Read a single compressed JSON block from a 'file' in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to read the file from.
        name : `str`
            Base name of the file.  An extension will be added.
        model_type : `type` [ `pydantic.BaseModel` ]
            Pydantic model to validate JSON with.
        decompressor : `Decompressor`
            Object with a `decompress` method that takes and returns `bytes`.

        Returns
        -------
        model : `pydantic.BaseModel`
            Validated model.
        """
        compressed_data = zf.read(f"{name}.json.zst")
        json_data = decompressor.decompress(compressed_data)
        return model_type.model_validate_json(json_data)

    def _read_single_block(self, name: str, model_type: type[_T]) -> _T:
        """Read a single compressed JSON block from a 'file' in a zip archive.

        Parameters
        ----------
        name : `str`
            Base name of the file.  An extension will be added.
        model_type : `type` [ `pydantic.BaseModel` ]
            Pydantic model to validate JSON with.

        Returns
        -------
        model : `pydantic.BaseModel`
            Validated model.
        """
        return self._read_single_block_static(name, model_type, self.zf, self.decompressor)

    def _read_single_block_raw(self, name: str) -> bytes:
        """Read a single compressed block from a 'file' in a zip archive.

        Parameters
        ----------
        name : `str`
            Base name of the file.  An extension will be added.

        Returns
        -------
        data : `bytes`
            Decompressed bytes.
        """
        compressed_data = self.zf.read(f"{name}.json.zst")
        return self.decompressor.decompress(compressed_data)
