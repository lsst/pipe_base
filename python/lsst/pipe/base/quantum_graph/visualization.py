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

__all__ = ("QuantumGraphDotVisualizer", "QuantumGraphMermaidVisualizer", "QuantumGraphVisualizer")

import html
import uuid
from abc import abstractmethod
from typing import IO, ClassVar, Generic, TypeVar

from ..pipeline_graph import NodeType
from ._common import BaseQuantumGraph, BipartiteEdgeInfo, DatasetInfo, QuantumInfo

_G = TypeVar("_G", bound=BaseQuantumGraph, contravariant=True)
_Q = TypeVar("_Q", bound=QuantumInfo, contravariant=True)
_D = TypeVar("_D", bound=DatasetInfo, contravariant=True)


class QuantumGraphVisualizer(Generic[_G, _Q, _D]):
    """A base class for exporting quantum graphs to graph-visualization
    languages.

    Notes
    -----
    This base class is not intended to support implementations for every
    possible visualization language, but it does neatly unify common logic for
    at least GraphViz and Mermaid.
    """

    @abstractmethod
    def render_header(self, qg: _G, is_bipartite: bool) -> str:
        """Return the beginning of a graph visualization.

        Parameters
        ----------
        qg : `.BaseQuantumGraph`
            Quantum graph to visualize.
        is_bipartite : `bool`
            Whether a bipartite graph visualization is being requested.

        Returns
        -------
        rendered : `str`
            String that starts the visualization.  May contain newlines, but
            an additional newline will automatically be added after the string,
            before the first node.
        """
        raise NotImplementedError()

    @abstractmethod
    def render_footer(self, qg: _G, is_bipartite: bool) -> str:
        """Return the ending of a graph visualization.

        Parameters
        ----------
        qg : `.BaseQuantumGraph`
            Quantum graph to visualize.
        is_bipartite : `bool`
            Whether a bipartite graph visualization is being requested.

        Returns
        -------
        rendered : `str`
            String that ends the visualization.  May contain newlines, but
            an additional newline will automatically be added after the string,
            at the end of the file.
        """
        raise NotImplementedError()

    @abstractmethod
    def render_quantum(self, quantum_id: uuid.UUID, data: _Q, is_bipartite: bool) -> str:
        """Return the representation of a quantum in a graph visualization.

        Parameters
        ----------
        quantum_id : `uuid.UUID`
            ID of the quantum.
        data : `.QuantumInfo`
            Mapping with additional information about the quantum.
        is_bipartite : `bool`
            Whether a bipartite graph visualization is being requested.

        Returns
        -------
        rendered : `str`
            String that represents the quantum.  May contain newlines, but an
            additional newline will automatically be added after the string,
            before the next node or edge.
        """
        raise NotImplementedError()

    @abstractmethod
    def render_dataset(self, dataset_id: uuid.UUID, data: _D) -> str:
        """Return the representation of a dataset in a graph visualization.

        Parameters
        ----------
        dataset_id : `uuid.UUID`
            ID of the dataset.
        data : `.DatasetInfo`
            Mapping with additional information about the dataset.

        Returns
        -------
        rendered : `str`
            String that represents the dataset.  May contain newlines, but an
            additional newline will automatically be added after the string,
            before the next node or edge.
        """
        raise NotImplementedError()

    @abstractmethod
    def render_edge(
        self,
        a: uuid.UUID,
        b: uuid.UUID,
        data: BipartiteEdgeInfo | None,
    ) -> str:
        """Return the representation of an edge a graph visualization.

        Parameters
        ----------
        a : `uuid.UUID`
            ID of the quantum or dataset for which this is an outgoing edge.
        b : `uuid.UUID`
            ID of the quantum or dataset for which this is an incoming edge.
        data : `.BipartiteEdgeInfo`
            Mapping with additional information about the dataset.

        Returns
        -------
        rendered : `str`
            String that represents the edge.  May contain newlines, but an
            additional newline will automatically be added after the string,
            before the next edge.
        """
        raise NotImplementedError()

    def write_quantum_only(self, qg: _G, stream: IO[str]) -> None:
        """Write a visualization for graph with only quantum nodes.

        Parameters
        ----------
        qg : `BaseQuantumGraph`
            Quantum graph to visualize.
        stream : `typing.IO`
            File-like object to write to.
        """
        print(self.render_header(qg, is_bipartite=False), file=stream)
        xgraph = qg.quantum_only_xgraph
        for node_id, node_data in xgraph.nodes.items():
            print(self.render_quantum(node_id, node_data, is_bipartite=False), file=stream)
        for a, b in xgraph.edges:
            print(self.render_edge(a, b, data=None), file=stream)
        print(self.render_footer(qg, is_bipartite=False), file=stream)

    def write_bipartite(self, qg: _G, stream: IO[str]) -> None:
        """Write a visualization for graph with both quantum and dataset nodes.

        Parameters
        ----------
        qg : `BaseQuantumGraph`
            Quantum graph to visualize.
        stream : `typing.IO`
            File-like object to write to.
        """
        print(self.render_header(qg, is_bipartite=True), file=stream)
        xgraph = qg.bipartite_xgraph
        for node_id, node_data in xgraph.nodes.items():
            match node_data["pipeline_node"].key.node_type:
                case NodeType.TASK:
                    print(self.render_quantum(node_id, node_data, is_bipartite=True), file=stream)
                case NodeType.DATASET_TYPE:
                    print(self.render_dataset(node_id, node_data), file=stream)
        for a, b, edge_data in xgraph.edges(data=True):
            print(self.render_edge(a, b, edge_data), file=stream)
        print(self.render_footer(qg, is_bipartite=True), file=stream)


class QuantumGraphDotVisualizer(QuantumGraphVisualizer[BaseQuantumGraph, QuantumInfo, DatasetInfo]):
    """A visualizer for quantum graphs in the GraphViz dot language."""

    def render_header(self, qg: BaseQuantumGraph, is_bipartite: bool) -> str:
        return "\n".join(
            [
                "digraph QuantumGraph {",
                self._render_default("graph", self._ATTRIBS["defaultGraph"]),
                self._render_default("node", self._ATTRIBS["defaultNode"]),
                self._render_default("edge", self._ATTRIBS["defaultEdge"]),
            ]
        )

    def render_footer(self, qg: BaseQuantumGraph, is_bipartite: bool) -> str:
        return "}"

    def render_quantum(self, quantum_id: uuid.UUID, data: QuantumInfo, is_bipartite: bool) -> str:
        labels = [f"{quantum_id}", html.escape(data["task_label"])]
        data_id = data["data_id"]
        labels.extend(f"{key} = {value}" for key, value in data_id.required.items())
        return self._render_node(self._make_node_id(quantum_id), "quantum", labels)

    def render_dataset(self, dataset_id: uuid.UUID, data: DatasetInfo) -> str:
        labels = [html.escape(data["dataset_type_name"]), f"run: {data['run']!r}"]
        data_id = data["data_id"]
        labels.extend(f"{key} = {value}" for key, value in data_id.required.items())
        return self._render_node(self._make_node_id(dataset_id), "dataset", labels)

    def render_edge(self, a: uuid.UUID, b: uuid.UUID, data: BipartiteEdgeInfo | None) -> str:
        return f'"{self._make_node_id(a)}" -> "{self._make_node_id(b)}";'

    _ATTRIBS: ClassVar = dict(
        defaultGraph=dict(splines="ortho", nodesep="0.5", ranksep="0.75", pad="0.5"),
        defaultNode=dict(shape="box", fontname="Monospace", fontsize="14", margin="0.2,0.1", penwidth="3"),
        defaultEdge=dict(color="black", arrowsize="1.5", penwidth="1.5"),
        task=dict(style="filled", color="black", fillcolor="#B1F2EF"),
        quantum=dict(style="filled", color="black", fillcolor="#B1F2EF"),
        dsType=dict(style="rounded,filled,bold", color="#00BABC", fillcolor="#F5F5F5"),
        dataset=dict(style="rounded,filled,bold", color="#00BABC", fillcolor="#F5F5F5"),
    )

    def _render_default(self, type: str, attribs: dict[str, str]) -> str:
        """Set default attributes for a given type."""
        default_attribs = ", ".join([f'{key}="{val}"' for key, val in attribs.items()])
        return f"{type} [{default_attribs}];"

    def _render_node(self, name: str, style: str, labels: list[str]) -> str:
        """Render GV node"""
        label = r"</TD></TR><TR><TD>".join(labels)
        attrib_dict = dict(self._ATTRIBS[style], label=label)
        pre = '<<TABLE BORDER="0" CELLPADDING="5"><TR><TD>'
        post = "</TD></TR></TABLE>>"
        attrib = ", ".join(
            [
                f'{key}="{val}"' if key != "label" else f"{key}={pre}{val}{post}"
                for key, val in attrib_dict.items()
            ]
        )
        return f'"{name}" [{attrib}];'

    def _make_node_id(self, node_id: uuid.UUID) -> str:
        """Return a GV node ID from a quantum or dataset UUID."""
        return f"u{node_id.hex}"


class QuantumGraphMermaidVisualizer(QuantumGraphVisualizer[BaseQuantumGraph, QuantumInfo, DatasetInfo]):
    """A visualizer for quantum graphs in the Mermaid language."""

    def render_header(self, qg: BaseQuantumGraph, is_bipartite: bool) -> str:
        return "flowchart TD"

    def render_footer(self, qg: BaseQuantumGraph, is_bipartite: bool) -> str:
        return ""

    def render_quantum(self, quantum_id: uuid.UUID, data: QuantumInfo, is_bipartite: bool) -> str:
        label_lines = [f"**{data['task_label']}**", f"ID: {quantum_id}"]
        for k, v in data["data_id"].required.items():
            label_lines.append(f"{k}={v}")
        label = "<br>".join(label_lines)
        return f'{self._make_node_id(quantum_id)}["{label}"]'

    def render_dataset(self, dataset_id: uuid.UUID, data: DatasetInfo) -> str:
        label_lines = [
            f"**{data['dataset_type_name']}**",
            f"ID: {dataset_id}",
            f"run: {data['run']}",
        ]
        for k, v in data["data_id"].required.items():
            label_lines.append(f"{k}={v}")
        label = "<br>".join(label_lines)
        return f'{self._make_node_id(dataset_id)}["{label}"]'

    def render_edge(self, a: uuid.UUID, b: uuid.UUID, data: BipartiteEdgeInfo | None) -> str:
        return f"{self._make_node_id(a)} --> {self._make_node_id(b)}"

    def _make_node_id(self, node_id: uuid.UUID) -> str:
        return f"u{node_id.hex}"
