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

__all__ = ("show_dot",)

import html
import sys
from collections.abc import Mapping
from typing import Any, TextIO

from .._nodes import NodeType
from .._pipeline_graph import PipelineGraph
from ._formatting import DisplayNodeKey, format_dimensions, format_task_class
from ._options import NodeAttributeOptions
from ._show import parse_display_args

_NODE_LABEL_POINT_SIZE = "18"
_ATTRIBS = {
    NodeType.TASK: dict(style="filled", color="black", fillcolor="#B1F2EF"),
    NodeType.DATASET_TYPE: dict(style="rounded,filled,bold", color="#00BABC", fillcolor="#F5F5F5"),
    NodeType.TASK_INIT: dict(style="filled", color="black", fillcolor="#F4DEFA"),
}
_DEFAULT_GRAPH = dict(splines="ortho", nodesep="0.5", ranksep="0.75", pad="0.5")
_DEFAULT_NODE = dict(shape="box", fontname="Monospace", fontsize="14", margin="0.2,0.1", penwidth="3")
_DEFAULT_EDGE = dict(color="black", arrowsize="1.5", penwidth="1.5")


def show_dot(
    pipeline_graph: PipelineGraph,
    stream: TextIO = sys.stdout,
    **kwargs: Any,
) -> None:
    """Write a DOT representation of the pipeline graph to a stream.

    Parameters
    ----------
    pipeline_graph : `PipelineGraph`
        Pipeline graph to show.
    stream : `TextIO`, optional
        Stream to write the DOT representation to.
    **kwargs
        Additional keyword arguments to pass to `parse_display_args`.
    """
    xgraph, options = parse_display_args(pipeline_graph, **kwargs)

    print("digraph Pipeline {", file=stream)
    _render_default("graph", _DEFAULT_GRAPH, stream)
    _render_default("node", _DEFAULT_NODE, stream)
    _render_default("edge", _DEFAULT_EDGE, stream)

    for node_key, node_data in xgraph.nodes.items():
        match node_key.node_type:
            case NodeType.TASK | NodeType.TASK_INIT:
                _render_task_node(node_key, node_data, options, stream)
            case NodeType.DATASET_TYPE:
                _render_dataset_type_node(node_key, node_data, options, stream)
            case _:
                raise AssertionError(f"Unexpected node type: {node_key.node_type}")

    for from_node, to_node, *_ in xgraph.edges:
        if xgraph.nodes[from_node].get("is_prerequisite", False):
            edge_data = dict(style="dashed")
        else:
            edge_data = {}
        _render_edge(_dot_node_name(from_node), _dot_node_name(to_node), stream, **edge_data)

    print("}", file=stream)


def _render_default(type: str, attribs: dict[str, str], stream: TextIO) -> None:
    """Set default attributes for a given type."""
    default_attribs = ", ".join([f'{key}="{val}"' for key, val in attribs.items()])
    print(f"{type} [{default_attribs}];", file=stream)


def _render_node(dot_node_name: str, node_type: NodeType, labels: list[str], stream: TextIO) -> None:
    """Render GV node"""
    label = r"</TD></TR><TR><TD>".join(labels)
    attrib_dict = dict(_ATTRIBS[node_type], label=label)
    pre = '<<TABLE BORDER="0" CELLPADDING="5"><TR><TD>'
    post = "</TD></TR></TABLE>>"
    attrib = ", ".join(
        [
            f'{key}="{val}"' if key != "label" else f"{key}={pre}{val}{post}"
            for key, val in attrib_dict.items()
        ]
    )
    print(f'"{dot_node_name}" [{attrib}];', file=stream)


def _dot_node_name(node: DisplayNodeKey) -> str:
    return f"{node}:{node.node_type.value}"


def _render_task_node(
    node: DisplayNodeKey,
    data: Mapping[str, Any],
    options: NodeAttributeOptions,
    stream: TextIO,
) -> None:
    """Render GV node for a task"""
    labels = [
        f'<B><FONT POINT-SIZE="{_NODE_LABEL_POINT_SIZE}">' + html.escape(str(node)) + "</FONT></B>",
    ]
    if options.task_classes and (node.node_type is NodeType.TASK or node.node_type is NodeType.TASK_INIT):
        labels.append(html.escape(format_task_class(options, data["task_class_name"])))
    if options.dimensions and node.node_type != NodeType.TASK_INIT:
        labels.append(
            f"<I>dimensions:</I>&nbsp;{html.escape(format_dimensions(options, data['dimensions']))}"
        )
    _render_node(_dot_node_name(node), node.node_type, labels, stream)


def _render_dataset_type_node(
    node: DisplayNodeKey,
    data: Mapping[str, Any],
    options: NodeAttributeOptions,
    stream: TextIO,
) -> None:
    """Render GV node for a dataset type"""
    labels = [f'<B><FONT POINT-SIZE="{_NODE_LABEL_POINT_SIZE}">' + html.escape(str(node)) + "</FONT></B>"]
    if options.dimensions:
        labels.append(
            "<I>dimensions:</I>&nbsp;" + html.escape(format_dimensions(options, data["dimensions"]))
        )
    if options.storage_classes:
        labels.append("<I>storage class:</I>&nbsp;" + html.escape(data["storage_class_name"]))
    _render_node(_dot_node_name(node), node.node_type, labels, stream)


def _render_edge(from_dot_name: str, to_dot_name: str, stream: TextIO, **kwargs: Any) -> None:
    """Render GV edge"""
    if kwargs:
        attrib = ", ".join([f'{key}="{val}"' for key, val in kwargs.items()])
        print(f'"{from_dot_name}" -> "{to_dot_name}" [{attrib}];', file=stream)
    else:
        print(f'"{from_dot_name}" -> "{to_dot_name}";', file=stream)
