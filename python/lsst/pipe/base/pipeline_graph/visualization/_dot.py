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
import os
import sys
from collections.abc import Mapping
from typing import Any, TextIO

from .._nodes import NodeType
from .._pipeline_graph import PipelineGraph
from ._formatting import NodeKey, format_dimensions, format_task_class
from ._options import NodeAttributeOptions
from ._show import parse_display_args

_ATTRIBS = {
    NodeType.TASK: dict(style="filled", color="black", fillcolor="#B1F2EF"),
    NodeType.DATASET_TYPE: dict(style="rounded,filled,bold", color="#00BABC", fillcolor="#F5F5F5"),
    NodeType.TASK_INIT: dict(style="filled", color="black", fillcolor="#F4DEFA"),
}
_DEFAULT_GRAPH = dict(splines="ortho", nodesep="0.5", ranksep="0.75")
_DEFAULT_NODE = dict(shape="box", fontname="Monospace", fontsize="14", margin="0.2,0.1", penwidth="3")
_DEFAULT_EDGE = dict(color="black", arrowsize="1.5", penwidth="1.5", pad="10mm")
_LABEL_POINT_SIZE = "18"
_LABEL_MAX_LINES_SOFT = 10
_LABEL_MAX_LINES_HARD = 15
_OVERFLOW_MAX_LINES = 20


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

    overflow_ref = 1
    overflow_ids = []
    for node_key, node_data in xgraph.nodes.items():
        match node_key.node_type:
            case NodeType.TASK | NodeType.TASK_INIT:
                _render_task_node(node_key, node_data, options, stream)
            case NodeType.DATASET_TYPE:
                overflow_ref, node_overflow_ids = _render_dataset_type_node(
                    node_key, node_data, options, stream, overflow_ref
                )
                if node_overflow_ids:
                    overflow_ids += node_overflow_ids
            case _:
                raise AssertionError(f"Unexpected node type: {node_key.node_type}")

    if overflow_ids:
        formatted_overflow_ids = [f'"{overflow_id}"' for overflow_id in overflow_ids]
        print(f"{{rank=sink; {'; '.join(formatted_overflow_ids)};}}", file=stream)

    for from_node, to_node, *_ in xgraph.edges:
        if xgraph.nodes[from_node].get("is_prerequisite", False):
            edge_data = dict(style="dashed")
        else:
            edge_data = {}
        _render_edge(from_node.node_id, to_node.node_id, stream, **edge_data)

    print("}", file=stream)


def _render_default(type: str, attribs: dict[str, str], stream: TextIO) -> None:
    """Set default attributes for a given type."""
    default_attribs = ", ".join([f'{key}="{val}"' for key, val in attribs.items()])
    print(f"{type} [{default_attribs}];", file=stream)


def _render_task_node(
    node_key: NodeKey,
    node_data: Mapping[str, Any],
    options: NodeAttributeOptions,
    stream: TextIO,
) -> None:
    """Render a Graphviz node for a task.

    Parameters
    ----------
    node_key : NodeKey
        The key for the node
    node_data : Mapping[str, Any]
        The data associated with the node
    options : NodeAttributeOptions
        Options for rendering the node
    stream : TextIO
        The stream to write the node to
    """
    labels, *_ = _format_label(str(node_key))

    # Add the fully resolved task class name
    if options.task_classes and (node_key.node_type in (NodeType.TASK, NodeType.TASK_INIT)):
        labels.append(html.escape(format_task_class(options, node_data["task_class_name"])))

    # Append dimensions to the node
    if options.dimensions and node_key.node_type != NodeType.TASK_INIT:
        labels.append(
            f"<I>dimensions:</I>&nbsp;{html.escape(format_dimensions(options, node_data['dimensions']))}"
        )

    _render_node(node_key.node_id, node_key.node_type, labels, stream)


def _render_dataset_type_node(
    node_key: NodeKey,
    node_data: Mapping[str, Any],
    options: NodeAttributeOptions,
    stream: TextIO,
    overflow_ref: int = 1,
) -> tuple[int, list[str]]:
    """Render a Graphviz node for a dataset type.

    Parameters
    ----------
    node_key : NodeKey
        The key for the node
    node_data : Mapping[str, Any]
        The data associated with the node
    options : NodeAttributeOptions
        Options for rendering the node
    stream : TextIO
        The stream to write the node to

    Returns
    -------
    overflow_ref : int
        The reference number for the next overflow node
    overflow_ids : str | None
        The ID of the overflow node, if any
    """
    labels, label_extras, common_prefix = _format_label(str(node_key), _LABEL_MAX_LINES_SOFT)
    if len(labels) + len(label_extras) <= _LABEL_MAX_LINES_HARD:
        labels += label_extras
        label_extras = []
    if common_prefix:
        labels.insert(0, common_prefix)

    # Add a reference to a free-floating overflow node
    label_extras_grouped = {}
    if label_extras:
        overflow_to_text = f"and {len(label_extras)} more, continued in [{overflow_ref}]"
        labels.append(f'<B><FONT POINT-SIZE="{_LABEL_POINT_SIZE}">{overflow_to_text}</FONT></B>')
        for i in range(0, len(label_extras), _OVERFLOW_MAX_LINES):
            overflow_id = f"{node_key.node_id}_{overflow_ref}_{i}"
            overflow_label_extras = label_extras[i : i + _OVERFLOW_MAX_LINES]
            if common_prefix:
                overflow_label_extras.insert(0, common_prefix)
            overflow_label_extras.insert(
                0, f'<B><FONT POINT-SIZE="{_LABEL_POINT_SIZE}">[{overflow_ref}]</FONT></B>'
            )
            label_extras_grouped[overflow_id] = overflow_label_extras
        overflow_ref += 1

    # Append dimensions to the node
    if options.dimensions:
        labels.append(
            "<I>dimensions:</I>&nbsp;" + html.escape(format_dimensions(options, node_data["dimensions"]))
        )

    # Append storage class to the node
    if options.storage_classes:
        labels.append("<I>storage class:</I>&nbsp;" + html.escape(node_data["storage_class_name"]))

    _render_node(node_key.node_id, node_key.node_type, labels, stream)

    # Render the overflow nodes and invisible edges, if any
    if label_extras_grouped:
        for overflow_id, overflow_labels in label_extras_grouped.items():
            _render_node(overflow_id, node_key.node_type, overflow_labels, stream)
            _render_edge(node_key.node_id, overflow_id, stream, **{"style": "invis"})

    overflow_ids = list(label_extras_grouped.keys())
    return overflow_ref, overflow_ids


def _render_node(
    node_id: str,
    node_type: NodeType,
    labels: list[str],
    stream: TextIO,
) -> None:
    """Render a Graphviz node.

    Parameters
    ----------
    node_id : str
        The unique name of the node
    node_type : NodeType
        The type of the node
    labels : list[str]
        The label elements to display on the node
    stream : TextIO
        The stream to write the node to
    """
    label = "".join([f'<TR><TD ALIGN="LEFT">{element}</TD></TR>' for element in labels])
    attrib_dict = dict(_ATTRIBS[node_type], label=label)
    pre = '<<TABLE BORDER="0" CELLPADDING="5">'
    post = "</TABLE>>"
    attrib = ", ".join(
        [
            f'{key}="{val}"' if key != "label" else f"{key}={pre}{val}{post}"
            for key, val in attrib_dict.items()
        ]
    )
    print(f'"{node_id}" [{attrib}];', file=stream)


def _render_edge(from_node_id: str, to_node_id: str, stream: TextIO, **kwargs: Any) -> None:
    """Render GV edge

    Parameters
    ----------
    from_node_id : str
        The unique ID of the node the edge is coming from
    to_node_id : str
        The unique ID of the node the edge is going to
    stream : TextIO
        The stream to write the edge to
    kwargs : Any
        Additional keyword arguments to pass to the edge
    """
    if kwargs:
        attrib = ", ".join([f'{key}="{val}"' for key, val in kwargs.items()])
        print(f'"{from_node_id}" -> "{to_node_id}" [{attrib}];', file=stream)
    else:
        print(f'"{from_node_id}" -> "{to_node_id}";', file=stream)


def _format_label(
    label: str,
    max_lines: int = 10,
    min_common_prefix_len: int = 1000,
) -> tuple[list[str], list[str], str]:
    """Add HTML-style formatting to label text.

    Parameters
    ----------
    label : str
        The label text to parse
    max_lines : int, optional
        The maximum number of lines to display
    min_common_prefix_len : int, optional
        The minimum length of a common prefix to consider

    Returns
    -------
    labels : list[str]
        Parsed and formatted label text elements
    label_extras : list[str]
        Parsed and formatted overflow text elements, if any
    common_prefix : str
        The common prefix of the label text, if any
    """
    parsed_labels, parsed_label_extras, common_prefix = _parse_label(label, max_lines, min_common_prefix_len)
    if common_prefix:
        common_prefix = f'<B><FONT POINT-SIZE="{_LABEL_POINT_SIZE}">{common_prefix}:</FONT></B>'

    labels = []
    label_extras = []
    indent = "&nbsp;&nbsp;" if common_prefix else ""
    for element in parsed_labels:
        labels.append(f'<B><FONT POINT-SIZE="{_LABEL_POINT_SIZE}">{indent}{element}</FONT></B>')
    for element in parsed_label_extras:
        label_extras.append(f'<B><FONT POINT-SIZE="{_LABEL_POINT_SIZE}">{indent}{element}</FONT></B>')

    return labels, label_extras, common_prefix


def _parse_label(
    label: str,
    max_lines: int,
    min_common_prefix_len: int,
) -> tuple[list[str], list[str], str]:
    """Parse label text into label elements.

    Parameters
    ----------
    label : str
        The label text to parse
    max_lines : int, optional
        The maximum number of lines to return (-1 if a common prefix present)
    min_common_prefix_len : int, optional
        The minimum length of a common prefix to consider

    Returns
    -------
    labels : list[str]
        Parsed label text elements
    label_extras : list[str]
        Overflow text elements, if any
    common_prefix : str
        The common prefix of the label text, if any
    """
    labels = label.split(", ")

    if len(labels) > 3 and len(common_prefix := os.path.commonprefix(labels)) > min_common_prefix_len:
        final_underscore_index = common_prefix.rfind("_")
        if final_underscore_index > 0:
            # Only use common prefixes that end in an underscore. This prevents
            # prefixes that may equal an entire element. For example, the label
            # "srcMatchFull, srcMatch" would return "srcMatch" as a common
            # prefix, and the labels list would contain an empty label.
            common_prefix = common_prefix[: final_underscore_index + 1]
            labels = [element[len(common_prefix) :] for element in labels]
        else:
            common_prefix = ""
    else:
        common_prefix = ""

    if (len(labels) + bool(common_prefix)) > max_lines:
        label_extras = labels[max_lines - bool(common_prefix) :]
        labels = labels[: max_lines - bool(common_prefix)]
    else:
        label_extras = []

    return labels, label_extras, common_prefix
