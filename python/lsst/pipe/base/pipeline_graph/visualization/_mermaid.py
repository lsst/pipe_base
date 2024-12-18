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

__all__ = ("show_mermaid",)

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

# Configuration constants for label formatting and overflow handling.
_LABEL_PX_SIZE = 18
_LABEL_MAX_LINES_SOFT = 10
_LABEL_MAX_LINES_HARD = 15
_OVERFLOW_MAX_LINES = 20


def show_mermaid(
    pipeline_graph: PipelineGraph,
    stream: TextIO = sys.stdout,
    **kwargs: Any,
) -> None:
    """Write a Mermaid flowchart representation of the pipeline graph to a
    stream.

    This function converts a given `PipelineGraph` into a Mermaid-based
    flowchart. Nodes represent tasks (and possibly task-init nodes) and dataset
    types, and edges represent connections between them. Dimensions and storage
    classes can be included as additional metadata on nodes. Prerequisite edges
    are rendered as dashed lines.

    Parameters
    ----------
    pipeline_graph : `PipelineGraph`
        The pipeline graph to visualize.
    stream : `TextIO`, optional
        The output stream where Mermaid code is written. Defaults to
        `sys.stdout`.
    **kwargs : Any
        Additional arguments passed to `parse_display_args` to control aspects
        such as displaying dimensions, storage classes, or full task class
        names.

    Notes
    -----
    - The diagram uses a top-down layout (`flowchart TD`).
    - Three Mermaid classes are defined:
      - `task` for normal tasks,
      - `dsType` for dataset-type nodes,
      - `taskInit` for task-init nodes.
    - Edges that represent prerequisite relationships are rendered as dashed
      lines using `linkStyle`.
    - If a node's label is too long, overflow nodes are created to hold extra
      lines.
    """
    # Parse display arguments to determine what to show.
    xgraph, options = parse_display_args(pipeline_graph, **kwargs)

    # Begin the Mermaid code block.
    print("flowchart TD", file=stream)

    # Define Mermaid classes for node styling.
    print(
        f"classDef task fill:#B1F2EF,color:#000,stroke:#000,stroke-width:3px,"
        f"font-family:Monospace,font-size:{_LABEL_PX_SIZE}px,text-align:left;",
        file=stream,
    )
    print(
        f"classDef dsType fill:#F5F5F5,color:#000,stroke:#00BABC,stroke-width:3px,"
        f"font-family:Monospace,font-size:{_LABEL_PX_SIZE}px,text-align:left,rx:8,ry:8;",
        file=stream,
    )
    print(
        f"classDef taskInit fill:#F4DEFA,color:#000,stroke:#000,stroke-width:3px,"
        f"font-family:Monospace,font-size:{_LABEL_PX_SIZE}px,text-align:left;",
        file=stream,
    )

    # `overflow_ref` tracks the reference numbers for overflow nodes.
    overflow_ref = 1
    overflow_ids = []

    # Render nodes.
    for node_key, node_data in xgraph.nodes.items():
        match node_key.node_type:
            case NodeType.TASK | NodeType.TASK_INIT:
                # Render a task or task-init node.
                _render_task_node(node_key, node_data, options, stream)
            case NodeType.DATASET_TYPE:
                # Render a dataset-type node with possible overflow handling.
                overflow_ref, node_overflow_ids = _render_dataset_type_node(
                    node_key, node_data, options, stream, overflow_ref
                )
                if node_overflow_ids:
                    overflow_ids += node_overflow_ids
            case _:
                raise AssertionError(f"Unexpected node type: {node_key.node_type}")

    # Collect edges for printing and track which ones are prerequisite
    # so we can apply dashed styling after printing them.
    edges = []
    for _, (from_node, to_node, *_rest) in enumerate(xgraph.edges):
        is_prereq = xgraph.nodes[from_node].get("is_prerequisite", False)
        edges.append((from_node.node_id, to_node.node_id, is_prereq))

    # Print all edges
    for _, (f, t, p) in enumerate(edges):
        _render_edge(f, t, p, stream)

    # After printing all edges, apply linkStyle to prerequisite edges to make
    # them dashed:

    # First, gather indices of prerequisite edges.
    prereq_indices = [str(i) for i, (_, _, p) in enumerate(edges) if p]

    # Then apply dashed styling to all prerequisite edges in one line.
    if prereq_indices:
        print(f"linkStyle {','.join(prereq_indices)} stroke-dasharray:5;", file=stream)


def _render_task_node(
    node_key: NodeKey,
    node_data: Mapping[str, Any],
    options: NodeAttributeOptions,
    stream: TextIO,
) -> None:
    """Render a Mermaid node for a task or task-init node.

    Parameters
    ----------
    node_key : NodeKey
        Identifies the node. The node type determines styling and whether
        dimensions apply.
    node_data : Mapping[str, Any]
        Node attributes, including possibly 'task_class_name' and 'dimensions'.
    options : NodeAttributeOptions
        Rendering options controlling whether to show dimensions, storage
        classes, etc.
    stream : TextIO
        The output stream for Mermaid syntax.
    """
    # Convert node_key into a label, handling line splitting and prefix
    # extraction.
    lines, _, _ = _format_label(str(node_key))

    # If requested, show the fully qualified task class name beneath the task
    # label.
    if options.task_classes and node_key.node_type in (NodeType.TASK, NodeType.TASK_INIT):
        lines.append(html.escape(format_task_class(options, node_data["task_class_name"])))

    # Show dimensions if requested and if this is not a task-init node.
    if options.dimensions and node_key.node_type != NodeType.TASK_INIT:
        dims_str = html.escape(format_dimensions(options, node_data["dimensions"])).replace(" ", "&nbsp;")
        lines.append(f"<i>dimensions:</i>&nbsp;{dims_str}")

    # Join lines with <br> for multi-line label.
    label = "<br>".join(lines)

    # Print Mermaid node.
    node_id = node_key.node_id
    print(f'{node_id}["{label}"]', file=stream)

    # Assign class based on node type.
    if node_key.node_type == NodeType.TASK:
        print(f"class {node_id} task;", file=stream)
    else:
        # For NodeType.TASK_INIT.
        print(f"class {node_id} taskInit;", file=stream)


def _render_dataset_type_node(
    node_key: NodeKey,
    node_data: Mapping[str, Any],
    options: NodeAttributeOptions,
    stream: TextIO,
    overflow_ref: int,
) -> tuple[int, list[str]]:
    """Render a Mermaid node for a dataset-type node, handling overflow lines
    if needed.

    Dataset-type nodes can have many lines of label text. If the label exceeds
    a certain threshold, we create separate "overflow" nodes.

    Parameters
    ----------
    node_key : NodeKey
        Identifies this dataset-type node.
    node_data : Mapping[str, Any]
        Node attributes, possibly including dimensions and storage class.
    options : NodeAttributeOptions
        Rendering options controlling whether to show dimensions and storage
        classes.
    stream : TextIO
        The output stream for Mermaid syntax.
    overflow_ref : int
        The current reference number for overflow nodes. If overflow occurs,
        this is incremented.

    Returns
    -------
    overflow_ref : int
        Possibly incremented overflow reference number.
    overflow_ids : list[str]
        IDs of overflow nodes created, if any.
    """
    # Format the node label, respecting soft/hard line limits.
    labels, label_extras, _ = _format_label(str(node_key), _LABEL_MAX_LINES_SOFT)

    overflow_ids = []
    total_lines = len(labels) + len(label_extras)
    if total_lines > _LABEL_MAX_LINES_HARD:
        # Too many lines, we must handle overflow by splitting extras.
        allowed_extras = _LABEL_MAX_LINES_HARD - len(labels)
        if allowed_extras < 0:
            allowed_extras = 0
        extras_for_overflow = label_extras[allowed_extras:]
        label_extras = label_extras[:allowed_extras]

        if extras_for_overflow:
            # Introduce an overflow anchor.
            overflow_anchor = f"[{overflow_ref}]"
            labels.append(f"<b>...more details in {overflow_anchor}</b>")

            # Create overflow nodes in chunks.
            for i in range(0, len(extras_for_overflow), _OVERFLOW_MAX_LINES):
                overflow_id = f"{node_key.node_id}_overflow_{overflow_ref}_{i}"
                chunk = extras_for_overflow[i : i + _OVERFLOW_MAX_LINES]
                chunk.insert(0, f"<b>{html.escape(overflow_anchor)}</b>")
                _render_simple_node(overflow_id, chunk, "dsType", stream)
                overflow_ids.append(overflow_id)

            overflow_ref += 1

    # Combine final lines after overflow handling.
    final_lines = labels + label_extras

    # Append dimensions if requested.
    if options.dimensions:
        dims_str = html.escape(format_dimensions(options, node_data["dimensions"])).replace(" ", "&nbsp;")
        final_lines.append(f"<i>dimensions:</i>&nbsp;{dims_str}")

    # Append storage class if requested.
    if options.storage_classes:
        final_lines.append(f"<i>storage&nbsp;class:</i>&nbsp;{html.escape(node_data['storage_class_name'])}")

    # Render the main dataset-type node.
    _render_simple_node(node_key.node_id, final_lines, "dsType", stream)

    return overflow_ref, overflow_ids


def _render_simple_node(node_id: str, lines: list[str], node_class: str, stream: TextIO) -> None:
    """Render a simple Mermaid node with given lines and a class.

    This helper function is used for both primary nodes and overflow nodes once
    the split has been decided.

    Parameters
    ----------
    node_id : str
        Mermaid node ID.
    lines : list[str]
        Lines of HTML-formatted text to display in the node.
    node_class : str
        Mermaid class name to style the node (e.g., 'dsType', 'task',
        'taskInit').
    stream : TextIO
        The output stream.
    """
    label = "<br>".join(lines)
    print(f'{node_id}["{label}"]', file=stream)
    print(f"class {node_id} {node_class};", file=stream)


def _render_edge(from_node_id: str, to_node_id: str, is_prerequisite: bool, stream: TextIO) -> None:
    """Render a Mermaid edge from one node to another.

    Edges in Mermaid are normally specified as `A --> B`. Prerequisite edges
    will later be styled as dashed lines using linkStyle after all edges have
    been printed.

    Parameters
    ----------
    from_node_id : str
        The ID of the 'from' node in the edge.
    to_node_id : str
        The ID of the 'to' node in the edge.
    is_prerequisite : bool
        If True, this edge represents a prerequisite connection and will be
        styled as dashed.
    stream : TextIO
        The output stream for Mermaid syntax.
    """
    # At this stage, we simply print the edge. The styling (dashed) for
    # prerequisite edges is applied afterwards via linkStyle lines.
    print(f"{from_node_id} --> {to_node_id}", file=stream)


def _format_label(
    label: str,
    max_lines: int = 10,
    min_common_prefix_len: int = 1000,
) -> tuple[list[str], list[str], str]:
    """Parse and format a label into multiple lines with optional overflow
    handling.

    This function attempts to cleanly format long labels by:
    - Splitting the label by ", ".
    - Identifying a common prefix to factor out if sufficiently long.
    - Limiting the number of lines to 'max_lines', storing extras for potential
      overflow.

    Parameters
    ----------
    label : str
        The raw label text, often derived from a NodeKey.
    max_lines : int, optional
        Maximum lines before overflow is triggered.
    min_common_prefix_len : int, optional
        Minimum length for considering a common prefix extraction.

    Returns
    -------
    labels : list[str]
        Main label lines as HTML-formatted text.
    label_extras : list[str]
        Overflow lines if the label is too long.
    common_prefix : str
        Extracted common prefix, if any.
    """
    parsed_labels, parsed_label_extras, common_prefix = _parse_label(label, max_lines, min_common_prefix_len)

    # If there's a common prefix, present it bolded.
    if common_prefix:
        common_prefix = f"<b>{html.escape(common_prefix)}:</b>"

    indent = "&nbsp;&nbsp;" if common_prefix else ""
    labels = [f"<b>{indent}{html.escape(el)}</b>" for el in parsed_labels]
    label_extras = [f"<b>{indent}{html.escape(el)}</b>" for el in parsed_label_extras]

    if common_prefix:
        labels.insert(0, common_prefix)

    return labels, label_extras, common_prefix or ""


def _parse_label(
    label: str,
    max_lines: int,
    min_common_prefix_len: int,
) -> tuple[list[str], list[str], str]:
    """Split and process label text for overflow and common prefix extraction.

    Parameters
    ----------
    label : str
        The raw label text.
    max_lines : int
        Maximum number of lines before overflow.
    min_common_prefix_len : int
        Minimum length for a common prefix to be considered.

    Returns
    -------
    labels : list[str]
        The primary label lines.
    label_extras : list[str]
        Any overflow lines that exceed max_lines.
    common_prefix : str
        The extracted common prefix, if applicable.
    """
    labels = label.split(", ")
    common_prefix = os.path.commonprefix(labels)

    # If there's a long common prefix for multiple labels, factor it out at the
    # nearest underscore.
    if len(labels) > 3 and len(common_prefix) > min_common_prefix_len:
        final_underscore_index = common_prefix.rfind("_")
        if final_underscore_index > 0:
            common_prefix = common_prefix[: final_underscore_index + 1]
            labels = [element[len(common_prefix) :] for element in labels]
        else:
            common_prefix = ""
    else:
        common_prefix = ""

    # Handle overflow if needed.
    if (len(labels) + bool(common_prefix)) > max_lines:
        label_extras = labels[max_lines - bool(common_prefix) :]
        labels = labels[: max_lines - bool(common_prefix)]
    else:
        label_extras = []

    return labels, label_extras, common_prefix
