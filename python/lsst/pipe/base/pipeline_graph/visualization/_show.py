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

__all__ = ("show", "parse_display_args")

import sys
from collections.abc import Sequence
from shutil import get_terminal_size
from typing import Any, Literal, TextIO

import networkx

from .._nodes import NodeKey
from .._pipeline_graph import PipelineGraph
from .._tasks import TaskInitNode, TaskNode
from ._formatting import GetNodeText, get_node_symbol
from ._layout import ColumnSelector, Layout
from ._merge import (
    MergedNodeKey,
    merge_graph_input_trees,
    merge_graph_intermediates,
    merge_graph_output_trees,
)
from ._options import NodeAttributeOptions
from ._printer import make_default_printer

DisplayNodeKey = NodeKey | MergedNodeKey


def parse_display_args(
    pipeline_graph: PipelineGraph,
    *,
    dataset_types: bool = False,
    init: bool | None = False,
    dimensions: Literal["full"] | Literal["concise"] | Literal[False] | None = None,
    task_classes: Literal["full"] | Literal["concise"] | Literal[False] = False,
    storage_classes: bool | None = None,
    merge_input_trees: int = 4,
    merge_output_trees: int = 4,
    merge_intermediates: bool = True,
    include_automatic_connections: bool = False,
) -> tuple[networkx.DiGraph | networkx.MultiDiGraph, NodeAttributeOptions]:
    """Print a text-based ~.PipelineGraph` visualization.

    Parameters
    ----------
    pipeline_graph : `PipelineGraph`
        Graph to display.
    dataset_types : `bool`, optional
        Whether to include dataset type nodes (default is `False`).
    init : `bool`, optional
        Whether to include task initialization nodes (i.e. the producers of
        init-input and init-output dataset types).  Default is `False`.  `None`
        will show both runtime and init nodes, while `True` will show only
        init nodes.
    dimensions : `str` or `False`, optional
        How to display the dimensions of tasks and dataset types.

        Possible values include:

        - ``"full"``: report fully-expanded dimensions.
        - ``"concise"``: report only dimensions that are not required or
          implied dependencies of any reported dimension.
        - `False`: do not report dimensions at all.
        - `None` (default): report concise dimensions only if
          `PipelineGraph.is_fully_resolved` is `True`.

        This also sets whether merge options consider dimensions when merging
        nodes.
    task_classes : `str` or `False`, optional
        How to display task class names (not task labels, which are always
        shown).

        Possible values include:

        - ``"full"``: report the fully-qualified task class name.
        - ``"concise"``: report the task class name with no module
          or package.
        - `False`: (default) do not report task classes at all.

        This also sets whether merge options consider task classes when merging
        nodes.  It is ignored if ``tasks=False``.
    storage_classes : `bool`, optional
        Whether to display storage classes in dataset type nodes. This also
        sets whether merge options consider storage classes when merging nodes.
        It is ignored if ``dataset_types=False``.
    merge_input_trees : `int`, optional
        If positive, merge input trees of the graph whose nodes have the same
        outputs and other properties (dimensions, task classes, storage
        classes), traversing this many nodes deep into the graph from the
        beginning.  Default is ``4``.
    merge_output_trees : `int`, optional
        If positive, merge output trees of the graph whose nodes have the same
        outputs and other properties (dimensions, task classes, storage
        classes), traversing this many nodes deep into the graph from the end.
        Default is ``4``.
    merge_intermediates : `bool`, optional
        If `True` (default) merge interior parallel nodes with the same inputs,
        outputs, and other properties (dimensions, task classes, storage
        classes).
    include_automatic_connections : `bool`, optional
        Whether to include automatically-added connections like the config,
        log, and metadata dataset types for each task.  Default is `False`.
    """
    if init is None:
        if not dataset_types:
            raise ValueError("Cannot show init and runtime graphs unless dataset types are shown.")
        xgraph = pipeline_graph.make_xgraph()
    elif dataset_types:
        xgraph = pipeline_graph.make_bipartite_xgraph(init)
    else:
        xgraph = pipeline_graph.make_task_xgraph(init)
        storage_classes = False

    options = NodeAttributeOptions(
        dimensions=dimensions, storage_classes=storage_classes, task_classes=task_classes
    )
    options = options.checked(pipeline_graph.is_fully_resolved)

    if dataset_types and not include_automatic_connections:
        taskish_nodes: list[TaskNode | TaskInitNode] = []
        for task_node in pipeline_graph.tasks.values():
            if init is None or init is False:
                taskish_nodes.append(task_node)
            if init is None or init is True:
                taskish_nodes.append(task_node.init)
        for t in taskish_nodes:
            xgraph.remove_nodes_from(
                edge.dataset_type_key
                for edge in t.iter_all_outputs()
                if edge.connection_name not in t.outputs and not xgraph.out_degree(edge.dataset_type_key)
            )

    if merge_input_trees:
        merge_graph_input_trees(xgraph, options, depth=merge_input_trees)
    if merge_output_trees:
        merge_graph_output_trees(xgraph, options, depth=merge_output_trees)
    if merge_intermediates:
        merge_graph_intermediates(xgraph, options)

    return xgraph, options


def show(
    pipeline_graph: PipelineGraph,
    stream: TextIO = sys.stdout,
    *,
    color: bool | Sequence[str] | None = None,
    width: int = -1,
    column_interior_penalty: int = 1,
    column_crossing_penalty: int = 1,
    column_insertion_penalty: int = 2,
    **kwargs: Any,
) -> None:
    """Print a text-based ~.PipelineGraph` visualization.

    Parameters
    ----------
    pipeline_graph : `PipelineGraph`
        Graph to display.
    stream : `TextIO`, optional
        Output stream.  Defaults to STDOUT.
    color : `bool` or `~collections.abc.Sequence` [ `str` ]
        Whether to use tto add color to the graph. Default is to add colors
        only if the `colorama` package can be imported. `False` disables colors
        unconditionally, while `True` or a sequence of colors (see
        `make_colorama_printer`) will result in `ImportError` being propagated
        up if `colorama` is unavailable.
    width : `int`, optional
        Number of columns the full graph should occupy, including text
        descriptions on the right.  If ``0``, there is no limit (and hence no
        text-wrapping).  If negative (default) use the current terminal width.
    column_interior_penalty : `int`
        Penalty applied to a prospective column for a node when that column is
        between two existing columns.
    column_crossing_penalty : `int`
        Penalty applied to a prospective column for a node for each ongoing
        (vertical) edge that node's incoming edges would have to "hop".
    column_insertion_penalty : `int`
        Penalty applied to a prospective column for a node when considering a
        new columns on the sides or between two existing columns.
    **kwargs
        Forwarded to parse_display_args.
    """
    xgraph, options = parse_display_args(pipeline_graph, **kwargs)

    column_selector = ColumnSelector(
        interior_penalty=column_interior_penalty,
        crossing_penalty=column_crossing_penalty,
        insertion_penalty=column_insertion_penalty,
    )
    layout = Layout[DisplayNodeKey](xgraph, column_selector)

    if width < 0:
        width, _ = get_terminal_size()

    printer = make_default_printer(layout.width, color, stream)
    printer.get_symbol = get_node_symbol

    get_text = GetNodeText(xgraph, options, (width - printer.width) if width else 0)
    printer.get_text = get_text

    printer.print(stream, layout)
    for line in get_text.format_deferrals(width):
        print(line, file=stream)
