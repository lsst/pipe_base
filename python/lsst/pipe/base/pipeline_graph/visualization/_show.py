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

__all__ = ("show",)

import sys
from collections.abc import Sequence
from shutil import get_terminal_size
from typing import TextIO

from .._abcs import NodeKey
from .._pipeline_graph import PipelineGraph
from .._tasks import TaskInitNode, TaskNode
from ._formatting import GetNodeText, get_node_symbol
from ._layout import Layout
from ._merge import (
    MergedNodeKey,
    merge_graph_input_trees,
    merge_graph_intermediates,
    merge_graph_output_trees,
)
from ._options import Brevity, NodeAttributeOptions
from ._printer import make_default_printer

DisplayNodeKey = NodeKey | MergedNodeKey


def show(
    pipeline_graph: PipelineGraph,
    stream: TextIO = sys.stdout,
    *,
    tasks: bool = True,
    dataset_types: bool = False,
    init: bool | None = False,
    color: bool | Sequence[str] | None = None,
    dimensions: Brevity | None = Brevity.CONCISE,
    task_classes: Brevity | None = None,
    storage_classes: bool = False,
    merge_input_trees: int = 4,
    merge_output_trees: int = 4,
    merge_intermediates: bool = True,
    include_automatic_connections: bool = False,
    width: int | None = None,
) -> None:
    if init is None:
        if not (tasks and dataset_types):
            raise ValueError(
                "Cannot show init and runtime graphs unless both tasks and dataset types are shown."
            )
        xgraph = pipeline_graph.make_xgraph(import_tasks=False)
    elif tasks:
        if dataset_types:
            xgraph = pipeline_graph.make_bipartite_xgraph(init, import_tasks=False)
        else:
            xgraph = pipeline_graph.make_task_xgraph(init, import_tasks=False)
            storage_classes = False
    else:
        if dataset_types:
            xgraph = pipeline_graph.make_dataset_type_xgraph(init)
            task_classes = None
        else:
            raise ValueError("No tasks or dataset types to show.")

    options = NodeAttributeOptions(
        dimensions=dimensions, storage_classes=storage_classes, task_classes=task_classes
    )
    options.check(pipeline_graph)

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
                if edge not in t.outputs and not xgraph.out_degree(edge.dataset_type_key)
            )

    if merge_input_trees:
        merge_graph_input_trees(xgraph, options, depth=merge_input_trees)
    if merge_output_trees:
        merge_graph_output_trees(xgraph, options, depth=merge_output_trees)
    if merge_intermediates:
        merge_graph_intermediates(xgraph, options)

    layout = Layout[DisplayNodeKey](xgraph)

    if width is None:
        width, _ = get_terminal_size()

    printer = make_default_printer(layout.width, color)
    printer.get_symbol = get_node_symbol

    get_text = GetNodeText(xgraph, options, width - printer.width)
    printer.get_text = get_text

    printer.print(stream, layout)
    for line in get_text.format_deferrals(width):
        print(line, file=stream)
