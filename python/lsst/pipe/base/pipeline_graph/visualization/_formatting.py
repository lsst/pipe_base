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

__all__ = ("get_node_symbol", "GetNodeText")

import textwrap
from collections.abc import Iterator

import networkx
import networkx.algorithms.community
from lsst.daf.butler import DimensionGraph

from .._abcs import NodeKey, NodeType
from ._merge import MergedNodeKey
from ._options import Brevity, NodeAttributeOptions

DisplayNodeKey = NodeKey | MergedNodeKey


def get_node_symbol(node: DisplayNodeKey, x: int | None = None) -> str:
    match node:
        case NodeKey(node_type=NodeType.TASK):
            return "■"
        case NodeKey(node_type=NodeType.DATASET_TYPE):
            return "○"
        case NodeKey(node_type=NodeType.TASK_INIT):
            return "▣"
        case MergedNodeKey(node_type=NodeType.TASK):
            return "▥"
        case MergedNodeKey(node_type=NodeType.DATASET_TYPE):
            return "◍"
        case MergedNodeKey(node_type=NodeType.TASK_INIT):
            return "▤"
    raise ValueError(f"Unexpected node key: {node} of type {type(node)}.")


class GetNodeText:
    def __init__(self, xgraph: networkx.DiGraph, options: NodeAttributeOptions, width: int):
        self.xgraph = xgraph
        self.options = options
        self.width = width
        self.deferred: list[tuple[str, tuple[str, str], list[str]]] = []

    def __call__(self, node: DisplayNodeKey, x: int, style: tuple[str, str]) -> str:
        state = self.xgraph.nodes[node]
        terms: list[str] = [f"{node}: "]
        if self.options.dimensions and node.node_type != NodeType.TASK_INIT:
            terms.append(self.format_dimensions(state["dimensions"]))
        if (
            self.options.task_classes
            and node.node_type is NodeType.TASK
            or node.node_type is NodeType.TASK_INIT
        ):
            terms.append(self.format_task_class(state["task_class_name"]))
        if self.options.storage_classes and node.node_type is NodeType.DATASET_TYPE:
            terms.append(state["storage_class_name"])
        description = " ".join(terms)
        if len(description) > self.width:
            index = f"[{len(self.deferred) + 1}]"
            self.deferred.append((index, style, terms))
            return f"{description[:self.width - len(index) - 6]}...{style[0]}{index}{style[1]} "
        return description

    def format_dimensions(self, dimensions: DimensionGraph) -> str:
        match self.options.dimensions:
            case Brevity.FULL:
                return str(dimensions)
            case Brevity.CONCISE:
                kept = set(dimensions)
                done = False
                while not done:
                    for dimension in kept:
                        if any(dimension != other and dimension in other.graph for other in kept):
                            kept.remove(dimension)
                            break
                    else:
                        done = True
                # We still iterate over dimensions instead of kept to preserve
                # order.
                return f"{{{', '.join(d.name for d in dimensions if d in kept)}}}"
            case None:
                return ""
        raise ValueError(
            f"Display option for dimensions is not a Brevity value or None: {self.options.dimensions}."
        )

    def format_task_class(self, task_class_name: str) -> str:
        match self.options.task_classes:
            case Brevity.FULL:
                return task_class_name
            case Brevity.CONCISE:
                return task_class_name.split(".")[-1]
            case None:
                return ""
        raise ValueError(
            f"Display option for task_classes is not a Brevity value or None: {self.options.task_classes}."
        )

    def format_deferrals(self, width: int) -> Iterator[str]:
        indent = "  "
        for index, style, terms in self.deferred:
            yield f"{style[0]}{index}{style[1]}"
            for term in terms:
                yield from textwrap.wrap(term, width, initial_indent=indent, subsequent_indent=indent)
