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

__all__ = ("GetNodeText", "format_dimensions", "format_task_class", "get_node_symbol")

import itertools
import textwrap
from collections.abc import Iterator

import networkx
import networkx.algorithms.community

from lsst.daf.butler import DimensionGroup

from .._nodes import NodeKey, NodeType
from ._merge import MergedNodeKey
from ._options import NodeAttributeOptions

DisplayNodeKey = NodeKey | MergedNodeKey
"""Type alias for graph keys that may be original task, task init, or dataset
type keys, or a merge of several keys for display purposes.
"""


def get_node_symbol(node: DisplayNodeKey, x: int | None = None) -> str:
    """Return a single-character symbol for a particular node type.

    Parameters
    ----------
    node : `DisplayNodeKey`
        Named tuple used as the node key.
    x : `str`, optional
        Ignored; may be passed for compatibility with the `Printer` class's
        ``get_symbol`` callback.

    Returns
    -------
    symbol : `str`
        A single-character symbol.
    """
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
    """A callback for the `Printer` class's `get_text` callback that
    prints detailed information about a node and can defer long entries to
    a footnote.

    Parameters
    ----------
    xgraph : `networkx.DiGraph` or `networkx.MultiDiGraph`
        NetworkX export of a `.PipelineGraph` that is being displayed.
    options : `NodeAttributeOptions`
        Options for how much information to display.
    width : `int` or `None`
        Number of display columns that node text can occupy.  `None` for
        unlimited.
    """

    def __init__(
        self,
        xgraph: networkx.DiGraph | networkx.MultiDiGraph,
        options: NodeAttributeOptions,
        width: int | None,
    ):
        self.xgraph = xgraph
        self.options = options
        self.width = width
        self.deferred: list[tuple[str, tuple[str, str], list[str]]] = []

    def __call__(self, node: DisplayNodeKey, x: int, style: tuple[str, str]) -> str:
        state = self.xgraph.nodes[node]
        terms: list[str] = [f"{node}:" if self.options.has_details(node.node_type) else str(node)]
        if self.options.dimensions and node.node_type != NodeType.TASK_INIT:
            terms.append(self.format_dimensions(state["dimensions"]))
        if self.options.task_classes and (
            node.node_type is NodeType.TASK or node.node_type is NodeType.TASK_INIT
        ):
            terms.append(self.format_task_class(state["task_class_name"]))
        if self.options.storage_classes and node.node_type is NodeType.DATASET_TYPE:
            terms.append(state["storage_class_name"])
        description = " ".join(terms)
        if self.width and len(description) > self.width:
            index = f"[{len(self.deferred) + 1}]"
            self.deferred.append((index, style, terms))
            return f"{description[: self.width - len(index) - 6]}...{style[0]}{index}{style[1]} "
        return description

    def format_dimensions(self, dimensions: DimensionGroup) -> str:
        """Format the dimensions of a task or dataset type node.

        Parameters
        ----------
        dimensions : `~lsst.daf.butler.DimensionGroup`
            The dimensions to be formatted.

        Returns
        -------
        formatted : `str`
            The formatted dimension string.
        """
        return format_dimensions(self.options, dimensions)

    def format_task_class(self, task_class_name: str) -> str:
        """Format the type object for a task or task init node.

        Parameters
        ----------
        task_class_name : `str`
            The name of the task class.

        Returns
        -------
        formatted : `str`
            The formatted string.
        """
        return format_task_class(self.options, task_class_name)

    def format_deferrals(self, width: int | None) -> Iterator[str]:
        """Iterate over all descriptions that were truncated earlier and
        replace with footnote placeholders.

        Parameters
        ----------
        width : `int` or `None`
            Number of columns to wrap descriptions at.

        Returns
        -------
        deferrals : `collections.abc.Iterator` [ `str` ]
            Lines of deferred text, already wrapped.
        """
        indent = "  "
        for index, style, terms in self.deferred:
            yield f"{style[0]}{index}{style[1]}"
            for term in terms:
                if width:
                    yield from textwrap.wrap(term, width, initial_indent=indent, subsequent_indent=indent)
                else:
                    yield term


def format_dimensions(options: NodeAttributeOptions, dimensions: DimensionGroup) -> str:
    """Format the dimensions of a task or dataset type node.

    Parameters
    ----------
    options : `NodeAttributeOptions`
        Options for how much information to display.
    dimensions : `~lsst.daf.butler.DimensionGroup`
        The dimensions to be formatted.

    Returns
    -------
    formatted : `str`
        The formatted dimension string.
    """
    match options.dimensions:
        case "full":
            return str(dimensions.names)
        case "concise":
            redundant: set[str] = set()
            for a, b in itertools.permutations(dimensions.required, 2):
                if a in dimensions.universe[b].required:
                    redundant.add(a)
            kept = [d for d in dimensions.required if d not in redundant]
            assert dimensions.universe.conform(kept) == dimensions
            return f"{{{', '.join(kept)}}}"
        case False:
            return ""
    raise ValueError(f"Invalid display option for dimensions: {options.dimensions!r}.")


def format_task_class(options: NodeAttributeOptions, task_class_name: str) -> str:
    """Format the type object for a task or task init node.

    Parameters
    ----------
    options : `NodeAttributeOptions`
        Options for how much information to display.
    task_class_name : `str`
        The name of the task class.

    Returns
    -------
    formatted : `str`
        The formatted string.
    """
    match options.task_classes:
        case "full":
            return task_class_name
        case "concise":
            return task_class_name.split(".")[-1]
        case False:
            return ""
    raise ValueError(f"Invalid display option for task_classes: {options.task_classes!r}.")
