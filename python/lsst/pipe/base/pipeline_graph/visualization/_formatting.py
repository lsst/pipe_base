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
import re
import textwrap
from collections.abc import Iterator

import networkx
import networkx.algorithms.community
from wcwidth import wcswidth  # type: ignore

from lsst.daf.butler import DimensionGroup

from .._nodes import NodeKey, NodeType
from ._merge import MergedNodeKey
from ._options import NodeAttributeOptions
from ._status_annotator import DatasetTypeStatusInfo, NodeStatusOptions, StatusColors, TaskStatusInfo

DisplayNodeKey = NodeKey | MergedNodeKey
"""Type alias for graph keys that may be original task, task init, or dataset
type keys, or a merge of several keys for display purposes.
"""


def strip_ansi(s: str) -> str:
    """Remove ANSI escape codes from a string, so that `wcswidth()` measures
    the real visible width of the string.

    Parameters
    ----------
    s : `str`
        String to strip of ANSI escape codes.

    Returns
    -------
    stripped : `str`
        String with ANSI escape codes removed.
    """
    # ANSI escape sequence remover
    ansi_escape = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")
    return ansi_escape.sub("", s)


def render_segment(f: float) -> str:
    """Convert a float into a string of blocks, rounding to the nearest whole
    number of columns.

    Parameters
    ----------
    f : `float`
        Number of columns to fill.

    Returns
    -------
    blocks : `str`
        String of blocks filling the specified number of columns.
    """
    return "█" * round(f)


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
        """Return a line of text describing a node.

        Parameters
        ----------
        node : `DisplayNodeKey`
            Named tuple used as the node key.
        x : `int`
            Ignored; may be passed for compatibility with the `Printer` class's
            ``get_text`` callback.
        style : `tuple` [`str`, `str`]
            Tuple of ANSI color codes for overflow markers.
        """
        state = self.xgraph.nodes[node]
        has_status = "status" in state

        # Build description.
        description = self._build_description(node, state)

        # Possibly build progress bar to append to description.
        progress_portion = ""
        if has_status:
            progress_portion = self.format_node_status(description, state["status"])

        # Stitch together the final line, handling overflow if needed.
        final_line = self._stitch_node_text(description, progress_portion, style)
        return final_line

    def _build_description(self, node: DisplayNodeKey, state: dict) -> str:
        """Build the node description, possibly with additional details.

        Parameters
        ----------
        node : `DisplayNodeKey`
            Named tuple used as the node key.
        state : `dict`
            Node attributes.

        Returns
        -------
        description : `str`
            The node description.
        """
        terms = [f"{node}:" if self.options.has_details(node.node_type) else str(node)]
        # Optionally append dimension info.
        if self.options.dimensions and node.node_type != NodeType.TASK_INIT:
            terms.append(self.format_dimensions(state["dimensions"]))

        # Optionally append task class name.
        if self.options.task_classes and (
            node.node_type is NodeType.TASK or node.node_type is NodeType.TASK_INIT
        ):
            terms.append(self.format_task_class(state["task_class_name"]))

        # Optionally append storage class name.
        if self.options.storage_classes and node.node_type is NodeType.DATASET_TYPE:
            terms.append(state["storage_class_name"])

        description = " ".join(terms)

        return description

    def _stitch_node_text(self, description: str, progress_portion: str, style: tuple[str, str]) -> str:
        """Make the final line of node text to display given the description
        and possibly a progress portion, and handle overflow.

        It measures the total width of the description and progress portion,
        and if it exceeds the screen width, it truncates the description and
        appends a footnote.

        Parameters
        ----------
        description : `str`
            The node description.
        progress_portion : `str`
            The progress portion of the node.
        style : `tuple` [`str`, `str`]
            Tuple of ANSI color codes for overflow markers.

        Returns
        -------
        final_line : `str`
            The final line of text to display.
        """
        final_line = f"{description}{progress_portion}" if progress_portion else description
        total_len = wcswidth(strip_ansi(final_line))

        if self.width and total_len > self.width:
            overflow_index = f"[{len(self.deferred) + 1}]"
            overflow_marker = f"...{style[0]}{overflow_index}{style[1]}"

            avail_desc_width = (
                self.width - wcswidth(strip_ansi(progress_portion)) - wcswidth(strip_ansi(overflow_marker))
            )
            if avail_desc_width < 0:
                avail_desc_width = 0

            truncated_desc = description[:avail_desc_width] + overflow_marker
            self.deferred.append((overflow_index, style, [description]))

            return f"{truncated_desc}{progress_portion}"
        else:
            return final_line

    def format_node_status(self, description: str, status: TaskStatusInfo | DatasetTypeStatusInfo) -> str:
        """Format the status of a task node.

        Parameters
        ----------
        description : `str`
            The node description.
        status : `TaskStatusInfo` or `DatasetTypeStatusInfo`
            Holds status information for a task or dataset type.

        Returns
        -------
        formatted : `str`
            The formatted status string.
        """
        if not isinstance(self.options.status, NodeStatusOptions):
            raise ValueError(f"Invalid node status options: {self.options.status!r}.")

        return format_node_status(
            description,
            self.options.status,
            status,
            self.width,
        )

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


def _build_progress_bar(
    description: str,
    prefix: str,
    suffix: str,
    segments: list[tuple[str, float]],
    width: int | None,
    colors: StatusColors,
    min_bar_width: int,
) -> str:
    """Shared helper that constructs a multi-segment progress bar.

    Parameters
    ----------
    description : `str`
        Main node description (used for measuring available space).
    prefix : `str`
        Text before the bar (e.g. ' · 42%▕').
    suffix : `str`
        Text after the bar (e.g. '▏exp: 107 ...').
    segments : `list` of `tuple` [`str`, `float`]
        Each tuple is (color_code, fractionOfBarWidth). We'll create these
        segments in sequence.
    width : `int` or None
        Overall maximum line width. None for unlimited. We'll still respect the
        minimum bar width.
    colors : `StatusColors`
        An instance containing .reset and color fields.
    min_bar_width : `int`
        Minimum number of display columns for the bar.

    Returns
    -------
    formatted : str
        The assembled prefix + bar + suffix, sized with respect to the width.
    """
    used_len = wcswidth(strip_ansi(prefix)) + wcswidth(strip_ansi(suffix))
    if width is not None:
        bar_width = max(width - wcswidth(strip_ansi(description)) - used_len, min_bar_width)
    else:
        bar_width = min_bar_width

    # Build bar from fractional segments.
    bar_str = ""
    total_cols = 0
    for ansi_color, fraction in segments:
        cols = round(bar_width * fraction)
        bar_str += f"{ansi_color}{render_segment(cols)}{colors.reset}"
        total_cols += cols

    # Pad the bar to the full width.
    if total_cols < bar_width:
        bar_str += " " * (bar_width - total_cols)

    return prefix + bar_str + suffix


def format_node_status(
    description: str,
    status_options: NodeStatusOptions,
    status: TaskStatusInfo | DatasetTypeStatusInfo,
    width: int | None,
) -> str:
    """Build a progress bar for a task or dataset type node.

    Parameters
    ----------
    description : `str`
        Node description for measuring leftover columns.
    status_options : `NodeStatusOptions`
        Options for node status visualization.
    status : `TaskStatusInfo` or `DatasetTypeStatusInfo`
        Holds status information for a task or dataset type.
    width : `int` or None
        Overall width limit (None => unlimited).

    Returns
    -------
    formatted : str
        The final prefix + bar + suffix line.
    """
    import dataclasses

    status_abbreviations = {
        "expected": "exp",
        "succeeded": "suc",
        "failed": "fail",
        "blocked": "blk",
        "ready": "rdy",
        "running": "run",
        "wonky": "wnk",
        "unknown": "unk",
        "produced": "prd",
    }

    colors = status_options.colors
    expected = status.expected
    done = status.succeeded if isinstance(status, TaskStatusInfo) else status.produced
    full_success = done == expected
    status_lookup = dataclasses.asdict(status)

    percent = 100.0 * done / expected if expected else 0.0
    total = float(expected) if expected else 1.0
    prefix = ""

    if status_options.display_percent or status_options.display_counts:
        if not status_options.visualize or (status_options.visualize and status_options.display_percent):
            full_success_color = colors.succeeded if isinstance(status, TaskStatusInfo) else colors.produced
            color_code = full_success_color if full_success else colors.failed
            prefix += f"{color_code} ▶ {colors.reset}"
        if status_options.display_percent:
            pct = round(percent)
            if pct == 100 and not full_success:
                pct == 99  # Avoid showing 100% if not fully successful.
            prefix += f"{pct}%"
        if not status_options.visualize and status_options.display_percent and status_options.display_counts:
            prefix += " | "

    if status_options.visualize:
        prefix += "▕"

    suffix_parts = []
    segments = []

    for key, value in status_lookup.items():
        if value is not None:
            color_code = getattr(colors, key)
            if status_options.display_counts:
                label = status_abbreviations[key] if status_options.abbreviate else key
                suffix_parts.append(f"{label}:{color_code}{value}{colors.reset}")
            if key != "expected":
                # Build a progress bar segment.
                segments.append((color_code, value / total))

    # Produce suffix from the parts.
    suffix = "▏" if status_options.visualize else ""
    suffix += " | ".join(suffix_parts)

    if status_options.visualize:
        return _build_progress_bar(
            description, prefix, suffix, segments, width, colors, status_options.min_bar_width
        )
    else:
        return f"{prefix}{suffix}"
