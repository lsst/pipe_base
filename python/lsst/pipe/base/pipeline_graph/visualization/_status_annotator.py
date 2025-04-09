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
    "QuantumGraphExecutionStatusAnnotator",
    "QuantumGraphExecutionStatusOptions",
    "QuantumProvenanceGraphStatusAnnotator",
    "QuantumProvenanceGraphStatusOptions",
)

import dataclasses
from typing import TYPE_CHECKING, Any, Literal, Protocol, overload

import networkx

from .._nodes import NodeKey, NodeType

if TYPE_CHECKING:
    from ... import quantum_provenance_graph as qpg

# ANSI color codes.
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
WHITE = "\033[37m"
GRAY = "\033[90m"
MAGENTA = "\033[35m"
BROWN = "\u001b[38;5;130m"
RESET = "\033[0m"


@dataclasses.dataclass
class TaskStatusInfo:
    """Holds status information for a task."""

    expected: int
    succeeded: int
    failed: int
    blocked: int
    ready: int | None = None
    running: int | None = None
    wonky: int | None = None
    unknown: int | None = None


@dataclasses.dataclass
class DatasetTypeStatusInfo:
    """Holds status information for a dataset type."""

    expected: int
    produced: int


@dataclasses.dataclass
class StatusColors:
    """Base class for holding ANSI color codes for different progress segments
    or statuses.
    """

    # Base task status colors.
    expected: str = WHITE
    succeeded: str = GREEN
    failed: str = RED

    # Base dataset type status colors.
    produced: str = GREEN

    # Reset to default color.
    reset: str = RESET


@dataclasses.dataclass
class QuantumGraphExecutionStatusColors(StatusColors):
    """Holds ANSI color codes for different progress segments or statuses for
    quantum graph execution reports.

    Status colors for both task and dataset type nodes are included.
    """

    def __post_init__(self) -> None:
        raise NotImplementedError("`QuantumGraphExecutionStatusColors` is not implemented yet.")


@dataclasses.dataclass
class QuantumProvenanceGraphStatusColors(StatusColors):
    """Holds ANSI color codes for different progress segments or statuses for
    quantum provenance graph reports.

    Status colors for both task and dataset type nodes are included.
    """

    # Additional task status colors.
    blocked: str = YELLOW
    ready: str = GRAY
    running: str = MAGENTA
    wonky: str = CYAN
    unknown: str = BROWN


@dataclasses.dataclass
class NodeStatusOptions:
    """Base options for node status visualization.

    Attributes
    ----------
    colors : `StatusColors`
        A dataclass specifying ANSI color codes for distinct progress segments
        or statuses.
    display_percent : `bool`
        Whether to show percentage of progress.
    display_counts : `bool`
        Whether to show numeric counts (e.g., succeeded/expected).
    visualize : `bool`
        If `True`, status information for task or dataset type nodes will be
        visually indicated by segmented fills in text-based bars or flowchart
        nodes.
    min_bar_width : `int`
        Minimum width of the visualized progress bar in characters. Only counts
        the width of the bar itself, not any surrounding text. Only relevant if
        `visualize` is `True` and it's a text-based visualization.
    abbreviate : `bool`
        If `True`, status labels will be abbreviated to save space. For
        example, 'expected' will be abbreviated to 'exp' and 'blocked' to
        'blk'.
    """

    colors: QuantumGraphExecutionStatusColors | QuantumProvenanceGraphStatusColors
    display_percent: bool = True
    display_counts: bool = True
    visualize: bool = True
    min_bar_width: int = 15
    abbreviate: bool = True

    def __post_init__(self) -> None:
        if not (self.display_percent or self.display_counts or self.visualize):
            raise ValueError(
                "At least one of 'display_percent', 'display_counts', or 'visualize' must be True."
            )


@dataclasses.dataclass
class QuantumGraphExecutionStatusOptions(NodeStatusOptions):
    """Specialized status options for quantum graph execution reports."""

    colors: QuantumGraphExecutionStatusColors = dataclasses.field(
        default_factory=QuantumGraphExecutionStatusColors
    )


@dataclasses.dataclass
class QuantumProvenanceGraphStatusOptions(NodeStatusOptions):
    """Specialized status options for quantum provenance graph reports."""

    colors: QuantumProvenanceGraphStatusColors = dataclasses.field(
        default_factory=QuantumProvenanceGraphStatusColors
    )


class NodeStatusAnnotator(Protocol):
    """Protocol for annotating a networkx graph with task and dataset type
    status information.
    """

    @overload
    def __call__(self, xgraph: networkx.DiGraph, dataset_types: Literal[False]) -> None: ...

    @overload
    def __call__(self, xgraph: networkx.MultiDiGraph, dataset_types: Literal[True]) -> None: ...

    def __call__(self, xgraph: networkx.DiGraph | networkx.MultiDiGraph, dataset_types: bool) -> None: ...


class QuantumGraphExecutionStatusAnnotator:
    """Annotates a networkx graph with task and dataset status information from
    a quantum graph execution summary, implementing the StatusAnnotator
    protocol to update the graph with status data.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("`QuantumGraphExecutionStatusAnnotator` is not implemented yet.")


class QuantumProvenanceGraphStatusAnnotator:
    """Annotates a networkx graph with task and dataset status information from
    a quantum provenance summary, implementing the StatusAnnotator protocol to
    update the graph with status data.

    Parameters
    ----------
    qpg_summary : `~lsst.pipe.base.quantum_provenance_graph.Summary`
        The quantum provenance summary to use for status information.
    """

    def __init__(self, qpg_summary: qpg.Summary) -> None:
        self.qpg_summary = qpg_summary

    @overload
    def __call__(self, xgraph: networkx.DiGraph, dataset_types: Literal[False]) -> None: ...

    @overload
    def __call__(self, xgraph: networkx.MultiDiGraph, dataset_types: Literal[True]) -> None: ...

    def __call__(self, xgraph: networkx.DiGraph | networkx.MultiDiGraph, dataset_types: bool) -> None:
        for task_label, task_summary in self.qpg_summary.tasks.items():
            fields = {
                name.replace("n_", "").replace("successful", "succeeded"): getattr(task_summary, name)
                for name in dir(task_summary)
                if name.startswith("n_")
            }
            assert sum(fields.values()) == 2 * task_summary.n_expected, f"Incosistent status counts: {fields}"
            task_status_info = TaskStatusInfo(**fields)

            key = NodeKey(NodeType.TASK, task_label)
            xgraph.nodes[key]["status"] = task_status_info

        if dataset_types:
            for dataset_type_name, dataset_type_summary in self.qpg_summary.datasets.items():
                expected = dataset_type_summary.n_expected
                produced = dataset_type_summary.n_visible + dataset_type_summary.n_shadowed
                assert produced <= expected, f"Dataset types produced ({produced}) > expected ({expected})"
                dataset_type_status_info = DatasetTypeStatusInfo(expected=expected, produced=produced)

                key = NodeKey(NodeType.DATASET_TYPE, dataset_type_name)
                xgraph.nodes[key]["status"] = dataset_type_status_info
