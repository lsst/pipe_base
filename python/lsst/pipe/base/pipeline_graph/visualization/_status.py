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
import dataclasses
from typing import Protocol, overload, Literal, TYPE_CHECKING
from .._nodes import NodeKey, NodeType

if TYPE_CHECKING:
    from ... import quantum_provenance_graph as qpg


@dataclasses.dataclass
class TaskStatusInfo:
    expected: int
    succeded: int
    failed: int
    blocked: int
    ready: int | None = None
    running: int | None = None
    wonky: int | None = None


@dataclasses.dataclass
class DatasetTypeStatusInfo:
    expected: int
    produced: int


@dataclasses.dataclass
class NodeStatusOptions:
    # Add colors here.
    pass


class StatusAnnotator(Protocol):
    """Annotate a networkx graph of tasks and possibly dataset types with
    status information."""

    @overload
    def __call__(self, xgraph: networkx.DiGraph, dataset_types: Literal[False]) -> None:
        ...

    @overload
    def __call__(self, xgraph: networkx.MultiDiGraph, dataset_types: Literal[True]) -> None:
        ...

    def __call__(self, xgraph: networkx.DiGraph | networkx.MultiDiGraph, dataset_types: bool) -> None:
        ...


class QuantumProvenanceGraphStatusAnnotator:
    """..."""

    def __init__(self, qpg_summary: qpg.Summary) -> None:
        self.qpg_summary = qpg_summary

    @overload
    def __call__(self, xgraph: networkx.DiGraph, dataset_types: Literal[False]) -> None:
        ...

    @overload
    def __call__(self, xgraph: networkx.MultiDiGraph, dataset_types: Literal[True]) -> None:
        ...

    def __call__(self, xgraph: networkx.DiGraph | networkx.MultiDiGraph, dataset_types: bool) -> None:
        for task_label, task_summary in self.qpg_summary.tasks.items():
            task_status_info = TaskStatusInfo(
                expected=task_summary.n_expected,
                succeded=task_summary.n_successful,
                failed=task_summary.n_failed,
                blocked=task_summary.n_blocked,
                wonky=task_summary.n_wonky,
            )
            # Note: `ready` and `running` are for bps! For bps, we want to add
            # `pending` to `ready`.

            key = NodeKey(NodeType.TASK, task_label)
            xgraph.nodes[key]["status"] = task_status_info

        if dataset_types:
            for dataset_type_name, dataset_type_summary in self.qpg_summary.datasets.items():
                dataset_type_status_info = DatasetTypeStatusInfo(
                    expected=dataset_type_summary.n_expected,
                    produced=dataset_type_summary.n_visible + dataset_type_summary.n_shadowed,
                )

            key = NodeKey(NodeType.DATASET_TYPE, dataset_type_name)
            xgraph.nodes[key]["status"] = dataset_type_status_info