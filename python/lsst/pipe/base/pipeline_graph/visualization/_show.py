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

import dataclasses
import os.path
import sys
from collections import defaultdict
from collections.abc import Sequence
from typing import ClassVar, TextIO

import networkx
import networkx.algorithms.dag
import networkx.algorithms.tree
from lsst.daf.butler import DimensionGraph

from .._abcs import NodeKey as UnmergedNodeKey
from .._abcs import NodeType
from .._pipeline_graph import PipelineGraph
from .._tasks import TaskInitNode, TaskNode
from ._layout import Layout
from ._printer import make_default_printer


class MergedNodeKey(frozenset[UnmergedNodeKey]):
    def __str__(self) -> str:
        members = [str(k) for k in self]
        members.sort(reverse=True)
        terms: list[str] = []
        prefix = members.pop()
        exact = True
        while members:
            member = members.pop()
            common = os.path.commonprefix([member, prefix])
            if len(common) < self.MIN_PREFIX_SIZE:
                terms.append(prefix if exact else prefix + "*")
                prefix = member
                exact = True
            elif len(common) < len(prefix):
                exact = False
                prefix = common
        terms.append(prefix if exact else prefix + "*")
        return f"({', '.join(terms)})"

    node_type: ClassVar[None] = None

    MIN_PREFIX_SIZE: ClassVar[int] = 3


NodeKey = UnmergedNodeKey | MergedNodeKey


@dataclasses.dataclass(frozen=True)
class MergeKey:
    parents: frozenset[UnmergedNodeKey]
    dimensions: DimensionGraph | None
    storage_class_name: str | None
    task_class_name: str | None
    children: frozenset[MergeKey]

    def without_parents(self) -> MergeKey:
        return dataclasses.replace(self, parents=frozenset())

    @classmethod
    def merge_input_trees(cls, xgraph: networkx.DiGraph, options: NodeAttributeOptions, depth: int) -> None:
        groups = cls._make_merge_groups(xgraph, options, depth)
        cls._apply_merges(xgraph, groups, is_tail=False)

    @classmethod
    def merge_output_trees(cls, xgraph: networkx.DiGraph, options: NodeAttributeOptions, depth: int) -> None:
        groups = cls._make_merge_groups(xgraph.reverse(copy=False), options, depth)
        cls._apply_merges(xgraph, groups, is_tail=True)

    @classmethod
    def _from_xgraph_node(
        cls,
        node: UnmergedNodeKey,
        xgraph: networkx.DiGraph,
        children: frozenset[MergeKey],
        options: NodeAttributeOptions,
    ) -> MergeKey:
        state = xgraph.nodes[node]
        return cls(
            parents=frozenset(xgraph.successors(node)),
            dimensions=state["dimensions"] if options.dimensions else None,
            storage_class_name=(
                state["storage_class_name"]
                if options.storage_classes and node.node_type is NodeType.DATASET_TYPE
                else None
            ),
            task_class_name=(
                state["task_class_name"]
                if options.task_classes and node.node_type is not NodeType.DATASET_TYPE
                else None
            ),
            children=children,
        )

    @classmethod
    def _make_merge_groups(
        cls,
        xgraph: networkx.DiGraph,
        options: NodeAttributeOptions,
        depth: int,
    ) -> list[dict[MergeKey, set[UnmergedNodeKey]]]:
        # Our goal is to obtain mappings that groups trees of nodes by the
        # attributes in a MergeKey.  The nested dictionaries are the root of a
        # tree and the nodes under that root, recursively (but not including
        # the root).  We nest these mappings inside a list, which each mapping
        # corresponding to a different depth for the trees it represents.  We
        # start with a special empty dict for "0-depth trees", since that makes
        # result[depth] valid and hence off-by-one errors less likely.
        result: list[dict[MergeKey, set[UnmergedNodeKey]]] = [{}]
        if depth == 0:
            return result
        # We start with the nodes that have no predecessors in the graph.
        # Ignore for now the fact that the 'current_candidates' data structure
        # we process is actually a dict that associates each of those nodes
        # with an empty dict.  All of these initial nodes are valid trees,
        # since they're just single nodes.
        first_generation = next(networkx.algorithms.dag.topological_generations(xgraph))
        current_candidates: dict[UnmergedNodeKey, dict[UnmergedNodeKey, MergeKey]] = dict.fromkeys(
            first_generation, {}
        )
        # Set up an outer loop over tree depth; we'll construct a new set of
        # candidates at each iteration.
        while current_candidates:
            # As we go, we'll remember nodes that have just one predecessor, as
            # those predecessors might be the roots of slightly taller trees.
            # We store the successors and their merge keys under them.
            next_candidates: dict[UnmergedNodeKey, dict[UnmergedNodeKey, MergeKey]] = defaultdict(dict)
            # We also want to track the nodes the level up that are not trees
            # because some node has both them and some other node as a
            # predecessor.
            nontrees: set[UnmergedNodeKey] = set()
            # Make a dictionary for the results at this depth, then start the
            # inner iteration over candidates and (after the first iteration)
            # their children.
            result_for_depth: dict[MergeKey, set[UnmergedNodeKey]] = defaultdict(set)
            for node, children in current_candidates.items():
                # Make a MergeKey for this node and add it to the results for
                # this depth.  Two nodes with the same MergeKey are roots of
                # isomorphic trees that have the same predecessor(s), and can
                # be merged (with isomorphism defined as both both structure
                # and whatever comparisons are in 'options').
                merge_key = MergeKey._from_xgraph_node(node, xgraph, frozenset(children.values()), options)
                result_for_depth[merge_key].add(node)
                if len(result) <= depth:
                    # See if this node's successor might be the root of a
                    # larger tree.
                    if len(merge_key.parents) == 1:
                        (parent,) = merge_key.parents
                        next_candidates[parent][node] = merge_key.without_parents()
                    else:
                        nontrees.update(merge_key.parents)
            # Append the results for this depth.
            result.append(result_for_depth)
            # Trim out candidates that aren't trees after all.
            for nontree_node in nontrees & next_candidates.keys():
                del next_candidates[nontree_node]
            current_candidates = next_candidates
        return result

    @classmethod
    def _apply_merges(
        cls,
        xgraph: networkx.DiGraph,
        groups: list[dict[MergeKey, set[UnmergedNodeKey]]],
        is_tail: bool,
    ) -> None:
        while groups:
            for merge_key, members in groups.pop().items():
                remaining_members = members & xgraph.nodes.keys()
                if len(remaining_members) < 2:
                    continue
                new_node_key = MergedNodeKey(remaining_members)
                for member_key in remaining_members:
                    if is_tail:
                        children = list(networkx.algorithms.dag.descendants(xgraph, member_key))
                    else:
                        children = list(networkx.algorithms.dag.ancestors(xgraph, member_key))
                    xgraph.remove_nodes_from(children)
                    xgraph.remove_node(member_key)
                new_edges: list[tuple[NodeKey, NodeKey]]
                if is_tail:
                    new_edges = [(parent, new_node_key) for parent in merge_key.parents]
                else:
                    new_edges = [(new_node_key, parent) for parent in merge_key.parents]
                xgraph.add_node(
                    new_node_key,
                    storage_class_name=merge_key.storage_class_name,
                    task_class_name=merge_key.task_class_name,
                    dimensions=merge_key.dimensions,
                )
                xgraph.add_edges_from(new_edges)


def show(
    pipeline_graph: PipelineGraph,
    stream: TextIO = sys.stdout,
    *,
    tasks: bool = True,
    dataset_types: bool = False,
    init: bool | None = False,
    color: bool | Sequence[str] | None = None,
    dimensions: bool,
    storage_classes: bool = False,
    task_classes: bool = False,
    merge_input_trees: int = 2,
    merge_output_trees: int = 2,
    include_automatic_connections: bool = False,
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
            task_classes = False
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
        MergeKey.merge_input_trees(xgraph, options, depth=merge_input_trees)
    if merge_output_trees:
        MergeKey.merge_output_trees(xgraph, options, depth=merge_output_trees)

    layout = Layout[NodeKey](xgraph)

    printer = make_default_printer(layout.width, color)
    printer.get_symbol = _get_symbol

    if dimensions or (tasks and task_classes) or (dataset_types and storage_classes):
        printer.get_text = _GetText(xgraph, options)

    printer.print(stream, layout)


def _get_symbol(node: NodeKey, x: int) -> str:
    match node.node_type:
        case NodeType.TASK:
            return "■"
        case NodeType.DATASET_TYPE:
            return "▱"
        case NodeType.TASK_INIT:
            return "▣"
        case None:
            return "∗"
    raise ValueError(f"Unexpected node key: {node} of type {type(node)}.")


class _GetText:
    def __init__(self, xgraph: networkx.DiGraph, options: NodeAttributeOptions):
        self.xgraph = xgraph
        self.options = options

    def __call__(self, node: NodeKey, x: int) -> str:
        state = self.xgraph.nodes[node]
        terms = [f"{node}:"]
        if self.options.dimensions:
            terms.append(str(state["dimensions"]))
        if (
            self.options.task_classes
            and node.node_type is NodeType.TASK
            or node.node_type is NodeType.TASK_INIT
        ):
            terms.append(state["task_class_name"])
        if self.options.storage_classes and node.node_type is NodeType.DATASET_TYPE:
            terms.append(state["storage_class_name"])
        return " ".join(terms)


@dataclasses.dataclass
class NodeAttributeOptions:
    dimensions: bool
    storage_classes: bool
    task_classes: bool

    def __bool__(self) -> bool:
        return self.dimensions or self.storage_classes or self.task_classes

    def check(self, pipeline_graph: PipelineGraph) -> None:
        is_resolved = hasattr(pipeline_graph, "universe")
        if self.dimensions and not is_resolved:
            raise ValueError("Cannot show dimensions unless they have been resolved.")
        if self.storage_classes and not is_resolved:
            raise ValueError("Cannot show storage classes unless they have been resolved.")
