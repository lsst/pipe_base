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

import json
import os
import tarfile
from abc import abstractmethod
from collections.abc import Sequence
from typing import Any, BinaryIO, Generic, TypeVar

import networkx
from lsst.daf.butler import DatasetType, DimensionConfig, DimensionUniverse

from ._abcs import NodeKey, NodeType
from ._dataset_types import DatasetTypeNode, ResolvedDatasetTypeNode
from ._edges import ReadEdge, WriteEdge
from ._exceptions import PipelineGraphReadError
from ._task_subsets import LabeledSubset, MutableLabeledSubset, ResolvedLabeledSubset
from ._tasks import ResolvedTaskNode, TaskInitNode, TaskNode, _TaskNodeSerializedData
from ._views import _D, _T

_S = TypeVar("_S", bound="LabeledSubset", covariant=True)


class PipelineGraphReader(Generic[_T, _D, _S]):
    def __init__(self) -> None:
        self.xgraph = networkx.DiGraph()
        self.sort_keys: Sequence[NodeKey] | None = None
        self.labeled_subsets: dict[str, _S] = {}
        self.description: str = ""

    def read_stream(self, stream: BinaryIO) -> None:
        with tarfile.open(fileobj=stream, mode="r|*") as archive:
            dir_tar_info = archive.next()
            if dir_tar_info is None or not dir_tar_info.isdir():
                raise PipelineGraphReadError(
                    f"Expected a tar archive with a directory as its first entry; got {dir_tar_info!r}."
                )
            graph_tar_info = archive.next()
            if graph_tar_info is None or not graph_tar_info.name.endswith("graph.json"):
                raise PipelineGraphReadError(
                    f"Expected a tar archive with a graph.json as its second entry; "
                    f"got {graph_tar_info!r}."
                )
            graph_stream = archive.extractfile(graph_tar_info)
            if graph_stream is None:
                raise PipelineGraphReadError("Nested graph.json tar member is not a valid file.")
            serialized_graph = json.load(graph_stream)
            config_strings = {}
            while (config_tar_info := archive.next()) is not None:
                task_label, ext = os.path.splitext(os.path.basename(config_tar_info.name))
                if ext != ".py":
                    raise PipelineGraphReadError(
                        f"Unexpected file {config_tar_info} in tar archive for serialized graph."
                    )
                config_stream = archive.extractfile(config_tar_info)
                if config_stream is None:
                    raise PipelineGraphReadError(
                        f"Nested {config_tar_info.name} tar member is not a valid file."
                    )
                config_strings[task_label] = config_stream.read().decode("utf-8")
        self.deserialize_graph(serialized_graph, config_strings)

    def deserialize_graph(
        self,
        serialized_graph: dict[str, Any],
        config_strings: dict[str, str],
    ) -> None:
        sort_index_map: dict[int, NodeKey] = {}
        serialized_dataset_type: dict[str, Any]
        for dataset_type_name, serialized_dataset_type in serialized_graph.pop("dataset_types").items():
            index = serialized_dataset_type.pop("index", None)
            dataset_type_node = self.deserialize_dataset_type(dataset_type_name, serialized_dataset_type)
            self.xgraph.add_node(
                dataset_type_node.key, instance=dataset_type_node, bipartite=NodeType.DATASET_TYPE.value
            )
            if index is not None:
                sort_index_map[index] = dataset_type_node.key
        serialized_task: dict[str, Any]
        for task_label, serialized_task in serialized_graph.pop("tasks").items():
            index = serialized_task.pop("index", None)
            init_index = serialized_task.pop("init_index", None)
            task_node = self.deserialize_task(task_label, serialized_task, config_strings.pop(task_label))
            if index is not None:
                sort_index_map[index] = task_node.key
            if init_index is not None:
                sort_index_map[init_index] = task_node.init.key
            self.xgraph.add_node(task_node.key, instance=task_node, bipartite=NodeType.TASK.bipartite)
            self.xgraph.add_node(
                task_node.init.key, instance=task_node.init, bipartite=NodeType.TASK_INIT.bipartite
            )
            self.xgraph.add_edge(task_node.init.key, task_node.key, instance=None)
            for read_edge in task_node.init.iter_all_inputs():
                self.xgraph.add_edge(read_edge.dataset_type_key, read_edge.task_key, instance=read_edge)
            for write_edge in task_node.init.iter_all_outputs():
                self.xgraph.add_edge(write_edge.task_key, write_edge.dataset_type_key, instance=write_edge)
            for read_edge in task_node.iter_all_inputs():
                self.xgraph.add_edge(read_edge.dataset_type_key, read_edge.task_key, instance=read_edge)
            for write_edge in task_node.iter_all_outputs():
                self.xgraph.add_edge(write_edge.task_key, write_edge.dataset_type_key, instance=write_edge)
        serialized_subset: dict[str, Any]
        for subset_label, serialized_subset in serialized_graph.pop("labeled_subsets").items():
            self.labeled_subsets[subset_label] = self.deserialize_labeled_subset(
                subset_label, serialized_subset
            )
        if sort_index_map:
            self.sort_keys = [sort_index_map[i] for i in range(len(self.xgraph))]
        self.description = serialized_graph.pop("description")

    @abstractmethod
    def deserialize_dataset_type(self, name: str, serialized_dataset_type: dict[str, Any]) -> _D:
        raise NotImplementedError()

    @abstractmethod
    def deserialize_task(self, label: str, serialized_task: dict[str, Any], config_str: str) -> _T:
        raise NotImplementedError()

    @abstractmethod
    def deserialize_labeled_subset(self, label: str, serialized_labeled_subset: dict[str, Any]) -> _S:
        raise NotImplementedError()

    def deserialize_task_init(
        self, label: str, serialized_task_init: dict[str, Any], data: _TaskNodeSerializedData
    ) -> TaskInitNode:
        key = NodeKey(NodeType.TASK_INIT, label)
        serialized_config_output = serialized_task_init.pop("config_output")
        return TaskInitNode(
            key,
            inputs={
                self.deserialize_read_edge(key, parent_dataset_type_name, serialized_edge, is_init=True)
                for parent_dataset_type_name, serialized_edge in serialized_task_init.pop("inputs").items()
            },
            outputs={
                self.deserialize_write_edge(key, parent_dataset_type_name, serialized_edge, is_init=True)
                for parent_dataset_type_name, serialized_edge in serialized_task_init.pop("outputs").items()
            },
            config_output=self.deserialize_write_edge(
                key, serialized_config_output.pop("dataset_type_name"), serialized_config_output, is_init=True
            ),
            data=data,
        )

    def deserialize_task_args(
        self, label: str, serialized_task: dict[str, Any], config_str: str
    ) -> dict[str, Any]:
        data = _TaskNodeSerializedData(serialized_task.pop("task_class"), config_str)
        init = self.deserialize_task_init(label, serialized_task.pop("init"), data)
        key = NodeKey(NodeType.TASK, label)
        inputs = {
            self.deserialize_read_edge(key, parent_dataset_type_name, serialized_edge)
            for parent_dataset_type_name, serialized_edge in serialized_task.pop("inputs").items()
        }
        prerequisite_inputs = {
            self.deserialize_read_edge(key, parent_dataset_type_name, serialized_edge, is_prerequisite=True)
            for parent_dataset_type_name, serialized_edge in serialized_task.pop(
                "prerequisite_inputs"
            ).items()
        }
        outputs = {
            self.deserialize_write_edge(key, parent_dataset_type_name, serialized_edge)
            for parent_dataset_type_name, serialized_edge in serialized_task.pop("outputs").items()
        }
        if (serialized_log_output := serialized_task.pop("log_output", None)) is not None:
            log_output = self.deserialize_write_edge(
                key, serialized_log_output.pop("dataset_type_name"), serialized_log_output
            )
        else:
            log_output = None
        serialized_metadata_output = serialized_task.pop("metadata_output")
        metadata_output = self.deserialize_write_edge(
            key, serialized_metadata_output.pop("dataset_type_name"), serialized_metadata_output
        )
        return dict(
            key=key,
            init=init,
            inputs=inputs,
            prerequisite_inputs=prerequisite_inputs,
            outputs=outputs,
            log_output=log_output,
            metadata_output=metadata_output,
        )

    def deserialize_read_edge(
        self,
        task_key: NodeKey,
        parent_dataset_type_name: str,
        serialized_edge: dict[str, Any],
        is_init: bool = False,
        is_prerequisite: bool = False,
    ) -> ReadEdge:
        # Look up dataset type key in the graph, both to validate as we read
        # and to reduce the number of distinct but equivalent NodeKey instances
        # present in the graph.
        dataset_type_key = self.xgraph.nodes[NodeKey(NodeType.DATASET_TYPE, parent_dataset_type_name)][
            "instance"
        ].key
        return ReadEdge(
            dataset_type_key,
            task_key,
            storage_class_name=serialized_edge.pop("storage_class_name"),
            is_init=is_init,
            is_prerequisite=is_prerequisite,
            component=serialized_edge.pop("component", None),
            connection_name=serialized_edge.pop("connection_name"),
        )

    def deserialize_write_edge(
        self,
        task_key: NodeKey,
        parent_dataset_type_name: str,
        serialized_edge: dict[str, Any],
        is_init: bool = False,
    ) -> WriteEdge:
        # Look up dataset type key in the graph, both to validate as we read
        # and to reduce the number of distinct but equivalent NodeKey instances
        # present in the graph.
        dataset_type_key = self.xgraph.nodes[NodeKey(NodeType.DATASET_TYPE, parent_dataset_type_name)][
            "instance"
        ].key
        return WriteEdge(
            task_key=task_key,
            dataset_type_key=dataset_type_key,
            storage_class_name=serialized_edge.pop("storage_class_name"),
            is_init=is_init,
            connection_name=serialized_edge.pop("connection_name"),
        )


class MutablePipelineGraphReader(PipelineGraphReader[TaskNode, DatasetTypeNode, MutableLabeledSubset]):
    def deserialize_dataset_type(self, name: str, serialized_dataset_type: dict[str, Any]) -> DatasetTypeNode:
        return DatasetTypeNode(NodeKey(NodeType.DATASET_TYPE, name))

    def deserialize_task(self, label: str, serialized_task: dict[str, Any], config_str: str) -> TaskNode:
        return TaskNode(**self.deserialize_task_args(label, serialized_task, config_str))

    def deserialize_labeled_subset(
        self, label: str, serialized_labeled_subset: dict[str, Any]
    ) -> MutableLabeledSubset:
        members = set(serialized_labeled_subset.pop("tasks"))
        return MutableLabeledSubset(
            self.xgraph, label, members, serialized_labeled_subset.pop("description", "")
        )


class ResolvedPipelineGraphReader(
    PipelineGraphReader[ResolvedTaskNode, ResolvedDatasetTypeNode, ResolvedLabeledSubset]
):
    def deserialize_graph(
        self,
        serialized_graph: dict[str, Any],
        config_strings: dict[str, str],
    ) -> None:
        self.universe = DimensionUniverse(config=DimensionConfig(serialized_graph.pop("dimensions")))
        super().deserialize_graph(serialized_graph, config_strings)

    def deserialize_dataset_type(
        self, name: str, serialized_dataset_type: dict[str, Any]
    ) -> ResolvedDatasetTypeNode:
        dataset_type = DatasetType(
            name,
            serialized_dataset_type.pop("dimensions"),
            storageClass=serialized_dataset_type.pop("storage_class_name"),
            isCalibration=serialized_dataset_type.pop("is_calibration"),
            universe=self.universe,
        )
        return ResolvedDatasetTypeNode(
            key=NodeKey(NodeType.DATASET_TYPE, name),
            dataset_type=dataset_type,
            is_prerequisite=serialized_dataset_type.pop("is_prerequisite"),
            is_initial_query_constraint=serialized_dataset_type.pop("is_initial_query_constraint"),
        )

    def deserialize_task(
        self, label: str, serialized_task: dict[str, Any], config_str: str
    ) -> ResolvedTaskNode:
        return ResolvedTaskNode(
            **self.deserialize_task_args(label, serialized_task, config_str),
            dimensions=self.universe.extract(serialized_task.pop("dimensions")),
        )

    def deserialize_labeled_subset(
        self, label: str, serialized_labeled_subset: dict[str, Any]
    ) -> ResolvedLabeledSubset:
        members = set(serialized_labeled_subset.pop("tasks"))
        return ResolvedLabeledSubset(
            self.xgraph, label, members, serialized_labeled_subset.pop("description", "")
        )
