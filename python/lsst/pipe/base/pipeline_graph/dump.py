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

__all__ = ()

import json
import os.path
from typing import Any

from ._pipeline_graph import PipelineGraph
from .visualization import show, show_dot


def dump_pipeline_graph(graph: PipelineGraph, directory: str) -> None:
    os.makedirs(directory)
    metadata: dict[str, Any] = {
        "description": graph.description,
        "data_id": dict(graph.data_id.mapping),
    }
    with open(os.path.join(directory, "metadata.json"), "w") as stream:
        json.dump(metadata, stream)
    with open(os.path.join(directory, "graph.txt"), "w") as stream:
        show(
            graph,
            dataset_types=True,
            init=False,
            dimensions="concise",
            storage_classes=False,
            task_classes=False,
            stream=stream,
        )
    with open(os.path.join(directory, "graph.dot"), "w") as stream:
        show_dot(
            graph,
            dataset_types=True,
            init=False,
            dimensions="concise",
            storage_classes=True,
            task_classes=False,
            stream=stream,
        )
    for task_node in graph.tasks.values():
        task_dir = os.path.join(directory, "tasks", task_node.label)
        os.makedirs(task_dir)
        single_task_graph = PipelineGraph(universe=graph.universe, data_id=graph.data_id)
        single_task_graph.add_task_nodes([task_node], parent=graph)
        with open(os.path.join(task_dir, "config.py"), "w") as stream:
            stream.write(task_node.get_config_str())
        containing_subsets = [s.label for s in graph.task_subsets.values() if task_node.label in s]
        containing_subsets.sort()
        with open(os.path.join(task_dir, "subsets.txt"), "w") as stream:
            print(*containing_subsets, sep="\n", file=stream)
        with open(os.path.join(task_dir, "graph.txt"), "w") as stream:
            show(
                single_task_graph,
                dataset_types=True,
                init=None,
                dimensions="full",
                storage_classes=True,
                task_classes="full",
                stream=stream,
            )
        with open(os.path.join(task_dir, "graph.dot"), "w") as stream:
            show_dot(
                single_task_graph,
                dataset_types=True,
                init=None,
                dimensions="full",
                storage_classes=True,
                task_classes="full",
                stream=stream,
            )
    for task_subset in graph.task_subsets.values():
        subset_dir = os.path.join(directory, "subsets", task_subset.label)
        os.makedirs(subset_dir)
        with open(os.path.join(subset_dir, "members.txt"), "w") as stream:
            print(*sorted(task_subset), sep="\n", file=stream)
        if not task_subset:
            continue
        subset_graph = PipelineGraph(universe=graph.universe, data_id=graph.data_id)
        subset_graph.add_task_nodes([graph.tasks[task_label] for task_label in task_subset], parent=graph)
        subset_graph.sort()
        with open(os.path.join(subset_dir, "graph.txt"), "w") as stream:
            show(
                subset_graph,
                dataset_types=True,
                dimensions="full",
                storage_classes=True,
                task_classes="full",
                stream=stream,
            )
        with open(os.path.join(subset_dir, "graph.dot"), "w") as stream:
            show_dot(
                subset_graph,
                dataset_types=True,
                dimensions="full",
                storage_classes=True,
                task_classes="full",
                stream=stream,
            )
