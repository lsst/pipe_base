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

"""Module defining few methods to generate Mermaid charts from pipelines or
quantum graphs.
"""

from __future__ import annotations

__all__ = ["graph2mermaid", "pipeline2mermaid"]

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal

from .pipeline import Pipeline

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef
    from lsst.pipe.base import QuantumGraph, TaskDef


def _datasetRefId(dsRef: DatasetRef) -> str:
    """Make a unique identifier string for a dataset ref based on its name and
    dataId.
    """
    dsIdParts = [dsRef.datasetType.name]
    dsIdParts.extend(f"{key}_{dsRef.dataId[key]}" for key in sorted(dsRef.dataId.required.keys()))
    return "_".join(dsIdParts)


def _makeDatasetNode(dsRef: DatasetRef, allDatasetRefs: dict[str, str], file: Any) -> str:
    """Create a Mermaid node for a dataset if it doesn't exist, and return its
    node ID.
    """
    dsId = _datasetRefId(dsRef)
    nodeName = allDatasetRefs.get(dsId)
    if nodeName is None:
        nodeName = f"DATASET_{len(allDatasetRefs)}"
        allDatasetRefs[dsId] = nodeName
        # Simple label: datasetType name and run.
        label_lines = [f"**{dsRef.datasetType.name}**", f"run: {dsRef.run}"]
        # Add dataId info.
        for k in sorted(dsRef.dataId.required.keys()):
            label_lines.append(f"{k}={dsRef.dataId[k]}")
        label = "<br>".join(label_lines)
        print(f'{nodeName}["{label}"]', file=file)
    return nodeName


def graph2mermaid(qgraph: QuantumGraph, file: Any) -> None:
    """Convert QuantumGraph into a Mermaid flowchart (top-down).

    This method is mostly for documentation/presentation purposes.

    Parameters
    ----------
    qgraph : `~lsst.pipe.base.QuantumGraph`
        QuantumGraph instance.
    file : `str` or file object
        File where Mermaid flowchart is written, can be a file name or file
        object.

    Raises
    ------
    OSError
        Raised if the output file cannot be opened.
    ImportError
        Raised if the task class cannot be imported.
    """
    # Open a file if needed.
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    # Start Mermaid code block with flowchart.
    print("flowchart TD", file=file)

    # To avoid duplicating dataset nodes, we track them.
    allDatasetRefs: dict[str, str] = {}

    # Process each task/quantum.
    for taskId, taskDef in enumerate(qgraph.taskGraph):
        quanta = qgraph.getNodesForTask(taskDef)
        for qId, quantumNode in enumerate(quanta):
            # Create quantum node.
            taskNodeName = f"TASK_{taskId}_{qId}"
            taskLabelLines = [f"**{taskDef.label}**", f"Node ID: {quantumNode.nodeId}"]
            dataId = quantumNode.quantum.dataId
            if dataId is not None:
                for k in sorted(dataId.required.keys()):
                    taskLabelLines.append(f"{k}={dataId[k]}")
            else:
                raise ValueError("Quantum DataId cannot be None")
            taskLabel = "<br>".join(taskLabelLines)
            print(f'{taskNodeName}["{taskLabel}"]', file=file)

            # Quantum inputs: datasets --> tasks
            for dsRefs in quantumNode.quantum.inputs.values():
                for dsRef in dsRefs:
                    dsNode = _makeDatasetNode(dsRef, allDatasetRefs, file)
                    print(f"{dsNode} --> {taskNodeName}", file=file)

            # Quantum outputs: tasks --> datasets
            for dsRefs in quantumNode.quantum.outputs.values():
                for dsRef in dsRefs:
                    dsNode = _makeDatasetNode(dsRef, allDatasetRefs, file)
                    print(f"{taskNodeName} --> {dsNode}", file=file)

    if close:
        file.close()


def pipeline2mermaid(
    pipeline: Pipeline | Iterable[TaskDef],
    file: Any,
    show_dimensions: bool = True,
    expand_dimensions: bool = False,
    show_storage: bool = True,
) -> None:
    """Convert a Pipeline into a Mermaid flowchart diagram.

    This function produces a Mermaid flowchart, representing tasks and their
    inputs/outputs as dataset nodes. It uses a top-down layout.

    This method is mostly for documentation/presentation purposes.

    Parameters
    ----------
    pipeline : Pipeline or Iterable[TaskDef]
        The pipeline or collection of tasks to represent.
    file : str or file-like
        The output file or file-like object into which the Mermaid code is
        written.
    show_dimensions : bool, optional
        If True, display dimension information for tasks and datasets.
        Default is True.
    expand_dimensions : bool, optional
        If True, expand dimension names to include all components. Default is
        False.
    show_storage : bool, optional
        If True, display storage class information for datasets. Default is
        True.

    Raises
    ------
    OSError
        Raised if the output file cannot be opened.
    ImportError
        Raised if the task class cannot be imported.
    """
    from .pipeline_graph import PipelineGraph, visualization

    # Ensure that pipeline is iterable of task definitions.
    if isinstance(pipeline, Pipeline):
        pipeline = pipeline.to_graph()._iter_task_defs()

    # Open file if needed.
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    if isinstance(pipeline, Pipeline):
        pg = pipeline.to_graph(visualization_only=True)
    else:
        pg = PipelineGraph()
        for task_def in pipeline:
            pg.add_task(
                task_def.label,
                task_class=task_def.taskClass,
                config=task_def.config,
                connections=task_def.connections,
            )
        pg.resolve(visualization_only=True)

    dimensions: Literal["full", "concise"] | None = None
    if show_dimensions:
        if expand_dimensions:
            dimensions = "full"
        else:
            dimensions = "concise"

    visualization.show_mermaid(
        pg, stream=file, dataset_types=True, dimensions=dimensions, storage_classes=show_storage
    )

    if close:
        file.close()
