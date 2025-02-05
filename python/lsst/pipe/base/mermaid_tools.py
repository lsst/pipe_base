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

import html
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import DatasetType, DimensionUniverse

from . import connectionTypes
from .connections import iterConnections
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


def _expand_dimensions(dimension_list: set[str] | Iterable[str], universe: DimensionUniverse) -> list[str]:
    """Return expanded list of dimensions, with special skypix treatment.

    Parameters
    ----------
    dimension_set : `set` [`str`] or iterable of `str`
        The original set of dimension names.
    universe : DimensionUniverse
        Used to conform the dimension set according to a known schema.

    Returns
    -------
    dimensions : `list` [`str`]
        Expanded list of dimensions.
    """
    dimension_set = set(dimension_list)
    skypix_dim = []
    if "skypix" in dimension_set:
        dimension_set.remove("skypix")
        skypix_dim = ["skypix"]
    dimensions = universe.conform(dimension_set)
    return list(dimensions.names) + skypix_dim


def _format_dimensions(dims: list[str]) -> str:
    """Format and sort dimension names as a comma-separated list inside curly
    braces.

    For example, if dims=["detector", "visit"], returns "{detector, visit}".

    Parameters
    ----------
    dims : list of str
        The dimension names to format and sort.

    Returns
    -------
    str
        The formatted dimension string, or an empty string if no dimensions.
    """
    if not dims:
        return ""
    sorted_dims = sorted(dims)
    return "{" + ",&nbsp;".join(sorted_dims) + "}"


def _render_task_node(
    task_id: str,
    taskDef: TaskDef,
    universe: DimensionUniverse,
    file: Any,
    show_dimensions: bool,
    expand_dimensions: bool,
) -> None:
    """Render a single task node in the Mermaid diagram.

    Parameters
    ----------
    task_id : str
        Unique Mermaid node identifier for this task.
    taskDef : TaskDef
        The pipeline task definition, which includes the task label, task name,
        and connections.
    universe : DimensionUniverse
        Used to conform and sort the task's dimensions.
    file : file-like
        The output file-like object to write the Mermaid node definition.
    show_dimensions : bool
        If True, display the task's dimensions after conforming them.
    expand_dimensions : bool
        If True, expand dimension names to include all components.
    """
    # Basic info: bold label, then task name.
    lines = [
        f"<b>{html.escape(taskDef.label)}</b>",
        html.escape(taskDef.taskName),
    ]

    # If requested, display the task's conformed dimensions.
    if show_dimensions and taskDef.connections and taskDef.connections.dimensions:
        if expand_dimensions:
            task_dims = _expand_dimensions(taskDef.connections.dimensions, universe)
        else:
            task_dims = list(taskDef.connections.dimensions)
        if task_dims:
            dim_str = _format_dimensions(task_dims)
            lines.append(f"<i>dimensions:</i>&nbsp;{dim_str}")

    # Join with <br> for line breaks and define the node with the label.
    label = "<br>".join(lines)
    print(f'{task_id}["{label}"]', file=file)
    print(f"class {task_id} task;", file=file)


def _render_dataset_node(
    ds_id: str,
    ds_name: str,
    connection: connectionTypes.BaseConnection,
    universe: DimensionUniverse,
    file: Any,
    show_dimensions: bool,
    expand_dimensions: bool,
    show_storage: bool,
) -> None:
    """Render a dataset-type node in the Mermaid diagram.

    Parameters
    ----------
    ds_id : str
        Unique Mermaid node identifier for this dataset.
    ds_name : str
        The dataset type name.
    connection : BaseConnection
        The dataset connection object, potentially dimensioned and having a
        storage class.
    universe : DimensionUniverse
        Used to conform and sort the dataset's dimensions if it is dimensioned.
    file : file-like
        The output file-like object to write the Mermaid node definition.
    show_dimensions : bool
        If True, display the dataset's conformed dimensions.
    expand_dimensions : bool
        If True, expand dimension names to include all components.
    show_storage : bool
        If True, display the dataset's storage class if available.
    """
    # Start with the dataset name in bold.
    lines = [f"<b>{html.escape(ds_name)}</b>"]

    # If dimensioned and requested, show conformed dimensions.
    ds_dims = []
    if show_dimensions and isinstance(connection, connectionTypes.DimensionedConnection):
        if expand_dimensions:
            ds_dims = _expand_dimensions(connection.dimensions, universe)
        else:
            ds_dims = list(connection.dimensions)

    if ds_dims:
        dim_str = _format_dimensions(ds_dims)
        lines.append(f"<i>dimensions:</i>&nbsp;{dim_str}")

    # If storage class is available and requested, display it.
    if show_storage and getattr(connection, "storageClass", None) is not None:
        lines.append(f"<i>storage&nbsp;class:</i>&nbsp;{html.escape(str(connection.storageClass))}")

    label = "<br>".join(lines)
    print(f'{ds_id}["{label}"]', file=file)
    print(f"class {ds_id} ds;", file=file)


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
    universe = DimensionUniverse()

    # Ensure that pipeline is iterable of task definitions.
    if isinstance(pipeline, Pipeline):
        pipeline = pipeline.to_graph()._iter_task_defs()

    # Open file if needed.
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    # Begin the Mermaid code block with top-down layout.
    print("flowchart TD", file=file)

    # Define classes for tasks and datasets.
    print(
        "classDef task fill:#B1F2EF,color:#000,stroke:#000,stroke-width:3px,"
        "font-family:Monospace,font-size:14px,text-align:left;",
        file=file,
    )
    print(
        "classDef ds fill:#F5F5F5,color:#000,stroke:#00BABC,stroke-width:3px,"
        "font-family:Monospace,font-size:14px,text-align:left,rx:10,ry:10;",
        file=file,
    )

    # Track which datasets have been rendered to avoid duplicates.
    allDatasets: set[str | tuple[str, str]] = set()

    # Used for linking metadata datasets after tasks are processed.
    labelToTaskName = {}
    metadataNodesToLink = set()

    # We'll store edges as (from_node, to_node, is_prerequisite) tuples.
    edges: list[tuple[str, str, bool]] = []

    def get_task_id(idx: int) -> str:
        """Generate a safe Mermaid node ID for a task.

        Parameters
        ----------
        idx : `int`
            Task index.

        Returns
        -------
        id : `str`
            Node ID for a task.
        """
        return f"TASK_{idx}"

    def get_dataset_id(name: str) -> str:
        """Generate a safe Mermaid node ID for a dataset.

        Parameters
        ----------
        name : `str`
            Dataset name.

        Returns
        -------
        id : `str`
            Node ID for the dataset.
        """
        # Replace non-alphanumerics with underscores.
        return "DATASET_" + re.sub(r"[^0-9A-Za-z_]", "_", name)

    metadata_pattern = re.compile(r"^(.*)_metadata$")

    # Sort tasks by label for consistent diagram ordering.
    pipeline_tasks = sorted(pipeline, key=lambda x: x.label)

    # Process each task and its connections.
    for idx, taskDef in enumerate(pipeline_tasks):
        task_id = get_task_id(idx)
        labelToTaskName[taskDef.label] = task_id

        # Render the task node.
        _render_task_node(task_id, taskDef, universe, file, show_dimensions, expand_dimensions)

        # Handle standard inputs (non-prerequisite).
        for attr in sorted(iterConnections(taskDef.connections, "inputs"), key=lambda x: x.name):
            ds_id = get_dataset_id(attr.name)
            if attr.name not in allDatasets:
                _render_dataset_node(
                    ds_id, attr.name, attr, universe, file, show_dimensions, expand_dimensions, show_storage
                )
                allDatasets.add(attr.name)
            edges.append((ds_id, task_id, False))

            # Handle component datasets (composite -> component).
            nodeName, component = DatasetType.splitDatasetTypeName(attr.name)
            if component is not None and (nodeName, attr.name) not in allDatasets:
                ds_id_parent = get_dataset_id(nodeName)
                if nodeName not in allDatasets:
                    _render_dataset_node(
                        ds_id_parent,
                        nodeName,
                        attr,
                        universe,
                        file,
                        show_dimensions,
                        expand_dimensions,
                        show_storage,
                    )
                    allDatasets.add(nodeName)
                edges.append((ds_id_parent, ds_id, False))
                allDatasets.add((nodeName, attr.name))

            # If this is a metadata dataset, record it for linking later.
            if (match := metadata_pattern.match(attr.name)) is not None:
                matchTaskLabel = match.group(1)
                metadataNodesToLink.add((matchTaskLabel, attr.name))

        # Handle prerequisite inputs (to be drawn with a dashed line).
        for attr in sorted(iterConnections(taskDef.connections, "prerequisiteInputs"), key=lambda x: x.name):
            ds_id = get_dataset_id(attr.name)
            if attr.name not in allDatasets:
                _render_dataset_node(
                    ds_id, attr.name, attr, universe, file, show_dimensions, expand_dimensions, show_storage
                )
                allDatasets.add(attr.name)
            edges.append((ds_id, task_id, True))

            # If this is a metadata dataset, record it for linking later.
            if (match := metadata_pattern.match(attr.name)) is not None:
                matchTaskLabel = match.group(1)
                metadataNodesToLink.add((matchTaskLabel, attr.name))

        # Handle outputs (task -> dataset).
        for attr in sorted(iterConnections(taskDef.connections, "outputs"), key=lambda x: x.name):
            ds_id = get_dataset_id(attr.name)
            if attr.name not in allDatasets:
                _render_dataset_node(
                    ds_id, attr.name, attr, universe, file, show_dimensions, expand_dimensions, show_storage
                )
                allDatasets.add(attr.name)
            edges.append((task_id, ds_id, False))

    # Link metadata datasets after all tasks processed.
    for matchLabel, dsTypeName in metadataNodesToLink:
        if (result := labelToTaskName.get(matchLabel)) is not None:
            ds_id = get_dataset_id(dsTypeName)
            edges.append((result, ds_id, False))

    # Print all edges and track which are prerequisite.
    prereq_indices = []
    for i, (f, t, p) in enumerate(edges):
        print(f"{f} --> {t}", file=file)
        if p:
            prereq_indices.append(i)

    # Apply default edge style
    print("linkStyle default stroke:#000,stroke-width:1.5px,font-family:Monospace,font-size:14px;", file=file)

    # Apply dashed style for all prerequisite edges in one line.
    if prereq_indices:
        prereq_str = ",".join(str(i) for i in prereq_indices)
        print(f"linkStyle {prereq_str} stroke-dasharray:5;", file=file)

    if close:
        file.close()
