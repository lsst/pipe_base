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

"""Module defining few methods to generate GraphViz diagrams from pipelines
or quantum graphs.
"""

from __future__ import annotations

__all__ = ["graph2dot", "pipeline2dot"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import html
import io
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import DatasetType, DimensionUniverse

from . import connectionTypes
from .connections import iterConnections
from .pipeline import Pipeline

if TYPE_CHECKING:
    from lsst.daf.butler import DatasetRef
    from lsst.pipe.base import QuantumGraph, QuantumNode, TaskDef

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# Attributes applied to directed graph objects.
_NODELABELPOINTSIZE = "18"
_ATTRIBS = dict(
    defaultGraph=dict(splines="ortho", nodesep="0.5", ranksep="0.75", pad="0.5"),
    defaultNode=dict(shape="box", fontname="Monospace", fontsize="14", margin="0.2,0.1", penwidth="3"),
    defaultEdge=dict(color="black", arrowsize="1.5", penwidth="1.5"),
    task=dict(style="filled", color="black", fillcolor="#B1F2EF"),
    quantum=dict(style="filled", color="black", fillcolor="#B1F2EF"),
    dsType=dict(style="rounded,filled,bold", color="#00BABC", fillcolor="#F5F5F5"),
    dataset=dict(style="rounded,filled,bold", color="#00BABC", fillcolor="#F5F5F5"),
)


def _renderDefault(type: str, attribs: dict[str, str], file: io.TextIOBase) -> None:
    """Set default attributes for a given type."""
    default_attribs = ", ".join([f'{key}="{val}"' for key, val in attribs.items()])
    print(f"{type} [{default_attribs}];", file=file)


def _renderNode(file: io.TextIOBase, nodeName: str, style: str, labels: list[str]) -> None:
    """Render GV node"""
    label = r"</TD></TR><TR><TD>".join(labels)
    attrib_dict = dict(_ATTRIBS[style], label=label)
    pre = '<<TABLE BORDER="0" CELLPADDING="5"><TR><TD>'
    post = "</TD></TR></TABLE>>"
    attrib = ", ".join(
        [
            f'{key}="{val}"' if key != "label" else f"{key}={pre}{val}{post}"
            for key, val in attrib_dict.items()
        ]
    )
    print(f'"{nodeName}" [{attrib}];', file=file)


def _renderTaskNode(nodeName: str, taskDef: TaskDef, file: io.TextIOBase, idx: Any = None) -> None:
    """Render GV node for a task"""
    labels = [
        f'<B><FONT POINT-SIZE="{_NODELABELPOINTSIZE}">' + html.escape(taskDef.label) + "</FONT></B>",
        html.escape(taskDef.taskName),
    ]
    if idx is not None:
        labels.append(f"<I>index:</I>&nbsp;{idx}")
    if taskDef.connections:
        # don't print collection of str directly to avoid visually noisy quotes
        dimensions_str = ", ".join(sorted(taskDef.connections.dimensions))
        labels.append(f"<I>dimensions:</I>&nbsp;{html.escape(dimensions_str)}")
    _renderNode(file, nodeName, "task", labels)


def _renderQuantumNode(
    nodeName: str, taskDef: TaskDef, quantumNode: QuantumNode, file: io.TextIOBase
) -> None:
    """Render GV node for a quantum"""
    labels = [f"{quantumNode.nodeId}", html.escape(taskDef.label)]
    dataId = quantumNode.quantum.dataId
    assert dataId is not None, "Quantum DataId cannot be None"
    labels.extend(f"{key} = {dataId[key]}" for key in sorted(dataId.required.keys()))
    _renderNode(file, nodeName, "quantum", labels)


def _renderDSTypeNode(name: str, dimensions: list[str], file: io.TextIOBase) -> None:
    """Render GV node for a dataset type"""
    labels = [f'<B><FONT POINT-SIZE="{_NODELABELPOINTSIZE}">' + html.escape(name) + "</FONT></B>"]
    if dimensions:
        labels.append("<I>dimensions:</I>&nbsp;" + html.escape(", ".join(sorted(dimensions))))
    _renderNode(file, name, "dsType", labels)


def _renderDSNode(nodeName: str, dsRef: DatasetRef, file: io.TextIOBase) -> None:
    """Render GV node for a dataset"""
    labels = [html.escape(dsRef.datasetType.name), f"run: {dsRef.run!r}"]
    labels.extend(f"{key} = {dsRef.dataId[key]}" for key in sorted(dsRef.dataId.required.keys()))
    _renderNode(file, nodeName, "dataset", labels)


def _renderEdge(fromName: str, toName: str, file: io.TextIOBase, **kwargs: Any) -> None:
    """Render GV edge"""
    if kwargs:
        attrib = ", ".join([f'{key}="{val}"' for key, val in kwargs.items()])
        print(f'"{fromName}" -> "{toName}" [{attrib}];', file=file)
    else:
        print(f'"{fromName}" -> "{toName}";', file=file)


def _datasetRefId(dsRef: DatasetRef) -> str:
    """Make an identifying string for given ref"""
    dsId = [dsRef.datasetType.name]
    dsId.extend(f"{key} = {dsRef.dataId[key]}" for key in sorted(dsRef.dataId.required.keys()))
    return ":".join(dsId)


def _makeDSNode(dsRef: DatasetRef, allDatasetRefs: dict[str, str], file: io.TextIOBase) -> str:
    """Make new node for dataset if  it does not exist.

    Returns node name.
    """
    dsRefId = _datasetRefId(dsRef)
    nodeName = allDatasetRefs.get(dsRefId)
    if nodeName is None:
        idx = len(allDatasetRefs)
        nodeName = f"dsref_{idx}"
        allDatasetRefs[dsRefId] = nodeName
        _renderDSNode(nodeName, dsRef, file)
    return nodeName


# ------------------------
#  Exported definitions --
# ------------------------


def graph2dot(qgraph: QuantumGraph, file: Any) -> None:
    """Convert QuantumGraph into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.

    Parameters
    ----------
    qgraph : `lsst.pipe.base.QuantumGraph`
        QuantumGraph instance.
    file : `str` or file object
        File where GraphViz graph (DOT language) is written, can be a file name
        or file object.

    Raises
    ------
    OSError
        Raised if the output file cannot be opened.
    ImportError
        Raised if the task class cannot be imported.
    """
    # open a file if needed
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    print("digraph QuantumGraph {", file=file)
    _renderDefault("graph", _ATTRIBS["defaultGraph"], file)
    _renderDefault("node", _ATTRIBS["defaultNode"], file)
    _renderDefault("edge", _ATTRIBS["defaultEdge"], file)

    allDatasetRefs: dict[str, str] = {}
    for taskId, taskDef in enumerate(qgraph.taskGraph):
        quanta = qgraph.getNodesForTask(taskDef)
        for qId, quantumNode in enumerate(quanta):
            # node for a task
            taskNodeName = f"task_{taskId}_{qId}"
            _renderQuantumNode(taskNodeName, taskDef, quantumNode, file)

            # quantum inputs
            for dsRefs in quantumNode.quantum.inputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    _renderEdge(nodeName, taskNodeName, file)

            # quantum outputs
            for dsRefs in quantumNode.quantum.outputs.values():
                for dsRef in dsRefs:
                    nodeName = _makeDSNode(dsRef, allDatasetRefs, file)
                    _renderEdge(taskNodeName, nodeName, file)

    print("}", file=file)
    if close:
        file.close()


def pipeline2dot(pipeline: Pipeline | Iterable[TaskDef], file: Any) -> None:
    """Convert `~lsst.pipe.base.Pipeline` into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.
    Unlike other methods this method does not validate graph consistency.

    Parameters
    ----------
    pipeline : `lsst.pipe.base.Pipeline`
        Pipeline description.
    file : `str` or file object
        File where GraphViz graph (DOT language) is written, can be a file name
        or file object.

    Raises
    ------
    OSError
        Raised if the output file cannot be opened.
    ImportError
        Raised if the task class cannot be imported.
    """
    universe = DimensionUniverse()

    def expand_dimensions(connection: connectionTypes.BaseConnection) -> list[str]:
        """Return expanded list of dimensions, with special skypix treatment.

        Parameters
        ----------
        connection : `list` [`str`]
            Connection to examine.

        Returns
        -------
        dimensions : `list` [`str`]
            Expanded list of dimensions.
        """
        dimension_set = set()
        if isinstance(connection, connectionTypes.DimensionedConnection):
            dimension_set = set(connection.dimensions)
        skypix_dim = []
        if "skypix" in dimension_set:
            dimension_set.remove("skypix")
            skypix_dim = ["skypix"]
        dimensions = universe.conform(dimension_set)
        return list(dimensions.names) + skypix_dim

    # open a file if needed
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    print("digraph Pipeline {", file=file)
    _renderDefault("graph", _ATTRIBS["defaultGraph"], file)
    _renderDefault("node", _ATTRIBS["defaultNode"], file)
    _renderDefault("edge", _ATTRIBS["defaultEdge"], file)

    allDatasets: set[str | tuple[str, str]] = set()
    if isinstance(pipeline, Pipeline):
        # TODO: DM-40639 will rewrite this code and finish off the deprecation
        # of toExpandedPipeline but for now use the compatibility API.
        pipeline = pipeline.to_graph()._iter_task_defs()

    # The next two lines are a workaround until DM-29658 at which time metadata
    # connections should start working with the above code
    labelToTaskName = {}
    metadataNodesToLink = set()

    for idx, taskDef in enumerate(sorted(pipeline, key=lambda x: x.label)):
        # node for a task
        taskNodeName = f"task{idx}"

        # next line is workaround until DM-29658
        labelToTaskName[taskDef.label] = taskNodeName

        _renderTaskNode(taskNodeName, taskDef, file, None)

        metadataRePattern = re.compile("^(.*)_metadata$")
        for attr in sorted(iterConnections(taskDef.connections, "inputs"), key=lambda x: x.name):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            nodeName, component = DatasetType.splitDatasetTypeName(attr.name)
            _renderEdge(attr.name, taskNodeName, file)
            # connect component dataset types to the composite type that
            # produced it
            if component is not None and (nodeName, attr.name) not in allDatasets:
                _renderEdge(nodeName, attr.name, file)
                allDatasets.add((nodeName, attr.name))
                if nodeName not in allDatasets:
                    dimensions = expand_dimensions(attr)
                    _renderDSTypeNode(nodeName, dimensions, file)
            # The next if block is a workaround until DM-29658 at which time
            # metadata connections should start working with the above code
            if (match := metadataRePattern.match(attr.name)) is not None:
                matchTaskLabel = match.group(1)
                metadataNodesToLink.add((matchTaskLabel, attr.name))

        for attr in sorted(iterConnections(taskDef.connections, "prerequisiteInputs"), key=lambda x: x.name):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            # use dashed line for prerequisite edges to distinguish them
            _renderEdge(attr.name, taskNodeName, file, style="dashed")

        for attr in sorted(iterConnections(taskDef.connections, "outputs"), key=lambda x: x.name):
            if attr.name not in allDatasets:
                dimensions = expand_dimensions(attr)
                _renderDSTypeNode(attr.name, dimensions, file)
                allDatasets.add(attr.name)
            _renderEdge(taskNodeName, attr.name, file)

    # This for loop is a workaround until DM-29658 at which time metadata
    # connections should start working with the above code
    for matchLabel, dsTypeName in metadataNodesToLink:
        # only render an edge to metadata if the label is part of the current
        # graph
        if (result := labelToTaskName.get(matchLabel)) is not None:
            _renderEdge(result, dsTypeName, file)

    print("}", file=file)
    if close:
        file.close()
