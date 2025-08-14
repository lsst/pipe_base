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

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from .pipeline import Pipeline

if TYPE_CHECKING:
    from .graph import QuantumGraph
    from .pipeline import TaskDef
    from .quantum_graph import PredictedQuantumGraph


def graph2dot(qgraph: QuantumGraph | PredictedQuantumGraph, file: Any) -> None:
    """Convert QuantumGraph into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.

    Parameters
    ----------
    qgraph : `lsst.pipe.base.QuantumGraph` or \
            `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
        Quantum graph object.
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
    from .quantum_graph import PredictedQuantumGraph, visualization

    if not isinstance(qgraph, PredictedQuantumGraph):
        qgraph = PredictedQuantumGraph.from_old_quantum_graph(qgraph)

    # open a file if needed
    close = False
    if not hasattr(file, "write"):
        file = open(file, "w")
        close = True

    v = visualization.QuantumGraphDotVisualizer()
    v.write_bipartite(qgraph, file)

    if close:
        file.close()


def pipeline2dot(pipeline: Pipeline | Iterable[TaskDef], file: Any) -> None:
    """Convert `~lsst.pipe.base.Pipeline` into GraphViz digraph.

    This method is mostly for documentation/presentation purposes.
    Unlike other methods this method does not validate graph consistency.

    Parameters
    ----------
    pipeline : `.Pipeline` or `~collections.abc.Iterable` [ `.TaskDef` ]
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
    from .pipeline_graph import PipelineGraph, visualization

    # open a file if needed
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
    visualization.show_dot(pg, stream=file, dataset_types=True)

    if close:
        file.close()
