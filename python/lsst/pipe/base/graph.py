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

"""Module defining quantum graph classes and related methods.

There could be different representations of the quantum graph depending
on the client needs. Presently this module contains graph implementation
which is based on requirements of command-line environment. In the future
we could add other implementations and methods to convert between those
representations.
"""

# "exported" names
__all__ = ["QuantumGraph", "QuantumGraphTaskNodes", "QuantumIterData"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from itertools import chain
from dataclasses import dataclass
import pickle
from typing import List, FrozenSet, Mapping

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .pipeline import TaskDef
from .pipeTools import orderPipeline
from lsst.daf.butler import DatasetRef, DatasetType, NamedKeyDict, Quantum

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


@dataclass
class QuantumIterData:
    """Helper class for iterating over quanta in a graph.

    The `QuantumGraph.traverse` method needs to return topologically ordered
    Quanta together with their dependencies. This class is used as a value
    for the iterator, it contains enumerated Quantum and its dependencies.
    """

    __slots__ = ["index", "quantum", "taskDef", "dependencies"]

    index: int
    """Index of this Quantum, a unique but arbitrary integer."""

    quantum: Quantum
    """Quantum corresponding to a graph node."""

    taskDef: TaskDef
    """Task class to be run on this quantum, and corresponding label and
    config.
    """

    dependencies: FrozenSet(int)
    """Possibly empty set of indices of dependencies for this Quantum.
    Dependencies include other nodes in the graph; they do not reflect data
    already in butler (there are no graph nodes for those).
    """


@dataclass
class QuantumGraphTaskNodes:
    """QuantumGraphTaskNodes represents a bunch of nodes in an quantum graph
    corresponding to a single task.

    The node in quantum graph is represented by the `PipelineTask` and a
    single `~lsst.daf.butler.Quantum` instance. One possible representation
    of the graph is just a list of nodes without edges (edges can be deduced
    from nodes' quantum inputs and outputs if needed). That representation can
    be reduced to the list of PipelineTasks (or their corresponding TaskDefs)
    and the corresponding list of Quanta. This class is used in this reduced
    representation for a single task, and full `QuantumGraph` is a sequence of
    tinstances of this class for one or more tasks.

    Different frameworks may use different graph representation, this
    representation was based mostly on requirements of command-line
    executor which does not need explicit edges information.
    """

    taskDef: TaskDef
    """Task defintion for this set of nodes."""

    quanta: List[Quantum]
    """List of quanta corresponding to the task."""

    initInputs: Mapping[DatasetType, DatasetRef]
    """Datasets that must be loaded or created to construct this task."""

    initOutputs: Mapping[DatasetType, DatasetRef]
    """Datasets that may be written after constructing this task."""


class QuantumGraph(list):
    """QuantumGraph is a sequence of `QuantumGraphTaskNodes` objects.

    Typically the order of the tasks in the list will be the same as the
    order of tasks in a pipeline (obviously depends on the code which
    constructs graph).

    Parameters
    ----------
    iterable : iterable of `QuantumGraphTaskNodes`, optional
        Initial sequence of per-task nodes.
    """
    def __init__(self, iterable=None):
        list.__init__(self, iterable or [])
        self.initInputs = NamedKeyDict()
        self.initIntermediates = NamedKeyDict()
        self.initOutputs = NamedKeyDict()

    initInputs: NamedKeyDict
    """Datasets that must be provided to construct one or more Tasks in this
    graph, and must be obtained from the data repository.

    This is disjoint with both `initIntermediates` and `initOutputs`.
    """

    initIntermediates: NamedKeyDict
    """Datasets that must be provided to construct one or more Tasks in this
    graph, but are also produced after constructing a Task in this graph.

    This is disjoint with both `initInputs` and `initOutputs`.
    """

    initOutputs: NamedKeyDict
    """Datasets that are produced after constructing a Task in this graph,
    and are not used to construct any other Task in this graph.

    This is disjoint from both `initInputs` and `initIntermediates`.
    """

    @classmethod
    def load(cls, file, universe):
        """Read QuantumGraph from a file that was made by `save`.

        Parameters
        ----------
        file : `io.BufferedIOBase`
            File with pickle data open in binary mode.
        universe: `~lsst.daf.butler.DimensionUniverse`
            DimensionUniverse instance, not used by the method itself but
            needed to ensure that registry data structures are initialized.

        Returns
        -------
        graph : `QuantumGraph`
            Resulting QuantumGraph instance.

        Raises
        ------
        TypeError
            Raised if pickle contains instance of a type other than
            QuantumGraph.

        Notes
        -----
        Reading Quanta from pickle requires existence of singleton
        DimensionUniverse which is usually instantiated during Registry
        initializaion. To make sure that DimensionUniverse exists this method
        accepts dummy DimensionUniverse argument.
        """
        qgraph = pickle.load(file)
        if not isinstance(qgraph, QuantumGraph):
            raise TypeError(f"QuantumGraph pickle file has contains unexpected object type: {type(qgraph)}")
        return qgraph

    def save(self, file):
        """Save QuantumGraph to a file.

        Presently we store QuantumGraph in pickle format, this could
        potentially change in the future if better format is found.

        Parameters
        ----------
        file : `io.BufferedIOBase`
            File to write pickle data open in binary mode.
        """
        pickle.dump(self, file)

    def quanta(self):
        """Iterator over quanta in a graph.

        Quanta are returned in unspecified order.

        Yields
        ------
        taskDef : `TaskDef`
            Task definition for a Quantum.
        quantum : `~lsst.daf.butler.Quantum`
            Single quantum.
        """
        for taskNodes in self:
            taskDef = taskNodes.taskDef
            for quantum in taskNodes.quanta:
                yield taskDef, quantum

    def quantaAsQgraph(self):
        """Iterator over quanta in a graph.

        QuantumGraph containing individual quanta are returned.

        Yields
        ------
        graph : `QuantumGraph`
        """
        for taskDef, quantum in self.quanta():
            node = QuantumGraphTaskNodes(taskDef, [quantum],
                                         quantum.initInputs, quantum.outputs)
            graph = QuantumGraph([node])
            yield graph

    def countQuanta(self):
        """Return total count of quanta in a graph.

        Returns
        -------
        count : `int`
            Number of quanta in a graph.
        """
        return sum(len(taskNodes.quanta) for taskNodes in self)

    def traverse(self):
        """Return topologically ordered Quanta and their dependencies.

        This method iterates over all Quanta in topological order, enumerating
        them during iteration. Returned `QuantumIterData` object contains
        Quantum instance, its ``index`` and the ``index`` of all its
        prerequsites (Quanta that produce inputs for this Quantum):

        - the ``index`` values are generated by an iteration of a
          QuantumGraph, and are not intrinsic to the QuantumGraph
        - during iteration, each ID will appear in index before it ever
          appears in dependencies.

        Yields
        ------
        quantumData : `QuantumIterData`
        """

        def orderedTaskNodes(graph):
            """Return topologically ordered task nodes.

            Yields
            ------
            nodes : `QuantumGraphTaskNodes`
            """
            # Tasks in a graph are probably topologically sorted already but there
            # is no guarantee for that. Just re-construct Pipeline and order tasks
            # in a pipeline using existing method.
            nodesMap = {id(item.taskDef): item for item in graph}
            pipeline = orderPipeline([item.taskDef for item in graph])
            for taskDef in pipeline:
                yield nodesMap[id(taskDef)]

        index = 0
        outputs = {}  # maps (DatasetType.name, dataId) to its producing quantum index
        for nodes in orderedTaskNodes(self):
            for quantum in nodes.quanta:

                # Find quantum dependencies (must be in `outputs` already)
                prereq = []
                for dataRef in chain.from_iterable(quantum.predictedInputs.values()):
                    # if data exists in butler then `id` is not None
                    if dataRef.id is None:
                        # Get the base name if this is a component
                        name, component = dataRef.datasetType.nameAndComponent()
                        key = (name, dataRef.dataId)
                        try:
                            prereq.append(outputs[key])
                        except KeyError:
                            # The Quantum that makes our inputs is not in the graph,
                            # this could happen if we run on a "split graph" which is
                            # usually just one quantum. Check for number of Quanta
                            # in a graph and ignore error if it's just one.
                            # TODO: This code has to be removed or replaced with
                            # something more generic
                            if not (len(self) == 1 and len(self[0].quanta) == 1):
                                raise

                # Update `outputs` with this quantum outputs
                for dataRef in chain.from_iterable(quantum.outputs.values()):
                    key = (dataRef.datasetType.name, dataRef.dataId)
                    outputs[key] = index

                yield QuantumIterData(index=index, quantum=quantum, taskDef=nodes.taskDef,
                                      dependencies=frozenset(prereq))
                index += 1
