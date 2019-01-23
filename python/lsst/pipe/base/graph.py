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

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .pipeline import Pipeline
from .pipeTools import orderPipeline
from lsst.daf.butler import DataId

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class QuantumIterData:
    """Helper class for iterating over quanta in a graph.

    `QuantumGraph.traverse` method needs to return topologically ordered
    Quanta together with their dependencies. This class is used as a value
    for iterator, it contains enumerated Quantum and its prerequisites.

    Parameters
    ----------
    quantumId : `int`
        Index of this Quantum, unique but arbitrary integer.
    quantum : `~lsst.daf.butler.Quantum`
        Quantum corresponding to a graph node.
    taskDef : `TaskDef`
        Task to be run on this quantum.
    prerequisites : iterable of `int`
        Possibly empty sequence of indices of prerequisites for this Quantum.
        Prerequisites include other nodes in a graph, they do not reflect
        data already in butler (there are no graph nodes for those).
    """

    __slots__ = ["quantumId", "quantum", "taskDef", "prerequisites"]

    def __init__(self, quantumId, quantum, taskDef, prerequisites):
        self.quantumId = quantumId
        self.quantum = quantum
        self.taskDef = taskDef
        self.prerequisites = frozenset(prerequisites)

    def __str__(self):
        return "QuantumIterData({}, {}, {})".format(self.quantumId,
                                                    self.taskDef,
                                                    self.prerequisites)


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

    Attributes
    ----------
    taskDef : `TaskDef`
        Task defintion for this set of nodes.
    quanta : `list` of `~lsst.daf.butler.Quantum`
        List of quanta corresponding to the task.
    """
    def __init__(self, taskDef, quanta):
        self.taskDef = taskDef
        self.quanta = quanta


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
        self.initInputs = []
        self.initOutputs = []
        self._inputDatasetTypes = set()
        self._outputDatasetTypes = set()

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

    def traverse(self):
        """Return topologically ordered Quanta and their prerequisites.

        This method iterates over all Quanta in topological order, enumerating
        them during iteration. Returned `QuantumIterData` object contains
        Quantum instance, its ``quantumId`` and ``quantumId`` of all its
        prerequsites (Quanta that produce inputs for this Quantum):
        - the ``quantumId`` values are generated by an iteration of a
          QuantumGraph, and are not intrinsic to the QuantumGraph
        - during iteration, each ID will appear in quantumId before it ever
          appears in prerequisites.

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
            pipeline = orderPipeline(Pipeline(item.taskDef for item in graph))
            for taskDef in pipeline:
                yield nodesMap[id(taskDef)]

        index = 0
        outputs = {}  # maps (DatasetType.name, DataId) to its producing quantum index
        for nodes in orderedTaskNodes(self):
            for quantum in nodes.quanta:

                # Find quantum prerequisites (must be in `outputs` already)
                prereq = []
                for dataRef in chain.from_iterable(quantum.predictedInputs.values()):
                    # if data exists in butler then `id` is not None
                    if dataRef.id is None:
                        key = (dataRef.datasetType.name, DataId(dataRef.dataId))
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
                    key = (dataRef.datasetType.name, DataId(dataRef.dataId))
                    outputs[key] = index

                yield QuantumIterData(index, quantum, nodes.taskDef, prereq)
                index += 1

    def getDatasetTypes(self, initInputs=True, initOutputs=True, inputs=True, outputs=True):
        total = set()
        if initInputs:
            for dsRef in self.initInputs:
                total.add(dsRef.datasetType)
        if initOutputs:
            for dsRef in self.initOutputs:
                total.add(dsRef.datasetType)
        if inputs:
            total |= self._inputDatasetTypes
        if outputs:
            total |= self._outputDatasetTypes
        return total
