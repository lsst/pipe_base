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
__all__ = ["QuantumGraphNodes", "QuantumGraph"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# -----------------------------
#  Imports for other modules --
# -----------------------------

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class QuantumGraphNodes:
    """QuantumGraphNodes represents a bunch of nodes in an quantum graph.

    The node in quantum graph is represented by the `PipelineTask` and a
    single `Quantum` instance. One possible representation of the graph is
    just a list of nodes without edges (edges can be deduced from nodes'
    quantum inputs and outputs if needed). That representation can be reduced
    to the list of PipelineTasks and the corresponding list of Quanta.
    This class defines this reduced representation.

    Different frameworks may use different graph representation, this
    representation was based mostly on requirements of command-line
    executor which does not need explicit edges information.

    Attributes
    ----------
    taskDef : :py:class:`TaskDef`
        Task defintion for this set of nodes.
    quanta : `list` of :py:class:`lsst.daf.butler.Quantum`
        List of quanta corresponding to the task.
    """
    def __init__(self, taskDef, quanta):
        self.taskDef = taskDef
        self.quanta = quanta


class QuantumGraph(list):
    """QuantumGraph is a sequence of QuantumGraphNodes objects.

    Typically the order of the tasks in the list will be the same as the
    order of tasks in a pipeline (obviously depends on the code which
    constructs graph).

    Parameters
    ----------
    iterable : iterable of :py:class:`QuantumGraphNodes` instances, optional
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

        Yields
        ------
        taskDef : `TaskDef`
            Task definition for a Quantum.
        quantum : `Quantum`
            Single quantum.
        """
        for taskNodes in self:
            taskDef = taskNodes.taskDef
            for quantum in taskNodes.quanta:
                yield taskDef, quantum

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
