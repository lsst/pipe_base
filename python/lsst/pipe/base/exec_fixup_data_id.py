# This file is part of ctrl_mpexec.
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

__all__ = ["ExecutionGraphFixup"]

import contextlib
import itertools
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

import networkx as nx

from lsst.pipe.base import QuantumGraph, QuantumNode

from .executionGraphFixup import ExecutionGraphFixup


class ExecFixupDataId(ExecutionGraphFixup):
    """Implementation of ExecutionGraphFixup for ordering of tasks based
    on DataId values.

    This class is a trivial implementation mostly useful as an example,
    though it can be used to make actual fixup instances by defining
    a method that instantiates it, e.g.::

        # lsst/ap/verify/ci_fixup.py

        from lsst.ctrl.mpexec.execFixupDataId import ExecFixupDataId


        def assoc_fixup():
            return ExecFixupDataId(
                taskLabel="ap_assoc", dimensions=("visit", "detector")
            )

    and then executing pipetask::

        pipetask run --graph-fixup=lsst.ap.verify.ci_fixup.assoc_fixup ...

    This will add new dependencies between quanta executed by the task with
    label "ap_assoc". Quanta with higher visit number will depend on quanta
    with lower visit number and their execution will wait until lower visit
    number finishes.

    Parameters
    ----------
    taskLabel : `str`
        The label of the task for which to add dependencies.
    dimensions : `str` or sequence [`str`]
        One or more dimension names, quanta execution will be ordered
        according to values of these dimensions.
    reverse : `bool`, optional
        If `False` (default) then quanta with higher values of dimensions
        will be executed after quanta with lower values, otherwise the order
        is reversed.
    """

    def __init__(self, taskLabel: str, dimensions: str | Sequence[str], reverse: bool = False):
        self.taskLabel = taskLabel
        self.dimensions = dimensions
        self.reverse = reverse
        if isinstance(self.dimensions, str):
            self.dimensions = (self.dimensions,)
        else:
            self.dimensions = tuple(self.dimensions)

    def _key(self, qnode: QuantumNode) -> tuple[Any, ...]:
        """Produce comparison key for quantum data.

        Parameters
        ----------
        qnode : `QuantumNode`
            An individual node in a `~lsst.pipe.base.QuantumGraph`

        Returns
        -------
        key : `tuple`
        """
        dataId = qnode.quantum.dataId
        assert dataId is not None, "Quantum DataId cannot be None"
        key = tuple(dataId[dim] for dim in self.dimensions)
        return key

    def fixupQuanta(self, graph: QuantumGraph) -> QuantumGraph:
        taskDef = graph.findTaskDefByLabel(self.taskLabel)
        if taskDef is None:
            raise ValueError(f"Cannot find task with label {self.taskLabel}")
        quanta = list(graph.getNodesForTask(taskDef))
        keyQuanta = defaultdict(list)
        for q in quanta:
            key = self._key(q)
            keyQuanta[key].append(q)
        keys = sorted(keyQuanta.keys(), reverse=self.reverse)
        networkGraph = graph.graph

        for prev_key, key in itertools.pairwise(keys):
            for prev_node in keyQuanta[prev_key]:
                for node in keyQuanta[key]:
                    # remove any existing edges between the two nodes, but
                    # don't fail if there are not any. Both directions need
                    # tried because in a directed graph, order maters
                    for edge in ((node, prev_node), (prev_node, node)):
                        with contextlib.suppress(nx.NetworkXException):
                            networkGraph.remove_edge(*edge)

                    networkGraph.add_edge(prev_node, node)
        return graph
