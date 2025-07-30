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

__all__ = ["ExecutionGraphFixup"]

import itertools
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence

import networkx as nx

from lsst.daf.butler import DataCoordinate, DataIdValue

from .execution_graph_fixup import ExecutionGraphFixup
from .graph import QuantumGraph


class ExecFixupDataId(ExecutionGraphFixup):
    """Implementation of ExecutionGraphFixup for ordering of tasks based
    on DataId values.

    This class is a trivial implementation mostly useful as an example,
    though it can be used to make actual fixup instances by defining
    a method that instantiates it, e.g.::

        # lsst/ap/verify/ci_fixup.py

        from lsst.pipe.base.exec_fixup_data_id import ExecFixupDataId


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

    def fixupQuanta(self, graph: QuantumGraph) -> QuantumGraph:
        raise NotImplementedError()

    def fixup_graph(
        self, xgraph: nx.DiGraph, quanta_by_task: Mapping[str, Mapping[DataCoordinate, uuid.UUID]]
    ) -> None:
        quanta_by_sort_key: defaultdict[tuple[DataIdValue, ...], list[uuid.UUID]] = defaultdict(list)
        for data_id, quantum_id in quanta_by_task[self.taskLabel].items():
            key = tuple(data_id[dim] for dim in self.dimensions)
            quanta_by_sort_key[key].append(quantum_id)
        sorted_keys = sorted(quanta_by_sort_key.keys(), reverse=self.reverse)
        for prev_key, key in itertools.pairwise(sorted_keys):
            xgraph.add_edges_from(itertools.product(quanta_by_sort_key[prev_key], quanta_by_sort_key[key]))
