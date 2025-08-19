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

import uuid
from abc import ABC
from collections.abc import Mapping

import networkx

from lsst.daf.butler import DataCoordinate

from .graph import QuantumGraph


class ExecutionGraphFixup(ABC):
    """Interface for classes which update quantum graphs before execution.

    Notes
    -----
    The primary goal of this class is to modify quanta dependencies which may
    not be possible to reflect in a quantum graph using standard tools. One
    known use case for that is to guarantee particular execution order of
    visits in CI jobs for cases when outcome depends on the processing order of
    visits (e.g. AP association pipeline).

    Instances of this class receive a preliminary graph and are allowed to
    add edges, as long as those edges do not result in a cycle.  Edges and
    nodes may not be removed.

    New subclasses should implement only `fixup_graph`, which will always be
    called first.  `fixupQuanta` is only called if `fixup_graph` raises
    `NotImplementedError`.
    """

    def fixupQuanta(self, graph: QuantumGraph) -> QuantumGraph:
        """Update quanta in a graph.

        Parameters
        ----------
        graph : `.QuantumGraph`
            Quantum Graph that will be executed by the executor.

        Returns
        -------
        graph : `.QuantumGraph`
            Modified graph.

        Notes
        -----
        This hook is provided for backwards compatibility only.
        """
        raise NotImplementedError()

    def fixup_graph(
        self, xgraph: networkx.DiGraph, quanta_by_task: Mapping[str, Mapping[DataCoordinate, uuid.UUID]]
    ) -> None:
        """Update a networkx graph of quanta in place by adding edges to
        further constrain the ordering.

        Parameters
        ----------
        xgraph : `networkx.DiGraph`
            A directed acyclic graph of quanta to modify in place.  Node keys
            are quantum UUIDs, and attributes include ``task_label`` (`str`)
            and ``data_id`` (a full `lsst.daf.butler.DataCoordinate`, without
            dimension records attached).  Edges may be added, but not removed.
            Nodes may not be modified.
        quanta_by_task : `~collections.abc.Mapping` [ `str`,\
                `~collections.abc.Mapping` [ `lsst.daf.butler.DataCoordinate`,\
                `uuid.UUID` ] ]
            All quanta in the graph, grouped first by task label and then by
            data ID.
        """
        raise NotImplementedError()
