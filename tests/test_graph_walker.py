# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import unittest
import uuid

import networkx

from lsst.daf.butler import DataCoordinate
from lsst.pipe.base.graph_walker import GraphWalker
from lsst.pipe.base.tests.mocks import DynamicConnectionConfig, InMemoryRepo


class GraphWalkerTestCase(unittest.TestCase):
    """Tests for the GraphWalker utility class."""

    def test_iteration(self) -> None:
        helper = InMemoryRepo("base.yaml", "spatial.yaml")
        self.enterContext(helper)
        helper.add_task("a", dimensions=["visit", "detector"])
        helper.add_task("b", dimensions=["visit", "detector"])
        helper.add_task(
            "c",  # Gathers outputs from 'b' by visit.
            dimensions=["visit"],
            inputs={
                "input_connection": DynamicConnectionConfig(
                    dataset_type_name=f"dataset_auto{helper.last_auto_dataset_type_index}",
                    dimensions=["visit", "detector"],
                    multiple=True,
                )
            },
        )
        helper.add_task(
            "d",  # Scatters visit-level outputs from 'c' per-detector.
            dimensions=["visit", "detector"],
            inputs={
                "input_connection": DynamicConnectionConfig(
                    dataset_type_name=f"dataset_auto{helper.last_auto_dataset_type_index}",
                    dimensions=["visit"],
                )
            },
        )
        qg = helper.make_quantum_graph_builder().finish(attach_datastore_records=False).assemble()
        walker = GraphWalker[uuid.UUID](qg.quantum_only_xgraph.copy())
        # First set of unblocked nodes consists of all 'a' nodes.
        a_nodes: dict[DataCoordinate, uuid.UUID] = {
            qg.quantum_only_xgraph.nodes[n]["data_id"]: n for n in next(walker)
        }
        self.assertEqual([qg.quantum_only_xgraph.nodes[n]["task_label"] for n in a_nodes.values()], ["a"] * 8)
        # Mark the detector=1 'a' nodes as finished, and iterate the walker;
        # this should unblock all of the detector=1 'b' nodes.
        walker.finish(
            a_nodes.pop(
                DataCoordinate.standardize(
                    instrument="Cam1", visit=1, detector=1, universe=helper.butler.dimensions
                )
            )
        )
        walker.finish(
            a_nodes.pop(
                DataCoordinate.standardize(
                    instrument="Cam1", visit=2, detector=1, universe=helper.butler.dimensions
                )
            )
        )
        b_nodes: dict[DataCoordinate, uuid.UUID] = {
            qg.quantum_only_xgraph.nodes[n]["data_id"]: n for n in next(walker)
        }
        self.assertEqual([qg.quantum_only_xgraph.nodes[n]["task_label"] for n in b_nodes.values()], ["b"] * 2)
        self.assertEqual([data_id["detector"] for data_id in b_nodes.keys()], [1] * 2)
        # Mark one quantum of 'a' as a failure, and check that this returns all
        # downstream nodes.
        bad_node = a_nodes.pop(
            DataCoordinate.standardize(
                instrument="Cam1", visit=1, detector=2, universe=helper.butler.dimensions
            )
        )
        downstream_of_bad = set(walker.fail(bad_node))
        self.assertEqual(downstream_of_bad, set(networkx.dag.descendants(qg.quantum_only_xgraph, bad_node)))
        # Mark the remaining 'a' nodes as finished.  This should unblock the
        # remaining 5 'b' nodes, for a total of 7 unblocked.
        for n in a_nodes.values():
            walker.finish(n)
        a_nodes.clear()
        b_nodes.update({qg.quantum_only_xgraph.nodes[n]["data_id"]: n for n in next(walker)})
        self.assertEqual([qg.quantum_only_xgraph.nodes[n]["task_label"] for n in b_nodes.values()], ["b"] * 7)
        # Check that iterating the walker again yields nothing new.  Note that
        # this is not the same as the iterator being exhausted, which only
        # happens after we've traversed the whole graph.
        self.assertFalse(next(walker))
        # Mark those 'b' nodes as finished, which should unblock one 'c' node
        # (the other was downstream of the failure).
        for n in b_nodes.values():
            walker.finish(n)
        c_nodes: dict[DataCoordinate, uuid.UUID] = {
            qg.quantum_only_xgraph.nodes[n]["data_id"]: n for n in next(walker)
        }
        self.assertEqual(len(c_nodes), 1)
        c_data_id, c_node = c_nodes.popitem()
        self.assertEqual(qg.quantum_only_xgraph.nodes[c_node]["task_label"], "c")
        self.assertEqual(
            c_data_id,
            DataCoordinate.standardize(instrument="Cam1", visit=2, universe=helper.butler.dimensions),
        )
        # Mark the 'c' node as finished, which should unblock the 4 'd' nodes
        # for visit=2.
        walker.finish(c_node)
        d_nodes: dict[DataCoordinate, uuid.UUID] = {
            qg.quantum_only_xgraph.nodes[n]["data_id"]: n for n in next(walker)
        }
        self.assertEqual([qg.quantum_only_xgraph.nodes[n]["task_label"] for n in d_nodes.values()], ["d"] * 4)
        self.assertEqual([data_id["visit"] for data_id in d_nodes.keys()], [2] * 4)
        # Finish the 'd' nodes and check that the walker iteration is done.
        for n in d_nodes.values():
            walker.finish(n)
        with self.assertRaises(StopIteration):
            next(walker)
