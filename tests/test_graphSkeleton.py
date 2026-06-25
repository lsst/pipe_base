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

"""Unit tests for QuantumGraphSkeleton."""

import unittest

from lsst.daf.butler import DataCoordinate, DimensionUniverse
from lsst.pipe.base.quantum_graph_skeleton import DatasetKey, QuantumGraphSkeleton


class RemoveUnanchoredQuantaTestCase(unittest.TestCase):
    """Tests for ``QuantumGraphSkeleton.remove_unanchored_quanta``.

    Graph:
        source1 -> d1 -> anchor1   (band 1: anchored)
        source2 -> d2 -> anchor2   (band 2: anchored)
        source3 -> d3              (band 3: unanchored)
        source4 -> d4              (band 4: unanchored)
    """

    def setUp(self):
        universe = DimensionUniverse()
        self.skeleton = QuantumGraphSkeleton(["source", "anchor"])
        for i in range(1, 5):
            data_id = DataCoordinate.standardize({"band": i}, universe=universe)
            source_quantum = self.skeleton.add_quantum_node("source", data_id)
            dataset = self.skeleton.add_dataset_node(f"d{i}", data_id)
            self.skeleton.add_output_edge(source_quantum, dataset)
            if i <= 2:
                anchor_quantum = self.skeleton.add_quantum_node("anchor", data_id)
                self.skeleton.add_input_edge(anchor_quantum, dataset)

    def test_remove_unanchored(self):
        """Unanchored source quanta and their descendants are removed."""
        removed = self.skeleton.remove_unanchored_quanta("source", "anchor")
        self.assertEqual(removed, {"source": 2})
        self.assertEqual(len(self.skeleton.get_quanta("source")), 2)
        self.assertEqual(len(self.skeleton.get_quanta("anchor")), 2)
        self.assertNotIn(DatasetKey("d3", (3,)), self.skeleton)
        self.assertNotIn(DatasetKey("d4", (4,)), self.skeleton)

        # Second call on an already-pruned skeleton is a no-op.
        removed = self.skeleton.remove_unanchored_quanta("source", "anchor")
        self.assertEqual(removed, {})

    def test_task_dropped_when_all_unanchored(self):
        """Task is dropped when all its quanta are removed."""
        removed = self.skeleton.remove_unanchored_quanta("source", "nonexistent")
        self.assertEqual(removed["source"], 4)
        self.assertFalse(self.skeleton.has_task("source"))


if __name__ == "__main__":
    unittest.main()
