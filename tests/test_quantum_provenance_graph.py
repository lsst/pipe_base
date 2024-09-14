# # This file is part of pipe_base.
# #
# # Developed for the LSST Data Management System.
# # This product includes software developed by the LSST Project
# # (http://www.lsst.org).
# # See the COPYRIGHT file at the top-level directory of this distribution
# # for details of code ownership.
# #
# # This software is dual licensed under the GNU General Public License and
# also
# # under a 3-clause BSD license. Recipients may choose which of these licenses
# # to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# # respectively.  If you choose the GPL option then the following text applies
# # (but note that there is still no warranty even if you opt for BSD instead):
# #
# # This program is free software: you can redistribute it and/or modify
# # it under the terms of the GNU General Public License as published by
# # the Free Software Foundation, either version 3 of the License, or
# # (at your option) any later version.
# #
# # This program is distributed in the hope that it will be useful,
# # but WITHOUT ANY WARRANTY; without even the implied warranty of
# # MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# # GNU General Public License for more details.
# #
# # You should have received a copy of the GNU General Public License
# # along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Simple unit test for quantum_provenance_graph.
"""

import unittest

from lsst.pipe.base.quantum_provenance_graph import QuantumProvenanceGraph, TaskSummary
from lsst.pipe.base.tests import simpleQGraph
from lsst.utils.tests import temporaryDirectory


class QuantumProvenanceGraphTestCase(unittest.TestCase):
    """Test reports from the QuantumProvenanceGraph.

    Verify that the `QuantumProvenanceGraph` is able to extract correct
    information from `simpleQgraph`.

    More tests are in lsst/ci_middleware/tests/test_prod_outputs.py and
    lsst/ci_middleware/tests/test_rc2_outputs.py
    """

    def test_qpg_reports(self) -> None:
        """Test that we can add a new graph to the
        `QuantumProvenanceGraph`.
        """
        with temporaryDirectory() as root:
            # make a simple qgraph to make an execution report on
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root)
            qpg = QuantumProvenanceGraph()
            qpg.assemble_quantum_provenance_graph(butler, [qgraph])
            summary = qpg.to_summary(butler)

            for task_summary in summary.tasks.values():
                # We know that we have one expected task that was not run.
                # As such, the following dictionary should describe all of
                # the mock tasks.
                self.assertEqual(
                    task_summary,
                    TaskSummary(
                        n_successful=0,
                        n_blocked=0,
                        n_unknown=1,
                        n_expected=1,
                        failed_quanta=[],
                        recovered_quanta=[],
                        wonky_quanta=[],
                        n_wonky=0,
                        n_failed=0,
                    ),
                )
            expected_mock_datasets = [
                "add_dataset1",
                "add2_dataset1",
                "task0_metadata",
                "task0_log",
                "add_dataset2",
                "add2_dataset2",
                "task1_metadata",
                "task1_log",
                "add_dataset3",
                "add2_dataset3",
                "task2_metadata",
                "task2_log",
                "add_dataset4",
                "add2_dataset4",
                "task3_metadata",
                "task3_log",
                "add_dataset5",
                "add2_dataset5",
                "task4_metadata",
                "task4_log",
            ]
            for dataset_type_name, dataset_type_summary in summary.datasets.items():
                self.assertListEqual(
                    dataset_type_summary.unsuccessful_datasets,
                    [{"instrument": "INSTR", "detector": 0}],
                )
                # Check dataset counts (can't be done all in one because
                # datasets have different producers), but all the counts for
                # each task should be the same.
                self.assertEqual(dataset_type_summary.n_visible, 0)
                self.assertEqual(dataset_type_summary.n_shadowed, 0)
                self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                self.assertEqual(dataset_type_summary.n_expected, 1)
                self.assertEqual(dataset_type_summary.n_cursed, 0)
                self.assertEqual(dataset_type_summary.n_unsuccessful, 1)
                # Make sure the cursed dataset is an empty list
                self.assertListEqual(dataset_type_summary.cursed_datasets, [])
                # Make sure we have the right datasets based on our mock
                self.assertIn(dataset_type_name, expected_mock_datasets)
            # Make sure the expected datasets were produced by the expected
            # tasks
            match dataset_type_name:
                case name if name in ["add_dataset1", "add2_dataset1", "task0_metadata", "task0_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task0")
                case name if name in ["add_dataset2", "add2_dataset2", "task1_metadata", "task1_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task1")
                case name if name in ["add_dataset3", "add2_dataset3", "task2_metadata", "task2_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task2")
                case name if name in ["add_dataset4", "add2_dataset4", "task3_metadata", "task3_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task3")
                case name if name in ["add_dataset5", "add2_dataset5", "task4_metadata", "task4_log"]:
                    self.assertEqual(dataset_type_summary.producer, "task4")
