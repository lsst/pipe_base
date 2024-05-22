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

from lsst.pipe.base.quantum_provenance_graph import DatasetTypeSummary, QuantumProvenanceGraph, TaskSummary
from lsst.pipe.base.tests import simpleQGraph
from lsst.utils.tests import temporaryDirectory


class QuantumProvenanceGraphTestCase(unittest.TestCase):
    """Test reports from the QuantumProvenanceGraph.

    Verify that the `QuantumProvenanceGraph` is able to extract correct
    information from `simpleQgraph`.

    More tests are in lsst/ci_middleware/tests/test_prod_outputs.py
    """

    def test_qpg_reports(self) -> None:
        """Test that we can add a new graph to the
        `QuantumProvenanceGraph`.
        """
        with temporaryDirectory() as root:
            # make a simple qgraph to make an execution report on
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root)
            qpg = QuantumProvenanceGraph()
            qpg.add_new_graph(butler, qgraph)
            qpg.resolve_duplicates(butler)
            d = qpg.to_summary(butler)
            self.assertIsNotNone(d)
            with open("testmodel.json", "w") as buffer:
                buffer.write(d.model_dump_json(indent=2))
            summary_dict = d.model_dump()
            for task in d.tasks:
                self.assertIsInstance(d.tasks[task], TaskSummary)
                # We know that we have one expected task that was not run.
                # As such, the following dictionary should describe all of
                # the mock tasks.
                self.assertDictEqual(
                    summary_dict["tasks"][task],
                    {
                        "n_successful": 0,
                        "n_blocked": 0,
                        "n_not_attempted": 1,
                        "n_expected": 1,
                        "failed_quanta": [],
                        "recovered_quanta": [],
                        "wonky_quanta": [],
                        "n_wonky": 0,
                        "n_failed": 0,
                    },
                )
            for dataset in d.datasets:
                self.assertIsInstance(d.datasets[dataset], DatasetTypeSummary)
                self.assertListEqual(
                    summary_dict["datasets"][dataset]["unsuccessful_datasets"],
                    [{"instrument": "INSTR", "detector": 0}],
                )
                # Check dataset counts (can't be done all in one because
                # datasets have different producers), but all the counts for
                # each task should be the same.
                self.assertEqual(summary_dict["datasets"][dataset]["n_published"], 0)
                self.assertEqual(summary_dict["datasets"][dataset]["n_unpublished"], 0)
                self.assertEqual(summary_dict["datasets"][dataset]["n_published"], 0)
                self.assertEqual(summary_dict["datasets"][dataset]["n_predicted_only"], 0)
                self.assertEqual(summary_dict["datasets"][dataset]["n_expected"], 1)
                self.assertEqual(summary_dict["datasets"][dataset]["n_published"], 0)
                self.assertEqual(summary_dict["datasets"][dataset]["n_cursed"], 0)
                self.assertEqual(summary_dict["datasets"][dataset]["n_published"], 0)
                self.assertEqual(summary_dict["datasets"][dataset]["n_unsuccessful"], 1)
                # Make sure the cursed dataset is an empty list
                self.assertIsInstance(summary_dict["datasets"][dataset]["cursed_datasets"], list)
                self.assertFalse(summary_dict["datasets"][dataset]["cursed_datasets"])
            # Make sure we have the right datasets based on the mock we have
            for task in [
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
            ]:
                self.assertIn(task, list(summary_dict["datasets"].keys()))
        # Make sure the expected datasets were produced by the expected tasks
        for dataset in ["add_dataset1", "add2_dataset1", "task0_metadata", "task0_log"]:
            self.assertEqual(summary_dict["datasets"][dataset]["producer"], "task0")
        for dataset in [
            "add_dataset2",
            "add2_dataset2",
            "task1_metadata",
            "task1_log",
        ]:
            self.assertEqual(summary_dict["datasets"][dataset]["producer"], "task1")
        for dataset in [
            "add_dataset3",
            "add2_dataset3",
            "task2_metadata",
            "task2_log",
        ]:
            self.assertEqual(summary_dict["datasets"][dataset]["producer"], "task2")
        for dataset in [
            "add_dataset4",
            "add2_dataset4",
            "task3_metadata",
            "task3_log",
        ]:
            self.assertEqual(summary_dict["datasets"][dataset]["producer"], "task3")
        for dataset in [
            "add_dataset5",
            "add2_dataset5",
            "task4_metadata",
            "task4_log",
        ]:
            self.assertEqual(summary_dict["datasets"][dataset]["producer"], "task4")
