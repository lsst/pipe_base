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

"""Simple unit test for quantum_provenance_graph."""

import unittest
import uuid

import lsst.utils.logging
from lsst.pipe.base.quantum_provenance_graph import (
    CursedDatasetSummary,
    DatasetTypeSummary,
    ExceptionInfo,
    ExceptionInfoSummary,
    QuantumProvenanceGraph,
    Summary,
    TaskSummary,
    UnsuccessfulQuantumSummary,
)
from lsst.pipe.base.tests import simpleQGraph
from lsst.utils.tests import temporaryDirectory

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
                self.assertEqual(task_summary.n_successful, 0)
                self.assertEqual(task_summary.n_blocked, 0)
                self.assertEqual(task_summary.n_unknown, 1)
                self.assertEqual(task_summary.n_expected, 1)
                self.assertListEqual(task_summary.failed_quanta, [])
                self.assertListEqual(task_summary.recovered_quanta, [])
                self.assertListEqual(task_summary.wonky_quanta, [])
                self.assertDictEqual(task_summary.exceptions, {})
                self.assertEqual(task_summary.n_wonky, 0)
                self.assertEqual(task_summary.n_failed, 0)

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

    def test_aggregate_reports(self) -> None:
        """Test aggregating reports from the `QuantumProvenanceGraph.`"""
        with temporaryDirectory() as root:
            # make a simple qgraph to make an execution report on
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root)
            qpg = QuantumProvenanceGraph()
            qpg.assemble_quantum_provenance_graph(butler, [qgraph])
            summary = qpg.to_summary(butler)
            # Check that aggregating one summary only does not cause an error
            one_graph_only_sum = Summary.aggregate([summary])

            # Do the same tests as in `test_qpg_reports`, but on the
            # 'aggregate' summary. Essentially, verify that the information in
            # the report is preserved during the aggregation step.
            for task_summary in one_graph_only_sum.tasks.values():
                self.assertEqual(task_summary.n_successful, 0)
                self.assertEqual(task_summary.n_blocked, 0)
                self.assertEqual(task_summary.n_unknown, 1)
                self.assertEqual(task_summary.n_expected, 1)
                self.assertListEqual(task_summary.failed_quanta, [])
                self.assertListEqual(task_summary.recovered_quanta, [])
                self.assertListEqual(task_summary.wonky_quanta, [])
                self.assertDictEqual(task_summary.exceptions, {})
                self.assertEqual(task_summary.n_wonky, 0)
                self.assertEqual(task_summary.n_failed, 0)
            for dataset_type_name, dataset_type_summary in one_graph_only_sum.datasets.items():
                self.assertListEqual(
                    dataset_type_summary.unsuccessful_datasets, [{"instrument": "INSTR", "detector": 0}]
                )
                self.assertEqual(dataset_type_summary.n_visible, 0)
                self.assertEqual(dataset_type_summary.n_shadowed, 0)
                self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                self.assertEqual(dataset_type_summary.n_expected, 1)
                self.assertEqual(dataset_type_summary.n_cursed, 0)
                self.assertEqual(dataset_type_summary.n_unsuccessful, 1)
                self.assertListEqual(dataset_type_summary.cursed_datasets, [])
                self.assertIn(dataset_type_name, expected_mock_datasets)
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

            # Now we test aggregating multiple summaries. First, we try
            # aggregating with an exact copy and make sure we just have double
            # the numbers.
            summary2 = summary.model_copy(deep=True)
            two_identical_graph_sum = Summary.aggregate([summary, summary2])
            for task_summary in two_identical_graph_sum.tasks.values():
                self.assertEqual(task_summary.n_successful, 0)
                self.assertEqual(task_summary.n_blocked, 0)
                self.assertEqual(task_summary.n_unknown, 2)
                self.assertEqual(task_summary.n_expected, 2)
                self.assertListEqual(task_summary.failed_quanta, [])
                self.assertListEqual(task_summary.recovered_quanta, [])
                self.assertListEqual(task_summary.wonky_quanta, [])
                self.assertDictEqual(task_summary.exceptions, {})
                self.assertEqual(task_summary.n_wonky, 0)
                self.assertEqual(task_summary.n_failed, 0)
            for dataset_type_name, dataset_type_summary in two_identical_graph_sum.datasets.items():
                self.assertListEqual(
                    dataset_type_summary.unsuccessful_datasets,
                    [{"instrument": "INSTR", "detector": 0}, {"instrument": "INSTR", "detector": 0}],
                )
                self.assertEqual(dataset_type_summary.n_visible, 0)
                self.assertEqual(dataset_type_summary.n_shadowed, 0)
                self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                self.assertEqual(dataset_type_summary.n_expected, 2)
                self.assertEqual(dataset_type_summary.n_cursed, 0)
                self.assertEqual(dataset_type_summary.n_unsuccessful, 2)
                self.assertListEqual(dataset_type_summary.cursed_datasets, [])
                self.assertIn(dataset_type_name, expected_mock_datasets)
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

            # Let's see if we can change lots of counts and info for a task
            # which exists in summary 1 and append to the overall summary
            # effectively. This summary has a lot of valid variations.
            uuid_a = uuid.uuid4()
            summary3 = summary.model_copy(deep=True)
            summary3.tasks["task4"] = TaskSummary.model_validate(
                {
                    "n_successful": 10,
                    "n_blocked": 20,
                    "n_unknown": 4,
                    "n_expected": 47,
                    "failed_quanta": [
                        {
                            "data_id": {"instrument": "INSTR", "detector": 1},
                            "runs": {"run1": "failed"},
                            "messages": ["Error on detector 1", "Second error on detector 1"],
                        },
                        {
                            "data_id": {"instrument": "INSTR", "detector": 2},
                            "runs": {"run1": "failed", "run2": "failed"},
                            "messages": [],
                        },
                        {
                            "data_id": {"instrument": "INSTR", "detector": 3},
                            "runs": {"run1": "failed"},
                            "messages": ["Error on detector 3"],
                        },
                    ],
                    "recovered_quanta": [
                        {"instrument": "INSTR", "detector": 4},
                        {"instrument": "INSTR", "detector": 5},
                        {"instrument": "INSTR", "detector": 6},
                    ],
                    "wonky_quanta": [
                        {
                            "data_id": {"instrument": "INSTR", "detector": 7},
                            "runs": {"run1": "successful", "run2": "failed"},
                            "messages": ["This one is wonky because it moved from successful to failed."],
                        }
                    ],
                    "caveats": {
                        "+A": [{"instrument": "INSTR", "detector": 10}],
                    },
                    "exceptions": {
                        "lsst.pipe.base.tests.mocks.MockAlgorithmError": [
                            {
                                "quantum_id": str(uuid_a),
                                "data_id": {"instrument": "INSTR", "detector": 10},
                                "run": "run2",
                                "exception": {
                                    "type_name": "lsst.pipe.base.tests.mocks.MockAlgorithmError",
                                    "message": "message A",
                                    "metadata": {"badness": 11},
                                },
                            }
                        ],
                    },
                    "n_wonky": 1,
                    "n_failed": 3,
                }
            )
            summary3.datasets["add_dataset5"] = DatasetTypeSummary.model_validate(
                {
                    "producer": "task4",
                    "n_visible": 0,
                    "n_shadowed": 0,
                    "n_predicted_only": 0,
                    "n_expected": 47,
                    "cursed_datasets": [
                        {
                            "producer_data_id": {"instrument": "INSTR", "detector": 7},
                            "data_id": {"instrument": "INSTR", "detector": 7},
                            "runs_produced": {"run1": True, "run2": False},
                            "run_visible": None,
                            "messages": ["Some kind of cursed dataset."],
                        }
                    ],
                    "unsuccessful_datasets": [
                        {"instrument": "INSTR", "detector": 0},
                        {"instrument": "INSTR", "detector": 1},
                        {"instrument": "INSTR", "detector": 2},
                        {"instrument": "INSTR", "detector": 3},
                    ],
                }
            )
            summary3.datasets["add2_dataset5"] = DatasetTypeSummary.model_validate(
                {
                    "producer": "task4",
                    "n_visible": 0,
                    "n_shadowed": 0,
                    "n_predicted_only": 0,
                    "n_expected": 47,
                    "cursed_datasets": [
                        {
                            "producer_data_id": {"instrument": "INSTR", "detector": 7},
                            "data_id": {"instrument": "INSTR", "detector": 7},
                            "runs_produced": {"run1": True, "run2": False},
                            "run_visible": None,
                            "messages": ["Some kind of cursed dataset."],
                        }
                    ],
                    "unsuccessful_datasets": [
                        {"instrument": "INSTR", "detector": 0},
                        {"instrument": "INSTR", "detector": 1},
                        {"instrument": "INSTR", "detector": 2},
                        {"instrument": "INSTR", "detector": 3},
                    ],
                }
            )
            # Test that aggregate with this file works
            two_graphs_different_numbers = Summary.aggregate([summary, summary3])
            for task_label, task_summary in two_graphs_different_numbers.tasks.items():
                if task_label == "task4":
                    self.assertEqual(task_summary.n_successful, 10)
                    self.assertEqual(task_summary.n_blocked, 20)
                    self.assertEqual(task_summary.n_unknown, 5)
                    self.assertEqual(task_summary.n_expected, 48)
                    self.assertListEqual(
                        task_summary.failed_quanta,
                        [
                            UnsuccessfulQuantumSummary(
                                data_id={"instrument": "INSTR", "detector": 1},
                                runs={"run1": "failed"},
                                messages=["Error on detector 1", "Second error on detector 1"],
                            ),
                            UnsuccessfulQuantumSummary(
                                data_id={"instrument": "INSTR", "detector": 2},
                                runs={"run1": "failed", "run2": "failed"},
                                messages=[],
                            ),
                            UnsuccessfulQuantumSummary(
                                data_id={"instrument": "INSTR", "detector": 3},
                                runs={"run1": "failed"},
                                messages=["Error on detector 3"],
                            ),
                        ],
                    )
                    self.assertListEqual(
                        task_summary.recovered_quanta,
                        [
                            {"instrument": "INSTR", "detector": 4},
                            {"instrument": "INSTR", "detector": 5},
                            {"instrument": "INSTR", "detector": 6},
                        ],
                    )
                    self.assertListEqual(
                        task_summary.wonky_quanta,
                        [
                            UnsuccessfulQuantumSummary(
                                data_id={"instrument": "INSTR", "detector": 7},
                                runs={"run1": "successful", "run2": "failed"},
                                messages=["This one is wonky because it moved from successful to failed."],
                            )
                        ],
                    )
                    self.assertDictEqual(
                        task_summary.caveats,
                        {"+A": [{"instrument": "INSTR", "detector": 10}]},
                    )
                    self.assertDictEqual(
                        task_summary.exceptions,
                        {
                            "lsst.pipe.base.tests.mocks.MockAlgorithmError": [
                                ExceptionInfoSummary(
                                    quantum_id=uuid_a,
                                    data_id={"instrument": "INSTR", "detector": 10},
                                    run="run2",
                                    exception=ExceptionInfo(
                                        type_name="lsst.pipe.base.tests.mocks.MockAlgorithmError",
                                        message="message A",
                                        metadata={"badness": 11},
                                    ),
                                )
                            ],
                        },
                    )
                    self.assertEqual(task_summary.n_wonky, 1)
                    self.assertEqual(task_summary.n_failed, 3)
                else:
                    self.assertEqual(task_summary.n_successful, 0)
                    self.assertEqual(task_summary.n_blocked, 0)
                    self.assertEqual(task_summary.n_unknown, 2)
                    self.assertEqual(task_summary.n_expected, 2)
                    self.assertListEqual(task_summary.failed_quanta, [])
                    self.assertListEqual(task_summary.recovered_quanta, [])
                    self.assertListEqual(task_summary.wonky_quanta, [])
                    self.assertEqual(task_summary.n_wonky, 0)
                    self.assertEqual(task_summary.n_failed, 0)
            for dataset_type_name, dataset_type_summary in two_graphs_different_numbers.datasets.items():
                if dataset_type_name in ["add_dataset5", "add2_dataset5"]:
                    self.assertListEqual(
                        dataset_type_summary.unsuccessful_datasets,
                        [
                            {"instrument": "INSTR", "detector": 0},
                            {"instrument": "INSTR", "detector": 0},
                            {"instrument": "INSTR", "detector": 1},
                            {"instrument": "INSTR", "detector": 2},
                            {"instrument": "INSTR", "detector": 3},
                        ],
                    )
                    self.assertEqual(dataset_type_summary.n_visible, 0)
                    self.assertEqual(dataset_type_summary.n_shadowed, 0)
                    self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                    self.assertEqual(dataset_type_summary.n_expected, 48)
                    self.assertEqual(dataset_type_summary.n_cursed, 1)
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 5)
                    self.assertListEqual(
                        dataset_type_summary.cursed_datasets,
                        [
                            CursedDatasetSummary(
                                producer_data_id={"instrument": "INSTR", "detector": 7},
                                data_id={"instrument": "INSTR", "detector": 7},
                                runs_produced={"run1": True, "run2": False},
                                run_visible=None,
                                messages=["Some kind of cursed dataset."],
                            )
                        ],
                    )
                else:
                    self.assertListEqual(
                        dataset_type_summary.unsuccessful_datasets,
                        [{"instrument": "INSTR", "detector": 0}, {"instrument": "INSTR", "detector": 0}],
                    )
                    self.assertEqual(dataset_type_summary.n_visible, 0)
                    self.assertEqual(dataset_type_summary.n_shadowed, 0)
                    self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                    self.assertEqual(dataset_type_summary.n_expected, 2)
                    self.assertEqual(dataset_type_summary.n_cursed, 0)
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 2)
                    self.assertListEqual(dataset_type_summary.cursed_datasets, [])
                    self.assertIn(dataset_type_name, expected_mock_datasets)
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

            # Now, let's add a task to one model and see if aggregate still
            # works
            summary4 = summary.model_copy(deep=True)
            summary4.tasks["task5"] = TaskSummary(
                n_successful=0,
                n_blocked=0,
                n_unknown=1,
                n_expected=1,
                failed_quanta=[],
                recovered_quanta=[],
                wonky_quanta=[],
            )
            summary4.datasets["add_dataset6"] = DatasetTypeSummary(
                producer="task5",
                n_visible=0,
                n_shadowed=0,
                n_predicted_only=0,
                n_expected=1,
                cursed_datasets=[],
                unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
            )
            summary4.datasets["task5_log"] = DatasetTypeSummary(
                producer="task5",
                n_visible=0,
                n_shadowed=0,
                n_predicted_only=0,
                n_expected=1,
                cursed_datasets=[],
                unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
            )
            summary4.datasets["task5_metadata"] = DatasetTypeSummary(
                producer="task5",
                n_visible=0,
                n_shadowed=0,
                n_predicted_only=0,
                n_expected=1,
                cursed_datasets=[],
                unsuccessful_datasets=[{"instrument": "INSTR", "detector": 0}],
            )
            two_graphs_extra_task = Summary.aggregate([summary4, summary])
            # Make sure the extra task is in there
            self.assertIn("task5", two_graphs_extra_task.tasks)
            for task_label, task_summary in two_graphs_extra_task.tasks.items():
                self.assertEqual(task_summary.n_successful, 0)
                self.assertEqual(task_summary.n_blocked, 0)
                self.assertListEqual(task_summary.failed_quanta, [])
                self.assertListEqual(task_summary.recovered_quanta, [])
                self.assertListEqual(task_summary.wonky_quanta, [])
                self.assertEqual(task_summary.n_wonky, 0)
                self.assertEqual(task_summary.n_failed, 0)
                self.assertIn(task_label, ["task0", "task1", "task2", "task3", "task4", "task5"])
                if task_label == "task5":
                    self.assertEqual(task_summary.n_unknown, 1)
                    self.assertEqual(task_summary.n_expected, 1)
                else:
                    self.assertEqual(task_summary.n_unknown, 2)
                    self.assertEqual(task_summary.n_expected, 2)
            for dataset_type_name, dataset_type_summary in two_graphs_extra_task.datasets.items():
                self.assertEqual(dataset_type_summary.n_visible, 0)
                self.assertEqual(dataset_type_summary.n_shadowed, 0)
                self.assertEqual(dataset_type_summary.n_predicted_only, 0)
                self.assertEqual(dataset_type_summary.n_cursed, 0)
                self.assertListEqual(dataset_type_summary.cursed_datasets, [])
                if dataset_type_summary.producer == "task5":
                    self.assertEqual(dataset_type_summary.n_expected, 1)
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 1)
                    self.assertListEqual(
                        dataset_type_summary.unsuccessful_datasets,
                        [{"instrument": "INSTR", "detector": 0}],
                    )
                else:
                    self.assertEqual(dataset_type_summary.n_expected, 2)
                    self.assertEqual(dataset_type_summary.n_unsuccessful, 2)
                    self.assertListEqual(
                        dataset_type_summary.unsuccessful_datasets,
                        [{"instrument": "INSTR", "detector": 0}, {"instrument": "INSTR", "detector": 0}],
                    )
                self.assertIn(
                    dataset_type_name,
                    expected_mock_datasets + ["add_dataset6", "task5_metadata", "task5_log"],
                )
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
                    case name if name in ["add_dataset6", "task5_metadata", "task5_log"]:
                        self.assertEqual(dataset_type_summary.producer, "task5")

        # Now we test that we properly catch task-dataset mismatches in
        # aggregated graphs. This is a problem because if task 1 produced
        # a certain dataset in graph 1, but task 2 produced the same dataset
        # in graph 2, the graphs are likely not comparable.
        summary5 = summary.model_copy(deep=True)
        summary5.datasets["add_dataset3"] = summary.datasets["add_dataset3"].model_copy(
            deep=True,
            update={
                "producer": "task0",
                "n_visible": 0,
                "n_shadowed": 0,
                "n_predicted_only": 0,
                "n_expected": 1,
                "cursed_datasets": [],
                "unsuccessful_datasets": [
                    {"instrument": "INSTR", "detector": 0},
                ],
                "n_cursed": 0,
                "n_unsuccessful": 1,
            },
        )
        with self.assertLogs("lsst.pipe.base", level=lsst.utils.logging.VERBOSE) as warning_logs:
            Summary.aggregate([summary, summary5])
            self.assertIn(
                "WARNING:lsst.pipe.base.quantum_provenance_graph:Producer for dataset type is not consistent"
                ": 'task2' != 'task0'.",
                warning_logs.output[0],
            )
            self.assertIn(
                "WARNING:lsst.pipe.base.quantum_provenance_graph:Ignoring 'task0'.", warning_logs.output[1]
            )

        # Next up, we're going to try to aggregate summary with a dictionary
        # and then with some garbage. Neither of these should work!
        with self.assertRaises(AttributeError):
            Summary.aggregate(
                [
                    summary,
                    {
                        "tasks": {
                            "task0": {
                                "n_successful": 0,
                                "n_blocked": 0,
                                "n_unknown": 1,
                                "n_expected": 1,
                                "failed_quanta": [],
                                "recovered_quanta": [],
                                "wonky_quanta": [],
                                "n_wonky": 0,
                                "n_failed": 0,
                            },
                            "datasets": {
                                "add_dataset1": {
                                    "producer": "task0",
                                    "n_visible": 0,
                                    "n_shadowed": 0,
                                    "n_predicted_only": 0,
                                    "n_expected": 1,
                                    "cursed_datasets": [],
                                    "unsuccessful_datasets": [{"instrument": "INSTR", "detector": 0}],
                                    "n_cursed": 0,
                                    "n_unsuccessful": 1,
                                },
                                "add2_dataset1": {
                                    "producer": "task0",
                                    "n_visible": 0,
                                    "n_shadowed": 0,
                                    "n_predicted_only": 0,
                                    "n_expected": 1,
                                    "cursed_datasets": [],
                                    "unsuccessful_datasets": [{"instrument": "INSTR", "detector": 0}],
                                    "n_cursed": 0,
                                    "n_unsuccessful": 1,
                                },
                                "task0_metadata": {
                                    "producer": "task0",
                                    "n_visible": 0,
                                    "n_shadowed": 0,
                                    "n_predicted_only": 0,
                                    "n_expected": 1,
                                    "cursed_datasets": [],
                                    "unsuccessful_datasets": [{"instrument": "INSTR", "detector": 0}],
                                    "n_cursed": 0,
                                    "n_unsuccessful": 1,
                                },
                                "task0_log": {
                                    "producer": "task0",
                                    "n_visible": 0,
                                    "n_shadowed": 0,
                                    "n_predicted_only": 0,
                                    "n_expected": 1,
                                    "cursed_datasets": [],
                                    "unsuccessful_datasets": [{"instrument": "INSTR", "detector": 0}],
                                    "n_cursed": 0,
                                    "n_unsuccessful": 1,
                                },
                            },
                        }
                    },
                ]
            )
            Summary.aggregate([summary, []])
            Summary.aggregate([summary, "some_garbage"])
