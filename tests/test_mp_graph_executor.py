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

import logging
import multiprocessing
import multiprocessing.context
import os
import signal
import sys
import unittest
import warnings
from typing import Literal

import psutil

from lsst.pipe.base.exec_fixup_data_id import ExecFixupDataId
from lsst.pipe.base.mp_graph_executor import MPGraphExecutor, MPGraphExecutorError, MPTimeoutError
from lsst.pipe.base.quantum_reports import ExecutionStatus, Report
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    InMemoryRepo,
)

logging.basicConfig(level=logging.DEBUG)

_LOG = logging.getLogger(__name__)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class NoMultiprocessingTask(DynamicTestPipelineTask):
    """A test pipeline task that declares that it cannot be used in
    multiprocessing.
    """

    canMultiprocess = False


def _count_status(report: Report, status: ExecutionStatus) -> int:
    """Count number of quanta with a given status."""
    return len([qrep for qrep in report.quantaReports if qrep.status is status])


class MPGraphExecutorTestCase(unittest.TestCase):
    """A test case for MPGraphExecutor class."""

    def test_mpexec_nomp(self) -> None:
        """Make simple graph and execute."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(dimensions=["detector"])
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        # run in single-process mode
        mpexec = MPGraphExecutor(num_proc=1, timeout=100, quantum_executor=qexec)
        mpexec.execute(qgraph)  # type: ignore[arg-type]
        self.assertCountEqual(
            [ref.dataId["detector"] for ref in butler.get_datasets("dataset_auto1")], [1, 2, 3, 4]
        )
        report = mpexec.getReport()
        assert report is not None
        self.assertEqual(report.status, ExecutionStatus.SUCCESS)
        self.assertIsNone(report.exitCode)
        self.assertIsNone(report.exceptionInfo)
        self.assertEqual(len(report.quantaReports), 4)
        self.assertTrue(all(qrep.status == ExecutionStatus.SUCCESS for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exitCode is None for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))
        self.assertTrue(all(qrep.taskLabel == "task_auto1" for qrep in report.quantaReports))

    def test_mpexec_mp(self) -> None:
        """Make simple graph and execute."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(dimensions=["detector"])
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()

        methods: list[Literal["spawn", "forkserver"]] = ["spawn"]
        if sys.platform == "linux":
            methods.append("forkserver")

        for method in methods:
            with self.subTest(startMethod=method):
                # Run in multi-process mode, the order of results is not
                # defined.
                mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec, start_method=method)
                mpexec.execute(qgraph)  # type: ignore[arg-type]
                report = mpexec.getReport()
                assert report is not None
                self.assertEqual(report.status, ExecutionStatus.SUCCESS)
                self.assertIsNone(report.exitCode)
                self.assertIsNone(report.exceptionInfo)
                self.assertEqual(len(report.quantaReports), 4)
                self.assertTrue(all(qrep.status == ExecutionStatus.SUCCESS for qrep in report.quantaReports))
                self.assertTrue(all(qrep.exitCode == 0 for qrep in report.quantaReports))
                self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))
                self.assertTrue(all(qrep.taskLabel == "task_auto1" for qrep in report.quantaReports))

    def test_mpexec_nompsupport(self) -> None:
        """Try to run MP for task that has no MP support which should fail."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(task_class=NoMultiprocessingTask, dimensions=["detector"])
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec)
        with self.assertRaisesRegex(
            MPGraphExecutorError, "Task 'task_auto1' does not support multiprocessing"
        ):
            mpexec.execute(qgraph)  # type: ignore[arg-type]

    def test_mpexec_fixup(self) -> None:
        """Make simple graph and execute, add dependencies by executing fixup
        code.
        """
        helper = InMemoryRepo("base.yaml")
        helper.add_task(dimensions=["detector"])
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        for reverse in (False, True):
            qexec, butler = helper.make_single_quantum_executor()
            fixup = ExecFixupDataId("task_auto1", "detector", reverse=reverse)
            mpexec = MPGraphExecutor(
                num_proc=1, timeout=100, quantum_executor=qexec, execution_graph_fixup=fixup
            )
            mpexec.execute(qgraph)  # type: ignore[arg-type]
            expected = [1, 2, 3, 4]
            if reverse:
                expected = list(reversed(expected))
            self.assertEqual(
                [ref.dataId["detector"] for ref in butler.get_datasets("dataset_auto1")], expected
            )

    def test_mpexec_timeout(self) -> None:
        """Fail due to timeout."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(label="a")
        helper.add_task(
            label="b",
            inputs={"input_connection": DynamicConnectionConfig(dataset_type_name="dataset_auto0")},
        )
        helper.add_task(
            label="c",
            inputs={"input_connection": DynamicConnectionConfig(dataset_type_name="dataset_auto0")},
            config=DynamicTestPipelineTaskConfig(sleep=100.0),
        )
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)

        # with failFast we'll get immediate MPTimeoutError
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=1, quantum_executor=qexec, fail_fast=True)
        with self.assertRaises(MPTimeoutError):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.TIMEOUT)
        self.assertEqual(report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPTimeoutError")
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.TIMEOUT), 1)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode < 0 for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

        # with failFast=False exception happens after last task finishes
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=3, quantum_executor=qexec, fail_fast=False)
        with self.assertRaises(MPTimeoutError):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.TIMEOUT)
        self.assertEqual(report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPTimeoutError")
        # We expect two tasks ('a' and 'b') to finish successfully and one task
        # ('c') to timeout, which should get us all three reports.
        # Unfortunately on busy CPU there is no guarantee that tasks finish on
        # time, so expect more timeouts and issue a warning.
        if len(report.quantaReports) != 3:
            warnings.warn(
                f"Possibly timed out tasks, expected three reports, received {len(report.quantaReports)})."
            )
        report = mpexec.getReport()
        self.assertGreater(_count_status(report, ExecutionStatus.TIMEOUT), 0)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode < 0 for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_failure(self) -> None:
        """Failure in one task should not stop other tasks."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(
            config=DynamicTestPipelineTaskConfig(fail_condition="detector=2"),
            dimensions=["detector"],
        )
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError"
        )
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 3)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_dep(self) -> None:
        """Failure in one task should skip dependents."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(
            "a", config=DynamicTestPipelineTaskConfig(fail_condition="detector=2"), dimensions=["detector"]
        )
        helper.add_task("b", dimensions=["detector"])  # depends on 'a', for the same detector.
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 6)
        self.assertEqual(_count_status(report, ExecutionStatus.SKIPPED), 1)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_dep_nomp(self) -> None:
        """Failure in one task should skip dependents, in-process version."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(
            "a", config=DynamicTestPipelineTaskConfig(fail_condition="detector=2"), dimensions=["detector"]
        )
        helper.add_task("b", dimensions=["detector"])  # depends on 'a', for the same detector.
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=1, timeout=100, quantum_executor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        self.assertCountEqual(
            [ref.dataId["detector"] for ref in butler.get_datasets("dataset_auto1")], [1, 3, 4]
        )
        self.assertCountEqual(
            [ref.dataId["detector"] for ref in butler.get_datasets("dataset_auto2")], [1, 3, 4]
        )
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 6)
        self.assertEqual(_count_status(report, ExecutionStatus.SKIPPED), 1)
        self.assertTrue(all(qrep.exitCode is None for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_failfast(self) -> None:
        """Fast fail stops quickly.

        Timing delay of task 'b' should be sufficient to process
        failure and raise exception before task 'c'.
        """
        helper = InMemoryRepo("base.yaml")
        helper.add_task(
            "a", config=DynamicTestPipelineTaskConfig(fail_condition="detector=2"), dimensions=["detector"]
        )
        helper.add_task("b", config=DynamicTestPipelineTaskConfig(sleep=100.0), dimensions=["detector"])
        helper.add_task("c", dimensions=["detector"])  # depends on 'b', for the same detector.
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec, fail_fast=True)
        with self.assertRaisesRegex(MPGraphExecutorError, "failed, exit code=1"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_crash(self) -> None:
        """Check task crash due to signal."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(
            config=DynamicTestPipelineTaskConfig(fail_condition="detector=2", fail_signal=signal.SIGILL),
            dimensions=["detector"],
        )
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 3)
        self.assertTrue(any(qrep.exitCode == -signal.SIGILL for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_crash_failfast(self) -> None:
        """Check task crash due to signal with --fail-fast."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task(
            "a",
            config=DynamicTestPipelineTaskConfig(fail_condition="detector=2", fail_signal=signal.SIGILL),
            dimensions=["detector"],
        )
        helper.add_task("b", config=DynamicTestPipelineTaskConfig(sleep=100.0), dimensions=["detector"])
        helper.add_task("c", dimensions=["detector"])  # depends on 'b', for the same detector.
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec, fail_fast=True)
        with self.assertRaisesRegex(MPGraphExecutorError, "failed, killed by signal 4 .Illegal instruction"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.pipe.base.mp_graph_executor.MPGraphExecutorError"
        )
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertTrue(any(qrep.exitCode == -signal.SIGILL for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_num_fd(self) -> None:
        """Check that number of open files stays reasonable."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task("a", task_class=NoMultiprocessingTask, dimensions=["detector", "visit"])
        helper.add_task("b", task_class=NoMultiprocessingTask, dimensions=["detector", "visit"])
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        qexec, butler = helper.make_single_quantum_executor()
        this_proc = psutil.Process()
        num_fds_0 = this_proc.num_fds()

        # run in multi-process mode, the order of results is not defined
        mpexec = MPGraphExecutor(num_proc=3, timeout=100, quantum_executor=qexec)
        mpexec.execute(qgraph)  # type: ignore[arg-type]

        num_fds_1 = this_proc.num_fds()
        # They should be the same but allow small growth just in case.
        # Without DM-26728 fix the difference would be equal to number of
        # quanta (20).
        self.assertLess(num_fds_1 - num_fds_0, 5)


def setup_module(module):
    """Force spawn to be used if no method given explicitly.

    This can be removed when Python 3.14 changes the default.

    Parameters
    ----------
    module : `~types.ModuleType`
        Module to set up.
    """
    multiprocessing.set_start_method("spawn", force=True)


if __name__ == "__main__":
    # Do not need to force start mode when running standalone.
    multiprocessing.set_start_method("spawn")
    unittest.main()
