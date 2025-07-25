# This file is part of ctrl_mpexec.
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

import faulthandler
import logging
import multiprocessing
import multiprocessing.context
import os
import signal
import sys
import time
import unittest
import warnings
from multiprocessing import Manager
from typing import TYPE_CHECKING, Any, Literal

import networkx as nx
import psutil

from lsst.ctrl.mpexec import (
    ExecutionStatus,
    MPGraphExecutor,
    MPGraphExecutorError,
    MPTimeoutError,
    QuantumExecutor,
    QuantumReport,
    Report,
    SingleQuantumExecutor,
)
from lsst.ctrl.mpexec.execFixupDataId import ExecFixupDataId
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base import NodeId, QgraphSummary, QgraphTaskSummary
from lsst.pipe.base.graph import BuildId
from lsst.pipe.base.tests.simpleQGraph import AddTaskFactoryMock, makeSimpleQGraph

if TYPE_CHECKING:
    import uuid
    from collections.abc import Iterator
    from multiprocessing.managers import ListProxy

    from lsst.daf.butler.dimensions import DataIdValue

logging.basicConfig(level=logging.DEBUG)

_LOG = logging.getLogger(__name__)

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class QuantumExecutorMock(QuantumExecutor):
    """Mock class for QuantumExecutor.

    Parameters
    ----------
    mp : `bool`
        Whether the mock should use multiprocessing or not.
    """

    def __init__(self, mp: bool = False):
        self.quanta: list[QuantumMock] | ListProxy = []
        if mp:
            # in multiprocess mode use shared list
            manager = Manager()
            self.quanta = manager.list()

    def execute(  # type: ignore[override]
        self,
        task_node: TaskNodeMock,
        /,
        quantum: QuantumMock,  # type: ignore[override]
        quantum_id: uuid.UUID | None = None,
    ) -> tuple[QuantumMock, QuantumReport | None]:
        _LOG.debug("QuantumExecutorMock.execute: task_node=%s dataId=%s", task_node, quantum.dataId)
        self._execute_called = True
        if task_node.task_class:
            # only works for one of the TaskMock classes below
            task_node.task_class().runQuantum()
        assert quantum.dataId is not None
        report = QuantumReport(dataId=quantum.dataId, taskLabel=task_node.label)
        self.quanta.append(quantum)
        return quantum, report

    def getDataIds(self, field: str) -> list[DataIdValue]:
        """Return values for dataId field for each visited quanta.

        Parameters
        ----------
        field : `str`
            Field to select.
        """
        return [quantum.dataId[field] for quantum in self.quanta if quantum.dataId is not None]


class QuantumMock:
    """Mock equivalent of a `~lsst.daf.butler.Quantum`.

    Parameters
    ----------
    dataId : `dict`
        The Data ID of this quantum.
    """

    def __init__(self, dataId: dict[str, DataIdValue]):
        self.dataId = dataId

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, QuantumMock):
            return NotImplemented
        return self.dataId == other.dataId

    def __hash__(self) -> int:
        # dict.__eq__ is order-insensitive
        return hash(tuple(sorted(kv for kv in self.dataId.items())))


class QuantumNodeMock:
    """Simple class to mock QuantumNode.

    Parameters
    ----------
    index : `int`
        The index of this mock.
    task_node : `TaskNodeMock`
        Mocked task definition.
    **dataId : `~typing.Any`
        The data ID of the mocked quantum.
    """

    def __init__(self, index: int, task_node: TaskNodeMock, **dataId: DataIdValue):
        self.index = index
        self.taskDef = task_node
        self.task_node = task_node
        self.quantum = QuantumMock(dataId)
        self.dependencies: set[int] = set()
        self.nodeId = NodeId(index, BuildId("DummyBuildString"))


class QuantumGraphMock:
    """Mock for quantum graph.

    Parameters
    ----------
    qdata : `list` [`QuantumNodeMock`]
        The nodes of the graph.
    """

    def __init__(self, qdata: list[QuantumNodeMock]):
        self._graph = nx.DiGraph()
        previous = qdata[0]
        for node in qdata[1:]:
            self._graph.add_edge(previous, node)
            previous = node

    def __iter__(self) -> Iterator:
        yield from nx.topological_sort(self._graph)

    def __len__(self) -> int:
        return len(self._graph)

    def findTaskDefByLabel(self, label: str) -> TaskNodeMock | None:
        for q in self:
            if q.task_node.label == label:
                return q.taskDef
        return None

    def getQuantaForTask(self, taskDef) -> set[QuantumMock]:
        nodes = self.getNodesForTask(taskDef)
        return {q.quantum for q in nodes}

    def getNodesForTask(self, taskDef: Any) -> set[QuantumNodeMock]:
        quanta = set()
        for q in self:
            if q.task_node.label == taskDef.label:
                quanta.add(q)
        return quanta

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    def findCycle(self) -> list:
        return []

    def determineInputsToQuantumNode(self, node: QuantumNodeMock) -> set[QuantumNodeMock]:
        result = set()
        for n in node.dependencies:
            for otherNode in self:
                if otherNode.index == n:
                    result.add(otherNode)
        return result

    def getSummary(self) -> QgraphSummary:
        summary = QgraphSummary(
            graphID="1712445133.605479-3902002",
            cmdLine="mock_pipetask -a 1 -b 2 -c 3 4 5 6",
            pipeBaseVersion="1.1.1",
            creationUTC="",
            inputCollection=["mock_input"],
            outputCollection="mock_output",
            outputRun="mock_run",
        )
        for q in self:
            qts = summary.qgraphTaskSummaries.setdefault(
                q.taskDef.label, QgraphTaskSummary(taskLabel=q.taskDef.label)
            )
            qts.numQuanta += 1

            for k in ["in1", "in2", "in3"]:
                qts.numInputs[k] += 1

            for k in ["out1", "out2", "out3"]:
                qts.numOutputs[k] += 1

        return summary


class TaskMockMP:
    """Simple mock class for task supporting multiprocessing."""

    canMultiprocess = True

    def runQuantum(self) -> None:
        _LOG.debug("TaskMockMP.runQuantum")
        pass


class TaskMockFail:
    """Simple mock class for task which fails."""

    canMultiprocess = True

    def runQuantum(self) -> None:
        _LOG.debug("TaskMockFail.runQuantum")
        raise ValueError("expected failure")


class TaskMockCrash:
    """Simple mock class for task which fails."""

    canMultiprocess = True

    def runQuantum(self) -> None:
        _LOG.debug("TaskMockCrash.runQuantum")
        # Disable fault handler to suppress long scary traceback.
        faulthandler.disable()
        signal.raise_signal(signal.SIGILL)


class TaskMockLongSleep:
    """Simple mock class for task which "runs" for very long time."""

    canMultiprocess = True

    def runQuantum(self) -> None:
        _LOG.debug("TaskMockLongSleep.runQuantum")
        time.sleep(100.0)


class TaskMockNoMP:
    """Simple mock class for task not supporting multiprocessing."""

    canMultiprocess = False


class TaskNodeMock:
    """Simple mock class for task definition in a pipeline graph.

    Parameters
    ----------
    label : `str`
        Label of the task in the pipeline.
    task_class : `type`
        Subclass of `lsst.pipe.base.PipelineTask`.
    config : `PipelineTaskConfig`, optional
        Configuration for the task.
    """

    def __init__(self, label: str = "task1", task_class: type = TaskMockMP, config: Any = None):
        self.label = label
        # taskClass to look like TaskDef, task_class to look like TaskNode.
        self.taskClass = task_class
        self.task_class = task_class
        self.config = config

    def __str__(self) -> str:
        return f"TaskNodeMock({self.label}, {self.taskClass.__name__})"


def _count_status(report: Report, status: ExecutionStatus) -> int:
    """Count number of quanta witha a given status."""
    return len([qrep for qrep in report.quantaReports if qrep.status is status])


class MPGraphExecutorTestCase(unittest.TestCase):
    """A test case for MPGraphExecutor class."""

    def test_mpexec_nomp(self) -> None:
        """Make simple graph and execute."""
        task_node = TaskNodeMock()
        qgraph = QuantumGraphMock(
            [QuantumNodeMock(index=i, task_node=task_node, detector=i) for i in range(3)]
        )

        # run in single-process mode
        qexec = QuantumExecutorMock()
        mpexec = MPGraphExecutor(numProc=1, timeout=100, quantumExecutor=qexec)
        mpexec.execute(qgraph)  # type: ignore[arg-type]
        self.assertEqual(qexec.getDataIds("detector"), [0, 1, 2])
        report = mpexec.getReport()
        assert report is not None
        self.assertEqual(report.status, ExecutionStatus.SUCCESS)
        self.assertIsNone(report.exitCode)
        self.assertIsNone(report.exceptionInfo)
        self.assertEqual(len(report.quantaReports), 3)
        self.assertTrue(all(qrep.status == ExecutionStatus.SUCCESS for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exitCode is None for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))
        self.assertTrue(all(qrep.taskLabel == "task1" for qrep in report.quantaReports))

    def test_mpexec_mp(self) -> None:
        """Make simple graph and execute."""
        task_node = TaskNodeMock()
        qgraph = QuantumGraphMock(
            [QuantumNodeMock(index=i, task_node=task_node, detector=i) for i in range(3)]
        )

        methods: list[Literal["spawn", "forkserver"]] = ["spawn"]
        if sys.platform == "linux":
            methods.append("forkserver")

        for method in methods:
            with self.subTest(startMethod=method):
                # Run in multi-process mode, the order of results is not
                # defined.
                qexec = QuantumExecutorMock(mp=True)
                mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec, startMethod=method)
                mpexec.execute(qgraph)  # type: ignore[arg-type]
                self.assertCountEqual(qexec.getDataIds("detector"), [0, 1, 2])
                report = mpexec.getReport()
                assert report is not None
                self.assertEqual(report.status, ExecutionStatus.SUCCESS)
                self.assertIsNone(report.exitCode)
                self.assertIsNone(report.exceptionInfo)
                self.assertEqual(len(report.quantaReports), 3)
                self.assertTrue(all(qrep.status == ExecutionStatus.SUCCESS for qrep in report.quantaReports))
                self.assertTrue(all(qrep.exitCode == 0 for qrep in report.quantaReports))
                self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))
                self.assertTrue(all(qrep.taskLabel == "task1" for qrep in report.quantaReports))

    def test_mpexec_nompsupport(self) -> None:
        """Try to run MP for task that has no MP support which should fail."""
        task_node = TaskNodeMock(task_class=TaskMockNoMP)
        qgraph = QuantumGraphMock(
            [QuantumNodeMock(index=i, task_node=task_node, detector=i) for i in range(3)]
        )

        # run in multi-process mode
        qexec = QuantumExecutorMock()
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "Task 'task1' does not support multiprocessing"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]

    def test_mpexec_fixup(self) -> None:
        """Make simple graph and execute, add dependencies by executing fixup
        code.
        """
        task_node = TaskNodeMock()

        for reverse in (False, True):
            qgraph = QuantumGraphMock(
                [QuantumNodeMock(index=i, task_node=task_node, detector=i) for i in range(3)]
            )

            qexec = QuantumExecutorMock()
            fixup = ExecFixupDataId("task1", "detector", reverse=reverse)
            mpexec = MPGraphExecutor(numProc=1, timeout=100, quantumExecutor=qexec, executionGraphFixup=fixup)
            mpexec.execute(qgraph)  # type: ignore[arg-type]

            expected = [0, 1, 2]
            if reverse:
                expected = list(reversed(expected))
            self.assertEqual(qexec.getDataIds("detector"), expected)

    def test_mpexec_timeout(self) -> None:
        """Fail due to timeout."""
        task_node = TaskNodeMock()
        task_nodeSleep = TaskNodeMock(task_class=TaskMockLongSleep)
        qgraph = QuantumGraphMock(
            [
                QuantumNodeMock(index=0, task_node=task_node, detector=0),
                QuantumNodeMock(index=1, task_node=task_nodeSleep, detector=1),
                QuantumNodeMock(index=2, task_node=task_node, detector=2),
            ]
        )

        # with failFast we'll get immediate MPTimeoutError
        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=1, quantumExecutor=qexec, failFast=True)
        with self.assertRaises(MPTimeoutError):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.TIMEOUT)
        self.assertEqual(report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPTimeoutError")
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.TIMEOUT), 1)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode < 0 for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

        # with failFast=False exception happens after last task finishes
        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=3, quantumExecutor=qexec, failFast=False)
        with self.assertRaises(MPTimeoutError):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        # We expect two tasks (0 and 2) to finish successfully and one task to
        # timeout. Unfortunately on busy CPU there is no guarantee that tasks
        # finish on time, so expect more timeouts and issue a warning.
        detectorIds = set(qexec.getDataIds("detector"))
        self.assertLess(len(detectorIds), 3)
        if detectorIds != {0, 2}:
            warnings.warn(f"Possibly timed out tasks, expected [0, 2], received {detectorIds}")
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.TIMEOUT)
        self.assertEqual(report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPTimeoutError")
        self.assertGreater(len(report.quantaReports), 0)
        self.assertGreater(_count_status(report, ExecutionStatus.TIMEOUT), 0)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode < 0 for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_failure(self) -> None:
        """Failure in one task should not stop other tasks."""
        task_node = TaskNodeMock()
        task_node_fail = TaskNodeMock(task_class=TaskMockFail)
        qgraph = QuantumGraphMock(
            [
                QuantumNodeMock(index=0, task_node=task_node, detector=0),
                QuantumNodeMock(index=1, task_node=task_node_fail, detector=1),
                QuantumNodeMock(index=2, task_node=task_node, detector=2),
            ]
        )

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        self.assertCountEqual(qexec.getDataIds("detector"), [0, 2])
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_dep(self) -> None:
        """Failure in one task should skip dependents."""
        task_node = TaskNodeMock()
        task_node_fail = TaskNodeMock(task_class=TaskMockFail)
        qdata = [
            QuantumNodeMock(index=0, task_node=task_node, detector=0),
            QuantumNodeMock(index=1, task_node=task_node_fail, detector=1),
            QuantumNodeMock(index=2, task_node=task_node, detector=2),
            QuantumNodeMock(index=3, task_node=task_node, detector=3),
            QuantumNodeMock(index=4, task_node=task_node, detector=4),
        ]
        qdata[2].dependencies.add(1)
        qdata[4].dependencies.add(3)
        qdata[4].dependencies.add(2)

        qgraph = QuantumGraphMock(qdata)

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        self.assertCountEqual(qexec.getDataIds("detector"), [0, 3])
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertEqual(_count_status(report, ExecutionStatus.SKIPPED), 2)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_dep_nomp(self) -> None:
        """Failure in one task should skip dependents, in-process version."""
        task_node = TaskNodeMock()
        task_node_fail = TaskNodeMock(task_class=TaskMockFail)
        qdata = [
            QuantumNodeMock(index=0, task_node=task_node, detector=0),
            QuantumNodeMock(index=1, task_node=task_node_fail, detector=1),
            QuantumNodeMock(index=2, task_node=task_node, detector=2),
            QuantumNodeMock(index=3, task_node=task_node, detector=3),
            QuantumNodeMock(index=4, task_node=task_node, detector=4),
        ]
        qdata[2].dependencies.add(1)
        qdata[4].dependencies.add(3)
        qdata[4].dependencies.add(2)

        qgraph = QuantumGraphMock(qdata)

        qexec = QuantumExecutorMock()
        mpexec = MPGraphExecutor(numProc=1, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        self.assertCountEqual(qexec.getDataIds("detector"), [0, 3])
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertEqual(_count_status(report, ExecutionStatus.SKIPPED), 2)
        self.assertTrue(all(qrep.exitCode is None for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_failure_failfast(self) -> None:
        """Fast fail stops quickly.

        Timing delay of task #3 should be sufficient to process
        failure and raise exception.
        """
        task_node = TaskNodeMock()
        task_node_fail = TaskNodeMock(task_class=TaskMockFail)
        task_nodeLongSleep = TaskNodeMock(task_class=TaskMockLongSleep)
        qdata = [
            QuantumNodeMock(index=0, task_node=task_node, detector=0),
            QuantumNodeMock(index=1, task_node=task_node_fail, detector=1),
            QuantumNodeMock(index=2, task_node=task_node, detector=2),
            QuantumNodeMock(index=3, task_node=task_nodeLongSleep, detector=3),
            QuantumNodeMock(index=4, task_node=task_node, detector=4),
        ]
        qdata[1].dependencies.add(0)
        qdata[2].dependencies.add(1)
        qdata[4].dependencies.add(3)
        qdata[4].dependencies.add(2)

        qgraph = QuantumGraphMock(qdata)

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec, failFast=True)
        with self.assertRaisesRegex(MPGraphExecutorError, "failed, exit code=1"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        self.assertCountEqual(qexec.getDataIds("detector"), [0])
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertTrue(any(qrep.exitCode is not None and qrep.exitCode > 0 for qrep in report.quantaReports))
        self.assertTrue(any(qrep.exceptionInfo is not None for qrep in report.quantaReports))

    def test_mpexec_crash(self) -> None:
        """Check task crash due to signal."""
        task_node = TaskNodeMock()
        task_node_crash = TaskNodeMock(task_class=TaskMockCrash)
        qgraph = QuantumGraphMock(
            [
                QuantumNodeMock(index=0, task_node=task_node, detector=0),
                QuantumNodeMock(index=1, task_node=task_node_crash, detector=1),
                QuantumNodeMock(index=2, task_node=task_node, detector=2),
            ]
        )

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        with self.assertRaisesRegex(MPGraphExecutorError, "One or more tasks failed"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        # Dependencies of failed tasks do not appear in quantaReports
        self.assertGreater(len(report.quantaReports), 0)
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertEqual(_count_status(report, ExecutionStatus.SUCCESS), 2)
        self.assertTrue(any(qrep.exitCode == -signal.SIGILL for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_crash_failfast(self) -> None:
        """Check task crash due to signal with --fail-fast."""
        task_node = TaskNodeMock()
        task_node_crash = TaskNodeMock(task_class=TaskMockCrash)
        qgraph = QuantumGraphMock(
            [
                QuantumNodeMock(index=0, task_node=task_node, detector=0),
                QuantumNodeMock(index=1, task_node=task_node_crash, detector=1),
                QuantumNodeMock(index=2, task_node=task_node, detector=2),
            ]
        )

        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec, failFast=True)
        with self.assertRaisesRegex(MPGraphExecutorError, "failed, killed by signal 4 .Illegal instruction"):
            mpexec.execute(qgraph)  # type: ignore[arg-type]
        report = mpexec.getReport()
        assert report is not None and report.exceptionInfo is not None
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(
            report.exceptionInfo.className, "lsst.ctrl.mpexec.mpGraphExecutor.MPGraphExecutorError"
        )
        self.assertEqual(_count_status(report, ExecutionStatus.FAILURE), 1)
        self.assertTrue(any(qrep.exitCode == -signal.SIGILL for qrep in report.quantaReports))
        self.assertTrue(all(qrep.exceptionInfo is None for qrep in report.quantaReports))

    def test_mpexec_num_fd(self) -> None:
        """Check that number of open files stays reasonable."""
        task_node = TaskNodeMock()
        qgraph = QuantumGraphMock(
            [QuantumNodeMock(index=i, task_node=task_node, detector=i) for i in range(20)]
        )

        this_proc = psutil.Process()
        num_fds_0 = this_proc.num_fds()

        # run in multi-process mode, the order of results is not defined
        qexec = QuantumExecutorMock(mp=True)
        mpexec = MPGraphExecutor(numProc=3, timeout=100, quantumExecutor=qexec)
        mpexec.execute(qgraph)  # type: ignore[arg-type]

        num_fds_1 = this_proc.num_fds()
        # They should be the same but allow small growth just in case.
        # Without DM-26728 fix the difference would be equal to number of
        # quanta (20).
        self.assertLess(num_fds_1 - num_fds_0, 5)


class SingleQuantumExecutorTestCase(unittest.TestCase):
    """Tests for SingleQuantumExecutor implementation."""

    instrument = "lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_simple_execute(self) -> None:
        """Run execute() method in simplest setup."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        taskFactory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler, taskFactory)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        # There must be one dataset of task's output connection
        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)

    def test_skip_existing_execute(self) -> None:
        """Run execute() method twice, with skip_existing_in."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        taskFactory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler, taskFactory)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_1 = refs[0].id

        # Re-run it with skipExistingIn, it should not run.
        assert butler.run is not None
        executor = SingleQuantumExecutor(butler, taskFactory, skipExistingIn=[butler.run])
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_2 = refs[0].id
        self.assertEqual(dataset_id_1, dataset_id_2)

    def test_clobber_outputs_execute(self) -> None:
        """Run execute() method twice, with clobber_outputs."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        taskFactory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler, taskFactory)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_1 = refs[0].id

        original_dataset = butler.get(refs[0])

        # Remove the dataset ourself, and replace it with something
        # different so we can check later whether it got replaced.
        butler.pruneDatasets([refs[0]], disassociate=False, unstore=True, purge=False)
        replacement = original_dataset + 10
        butler.put(replacement, refs[0])

        # Re-run it with clobberOutputs and skipExistingIn, it should not
        # clobber but should skip instead.
        assert butler.run is not None
        executor = SingleQuantumExecutor(
            butler, taskFactory, skipExistingIn=[butler.run], clobberOutputs=True
        )
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(taskFactory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_2 = refs[0].id
        self.assertEqual(dataset_id_1, dataset_id_2)

        second_dataset = butler.get(refs[0])
        self.assertEqual(list(second_dataset), list(replacement))

        # Re-run it with clobberOutputs but without skipExistingIn, it should
        # clobber.
        assert butler.run is not None
        executor = SingleQuantumExecutor(butler, taskFactory, clobberOutputs=True)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(taskFactory.countExec, 2)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_3 = refs[0].id

        third_dataset = butler.get(refs[0])
        self.assertEqual(list(third_dataset), list(original_dataset))

        # No change in UUID even after replacement
        self.assertEqual(dataset_id_1, dataset_id_3)


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
