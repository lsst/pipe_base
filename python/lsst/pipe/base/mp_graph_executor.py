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

from __future__ import annotations

__all__ = ["MPGraphExecutor", "MPGraphExecutorError", "MPTimeoutError"]

import enum
import importlib
import logging
import multiprocessing
import pickle
import signal
import sys
import threading
import time
import uuid
from typing import Literal, cast

import networkx

from lsst.daf.butler import DataCoordinate, Quantum
from lsst.daf.butler.cli.cliLog import CliLog
from lsst.utils.threads import disable_implicit_threading

from ._status import InvalidQuantumError, RepeatableQuantumError
from .execution_graph_fixup import ExecutionGraphFixup
from .graph import QuantumGraph
from .graph_walker import GraphWalker
from .pipeline_graph import TaskNode
from .quantum_graph import PredictedQuantumGraph, PredictedQuantumInfo
from .quantum_graph_executor import QuantumExecutor, QuantumGraphExecutor
from .quantum_reports import ExecutionStatus, QuantumReport, Report

_LOG = logging.getLogger(__name__)


class JobState(enum.Enum):
    """Possible state for an executing task."""

    PENDING = enum.auto()
    """The job has not started yet."""

    RUNNING = enum.auto()
    """The job is currently executing."""

    FINISHED = enum.auto()
    """The job finished successfully."""

    FAILED = enum.auto()
    """The job execution failed (process returned non-zero status)."""

    TIMED_OUT = enum.auto()
    """The job was killed due to too long execution time."""

    FAILED_DEP = enum.auto()
    """One of the dependencies of this job failed or timed out."""


class _Job:
    """Class representing a job running single task.

    Parameters
    ----------
    quantum_id : `uuid.UUID`
        ID of the quantum this job executes.
    quantum : `lsst.daf.butler.Quantum`
        Description of the inputs and outputs.
    task_node : `.pipeline_graph.TaskNode`
        Description of the task and configuration.
    """

    def __init__(self, quantum_id: uuid.UUID, quantum: Quantum, task_node: TaskNode):
        self.quantum_id = quantum_id
        self.quantum = quantum
        self.task_node = task_node
        self.process: multiprocessing.process.BaseProcess | None = None
        self._state = JobState.PENDING
        self.started: float = 0.0
        self._rcv_conn: multiprocessing.connection.Connection | None = None
        self._terminated = False

    @property
    def state(self) -> JobState:
        """Job processing state (JobState)."""
        return self._state

    @property
    def terminated(self) -> bool:
        """Return `True` if job was killed by stop() method and negative exit
        code is returned from child process (`bool`).
        """
        if self._terminated:
            assert self.process is not None, "Process must be started"
            if self.process.exitcode is not None:
                return self.process.exitcode < 0
        return False

    def start(
        self,
        quantumExecutor: QuantumExecutor,
        startMethod: Literal["spawn"] | Literal["forkserver"],
        fail_fast: bool,
    ) -> None:
        """Start process which runs the task.

        Parameters
        ----------
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        startMethod : `str`, optional
            Start method from `multiprocessing` module.
        fail_fast : `bool`, optional
            If `True` then kill subprocess on RepeatableQuantumError.
        """
        # Unpickling of quantum has to happen after butler/executor, also we
        # want to setup logging before unpickling anything that can generate
        # messages, this is why things are pickled manually here.
        qe_pickle = pickle.dumps(quantumExecutor)
        task_node_pickle = pickle.dumps(self.task_node)
        quantum_pickle = pickle.dumps(self.quantum)
        self._rcv_conn, snd_conn = multiprocessing.Pipe(False)
        logConfigState = CliLog.configState

        mp_ctx = multiprocessing.get_context(startMethod)
        self.process = mp_ctx.Process(  # type: ignore[attr-defined]
            target=_Job._executeJob,
            args=(
                qe_pickle,
                task_node_pickle,
                quantum_pickle,
                self.quantum_id,
                logConfigState,
                snd_conn,
                fail_fast,
            ),
            name=f"task-{self.quantum.dataId}",
        )
        # mypy is getting confused by multiprocessing.
        assert self.process is not None
        self.process.start()
        self.started = time.time()
        self._state = JobState.RUNNING

    @staticmethod
    def _executeJob(
        quantumExecutor_pickle: bytes,
        task_node_pickle: bytes,
        quantum_pickle: bytes,
        quantum_id: uuid.UUID,
        logConfigState: list,
        snd_conn: multiprocessing.connection.Connection,
        fail_fast: bool,
    ) -> None:
        """Execute a job with arguments.

        Parameters
        ----------
        quantumExecutor_pickle : `bytes`
            Executor for single quantum, pickled.
        task_node_pickle : `bytes`
            Task definition structure, pickled.
        quantum_pickle : `bytes`
            Quantum for this task execution in pickled form.
        quantum_id : `uuid.UUID`
            Unique ID for the quantum.
        logConfigState : `list`
            Logging state from parent process.
        snd_conn : `multiprocessing.Connection`
            Connection to send job report to parent process.
        fail_fast : `bool`
            If `True` then kill subprocess on RepeatableQuantumError.
        """
        # This terrible hack is a workaround for Python threading bug:
        # https://github.com/python/cpython/issues/102512. Should be removed
        # when fix for that bug is deployed. Inspired by
        # https://github.com/QubesOS/qubes-core-admin-client/pull/236/files.
        thread = threading.current_thread()
        if isinstance(thread, threading._DummyThread):
            if getattr(thread, "_tstate_lock", "") is None:
                thread._set_tstate_lock()  # type: ignore[attr-defined]

        if logConfigState and not CliLog.configState:
            # means that we are in a new spawned Python process and we have to
            # re-initialize logging
            CliLog.replayConfigState(logConfigState)

        quantumExecutor: QuantumExecutor = pickle.loads(quantumExecutor_pickle)
        task_node: TaskNode = pickle.loads(task_node_pickle)
        quantum = pickle.loads(quantum_pickle)
        report: QuantumReport | None = None
        # Catch a few known failure modes and stop the process immediately,
        # with exception-specific exit code.
        try:
            _, report = quantumExecutor.execute(task_node, quantum, quantum_id=quantum_id)
        except RepeatableQuantumError as exc:
            report = QuantumReport.from_exception(
                quantumId=quantum_id,
                exception=exc,
                dataId=quantum.dataId,
                taskLabel=task_node.label,
                exitCode=exc.EXIT_CODE if fail_fast else None,
            )
            if fail_fast:
                _LOG.warning("Caught repeatable quantum error for %s (%s):", task_node.label, quantum.dataId)
                _LOG.warning(exc, exc_info=True)
                sys.exit(exc.EXIT_CODE)
            else:
                raise
        except InvalidQuantumError as exc:
            _LOG.fatal("Invalid quantum error for %s (%s): %s", task_node.label, quantum.dataId)
            _LOG.fatal(exc, exc_info=True)
            report = QuantumReport.from_exception(
                quantumId=quantum_id,
                exception=exc,
                dataId=quantum.dataId,
                taskLabel=task_node.label,
                exitCode=exc.EXIT_CODE,
            )
            sys.exit(exc.EXIT_CODE)
        except Exception as exc:
            _LOG.debug("exception from task %s dataId %s: %s", task_node.label, quantum.dataId, exc)
            report = QuantumReport.from_exception(
                quantumId=quantum_id,
                exception=exc,
                dataId=quantum.dataId,
                taskLabel=task_node.label,
            )
            raise
        finally:
            if report is not None:
                # If sending fails we do not want this new exception to be
                # exposed.
                try:
                    _LOG.debug("sending report for task %s dataId %s", task_node.label, quantum.dataId)
                    snd_conn.send(report)
                except Exception:
                    pass

    def stop(self) -> None:
        """Stop the process."""
        assert self.process is not None, "Process must be started"
        self.process.terminate()
        # give it 1 second to finish or KILL
        for _ in range(10):
            time.sleep(0.1)
            if not self.process.is_alive():
                break
        else:
            _LOG.debug("Killing process %s", self.process.name)
            self.process.kill()
        self._terminated = True

    def cleanup(self) -> None:
        """Release processes resources, has to be called for each finished
        process.
        """
        if self.process and not self.process.is_alive():
            self.process.close()
            self.process = None
            self._rcv_conn = None

    def report(self) -> QuantumReport:
        """Return task report, should be called after process finishes and
        before cleanup().
        """
        assert self.process is not None, "Process must be started"
        assert self._rcv_conn is not None, "Process must be started"
        try:
            report = self._rcv_conn.recv()
            report.exitCode = self.process.exitcode
        except Exception:
            # Likely due to the process killed, but there may be other reasons.
            # Exit code should not be None, this is to keep mypy happy.
            exitcode = self.process.exitcode if self.process.exitcode is not None else -1
            assert self.quantum.dataId is not None, "Quantum DataId cannot be None"
            report = QuantumReport.from_exit_code(
                quantumId=self.quantum_id,
                exitCode=exitcode,
                dataId=self.quantum.dataId,
                taskLabel=self.task_node.label,
            )
        if self.terminated:
            # Means it was killed, assume it's due to timeout
            report.status = ExecutionStatus.TIMEOUT
        return report

    def failMessage(self) -> str:
        """Return a message describing task failure."""
        assert self.process is not None, "Process must be started"
        assert self.process.exitcode is not None, "Process has to finish"
        exitcode = self.process.exitcode
        if exitcode < 0:
            # Negative exit code means it is killed by signal
            signum = -exitcode
            msg = f"Task {self} failed, killed by signal {signum}"
            # Just in case this is some very odd signal, expect ValueError
            try:
                strsignal = signal.strsignal(signum)
                msg = f"{msg} ({strsignal})"
            except ValueError:
                pass
        elif exitcode > 0:
            msg = f"Task {self} failed, exit code={exitcode}"
        else:
            msg = ""
        return msg

    def __str__(self) -> str:
        return f"<{self.task_node.label} dataId={self.quantum.dataId}>"


class _JobList:
    """Simple list of _Job instances with few convenience methods.

    Parameters
    ----------
    xgraph : `networkx.DiGraph`
        Directed acyclic graph of quantum IDs.
    """

    def __init__(self, xgraph: networkx.DiGraph):
        self.jobs = {
            quantum_id: _Job(
                quantum_id=quantum_id,
                quantum=xgraph.nodes[quantum_id]["quantum"],
                task_node=xgraph.nodes[quantum_id]["pipeline_node"],
            )
            for quantum_id in xgraph
        }
        self.walker: GraphWalker[uuid.UUID] = GraphWalker(xgraph.copy())
        self.pending = set(next(self.walker, ()))
        self.running: set[uuid.UUID] = set()
        self.finished: set[uuid.UUID] = set()
        self.failed: set[uuid.UUID] = set()
        self.timed_out: set[uuid.UUID] = set()

    def submit(
        self,
        quantumExecutor: QuantumExecutor,
        startMethod: Literal["spawn"] | Literal["forkserver"],
        fail_fast: bool = False,
    ) -> _Job:
        """Submit a pending job for execution.

        Parameters
        ----------
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        startMethod : `str`, optional
            Start method from `multiprocessing` module.
        fail_fast : `bool`, optional
            If `True` then kill subprocess on RepeatableQuantumError.

        Returns
        -------
        job : `_Job`
            The job that was submitted.
        """
        quantum_id = self.pending.pop()
        job = self.jobs[quantum_id]
        job.start(quantumExecutor, startMethod, fail_fast=fail_fast)
        self.running.add(job.quantum_id)
        return job

    def setJobState(self, job: _Job, state: JobState) -> list[_Job]:
        """Update job state.

        Parameters
        ----------
        job : `_Job`
            Job to submit.
        state : `JobState`
            New job state; note that only the FINISHED, FAILED, and TIMED_OUT
            states are acceptable.

        Returns
        -------
        blocked : `list` [ `_Job` ]
            Additional jobs that have been marked as failed because this job
            was upstream of them and failed or timed out.
        """
        allowedStates = (JobState.FINISHED, JobState.FAILED, JobState.TIMED_OUT)
        assert state in allowedStates, f"State {state} not allowed here"

        # remove job from pending/running lists
        if job.state == JobState.PENDING:
            self.pending.remove(job.quantum_id)
        elif job.state == JobState.RUNNING:
            self.running.remove(job.quantum_id)

        quantum_id = job.quantum_id
        # it should not be in any of these, but just in case
        self.finished.discard(quantum_id)
        self.failed.discard(quantum_id)
        self.timed_out.discard(quantum_id)
        job._state = state
        match job.state:
            case JobState.FINISHED:
                self.finished.add(quantum_id)
                self.walker.finish(quantum_id)
                self.pending.update(next(self.walker, ()))
                return []
            case JobState.FAILED:
                self.failed.add(quantum_id)
            case JobState.TIMED_OUT:
                self.failed.add(quantum_id)
                self.timed_out.add(quantum_id)
            case _:
                raise ValueError(f"Unexpected state value: {state}")
        blocked: list[_Job] = []
        for downstream_quantum_id in self.walker.fail(quantum_id):
            self.failed.add(downstream_quantum_id)
            blocked.append(self.jobs[downstream_quantum_id])
            self.jobs[downstream_quantum_id]._state = JobState.FAILED_DEP
        return blocked

    def cleanup(self) -> None:
        """Do periodic cleanup for jobs that did not finish correctly.

        If timed out jobs are killed but take too long to stop then regular
        cleanup will not work for them. Here we check all timed out jobs
        periodically and do cleanup if they managed to die by this time.
        """
        for quantum_id in self.timed_out:
            job = self.jobs[quantum_id]
            assert job.state == JobState.TIMED_OUT, "Job state should be consistent with the set it's in."
            if job.process is not None:
                job.cleanup()


class MPGraphExecutorError(Exception):
    """Exception class for errors raised by MPGraphExecutor."""

    pass


class MPTimeoutError(MPGraphExecutorError):
    """Exception raised when task execution times out."""

    pass


class MPGraphExecutor(QuantumGraphExecutor):
    """Implementation of QuantumGraphExecutor using same-host multiprocess
    execution of Quanta.

    Parameters
    ----------
    num_proc : `int`
        Number of processes to use for executing tasks.
    timeout : `float`
        Time in seconds to wait for tasks to finish.
    quantum_executor : `.quantum_graph_executor.QuantumExecutor`
        Executor for single quantum. For multiprocess-style execution when
        ``num_proc`` is greater than one this instance must support pickle.
    start_method : `str`, optional
        Start method from `multiprocessing` module, `None` selects the best
        one for current platform.
    fail_fast : `bool`, optional
        If set to ``True`` then stop processing on first error from any task.
    pdb : `str`, optional
        Debugger to import and use (via the ``post_mortem`` function) in the
        event of an exception.
    execution_graph_fixup : `.execution_graph_fixup.ExecutionGraphFixup`, \
            optional
        Instance used for modification of execution graph.
    """

    def __init__(
        self,
        *,
        num_proc: int,
        timeout: float,
        quantum_executor: QuantumExecutor,
        start_method: Literal["spawn"] | Literal["forkserver"] | None = None,
        fail_fast: bool = False,
        pdb: str | None = None,
        execution_graph_fixup: ExecutionGraphFixup | None = None,
    ):
        self._num_proc = num_proc
        self._timeout = timeout
        self._quantum_executor = quantum_executor
        self._fail_fast = fail_fast
        self._pdb = pdb
        self._execution_graph_fixup = execution_graph_fixup
        self._report: Report | None = None

        # We set default start method as spawn for all platforms.
        if start_method is None:
            start_method = "spawn"
        self._start_method = start_method

    def execute(self, graph: QuantumGraph | PredictedQuantumGraph) -> None:
        # Docstring inherited from QuantumGraphExecutor.execute
        old_graph: QuantumGraph | None = None
        if isinstance(graph, QuantumGraph):
            old_graph = graph
            new_graph = PredictedQuantumGraph.from_old_quantum_graph(old_graph)
        else:
            new_graph = graph
        xgraph = self._make_xgraph(new_graph, old_graph)
        self._report = Report(qgraphSummary=new_graph._make_summary())
        try:
            if self._num_proc > 1:
                self._execute_quanta_mp(xgraph, self._report)
            else:
                self._execute_quanta_in_process(xgraph, self._report)
        except Exception as exc:
            self._report.set_exception(exc)
            raise

    def _make_xgraph(
        self, new_graph: PredictedQuantumGraph, old_graph: QuantumGraph | None
    ) -> networkx.DiGraph:
        """Obtain a networkx DAG from a quantum graph, applying any fixup and
        adding `lsst.daf.butler.Quantum` and `~.pipeline_graph.TaskNode`
        attributes.

        Parameters
        ----------
        new_graph : `.quantum_graph.PredictedQuantumGraph`
            New quantum graph object.
        old_graph : `.QuantumGraph` or `None`
            Equivalent old quantum graph object.

        Returns
        -------
        xgraph : `networkx.DiGraph`
            NetworkX DAG with quantum IDs as node keys.

        Raises
        ------
        MPGraphExecutorError
            Raised if execution graph cannot be ordered after modification,
            i.e. it has dependency cycles.
        """
        new_graph.build_execution_quanta()
        xgraph = new_graph.quantum_only_xgraph.copy()
        if self._execution_graph_fixup:
            try:
                self._execution_graph_fixup.fixup_graph(xgraph, new_graph.quanta_by_task)
            except NotImplementedError:
                # Backwards compatibility.
                if old_graph is None:
                    old_graph = new_graph.to_old_quantum_graph()
                old_graph = self._execution_graph_fixup.fixupQuanta(old_graph)
                # Adding all of the edges from old_graph is overkill, but the
                # only option we really have to make sure we add any new ones.
                xgraph.update([(a.nodeId, b.nodeId) for a, b in old_graph.graph.edges])
            if networkx.dag.has_cycle(xgraph):
                raise MPGraphExecutorError("Updated execution graph has dependency cycle.")
        return xgraph

    def _execute_quanta_in_process(self, xgraph: networkx.DiGraph, report: Report) -> None:
        """Execute all Quanta in current process.

        Parameters
        ----------
        xgraph : `networkx.DiGraph`
            DAG to execute.  Should have quantum IDs for nodes and ``quantum``
            (`lsst.daf.butler.Quantum`) and ``pipeline_node``
            (`lsst.pipe.base.pipeline_graph.TaskNode`) attributes in addition
            to those provided by
            `.quantum_graph.PredictedQuantumGraph.quantum_only_xgraph`.
        report : `Report`
            Object for reporting execution status.
        """

        def tiebreaker_sort_key(quantum_id: uuid.UUID) -> tuple:
            node_state = xgraph.nodes[quantum_id]
            return (node_state["task_label"],) + node_state["data_id"].required_values

        success_count, failed_count, total_count = 0, 0, len(xgraph.nodes)
        walker = GraphWalker[uuid.UUID](xgraph.copy())
        for unblocked_quanta in walker:
            for quantum_id in sorted(unblocked_quanta, key=tiebreaker_sort_key):
                node_state: PredictedQuantumInfo = xgraph.nodes[quantum_id]
                data_id = node_state["data_id"]
                task_node = node_state["pipeline_node"]
                quantum = node_state["quantum"]

                _LOG.debug("Executing %s (%s@%s)", quantum_id, task_node.label, data_id)
                fail_exit_code: int | None = None
                try:
                    # For some exception types we want to exit immediately with
                    # exception-specific exit code, but we still want to start
                    # debugger before exiting if debugging is enabled.
                    try:
                        _, quantum_report = self._quantum_executor.execute(
                            task_node, quantum, quantum_id=quantum_id
                        )
                        if quantum_report:
                            report.quantaReports.append(quantum_report)
                        success_count += 1
                        walker.finish(quantum_id)
                    except RepeatableQuantumError as exc:
                        if self._fail_fast:
                            _LOG.warning(
                                "Caught repeatable quantum error for %s (%s@%s):",
                                quantum_id,
                                task_node.label,
                                data_id,
                            )
                            _LOG.warning(exc, exc_info=True)
                            fail_exit_code = exc.EXIT_CODE
                        raise
                    except InvalidQuantumError as exc:
                        _LOG.fatal(
                            "Invalid quantum error for %s (%s@%s):", quantum_id, task_node.label, data_id
                        )
                        _LOG.fatal(exc, exc_info=True)
                        fail_exit_code = exc.EXIT_CODE
                        raise
                except Exception as exc:
                    quantum_report = QuantumReport.from_exception(
                        exception=exc,
                        dataId=data_id,
                        taskLabel=task_node.label,
                    )
                    report.quantaReports.append(quantum_report)

                    if self._pdb and sys.stdin.isatty() and sys.stdout.isatty():
                        _LOG.error(
                            "%s (%s@%s) failed; dropping into pdb.",
                            quantum_id,
                            task_node.label,
                            data_id,
                            exc_info=exc,
                        )
                        try:
                            pdb = importlib.import_module(self._pdb)
                        except ImportError as imp_exc:
                            raise MPGraphExecutorError(
                                f"Unable to import specified debugger module ({self._pdb}): {imp_exc}"
                            ) from exc
                        if not hasattr(pdb, "post_mortem"):
                            raise MPGraphExecutorError(
                                f"Specified debugger module ({self._pdb}) can't debug with post_mortem",
                            ) from exc
                        pdb.post_mortem(exc.__traceback__)

                    report.status = ExecutionStatus.FAILURE
                    failed_count += 1

                    # If exception specified an exit code then just exit with
                    # that code, otherwise crash if fail-fast option is
                    # enabled.
                    if fail_exit_code is not None:
                        sys.exit(fail_exit_code)
                    if self._fail_fast:
                        raise MPGraphExecutorError(
                            f"Quantum {quantum_id} ({task_node.label}@{data_id}) failed."
                        ) from exc
                    else:
                        _LOG.error(
                            "%s (%s@%s) failed; processing will continue for remaining tasks.",
                            quantum_id,
                            task_node.label,
                            data_id,
                            exc_info=exc,
                        )

                    for downstream_quantum_id in walker.fail(quantum_id):
                        downstream_node_state = xgraph.nodes[downstream_quantum_id]
                        failed_quantum_report = QuantumReport(
                            status=ExecutionStatus.SKIPPED,
                            dataId=downstream_node_state["data_id"],
                            taskLabel=downstream_node_state["task_label"],
                        )
                        report.quantaReports.append(failed_quantum_report)
                        _LOG.error(
                            "Upstream job failed for task %s (%s@%s), skipping this quantum.",
                            downstream_quantum_id,
                            downstream_node_state["task_label"],
                            downstream_node_state["data_id"],
                        )
                        failed_count += 1

                _LOG.info(
                    "Executed %d quanta successfully, %d failed and %d remain out of total %d quanta.",
                    success_count,
                    failed_count,
                    total_count - success_count - failed_count,
                    total_count,
                )

        # Raise an exception if there were any failures.
        if failed_count:
            raise MPGraphExecutorError("One or more tasks failed during execution.")

    def _execute_quanta_mp(self, xgraph: networkx.DiGraph, report: Report) -> None:
        """Execute all Quanta in separate processes.

        Parameters
        ----------
        xgraph : `networkx.DiGraph`
            DAG to execute.  Should have quantum IDs for nodes and ``quantum``
            (`lsst.daf.butler.Quantum`) and ``task_node``
            (`lsst.pipe.base.pipeline_graph.TaskNode`) attributes in addition
            to those provided by
            `.quantum_graph.PredictedQuantumGraph.quantum_only_xgraph`.
        report : `Report`
            Object for reporting execution status.
        """
        disable_implicit_threading()  # To prevent thread contention

        _LOG.debug("Using %r for multiprocessing start method", self._start_method)

        # re-pack input quantum data into jobs list
        jobs = _JobList(xgraph)

        # check that all tasks can run in sub-process
        for job in jobs.jobs.values():
            if not job.task_node.task_class.canMultiprocess:
                raise MPGraphExecutorError(
                    f"Task {job.task_node.label!r} does not support multiprocessing; use single process"
                )

        finishedCount, failedCount = 0, 0
        while jobs.pending or jobs.running:
            _LOG.debug("#pendingJobs: %s", len(jobs.pending))
            _LOG.debug("#runningJobs: %s", len(jobs.running))

            # See if any jobs have finished
            for quantum_id in list(jobs.running):  # iterate over a copy so we can remove.
                job = jobs.jobs[quantum_id]
                assert job.process is not None, "Process cannot be None"
                blocked: list[_Job] = []
                if not job.process.is_alive():
                    _LOG.debug("finished: %s", job)
                    # finished
                    exitcode = job.process.exitcode
                    quantum_report = job.report()
                    report.quantaReports.append(quantum_report)
                    if exitcode == 0:
                        jobs.setJobState(job, JobState.FINISHED)
                        job.cleanup()
                        _LOG.debug("success: %s took %.3f seconds", job, time.time() - job.started)
                    else:
                        if job.terminated:
                            # Was killed due to timeout.
                            if report.status == ExecutionStatus.SUCCESS:
                                # Do not override global FAILURE status
                                report.status = ExecutionStatus.TIMEOUT
                            message = f"Timeout ({self._timeout} sec) for task {job}, task is killed"
                            blocked = jobs.setJobState(job, JobState.TIMED_OUT)
                        else:
                            report.status = ExecutionStatus.FAILURE
                            # failMessage() has to be called before cleanup()
                            message = job.failMessage()
                            blocked = jobs.setJobState(job, JobState.FAILED)

                        job.cleanup()
                        _LOG.debug("failed: %s", job)
                        if self._fail_fast or exitcode == InvalidQuantumError.EXIT_CODE:
                            # stop all running jobs
                            for stop_quantum_id in jobs.running:
                                stop_job = jobs.jobs[stop_quantum_id]
                                if stop_job is not job:
                                    stop_job.stop()
                            if job.state is JobState.TIMED_OUT:
                                raise MPTimeoutError(f"Timeout ({self._timeout} sec) for task {job}.")
                            else:
                                raise MPGraphExecutorError(message)
                        else:
                            _LOG.error("%s; processing will continue for remaining tasks.", message)
                else:
                    # check for timeout
                    now = time.time()
                    if now - job.started > self._timeout:
                        # Try to kill it, and there is a chance that it
                        # finishes successfully before it gets killed. Exit
                        # status is handled by the code above on next
                        # iteration.
                        _LOG.debug("Terminating job %s due to timeout", job)
                        job.stop()

                for downstream_job in blocked:
                    quantum_report = QuantumReport(
                        quantumId=downstream_job.quantum_id,
                        status=ExecutionStatus.SKIPPED,
                        dataId=cast(DataCoordinate, downstream_job.quantum.dataId),
                        taskLabel=downstream_job.task_node.label,
                    )
                    report.quantaReports.append(quantum_report)
                    _LOG.error("Upstream job failed for task %s, skipping this task.", downstream_job)

            # see if we can start more jobs
            while len(jobs.running) < self._num_proc and jobs.pending:
                job = jobs.submit(self._quantum_executor, self._start_method)
                _LOG.debug("Submitted %s", job)

            # Do cleanup for timed out jobs if necessary.
            jobs.cleanup()

            # Print progress message if something changed.
            newFinished, newFailed = len(jobs.finished), len(jobs.failed)
            if (finishedCount, failedCount) != (newFinished, newFailed):
                finishedCount, failedCount = newFinished, newFailed
                totalCount = len(jobs.jobs)
                _LOG.info(
                    "Executed %d quanta successfully, %d failed and %d remain out of total %d quanta.",
                    finishedCount,
                    failedCount,
                    totalCount - finishedCount - failedCount,
                    totalCount,
                )

            # Here we want to wait until one of the running jobs completes
            # but multiprocessing does not provide an API for that, for now
            # just sleep a little bit and go back to the loop.
            if jobs.running:
                time.sleep(0.1)

        if jobs.failed:
            # print list of failed jobs
            _LOG.error("Failed jobs:")
            for quantum_id in jobs.failed:
                job = jobs.jobs[quantum_id]
                _LOG.error("  - %s: %s", job.state.name, job)

            # if any job failed raise an exception
            if jobs.failed == jobs.timed_out:
                raise MPTimeoutError("One or more tasks timed out during execution.")
            else:
                raise MPGraphExecutorError("One or more tasks failed or timed out during execution.")

    def getReport(self) -> Report:
        # Docstring inherited from base class
        if self._report is None:
            raise RuntimeError("getReport() called before execute()")
        return self._report
