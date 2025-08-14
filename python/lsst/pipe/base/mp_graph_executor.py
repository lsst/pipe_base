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
from collections.abc import Iterable
from typing import Literal

from lsst.daf.butler.cli.cliLog import CliLog
from lsst.utils.threads import disable_implicit_threading

from ._status import InvalidQuantumError, RepeatableQuantumError
from .execution_graph_fixup import ExecutionGraphFixup
from .graph import QuantumGraph, QuantumNode
from .pipeline_graph import TaskNode
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
    qnode: `QuantumNode`
        Quantum and some associated information.
    """

    def __init__(self, qnode: QuantumNode, fail_fast: bool = False):
        self.qnode = qnode
        self._fail_fast = fail_fast
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
    ) -> None:
        """Start process which runs the task.

        Parameters
        ----------
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        startMethod : `str`, optional
            Start method from `multiprocessing` module.
        """
        # Unpickling of quantum has to happen after butler/executor, also we
        # want to setup logging before unpickling anything that can generate
        # messages, this is why things are pickled manually here.
        qe_pickle = pickle.dumps(quantumExecutor)
        task_node_pickle = pickle.dumps(self.qnode.task_node)
        quantum_pickle = pickle.dumps(self.qnode.quantum)
        self._rcv_conn, snd_conn = multiprocessing.Pipe(False)
        logConfigState = CliLog.configState

        mp_ctx = multiprocessing.get_context(startMethod)
        self.process = mp_ctx.Process(  # type: ignore[attr-defined]
            target=_Job._executeJob,
            args=(
                qe_pickle,
                task_node_pickle,
                quantum_pickle,
                self.qnode.nodeId,
                logConfigState,
                snd_conn,
                self._fail_fast,
            ),
            name=f"task-{self.qnode.quantum.dataId}",
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
            assert self.qnode.quantum.dataId is not None, "Quantum DataId cannot be None"
            report = QuantumReport.from_exit_code(
                quantumId=self.qnode.nodeId,
                exitCode=exitcode,
                dataId=self.qnode.quantum.dataId,
                taskLabel=self.qnode.task_node.label,
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
        return f"<{self.qnode.task_node.label} dataId={self.qnode.quantum.dataId}>"


class _JobList:
    """Simple list of _Job instances with few convenience methods.

    Parameters
    ----------
    iterable : ~collections.abc.Iterable` [ `QuantumNode` ]
        Sequence of Quanta to execute. This has to be ordered according to
        task dependencies.
    """

    def __init__(self, iterable: Iterable[QuantumNode]):
        self.jobs = [_Job(qnode) for qnode in iterable]
        self.pending = self.jobs[:]
        self.running: list[_Job] = []
        self.finishedNodes: set[QuantumNode] = set()
        self.failedNodes: set[QuantumNode] = set()
        self.timedOutNodes: set[QuantumNode] = set()

    def submit(
        self,
        job: _Job,
        quantumExecutor: QuantumExecutor,
        startMethod: Literal["spawn"] | Literal["forkserver"],
    ) -> None:
        """Submit one more job for execution.

        Parameters
        ----------
        job : `_Job`
            Job to submit.
        quantumExecutor : `QuantumExecutor`
            Executor for single quantum.
        startMethod : `str`, optional
            Start method from `multiprocessing` module.
        """
        # this will raise if job is not in pending list
        self.pending.remove(job)
        job.start(quantumExecutor, startMethod)
        self.running.append(job)

    def setJobState(self, job: _Job, state: JobState) -> None:
        """Update job state.

        Parameters
        ----------
        job : `_Job`
            Job to submit.
        state : `JobState`
            New job state, note that only FINISHED, FAILED, TIMED_OUT, or
            FAILED_DEP state is acceptable.
        """
        allowedStates = (JobState.FINISHED, JobState.FAILED, JobState.TIMED_OUT, JobState.FAILED_DEP)
        assert state in allowedStates, f"State {state} not allowed here"

        # remove job from pending/running lists
        if job.state == JobState.PENDING:
            self.pending.remove(job)
        elif job.state == JobState.RUNNING:
            self.running.remove(job)

        qnode = job.qnode
        # it should not be in any of these, but just in case
        self.finishedNodes.discard(qnode)
        self.failedNodes.discard(qnode)
        self.timedOutNodes.discard(qnode)

        job._state = state
        if state == JobState.FINISHED:
            self.finishedNodes.add(qnode)
        elif state == JobState.FAILED:
            self.failedNodes.add(qnode)
        elif state == JobState.FAILED_DEP:
            self.failedNodes.add(qnode)
        elif state == JobState.TIMED_OUT:
            self.failedNodes.add(qnode)
            self.timedOutNodes.add(qnode)
        else:
            raise ValueError(f"Unexpected state value: {state}")

    def cleanup(self) -> None:
        """Do periodic cleanup for jobs that did not finish correctly.

        If timed out jobs are killed but take too long to stop then regular
        cleanup will not work for them. Here we check all timed out jobs
        periodically and do cleanup if they managed to die by this time.
        """
        for job in self.jobs:
            if job.state == JobState.TIMED_OUT and job.process is not None:
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

    def execute(self, graph: QuantumGraph) -> None:
        # Docstring inherited from QuantumGraphExecutor.execute
        graph = self._fixupQuanta(graph)
        self._report = Report(qgraphSummary=graph.getSummary())
        try:
            if self._num_proc > 1:
                self._executeQuantaMP(graph, self._report)
            else:
                self._executeQuantaInProcess(graph, self._report)
        except Exception as exc:
            self._report.set_exception(exc)
            raise

    def _fixupQuanta(self, graph: QuantumGraph) -> QuantumGraph:
        """Call fixup code to modify execution graph.

        Parameters
        ----------
        graph : `.QuantumGraph`
            `.QuantumGraph` to modify.

        Returns
        -------
        graph : `.QuantumGraph`
            Modified `.QuantumGraph`.

        Raises
        ------
        MPGraphExecutorError
            Raised if execution graph cannot be ordered after modification,
            i.e. it has dependency cycles.
        """
        if not self._execution_graph_fixup:
            return graph

        _LOG.debug("Call execution graph fixup method")
        graph = self._execution_graph_fixup.fixupQuanta(graph)

        # Detect if there is now a cycle created within the graph
        if graph.findCycle():
            raise MPGraphExecutorError("Updated execution graph has dependency cycle.")

        return graph

    def _executeQuantaInProcess(self, graph: QuantumGraph, report: Report) -> None:
        """Execute all Quanta in current process.

        Parameters
        ----------
        graph : `.QuantumGraph`
            `.QuantumGraph` that is to be executed.
        report : `Report`
            Object for reporting execution status.
        """
        successCount, totalCount = 0, len(graph)
        failedNodes: set[QuantumNode] = set()
        for qnode in graph:
            assert qnode.quantum.dataId is not None, "Quantum DataId cannot be None"
            task_node = qnode.task_node

            # Any failed inputs mean that the quantum has to be skipped.
            inputNodes = graph.determineInputsToQuantumNode(qnode)
            if inputNodes & failedNodes:
                _LOG.error(
                    "Upstream job failed for task <%s dataId=%s>, skipping this task.",
                    task_node.label,
                    qnode.quantum.dataId,
                )
                failedNodes.add(qnode)
                failed_quantum_report = QuantumReport(
                    quantumId=qnode.nodeId,
                    status=ExecutionStatus.SKIPPED,
                    dataId=qnode.quantum.dataId,
                    taskLabel=task_node.label,
                )
                report.quantaReports.append(failed_quantum_report)
                continue

            _LOG.debug("Executing %s", qnode)
            fail_exit_code: int | None = None
            try:
                # For some exception types we want to exit immediately with
                # exception-specific exit code, but we still want to start
                # debugger before exiting if debugging is enabled.
                try:
                    _, quantum_report = self._quantum_executor.execute(
                        task_node, qnode.quantum, quantum_id=qnode.nodeId
                    )
                    if quantum_report:
                        report.quantaReports.append(quantum_report)
                    successCount += 1
                except RepeatableQuantumError as exc:
                    if self._fail_fast:
                        _LOG.warning(
                            "Caught repeatable quantum error for %s (%s):",
                            task_node.label,
                            qnode.quantum.dataId,
                        )
                        _LOG.warning(exc, exc_info=True)
                        fail_exit_code = exc.EXIT_CODE
                    raise
                except InvalidQuantumError as exc:
                    _LOG.fatal("Invalid quantum error for %s (%s):", task_node.label, qnode.quantum.dataId)
                    _LOG.fatal(exc, exc_info=True)
                    fail_exit_code = exc.EXIT_CODE
                    raise
            except Exception as exc:
                quantum_report = QuantumReport.from_exception(
                    quantumId=qnode.nodeId,
                    exception=exc,
                    dataId=qnode.quantum.dataId,
                    taskLabel=task_node.label,
                )
                report.quantaReports.append(quantum_report)

                if self._pdb and sys.stdin.isatty() and sys.stdout.isatty():
                    _LOG.error(
                        "Task <%s dataId=%s> failed; dropping into pdb.",
                        task_node.label,
                        qnode.quantum.dataId,
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
                failedNodes.add(qnode)
                report.status = ExecutionStatus.FAILURE

                # If exception specified an exit code then just exit with that
                # code, otherwise crash if fail-fast option is enabled.
                if fail_exit_code is not None:
                    sys.exit(fail_exit_code)
                if self._fail_fast:
                    raise MPGraphExecutorError(
                        f"Task <{task_node.label} dataId={qnode.quantum.dataId}> failed."
                    ) from exc
                else:
                    # Note that there could be exception safety issues, which
                    # we presently ignore.
                    _LOG.error(
                        "Task <%s dataId=%s> failed; processing will continue for remaining tasks.",
                        task_node.label,
                        qnode.quantum.dataId,
                        exc_info=exc,
                    )

            _LOG.info(
                "Executed %d quanta successfully, %d failed and %d remain out of total %d quanta.",
                successCount,
                len(failedNodes),
                totalCount - successCount - len(failedNodes),
                totalCount,
            )

        # Raise an exception if there were any failures.
        if failedNodes:
            raise MPGraphExecutorError("One or more tasks failed during execution.")

    def _executeQuantaMP(self, graph: QuantumGraph, report: Report) -> None:
        """Execute all Quanta in separate processes.

        Parameters
        ----------
        graph : `.QuantumGraph`
            `.QuantumGraph` that is to be executed.
        report : `Report`
            Object for reporting execution status.
        """
        disable_implicit_threading()  # To prevent thread contention

        _LOG.debug("Using %r for multiprocessing start method", self._start_method)

        # re-pack input quantum data into jobs list
        jobs = _JobList(graph)

        # check that all tasks can run in sub-process
        for job in jobs.jobs:
            task_node = job.qnode.task_node
            if not task_node.task_class.canMultiprocess:
                raise MPGraphExecutorError(
                    f"Task {task_node.label!r} does not support multiprocessing; use single process"
                )

        finishedCount, failedCount = 0, 0
        while jobs.pending or jobs.running:
            _LOG.debug("#pendingJobs: %s", len(jobs.pending))
            _LOG.debug("#runningJobs: %s", len(jobs.running))

            # See if any jobs have finished
            for job in jobs.running:
                assert job.process is not None, "Process cannot be None"
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
                            jobs.setJobState(job, JobState.TIMED_OUT)
                        else:
                            report.status = ExecutionStatus.FAILURE
                            # failMessage() has to be called before cleanup()
                            message = job.failMessage()
                            jobs.setJobState(job, JobState.FAILED)

                        job.cleanup()
                        _LOG.debug("failed: %s", job)
                        if self._fail_fast or exitcode == InvalidQuantumError.EXIT_CODE:
                            # stop all running jobs
                            for stopJob in jobs.running:
                                if stopJob is not job:
                                    stopJob.stop()
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

            # Fail jobs whose inputs failed, this may need several iterations
            # if the order is not right, will be done in the next loop.
            if jobs.failedNodes:
                for job in jobs.pending:
                    jobInputNodes = graph.determineInputsToQuantumNode(job.qnode)
                    assert job.qnode.quantum.dataId is not None, "Quantum DataId cannot be None"
                    if jobInputNodes & jobs.failedNodes:
                        quantum_report = QuantumReport(
                            quantumId=job.qnode.nodeId,
                            status=ExecutionStatus.SKIPPED,
                            dataId=job.qnode.quantum.dataId,
                            taskLabel=job.qnode.task_node.label,
                        )
                        report.quantaReports.append(quantum_report)
                        jobs.setJobState(job, JobState.FAILED_DEP)
                        _LOG.error("Upstream job failed for task %s, skipping this task.", job)

            # see if we can start more jobs
            if len(jobs.running) < self._num_proc:
                for job in jobs.pending:
                    jobInputNodes = graph.determineInputsToQuantumNode(job.qnode)
                    if jobInputNodes <= jobs.finishedNodes:
                        # all dependencies have completed, can start new job
                        if len(jobs.running) < self._num_proc:
                            _LOG.debug("Submitting %s", job)
                            jobs.submit(job, self._quantum_executor, self._start_method)
                        if len(jobs.running) >= self._num_proc:
                            # Cannot start any more jobs, wait until something
                            # finishes.
                            break

            # Do cleanup for timed out jobs if necessary.
            jobs.cleanup()

            # Print progress message if something changed.
            newFinished, newFailed = len(jobs.finishedNodes), len(jobs.failedNodes)
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

        if jobs.failedNodes:
            # print list of failed jobs
            _LOG.error("Failed jobs:")
            for job in jobs.jobs:
                if job.state != JobState.FINISHED:
                    _LOG.error("  - %s: %s", job.state.name, job)

            # if any job failed raise an exception
            if jobs.failedNodes == jobs.timedOutNodes:
                raise MPTimeoutError("One or more tasks timed out during execution.")
            else:
                raise MPGraphExecutorError("One or more tasks failed or timed out during execution.")

    def getReport(self) -> Report | None:
        # Docstring inherited from base class
        if self._report is None:
            raise RuntimeError("getReport() called before execute()")
        return self._report
