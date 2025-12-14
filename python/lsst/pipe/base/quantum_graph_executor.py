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

__all__ = ["QuantumExecutionResult", "QuantumExecutor", "QuantumGraphExecutor"]

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Self

from lsst.daf.butler import Quantum

from .quantum_reports import QuantumReport, Report

if TYPE_CHECKING:
    import uuid

    from lsst.daf.butler.logging import ButlerLogRecords

    from ._task_metadata import TaskMetadata
    from .graph import QuantumGraph
    from .pipeline_graph import TaskNode
    from .quantum_graph import PredictedQuantumGraph


class QuantumExecutionResult(tuple[Quantum, QuantumReport | None]):
    """A result struct that captures information about a single quantum's
    execution.

    Parameters
    ----------
    quantum : `lsst.daf.butler.Quantum`
        Quantum that was executed.
    report : `.quantum_reports.QuantumReport`
        Report with basic information about the execution.
    task_metadata : `TaskMetadata`, optional
        Metadata saved by the task and executor during execution.
    skipped_existing : `bool`, optional
        If `True`, this quantum was not executed because it appeared to have
        already been executed successfully.
    adjusted_no_work : `bool`, optional
        If `True`, this quantum was not executed because the
        `PipelineTaskConnections.adjustQuanta` hook raised `NoWorkFound`.

    Notes
    -----
    For backwards compatibility, this class is a two-element tuple that allows
    the ``quantum`` and ``report`` attributes to be unpacked.  Additional
    regular attributes may be added by executors (but the tuple must remain
    only two elements to enable the current unpacking interface).
    """

    def __new__(
        cls,
        quantum: Quantum,
        report: QuantumReport | None,
        *,
        task_metadata: TaskMetadata | None = None,
        skipped_existing: bool | None = None,
        adjusted_no_work: bool | None = None,
    ) -> Self:
        return super().__new__(cls, (quantum, report))

    # We need to define both __init__ and __new__ because tuple inheritance
    # requires __new__ and numpydoc requires __init__.

    def __init__(
        self,
        quantum: Quantum,
        report: QuantumReport | None,
        *,
        task_metadata: TaskMetadata | None = None,
        skipped_existing: bool | None = None,
        adjusted_no_work: bool | None = None,
    ):
        self._task_metadata = task_metadata
        self._skipped_existing = skipped_existing
        self._adjusted_no_work = adjusted_no_work

    @property
    def quantum(self) -> Quantum:
        """The quantum actually executed."""
        return self[0]

    @property
    def report(self) -> QuantumReport | None:
        """Structure describing the status of the execution of a quantum.

        This is `None` if the implementation does not support this feature.
        """
        return self[1]

    @property
    def task_metadata(self) -> TaskMetadata | None:
        """Metadata saved by the task and executor during execution."""
        return self._task_metadata

    @property
    def skipped_existing(self) -> bool | None:
        """If `True`, this quantum was not executed because it appeared to have
        already been executed successfully.
        """
        return self._skipped_existing

    @property
    def adjusted_no_work(self) -> bool | None:
        """If `True`, this quantum was not executed because the
        `PipelineTaskConnections.adjustQuanta` hook raised `NoWorkFound`.
        """
        return self._adjusted_no_work


class QuantumExecutor(ABC):
    """Class which abstracts execution of a single Quantum.

    In general implementation should not depend on execution model and
    execution should always happen in-process. Main reason for existence
    of this class is to provide do-nothing implementation that can be used
    in the unit tests.
    """

    @abstractmethod
    def execute(
        self,
        task_node: TaskNode,
        /,
        quantum: Quantum,
        quantum_id: uuid.UUID | None = None,
        *,
        log_records: ButlerLogRecords | None = None,
    ) -> QuantumExecutionResult:
        """Execute single quantum.

        Parameters
        ----------
        task_node : `~.pipeline_graph.TaskNode`
            Task definition structure.
        quantum : `~lsst.daf.butler.Quantum`
            Quantum for this execution.
        quantum_id : `uuid.UUID` or `None`, optional
            The ID of the quantum to be executed.
        log_records : `lsst.daf.butler.ButlerLogRecords`, optional
            Container that should be used to store logs in memory before
            writing them to the butler.  This disables streaming log (since
            we'd have to store them in memory anyway), but it permits the
            caller to prepend logs to be stored in the butler and allows task
            logs to be inspected by the caller after execution is complete.

        Returns
        -------
        result : `QuantumExecutionResult`
            Result struct.  May also be unpacked as a 2-tuple (see type
            documentation).

        Notes
        -----
        Any exception raised by the task or code that wraps task execution is
        propagated to the caller of this method.
        """
        raise NotImplementedError()


class QuantumGraphExecutor(ABC):
    """Class which abstracts QuantumGraph execution.

    Any specific execution model is implemented in sub-class by overriding
    the `execute` method.
    """

    @abstractmethod
    def execute(self, graph: QuantumGraph | PredictedQuantumGraph) -> None:
        """Execute whole graph.

        Implementation of this method depends on particular execution model
        and it has to be provided by a subclass. Execution model determines
        what happens here; it can be either actual running of the task or,
        for example, generation of the scripts for delayed batch execution.

        Parameters
        ----------
        graph : `.QuantumGraph` or `.quantum_graph.PredictedQuantumGraph`
            Execution graph.
        """
        raise NotImplementedError()

    def getReport(self) -> Report | None:
        """Return execution report from last call to `execute`.

        Returns
        -------
        report : `~.quantum_reports.Report`, optional
            Structure describing the status of the execution of a quantum
            graph. `None` is returned if implementation does not support
            this feature.

        Raises
        ------
        RuntimeError
            Raised if this method is called before `execute`.
        """
        return None
