# This file is part of ctrl_mpexec.
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

__all__ = ["LogCapture"]

import logging
import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from logging import FileHandler

from lsst.daf.butler import Butler, FileDataset, LimitedButler, Quantum
from lsst.daf.butler.logging import ButlerLogRecordHandler, ButlerLogRecords, ButlerMDC, JsonLogFormatter
from lsst.pipe.base import InvalidQuantumError
from lsst.pipe.base.pipeline_graph import TaskNode

_LOG = logging.getLogger(__name__)


class _LogCaptureFlag:
    """Simple flag to enable/disable log-to-butler saving."""

    store: bool = True


class LogCapture:
    """Class handling capture of logging messages and their export to butler.

    Parameters
    ----------
    butler : `~lsst.daf.butler.LimitedButler`
        Data butler with limited API.
    full_butler : `~lsst.daf.butler.Butler` or `None`
        Data butler with full API, or `None` if full Butler is not available.
        If not none, then this must be the same instance as ``butler``.
    """

    stream_json_logs = True
    """If True each log record is written to a temporary file and ingested
    when quantum completes. If False the records are accumulated in memory
    and stored in butler on quantum completion. If full butler is not available
    then temporary file is not used."""

    def __init__(
        self,
        butler: LimitedButler,
        full_butler: Butler | None,
    ):
        self.butler = butler
        self.full_butler = full_butler

    @classmethod
    def from_limited(cls, butler: LimitedButler) -> LogCapture:
        return cls(butler, None)

    @classmethod
    def from_full(cls, butler: Butler) -> LogCapture:
        return cls(butler, butler)

    @contextmanager
    def capture_logging(self, task_node: TaskNode, /, quantum: Quantum) -> Iterator[_LogCaptureFlag]:
        """Configure logging system to capture logs for execution of this task.

        Parameters
        ----------
        task_node : `~lsst.pipe.base.pipeline_graph.TaskNode`
            The task definition.
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.

        Notes
        -----
        Expected to be used as a context manager to ensure that logging
        records are inserted into the butler once the quantum has been
        executed:

        .. code-block:: py

           with self.capture_logging(task_node, quantum):
               # Run quantum and capture logs.

        Ths method can also setup logging to attach task- or
        quantum-specific information to log messages. Potentially this can
        take into account some info from task configuration as well.
        """
        # include quantum dataId and task label into MDC
        mdc = {"LABEL": task_node.label, "RUN": ""}
        if quantum.dataId:
            mdc["LABEL"] += f":{quantum.dataId}"
        if self.full_butler is not None:
            mdc["RUN"] = self.full_butler.run or ""
        ctx = _LogCaptureFlag()
        log_dataset_name = (
            task_node.log_output.dataset_type_name if task_node.log_output is not None else None
        )

        # Add a handler to the root logger to capture execution log output.
        if log_dataset_name is not None:
            # Either accumulate into ButlerLogRecords or stream JSON records to
            # file and ingest that (ingest is possible only with full butler).
            if self.stream_json_logs and self.full_butler is not None:
                # Create the log file in a temporary directory rather than
                # creating a temporary file. This is necessary because
                # temporary files are created with restrictive permissions
                # and during file ingest these permissions persist in the
                # datastore. Using a temp directory allows us to create
                # a file with umask default permissions.
                tmpdir = tempfile.mkdtemp(prefix="butler-temp-logs-")

                # Construct a file to receive the log records and "touch" it.
                log_file = os.path.join(tmpdir, f"butler-log-{task_node.label}.json")
                with open(log_file, "w"):
                    pass
                log_handler_file = FileHandler(log_file)
                log_handler_file.setFormatter(JsonLogFormatter())
                logging.getLogger().addHandler(log_handler_file)

                try:
                    with ButlerMDC.set_mdc(mdc):
                        yield ctx
                finally:
                    # Ensure that the logs are stored in butler.
                    logging.getLogger().removeHandler(log_handler_file)
                    log_handler_file.close()
                    if ctx.store:
                        self._ingest_log_records(quantum, log_dataset_name, log_file)
                    shutil.rmtree(tmpdir, ignore_errors=True)

            else:
                log_handler_memory = ButlerLogRecordHandler()
                logging.getLogger().addHandler(log_handler_memory)

                try:
                    with ButlerMDC.set_mdc(mdc):
                        yield ctx
                finally:
                    # Ensure that the logs are stored in butler.
                    logging.getLogger().removeHandler(log_handler_memory)
                    if ctx.store:
                        self._store_log_records(quantum, log_dataset_name, log_handler_memory)
                    log_handler_memory.records.clear()

        else:
            with ButlerMDC.set_mdc(mdc):
                yield ctx

    def _store_log_records(
        self, quantum: Quantum, dataset_type: str, log_handler: ButlerLogRecordHandler
    ) -> None:
        # DatasetRef has to be in the Quantum outputs, can lookup by name.
        try:
            [ref] = quantum.outputs[dataset_type]
        except LookupError as exc:
            raise InvalidQuantumError(
                f"Quantum outputs is missing log output dataset type {dataset_type};"
                " this could happen due to inconsistent options between QuantumGraph generation"
                " and execution"
            ) from exc

        self.butler.put(log_handler.records, ref)

    def _ingest_log_records(self, quantum: Quantum, dataset_type: str, filename: str) -> None:
        # If we are logging to an external file we must always try to
        # close it.
        assert self.full_butler is not None, "Expected to have full butler for ingest"
        ingested = False
        try:
            # DatasetRef has to be in the Quantum outputs, can lookup by name.
            try:
                [ref] = quantum.outputs[dataset_type]
            except LookupError as exc:
                raise InvalidQuantumError(
                    f"Quantum outputs is missing log output dataset type {dataset_type};"
                    " this could happen due to inconsistent options between QuantumGraph generation"
                    " and execution"
                ) from exc

            # Need to ingest this file directly into butler.
            dataset = FileDataset(path=filename, refs=ref)
            try:
                self.full_butler.ingest(dataset, transfer="move")
                ingested = True
            except NotImplementedError:
                # Some datastores can't receive files (e.g. in-memory datastore
                # when testing), we store empty list for those just to have a
                # dataset. Alternative is to read the file as a
                # ButlerLogRecords object and put it.
                _LOG.info(
                    "Log records could not be stored in this butler because the"
                    " datastore can not ingest files, empty record list is stored instead."
                )
                records = ButlerLogRecords.from_records([])
                self.full_butler.put(records, ref)
        finally:
            # remove file if it is not ingested
            if not ingested:
                with suppress(OSError):
                    os.remove(filename)
