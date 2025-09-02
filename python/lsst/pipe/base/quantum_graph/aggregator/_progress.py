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

__all__ = ("Progress", "make_worker_log")

import logging
import os
import time
from types import TracebackType
from typing import Self

from lsst.utils.logging import TRACE, VERBOSE, LsstLogAdapter, PeriodicLogger, getLogger

from ._config import AggregatorConfig


class Progress:
    """A helper class for the provenance aggregator that handles reporting
    progress to the user.

    This includes both logging (including periodic logging) and optional
    progress bars.

    Parameters
    ----------
    log : `lsst.utils.logging.LsstLogAdapter`
        LSST-customized logger.
    config : `AggregatorConfig`
        Configuration for the aggregator.

    Notes
    -----
    This class is a context manager in order to manage the redirection of
    logging when progress bars for interactive display are in use.  The context
    manager does nothing otherwise.
    """

    def __init__(self, log: LsstLogAdapter, config: AggregatorConfig):
        self.start = time.time()
        self.log = log
        self.config = config
        self._periodic_log = PeriodicLogger(self.log, config.log_status_interval)
        self._n_scanned: int = 0
        self._n_ingested: int = 0
        self._n_written: int = 0
        self._n_quanta: int | None = None
        self.interactive = config.interactive_status

    def __enter__(self) -> Self:
        if self.interactive:
            from tqdm.contrib.logging import logging_redirect_tqdm

            self._logging_redirect = logging_redirect_tqdm()
            self._logging_redirect.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        if self.interactive:
            self._logging_redirect.__exit__(exc_type, exc_value, traceback)
        return None

    def set_n_quanta(self, n_quanta: int) -> None:
        """Set the total number of quanta.

        Parameters
        ----------
        n_quanta : `int`
            Total number of quanta, including special "init" quanta.

        Notes
        -----
        This method must be called before any of the ``report_*`` methods or
        `finish_resuming`.
        """
        self._n_quanta = n_quanta
        if self.interactive:
            from tqdm import tqdm

            self._scan_progress = tqdm(desc="Scanning", total=n_quanta, leave=False, unit="quanta")
            self._ingest_progress = tqdm(
                desc="Ingesting", total=n_quanta, leave=False, smoothing=0.1, unit="quanta"
            )
            if self.config.output_path is not None:
                self._write_progress = tqdm(desc="Writing", total=n_quanta, leave=False, unit="quanta")

    @property
    def elapsed_time(self) -> float:
        """The time in seconds since the start of the aggregator."""
        return time.time() - self.start

    def _log_status(self) -> None:
        """Invoke the periodic logger with the current status."""
        self._periodic_log.log(
            "%s quanta scanned, %s quantum outputs ingested, "
            "%s provenance quanta written (of %s) after %0.1fs.",
            self._n_scanned,
            self._n_ingested,
            self._n_written,
            self._n_quanta,
            self.elapsed_time,
        )

    def report_scan(self) -> None:
        """Report that a quantum was scanned."""
        self._n_scanned += 1
        if self.interactive:
            self._scan_progress.update(1)
        else:
            self._log_status()

    def finish_scans(self) -> None:
        """Report that all scanning is done."""
        if self.interactive:
            self._scan_progress.close()

    def report_ingests(self, n_quanta: int) -> None:
        """Report that ingests for multiple quanta were completed.

        Parameters
        ----------
        n_quanta : `int`
            Number of quanta whose outputs were ingested.
        """
        self._n_ingested += n_quanta
        if self.interactive:
            self._ingest_progress.update(n_quanta)
        else:
            self._log_status()

    def finish_ingests(self) -> None:
        """Report that all ingests are done."""
        if self.interactive:
            self._ingest_progress.close()

    def report_write(self) -> None:
        """Report that a quantum's provenance was written."""
        self._n_written += 1
        if self.interactive:
            self._write_progress.update()
        else:
            self._log_status()

    def finish_writes(self) -> None:
        """Report that all writes are done."""
        if self.interactive:
            self._write_progress.close()

    def finish_resuming(self) -> None:
        """Report that we're done resuming from past saved scans."""
        if self.interactive:
            # Reset the timing in the progress bars so quickly resuming doesn't
            # affect future timing estimates.
            self._scan_progress.unpause()
            self._ingest_progress.unpause()


def make_worker_log(name: str, config: AggregatorConfig) -> LsstLogAdapter:
    """Make a logger for a worker.

    Parameters
    ----------
    name : `str`
        Name of the worker, to be used as part of the name for the logger.
    config : `AggregatorConfig`
        Configuration for the aggregator.
    """
    base_log = logging.getLogger(f"lsst.pipe.base.quantum_graph.aggregator.{name}")
    base_log.propagate = False
    log = getLogger(logger=base_log)
    if config.worker_log_dir is not None:
        os.makedirs(config.worker_log_dir, exist_ok=True)
        match config.worker_log_level.upper():
            case "VERBOSE":
                log.setLevel(VERBOSE)
            case "TRACE":
                log.setLevel(TRACE)
            case std:
                log.setLevel(getattr(logging, std))
        handler = logging.FileHandler(os.path.join(config.worker_log_dir, f"{name}.log"))
        handler.setFormatter(
            logging.Formatter("%(levelname)s %(asctime)s.%(msecs)03d %(message)s", "%Y-%m-%dT%H:%M:%S")
        )
        log.addHandler(handler)
    else:
        log.addHandler(logging.NullHandler())
    return log
