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

__all__ = ("ProgressCounter", "ProgressManager", "make_worker_log")

import logging
import os
import time
from types import TracebackType
from typing import Any, Self

from lsst.utils.logging import TRACE, VERBOSE, LsstLogAdapter, PeriodicLogger, getLogger

from ._config import AggregatorConfig


class ProgressCounter:
    """A progress tracker for an individual aspect of the aggregation process.

    Parameters
    ----------
    parent : `ProgressManager`
        The parent progress manager object.
    description : `str`
        Human-readable description of this aspect.
    unit : `str`
        Unit (in plural form) for the items being counted.
    total : `int`, optional
        Expected total number of items.  May be set later.
    """

    def __init__(self, parent: ProgressManager, description: str, unit: str, total: int | None = None):
        self._parent = parent
        self.total = total
        self._description = description
        self._current = 0
        self._unit = unit
        self._bar: Any = None

    def update(self, n: int) -> None:
        """Report that ``n`` new items have been processed.

        Parameters
        ----------
        n : `int`
            Number of new items processed.
        """
        self._current += n
        if self._parent.interactive:
            if self._bar is None:
                if n == self.total:
                    return
                from tqdm import tqdm

                self._bar = tqdm(desc=self._description, total=self.total, leave=False, unit=f" {self._unit}")
            else:
                self._bar.update(n)
                if self._current == self.total:
                    self._bar.close()
        self._parent._log_status()

    def close(self) -> None:
        """Close the counter, guaranteeing that `update` will not be called
        again.
        """
        if self._bar is not None:
            self._bar.close()
            self._bar = None

    def append_log_terms(self, msg: list[str]) -> None:
        """Append a log message for this counter to a list if it is active.

        Parameters
        ----------
        msg : `list` [ `str` ]
            List of messages to concatenate into a single line and log
            together, to be modified in-place.
        """
        if self.total is not None and self._current > 0 and self._current < self.total:
            msg.append(f"{self._description} ({self._current} of {self.total} {self._unit})")


class ProgressManager:
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
        self.scans = ProgressCounter(self, "scanning", "quanta")
        self.writes = ProgressCounter(self, "writing", "quanta")
        self.quantum_ingests = ProgressCounter(self, "ingesting outputs", "quanta")
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

    @property
    def elapsed_time(self) -> float:
        """The time in seconds since the start of the aggregator."""
        return time.time() - self.start

    def _log_status(self) -> None:
        """Invoke the periodic logger with the current status."""
        log_terms: list[str] = []
        self.scans.append_log_terms(log_terms)
        self.writes.append_log_terms(log_terms)
        self.quantum_ingests.append_log_terms(log_terms)
        self._periodic_log.log("Status after %0.1fs: %s.", self.elapsed_time, "; ".join(log_terms))


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
