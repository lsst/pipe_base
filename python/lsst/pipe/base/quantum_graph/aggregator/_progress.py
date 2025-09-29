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
import sys
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from types import TracebackType
from typing import Self, TypeVar

from lsst.utils.logging import TRACE, VERBOSE, LsstLogAdapter, PeriodicLogger, getLogger

from ._config import AggregatorConfig

_T = TypeVar("_T")


class Progress:
    """A helper class for the provenance scanner than handles reporting
    progress to the user.

    This includes both logging (including periodic logging) and optional
    progress bars.
    """

    def __init__(self, config: AggregatorConfig):
        self.log = getLogger("aggregate-graph")
        self.config = config
        self._periodic_log = PeriodicLogger(self.log, config.log_status_interval)
        self._n_scanned: int = 0
        self._n_ingested: int = 0
        self._n_written: int = 0
        self._n_quanta: int | None = None
        self.interactive = (
            config.interactive_status
            if config.interactive_status is not None
            else sys.stdout.isatty() and self._logging_to_stderr()
        )
        self._log_template = "%s quanta scanned, %s datasets ingested, %s provenance quanta written"

    def __enter__(self) -> Self:
        if self.interactive:
            logging.getLogger().handlers
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

    def _logging_to_stderr(self) -> bool:
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.StreamHandler):
                return True
        return False

    @contextmanager
    def quanta(self, n_quanta: int) -> Iterator[None]:
        self._n_quanta = n_quanta
        if self.interactive:
            from tqdm import tqdm
            from tqdm.contrib.logging import logging_redirect_tqdm

            self._scan_progress = tqdm(desc="Scanning", total=n_quanta, leave=True, unit="quanta")
            self._ingest_progress = tqdm(
                desc="Ingesting", total=n_quanta, leave=True, smoothing=0.1, unit="quanta"
            )
            if self.config.output_path is not None:
                self._write_progress = tqdm(desc="Writing", total=n_quanta, leave=True, unit="quanta")
            with logging_redirect_tqdm(tqdm_class=tqdm):
                yield
        else:
            yield

    @staticmethod
    def _format_fraction(n_done: int, n_total: int | None) -> str:
        if n_total is None:
            return str(n_done)
        else:
            return f"{n_done}/{n_total}"

    def log_status(self) -> None:
        self._periodic_log.log(
            self._log_template,
            self._format_fraction(self._n_scanned, self._n_quanta),
            self._format_fraction(self._n_ingested, self._n_quanta),
            self._format_fraction(self._n_written, self._n_quanta),
        )

    def report_scan(self) -> None:
        self._n_scanned += 1
        if self.interactive:
            self._scan_progress.update(1)
        else:
            self.log_status()

    def report_ingests(self, n_quanta: int) -> None:
        self._n_ingested += n_quanta
        if self.interactive:
            self._ingest_progress.update(n_quanta)
        else:
            self.log_status()

    def report_write(self) -> None:
        self._n_written += 1
        if self.interactive:
            self._write_progress.update()
        else:
            self.log_status()

    def finish_resuming(self) -> None:
        if self.interactive:
            # Reset the timing in the progress bars so quickly resuming doesn't
            # affect future timing estimates.
            self._scan_progress.unpause()
            self._ingest_progress.unpause()

    @contextmanager
    def wrap_iterable(
        self, iterable: Iterable[_T], desc: str, total: int | None = None, *, unit: str
    ) -> Iterator[Iterable[_T]]:
        """Wrap an iterable in a progress bar or periodic logging.

        Parameters
        ----------
        iterable : `~collections.abc.Iterable`
            Iterable to wrap.
        desc : `str`
            Description for the progress bar or log message.
        total : `int`, optional
            Total number of elements expected in the iterable.
        unit : `str`
            Type of thing being iterated over (human-readable).

        Returns
        -------
        wrapped : `~collections.abc.Iterable`
            A wrapped iterable that updates a progress bar or periodically logs
            when iterated over.
        """
        self.log.verbose("%s.", desc)
        if self.interactive:
            from tqdm import tqdm

            with tqdm(iterable, desc=desc, total=total, leave=False, unit=unit) as pbar:
                yield pbar
        else:
            if total is None:
                total = len(iterable)  # type: ignore[arg-type]

            def generator() -> Iterator[_T]:
                for n, item in enumerate(iterable):
                    yield item
                    self._periodic_log.log(f"{desc}: {n + 1}/{total} {unit}.")

            yield generator()


def make_worker_log(name: str, config: AggregatorConfig) -> LsstLogAdapter:
    base_log = logging.getLogger(f"aggregate-graph.{name}")
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
