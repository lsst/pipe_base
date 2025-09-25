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

__all__ = ("Activity", "BaseProgress", "SupervisorProgress", "WorkerProgress")

import csv
import enum
import logging
import os
import sys
import time
from abc import abstractmethod
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import IO, Any, Self, TypeVar

from lsst.utils.logging import PeriodicLogger, getLogger

from ._config import AggregatorConfig
from .utils import Timer

_T = TypeVar("_T")


class Activity(enum.Enum):
    _ = enum.auto()
    READING_METADATA = enum.auto()
    READING_LOGS = enum.auto()
    READING_PREDICTED = enum.auto()
    CHECKING_EXISTENCE = enum.auto()
    WRITING_TO_STORAGE = enum.auto()
    QUERYING_STORAGE = enum.auto()
    CHECKPOINTING = enum.auto()
    INGESTING = enum.auto()
    DELETING = enum.auto()
    SENDING_WORK = enum.auto()
    RECEIVING_WORK = enum.auto()
    RETURNING_WORK = enum.auto()
    FINISHING_WORK = enum.auto()
    CLOSING_WORKERS = enum.auto()

    def __str__(self) -> str:
        return self.name.lower().replace("_", " ")

    def as_postfix(self) -> str:
        return _ACTIVITY_POSTFIX_TEMPLATE.format(self)


_ACTIVITY_POSTFIX_TEMPLATE: str = "{:>" + str(max(len(str(m)) for m in Activity)) + "}"


class BaseProgress:
    def __init__(self, name: str, config: AggregatorConfig):
        self.log = getLogger(name)
        self.name = name
        self.config = config
        self._activity = Activity._
        self._timers = {m: Timer() for m in Activity}
        self._last_timer_write = time.time()
        self._timer_writer: csv.DictWriter | None = None
        self._timer_file: IO[str] | None = None

    def __enter__(self) -> Self:
        if self.config.timer_dir is not None:
            os.makedirs(self.config.timer_dir, exist_ok=True)
            self._timer_file = open(os.path.join(self.config.timer_dir, f"{self.name}.csv"), "w")
            self._timer_file.__enter__()
            self._timer_writer = csv.DictWriter(self._timer_file, fieldnames=[m.name for m in Activity])
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if self._timer_file is not None:
            self._timer_file.__exit__(exc_type, exc_value, traceback)

    @property
    def activity(self) -> Activity:
        return self._activity

    @contextmanager
    def working_on(self, activity: Activity) -> Iterator[None]:
        if activity is self._activity:
            yield
            return
        old_activity = self._activity
        if activity is not Activity._:
            self.log.debug("Started %s.", activity)
        self._activity = activity
        with self._timers[activity].activate():
            yield
        self._activity = old_activity
        if old_activity is not Activity._:
            self.log.debug("Resumed %s.", old_activity)
        else:
            self.log.debug("Finished %s.", activity)

    @abstractmethod
    def wrap_iterable(
        self, iterable: Iterable[_T], desc: str, total: int | None = None, *, unit: str
    ) -> AbstractContextManager[Iterable[_T]]:
        raise NotImplementedError()

    def periodically_write_times(self) -> None:
        if (
            self._timer_writer is not None
            and time.time() - self._last_timer_write > self.config.timer_interval
        ):
            self._timer_writer.writerow({m.name: self._timers[m].total for m in Activity})


class SupervisorProgress(BaseProgress):
    """A helper class for the provenance scanner than handles reporting
    progress to the user.

    This includes both logging (including periodic logging) and optional
    progress bars.
    """

    def __init__(self, config: AggregatorConfig):
        super().__init__("scanner", config)
        self._periodic_log = PeriodicLogger(self.log, config.log_status_interval)
        self._activity = Activity._
        self._n_scanned: int = 0
        self._n_ingested: int = 0
        self._n_written: int = 0
        self._n_quanta: int | None = None
        self.interactive = (
            config.interactive_status
            if config.interactive_status is not None
            else sys.stdout.isatty() and not self.log.isEnabledFor(logging.DEBUG)
        )
        if self.interactive:
            from tqdm.contrib.logging import logging_redirect_tqdm

            self._logging_redirect = logging_redirect_tqdm()
        self._log_template = "%s quanta scanned, %s datasets ingested, %s provenance quanta written"

    @contextmanager
    def quanta(self, n_quanta: int) -> Iterator[None]:
        self._n_quanta = n_quanta
        if self.interactive:
            from tqdm import tqdm
            from tqdm.contrib.logging import logging_redirect_tqdm

            self._scan_progress = tqdm(desc="Scanning", total=n_quanta, leave=True, unit="quanta")
            self._ingest_progress = tqdm(desc="Ingesting", total=n_quanta, leave=True, unit="quanta")
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
        if self.interactive:
            self._ingest_progress.update(n_quanta)
        else:
            self.log_status()

    def report_write(self) -> None:
        if self.interactive:
            self._write_progress.update(1)
        else:
            self.log_status()

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


class WorkerProgress(BaseProgress):
    def __init__(self, name: str, config: AggregatorConfig):
        super().__init__(name, config)
        if config.worker_log_dir is not None:
            os.makedirs(config.worker_log_dir, exist_ok=True)
            self.log.setLevel(logging.DEBUG)
            logging.basicConfig(
                filename=os.path.join(config.worker_log_dir, f"{name}.log"),
                format="%(levelname)s %(asctime)s.%(msecs)03d %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        else:
            self.log.addHandler(logging.NullHandler())

    @contextmanager
    def wrap_iterable(
        self, iterable: Iterable[_T], desc: str, total: int | None = None, *, unit: str
    ) -> Iterator[Iterable[_T]]:
        self.log.verbose("%s.", desc)
        yield iterable
