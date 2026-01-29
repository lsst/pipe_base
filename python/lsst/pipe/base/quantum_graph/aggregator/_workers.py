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

__all__ = ("Event", "Queue", "SpawnWorkerFactory", "ThreadWorkerFactory", "Worker", "WorkerFactory")

import multiprocessing.context
import multiprocessing.synchronize
import queue
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

_TINY_TIMEOUT = 0.01

type Queue[T] = "queue.Queue[T]" | "multiprocessing.Queue[T]"

type Event = threading.Event | multiprocessing.synchronize.Event


class Worker(ABC):
    """A thin abstraction over threading.Thread and multiprocessing.Process."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the worker, as assigned at creation."""
        raise NotImplementedError()

    @abstractmethod
    def join(self, timeout: float | None = None) -> None:
        """Wait for the worker to finish.

        Parameters
        ----------
        timeout : `float`, optional
            How long to wait in seconds.  If the timeout is exceeded,
            `is_alive` can be used to see whether the worker finished or not.
        """
        raise NotImplementedError()

    @abstractmethod
    def is_alive(self) -> bool:
        """Return whether the worker is still running."""
        raise NotImplementedError()


class WorkerFactory(ABC):
    """A simple abstract interface that can be implemented by both threading
    and multiprocessing.
    """

    @abstractmethod
    def make_queue(self) -> Queue[Any]:
        """Make an empty queue that can be used to pass objects between
        workers created by this factory.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_event(self) -> Event:
        """Make an event that can be used to communicate a boolean state change
        to workers created by this factory.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        """Make a worker that runs the given callable.

        Parameters
        ----------
        target : `~collections.abc.Callable`
            A callable to invoke on the worker.
        args : `tuple`
            Positional arguments to pass to the callable.
        name : `str`, optional
            Human-readable name for the worker.

        Returns
        -------
        worker : `Worker`
            Process or thread that is already running the given callable.
        """
        raise NotImplementedError()


class _ThreadWorker(Worker):
    """An implementation of `Worker` backed by the `threading` module."""

    def __init__(self, thread: threading.Thread):
        super().__init__()
        self._thread = thread

    @property
    def name(self) -> str:
        return self._thread.name

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()


class ThreadWorkerFactory(WorkerFactory):
    """An implementation of `WorkerFactory` backed by the `threading`
    module.
    """

    def make_queue(self) -> Queue[Any]:
        return queue.Queue()

    def make_event(self) -> Event:
        return threading.Event()

    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        thread = threading.Thread(target=target, args=args, name=name)
        thread.start()
        return _ThreadWorker(thread)


class _ProcessWorker(Worker):
    """An implementation of `Worker` backed by the `multiprocessing` module."""

    def __init__(self, process: multiprocessing.context.SpawnProcess):
        super().__init__()
        self._process = process

    @property
    def name(self) -> str:
        return self._process.name

    def join(self, timeout: float | None = None) -> None:
        self._process.join(timeout=timeout)

    def is_alive(self) -> bool:
        return self._process.is_alive()


class SpawnWorkerFactory(WorkerFactory):
    """An implementation of `WorkerFactory` backed by the `multiprocessing`
    module, with new processes started by spawning.
    """

    def __init__(self) -> None:
        self._ctx = multiprocessing.get_context("spawn")

    def make_queue(self) -> Queue[Any]:
        return self._ctx.Queue()

    def make_event(self) -> Event:
        return self._ctx.Event()

    def make_worker(
        self, target: Callable[..., None], args: tuple[Any, ...], name: str | None = None
    ) -> Worker:
        process = self._ctx.Process(target=target, args=args, name=name)
        process.start()
        return _ProcessWorker(process)
