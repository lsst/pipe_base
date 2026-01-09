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

__all__ = ("LogOnClose",)

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import TypeVar

from lsst.utils.logging import VERBOSE

_T = TypeVar("_T")


class LogOnClose:
    """A factory for context manager wrappers that emit a log message when
    they are closed.

    Parameters
    ----------
    log_func : `~collections.abc.Callable` [ `int`, `str` ]
        Callable that takes an integer log level and a string message and emits
        a log message.  Note that placeholder formatting is not supported.
    """

    def __init__(self, log_func: Callable[[int, str], None]):
        self.log_func = log_func

    def wrap(
        self,
        cm: AbstractContextManager[_T],
        msg: str,
        level: int = VERBOSE,
    ) -> AbstractContextManager[_T]:
        """Wrap a context manager to log when it is exited.

        Parameters
        ----------
        cm : `contextlib.AbstractContextManager`
            Context manager to wrap.
        msg : `str`
            Log message.
        level : `int`, optional
            Log level.
        """

        @contextmanager
        def wrapper() -> Iterator[_T]:
            with cm as result:
                yield result
                self.log_func(level, msg)

        return wrapper()
