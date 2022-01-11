#
# LSST Data Management System
# Copyright 2008, 2009, 2010, 2011 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
"""Utilities for measuring execution time.
"""
__all__ = ["logInfo", "timeMethod"]

import logging
from typing import Any, Callable

import lsst.utils.timer
from deprecated.sphinx import deprecated


@deprecated(
    reason="logInfo has been replaced by lsst.utils.timer.logInfo. Will be removed after v25.",
    version="v24",
    category=FutureWarning,
)
def logInfo(obj, prefix, logLevel=logging.DEBUG, metadata=None, logger=None):
    """Log timer information to ``obj.metadata`` and ``obj.log``.

    Parameters
    ----------
    obj : `lsst.pipe.base.Task`-type or `None`
        A `~lsst.pipe.base.Task` or any other object with these two attributes:

        - ``metadata`` an instance of `~lsst.pipe.base.TaskMetadata` (or other
          object with ``add(name, value)`` method).
        - ``log`` an instance of `logging.Logger` or subclass.

        If `None`, at least one of ``metadata`` or ``logger`` should be passed
        or this function will do nothing.
    prefix : `str`
        Name prefix, the resulting entries are ``CpuTime``, etc.. For example
        timeMethod uses ``prefix = Start`` when the method begins and
        ``prefix = End`` when the method ends.
    logLevel : `int`, optional
        Log level (an `logging` level constant, such as `logging.DEBUG`).
    metadata : `lsst.pipe.base.TaskMetadata`, optional
        Metadata object to write entries to, overriding ``obj.metadata``.
    logger : `logging.Logger`
        Log object to write entries to, overriding ``obj.log``.

    Notes
    -----
    Logged items include:

    - ``Utc``: UTC date in ISO format (only in metadata since log entries have
      timestamps).
    - ``CpuTime``: System + User CPU time (seconds). This should only be used
        in differential measurements; the time reference point is undefined.
    - ``MaxRss``: maximum resident set size.

    All logged resource information is only for the current process; child
    processes are excluded.
    """
    return lsst.utils.timer.logInfo(obj, prefix, logLevel=logLevel, metadata=metadata, logger=logger)


@deprecated(
    reason="timeMethod has been replaced by lsst.utils.timer.timeMethod. Will be removed after v25.",
    version="v24",
    category=FutureWarning,
)
def timeMethod(*args: Any, **kwargs: Any) -> Callable:
    """Decorator to measure duration of a method.

    Notes
    -----
    This is a just a forwarding method for `lsst.utils.timer.timeMethod`. For
    documentation look at `lsst.utils.timer.timeMethod`.
    """
    return lsst.utils.timer.timeMethod(*args, **kwargs)
