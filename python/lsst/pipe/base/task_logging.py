# This file is part of task_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = ("TRACE", "VERBOSE", "getLogger")

import logging
from logging import LoggerAdapter
from deprecated.sphinx import deprecated
from contextlib import contextmanager

try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None

# log level for trace (verbose debug).
TRACE = 5
logging.addLevelName(TRACE, "TRACE")

# Verbose logging is midway between INFO and DEBUG.
VERBOSE = (logging.INFO + logging.DEBUG) // 2
logging.addLevelName(VERBOSE, "VERBOSE")


class _F:
    """
    Format, supporting str.format() syntax
    see using-custom-message-objects in logging-cookbook.html
    """
    def __init__(self, fmt, /, *args, **kwargs):
        self.fmt = fmt
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return self.fmt.format(*self.args, **self.kwargs)


class TaskLogAdapter(LoggerAdapter):
    """A special logging adapter to provide log features for `Task`.

    This is easier to use for a unified interface to task logging
    than calling `logging.setLoggerClass`.  The main simplification
    is that this will work with the root logger as well as loggers
    created for tasks.
    """

    # Store logging constants in the class for convenience. This is not
    # something supported by Python loggers but can simplify some
    # logic if the logger is available.
    CRITICAL = logging.CRITICAL
    ERROR = logging.ERROR
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING

    # Python supports these but prefers they are not used.
    FATAL = logging.FATAL
    WARN = logging.WARN

    # These are specific to Tasks
    TRACE = TRACE
    VERBOSE = VERBOSE

    @contextmanager
    def temporary_log_level(self, level):
        """A context manager that temporarily sets the level of this logger.

        Parameters
        ----------
        level : `int`
            The new temporary log level.
        """
        old = self.level
        self.setLevel(level)
        try:
            yield
        finally:
            self.setLevel(old)

    @property
    def level(self):
        return self.logger.level

    def getChild(self, name):
        return getLogger(name=name, logger=self.logger)

    @deprecated(reason="Use Python Logger compatible isEnabledFor Will be removed after v23.",
                version="v23", category=FutureWarning)
    def isDebugEnabled(self):
        return self.isEnabledFor(self.DEBUG)

    @deprecated(reason="Use Python Logger compatible 'name' attribute. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def getName(self):
        return self.name

    @deprecated(reason="Use Python Logger compatible .level property. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def getLevel(self):
        return self.logger.level

    @deprecated(reason="Use Python Logger compatible critical(). Will be removed after v23.",
                version="v23", category=FutureWarning)
    def fatal(self, msg, *args, **kwargs):
        # stacklevel=5 accounts for the deprecation and the forwarding
        # of LoggerAdapter.
        return self.critical(msg, *args, **kwargs, stacklevel=5)

    def verbose(self, fmt, *args, **kwargs):
        """Issue a VERBOSE level log message.

        Arguments are as for `logging.info`.
        ``VERBOSE`` is between ``DEBUG`` and ``INFO``.
        """
        # There is no other way to achieve this other than a special logger
        # method.
        # Stacklevel is passed in so that the correct line is reported
        # in the log record and not this line. 3 is this method,
        # 2 is the level from `self.log` and 1 is the log infrastructure
        # itself.
        self.log(VERBOSE, fmt, *args, stacklevel=3, **kwargs)

    def trace(self, fmt, *args):
        """Issue a TRACE level log message.

        Arguments are as for `logging.info`.
        ``TRACE`` is lower than ``DEBUG``.
        """
        # There is no other way to achieve this other than a special logger
        # method. For stacklevel discussion see `verbose()`.
        self.log(TRACE, fmt, *args, stacklevel=3)

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def tracef(self, fmt, *args, **kwargs):
        # Stacklevel is 4 to account for the deprecation wrapper
        self.log(5, _F(fmt, *args, **kwargs), stacklevel=4)

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def debugf(self, fmt, *args, **kwargs):
        self.log(logging.DEBUG, _F(fmt, *args, **kwargs), stacklevel=4)

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def infof(self, fmt, *args, **kwargs):
        self.log(logging.INFO, _F(fmt, *args, **kwargs), stacklevel=4)

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def warnf(self, fmt, *args, **kwargs):
        self.log(logging.WARNING, _F(fmt, *args, **kwargs), stacklevel=4)

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def errorf(self, fmt, *args, **kwargs):
        self.log(logging.ERROR, _F(fmt, *args, **kwargs), stacklevel=4)

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def fatalf(self, fmt, *args, **kwargs):
        self.log(logging.CRITICAL, _F(fmt, *args, **kwargs), stacklevel=4)

    def setLevel(self, level):
        """Set the level for the logger, trapping lsst.log values.

        Parameters
        ----------
        level : `int`
            The level to use. If the level looks too big to be a Python
            logging level it is assumed to be a lsst.log level.
        """
        if level > logging.CRITICAL:
            self.logger.warning("Attempting to set level to %d -- looks like an lsst.log level so scaling it"
                                " accordingly.", level)
            level //= 1000

        self.logger.setLevel(level)

    @property
    def handlers(self):
        return self.logger.handlers

    def addHandler(self, handler):
        self.logger.addHandler(handler)

    def removeHandler(self, handler):
        self.logger.removeHandler(handler)


def getLogger(name=None, logger=None):
    """Get a logger compatible with LSST usage.

    Parameters
    ----------
    name : `str`, optional
        Name of the logger. Root logger if `None`.
    logger : `logging.Logger`
        If given the logger is converted to the relevant logger class.
        If ``name`` is given the logger is assumed to be a child of the
        supplied logger.

    Returns
    -------
    logger : `TaskLogAdapter`
        The relevant logger.
    """
    if not logger:
        logger = logging.getLogger(name)
    elif name:
        logger = logger.getChild(name)
    return TaskLogAdapter(logger, {})
