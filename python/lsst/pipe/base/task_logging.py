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

import logging
from deprecated.sphinx import deprecated

try:
    import lsst.log as lsstLog
except ModuleNotFoundError:
    lsstLog = None

# log level for trace (verbose debug)
TRACE = 5


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


class LSSTCompatibleLogger(logging.Logger):

    @property
    @deprecated(reason="Use logging.DEBUG. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def DEBUG(self):
        return logging.DEBUG

    @property
    @deprecated(reason="Use logging.INFO. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def INFO(self):
        return logging.INFO

    @property
    @deprecated(reason="Use logging.WARNING. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def WARNING(self):
        return logging.WARNING

    @property
    @deprecated(reason="Use logging.WARNING. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def WARN(self):
        return logging.WARNING

    @property
    @deprecated(reason="Use logging.ERROR. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def ERROR(self):
        return logging.ERROR

    @property
    @deprecated(reason="Use logging.CRITICAL. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def FATAL(self):
        return logging.CRITICAL

    def __init__(self, name):
        super(LSSTCompatibleLogger, self).__init__(name)
        self._lsstLogHandler = None

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def configure_prop(self, properties):
        if lsstLog is None:
            return
        lsstLog.getLogger(self.name).configure_prop(properties)
        if self._lsstLogHandler is None:
            self._lsstLogHandler = lsstLog.LogHandler()
            # Forward all Python logging to lsstLog
            self.addHandler(self._lsstLogHandler)

    @deprecated(reason="Use Python Logger compatible name attribute. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def isDebugEnabled(self):
        return self.isEnabledFor(logging.DEBUG)

    @deprecated(reason="Use Python Logger compatible name attribute. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def getName(self):
        return self.name

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def getLevel(self):
        return self.getEffectiveLevel()

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def trace(self, fmt, *args):
        self.log(TRACE, fmt, *args)

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def tracef(self, fmt, *args, **kwargs):
        self.log(5, _F(fmt, *args, **kwargs))

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def debugf(self, fmt, *args, **kwargs):
        self.log(logging.DEBUG, _F(fmt, *args, **kwargs))

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def infof(self, fmt, *args, **kwargs):
        self.log(logging.INFO, _F(fmt, *args, **kwargs))

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def warnf(self, fmt, *args, **kwargs):
        self.log(logging.WARNING, _F(fmt, *args, **kwargs))

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def errorf(self, fmt, *args, **kwargs):
        self.log(logging.ERROR, _F(fmt, *args, **kwargs))

    @deprecated(reason="Use Python Logger compatible method. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def fatalf(self, fmt, *args, **kwargs):
        self.log(logging.CRITICAL, _F(fmt, *args, **kwargs))


def getLogger(name=None, lsstCompatible=True):
    if not lsstCompatible:
        return logging.getLogger(name)
    logging_class = logging.getLoggerClass()  # store the current logger factory for later
    logging._acquireLock()  # use the global logging lock for thread safety
    try:
        logging.setLoggerClass(LSSTCompatibleLogger)  # temporarily change the logger factory
        logger = logging.getLogger(name)
        logging.setLoggerClass(logging_class)  # be nice, revert the logger factory change
        return logger
    finally:
        logging._releaseLock()
