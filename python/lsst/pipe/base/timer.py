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
from __future__ import absolute_import, division
import functools
import resource
import time
import datetime

from lsst.log import Log, log

__all__ = ["logInfo", "timeMethod"]


def logPairs(obj, pairs, logLevel=Log.DEBUG):
    """Log ``(name, value)`` pairs to ``obj.metadata`` and ``obj.log``

    Parameters
    ----------
    obj : `lsst.pipe.base.Task`-type
        A `~lsst.pipe.base.Task` or any other object with these two attributes:

        - ``metadata`` an instance of `lsst.daf.base.PropertyList`` (or other object with
          ``add(name, value)`` method).
        - ``log`` an instance of `lsst.log.Log`.

    pairs : sequence
        A sequence of ``(name, value)`` pairs.
    logLevel : optional
        Log level (an `lsst.log` level constant, such as `lsst.log.Log.DEBUG`).
    """
    strList = []
    for name, value in pairs:
        try:
            obj.metadata.add(name, value)
        except Exception as e:
            obj.log.fatal("%s.metadata.add(name=%r, value=%r) failed with error=%s",
                          type(obj).__name__, name, value, e)
        strList.append("%s=%s" % (name, value))
    log(obj.log.getName(), logLevel, "; ".join(strList))


def logInfo(obj, prefix, logLevel=Log.DEBUG):
    """Log timer information to ``obj.metadata`` and ``obj.log``.

    Parameters
    ----------
    obj : `lsst.pipe.base.Task`-type
        A `~lsst.pipe.base.Task` or any other object with these two attributes:

        - ``metadata`` an instance of `lsst.daf.base.PropertyList`` (or other object with
          ``add(name, value)`` method).
        - ``log`` an instance of `lsst.log.Log`.

    prefix
        Name prefix, the resulting entries are ``CpuTime``, etc.. For example timeMethod uses
        ``prefix = Start`` when the method begins and ``prefix = End`` when the method ends.
    logLevel : optional
        Log level (an `lsst.log` level constant, such as `lsst.log.Log.DEBUG`).

    Notes
    -----
    Logged items include:

    - ``Utc``: UTC date in ISO format (only in metadata since log entries have timestamps).
    - ``CpuTime``: CPU time (seconds).
    - ``MaxRss``: maximum resident set size.

    All logged resource information is only for the current process; child processes are excluded.
    """
    cpuTime = time.clock()
    utcStr = datetime.datetime.utcnow().isoformat()
    res = resource.getrusage(resource.RUSAGE_SELF)
    obj.metadata.add(name=prefix + "Utc", value=utcStr)  # log messages already have timestamps
    logPairs(obj=obj,
             pairs=[
                 (prefix + "CpuTime", cpuTime),
                 (prefix + "UserTime", res.ru_utime),
                 (prefix + "SystemTime", res.ru_stime),
                 (prefix + "MaxResidentSetSize", int(res.ru_maxrss)),
                 (prefix + "MinorPageFaults", int(res.ru_minflt)),
                 (prefix + "MajorPageFaults", int(res.ru_majflt)),
                 (prefix + "BlockInputs", int(res.ru_inblock)),
                 (prefix + "BlockOutputs", int(res.ru_oublock)),
                 (prefix + "VoluntaryContextSwitches", int(res.ru_nvcsw)),
                 (prefix + "InvoluntaryContextSwitches", int(res.ru_nivcsw)),
             ],
             logLevel=logLevel,
             )


def timeMethod(func):
    """Decorator to measure duration of a task method.

    Parameters
    ----------
    func
        The method to wrap.

    Notes
    -----
    Writes various measures of time and possibly memory usage to the task's metadata; all items are prefixed
    with the function name.

    .. warning::

       This decorator only works with instance methods of Task, or any class with these attributes:

       - ``metadata``: an instance of `lsst.daf.base.PropertyList` (or other object with
         ``add(name, value)`` method).
       - ``log``: an instance of `lsst.log.Log`.

    Examples
    --------
    To use::

        import lsst.pipe.base as pipeBase
        class FooTask(pipeBase.Task):
            pass

            @pipeBase.timeMethod
            def run(self, ...): # or any other instance method you want to time
                pass
    """

    @functools.wraps(func)
    def wrapper(self, *args, **keyArgs):
        logInfo(obj=self, prefix=func.__name__ + "Start")
        try:
            res = func(self, *args, **keyArgs)
        finally:
            logInfo(obj=self, prefix=func.__name__ + "End")
        return res
    return wrapper
