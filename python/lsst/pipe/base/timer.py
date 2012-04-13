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
import contextlib
import functools
import resource
import time
import datetime

import lsst.pex.logging as pexLog

__all__ = ["logInfo", "timeMethod"]

def logPairs(obj, pairs, logLevel=pexLog.Log.DEBUG):
    """Log (name, value) pairs to obj.metadata and obj.log
    
    @param obj: an object with two attributes:
        * metadata: a daf_data PropertyList (or other object with add(name, value) method)
        * log: a pex_logging Log
    @param pairs: a collection of (name, value) pairs
    @param logLevel: one of the pexLog.Log level constants
    """
    strList = []
    for name, value in pairs:
        try:
            obj.metadata.add(name, value)
        except Exception, e:
            obj.log.log(obj.log.FATAL, "%s.metadata.add(name=%r, value=%r) failed with error=%s" % \
                (type(obj).__name__, name, value, e))
        strList.append("%s=%s" % (name, value))
    obj.log.log(logLevel, "; ".join(strList))

def logInfo(obj, prefix, logLevel=pexLog.Log.DEBUG):
    """Log timer information to obj.metadata and obj.log

    @param obj: an object with two attributes:
        * metadata: a daf_data PropertyList (or other object with add(name, value) method)
        * log: a pex_logging Log
    @param prefix: name prefix, e.g. <funcName>Start
    @param logLevel: one of the pexLog.Log level constants

            
    Logged items include:
    * Utc:  UTC date in ISO format (only in metadata since log entries have timestamps)
    * CpuTime: CPU time (seconds)
    * MaxRss: maximum resident set size
    All logged resource information is only for the current process; child processes are excluded
    """
    cpuTime = time.clock()
    utcStr = datetime.datetime.utcnow().isoformat()
    res = resource.getrusage(resource.RUSAGE_SELF)
    obj.metadata.add(name = prefix + "Utc", value = utcStr) # log messages already have timestamps
    logPairs(obj = obj,
        pairs = [
            (prefix + "CpuTime",  long(cpuTime)),
            (prefix + "UTime",    long(res.ru_utime)),
            (prefix + "STime",    long(res.ru_stime)),
        ],
        logLevel = logLevel,
    )
    logPairs(obj = obj,
        pairs = [
            (prefix + "MaxRss",   long(res.ru_maxrss)),
            (prefix + "MinFlt",   long(res.ru_minflt)),
            (prefix + "MajFlt",   long(res.ru_majflt)),
        ],
        logLevel = logLevel,
    )
    logPairs(obj = obj,
        pairs = [
            (prefix + "InBlock",  long(res.ru_inblock)),
            (prefix + "OuBlock",  long(res.ru_oublock)),
            (prefix + "NVCsw",    long(res.ru_nvcsw)),
            (prefix + "NIvCsw",   long(res.ru_nivcsw)),
        ],
        logLevel = logLevel,
    )

def timeMethod(func):
    """Decorator to measure duration of a task method
    
    To use:
    import lsst.pipe.base as pipeBase
    class FooTask(pipeBase.Task):
        ...
            
        @timeMethod
        def run(self, ...): # or any other instance method you want to time
            ...
    
    Writes various measures of time and possibly memory usage to the task's metadata;
    all items are prefixed with the function name.
    
    @warning This decorator only works with instance methods of Task (or any class with a metadata attribute
      that supports add(name, value)).
    """
    @functools.wraps(func)
    def wrapper(self, *args, **keyArgs):
        logInfo(obj = self, prefix = func.__name__ + "Start")
        try:
            res = func(self, *args, **keyArgs)
        finally:
            logInfo(obj = self, prefix = func.__name__ + "End")
        return res
    return wrapper
