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
import time
import contextlib

__all__ = ["timeMethod"]

def timeMethod(func):
    """Decorator to measure duration of a task method
    
    To use:
    import lsst.pipe.base as pipeBase
    class FooTask(pipeBase.Task):
        ...
            
        @timeMethod
        def run(self, ...): # or any other instance method you want to time
            ...
    
    Writes the duration (in CPU seconds) to the task's metadata using name <func name>Duration
    
    @warning This decorator only works with instance methods of Task (or any class with a metadata attribute
      that supports add(name, value)).
    """
    itemName = func.__name__ + "Duration"
    def wrapper(self, *args, **keyArgs):
        t1 = time.clock()
        res = func(self, *args, **keyArgs)
        duration = time.clock() - t1
        self.metadata.add(itemName, duration)
        return res
    return wrapper
