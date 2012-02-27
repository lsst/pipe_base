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
import contextlib

import lsstDebug

import lsst.afw.display.ds9 as ds9
import lsst.pex.logging as pexLog
import lsst.daf.base as dafBase
from .timer import logInfo

__all__ = ["Task"]

class Task(object):
    """A data processing task
    
    Useful attributes include:
    * log: an lsst.pex.logging.Log
    * config: task-specific configuration; an instance of ConfigClass (see below)
    * metadata: an lsst.daf.data.PropertyList for collecting task-specific metadata,
        e.g. data quality and performance metrics. This is data that is only meant to be
        persisted, never to be used by the task.
    
    Subclasses should have a method named "run" to perform the main data processing. Details:
    * run should process the minimum reasonable amount of data, typically a single CCD.
      Iteration, if desired, is performed by a caller of the run method.
    * If "run" can persist or unpersist data: "run" should accept a butler data reference
      (or a collection of data references, if appropriate, e.g. coaddition).
      The default level for data references is "sensor" (aka "ccd" for some cameras).
    * If "run" can persist data then the task's config should offer a flag to disable persistence.
    
    Subclasses must also have an attribute ConfigClass that is a subclass of pexConfig.Config
    which configures this task.
    """
    def __init__(self, config=None, name=None, parentTask=None, log=None):
        """Create a Task
        
        @param config: config to configure this task (task-specific subclass of pexConfig.Config);
            If parentTask specified then defaults to parentTask.config.<name>
        @param name: brief name of task; defaults to the class name if parentTask=None
        @param parentTask: the parent task of this subtask, if any.
            If None (a top-level task) then you must specify config and name is ignored.
            If not None (a subtask) then you must specify name
        @param log: pexLog log; if None then a log is created using the full task name.
        
        @raise RuntimeError if parentTask is None and config is None.
        @raise RuntimeError if parentTask is not None and name is None.
        """
        self.metadata = dafBase.PropertyList()

        if parentTask != None:
            if name == None:
                raise RuntimeError("name is required for a subtask")
            self._name = name
            self._fullName = parentTask._computeFullName(name)
            if config == None:
                config = getattr(parentTask.config, name)
            self._taskDict = parentTask._taskDict
        else:
            if name is None:
                name = type(self).__name__
            self._name = name
            self._fullName = self._name
            if config == None:
                raise RuntimeError("config is required for a top-level task")
            self._taskDict = dict()
  
        self.config = config
        if log == None:
            log = pexLog.Log(pexLog.getDefaultLog(), self._fullName)
        self.log = log
        self._display = lsstDebug.Info(self.__module__).display
        self._taskDict[self._fullName] = self
    
    def getFullName(self):
        """Return the full name of the task.

        Subtasks use dotted notation. Thus subtask "foo" of subtask "bar" of the top level task
        has the full name "<top>.foo.bar" where <top> is the name of the top-level task
        (which defaults to the name of its class).
        """
        return self._fullname

    def getName(self):
        """Return the brief name of the task (the last field in the full name).
        """
        return self._name
    
    def getTaskDict(self):
        """Return a dictionary of tasks as a shallow copy.
        
        Keys are full task names. Values are Task objects.
        The dictionary includes the top-level task and all subtasks, sub-subtasks, etc.
        """
        return self._taskDict.copy()
    
    def makeSubtask(self, name, taskClass, **keyArgs):
        """Create a subtask as a new instance self.<name>
        
        @param name: brief name of subtask
        @param taskClass: class to use for the task
        """
        subtask = taskClass(name=name, parentTask=self, **keyArgs)
        setattr(self, name, subtask)

    @contextlib.contextmanager
    def timer(self, name, logLevel = pexLog.Log.INFO):
        """Context manager to log performance data for an arbitrary block of code
        
        @param[in] name: name of code being timed;
            data will be logged using item name: <name>Start<item> and <name>End<item>
        @param logLevel: one of the pexLog.Log level constants
        
        To use:
        with self.timer(name):
            ...code to time...

        See timer.logInfo for the information logged            
        """
        logInfo(obj = self, prefix = name + "Start", logLevel = logLevel)
        try:
            yield
        finally:
            logInfo(obj = self, prefix = name + "End",   logLevel = logLevel)
    
    def display(self, name, exposure=None, sources=[], matches=None,
                ctypes=[ds9.GREEN, ds9.YELLOW, ds9.RED, ds9.BLUE,], ptypes=["o", "+", "x", "*",],
                sizes=[4,],
                pause=None, prompt=None):
        """Display image and/or sources

        @param name Name of product to display
        @param exposure Exposure to display, or None
        @param sources list of sets of Sources to display, or []
        @param matches Matches to display, or None
        @param ctypes Array of colours to use on ds9 (will be reused as necessary)
        @param ptypes Array of ptypes to use on ds9 (will be reused as necessary)
        @param sizes Array of sizes to use on ds9 (will be reused as necessary)
        @param pause Pause execution?
        @param prompt Prompt for user

N.b. the ds9 arrays (ctypes etc.) are used for the lists of the list of lists-of-sources (so the
first ctype is used for the first SourceSet); the matches are interpreted as an extra pair of SourceSets
but the sizes are doubled
        """
        if not self._display or not self._display.has_key(name) or self._display < 0 or \
               self._display in (False, None) or self._display[name] in (False, None):
            return

        if isinstance(self._display, int):
            frame = self._display
        elif isinstance(self._display, dict):
            frame = self._display[name]
        else:
            frame = 1

        if exposure:
            if isinstance(exposure, list):
                if len(exposure) == 1:
                    exposure = exposure[0]
                else:
                    exposure = ipIsr.assembleCcd(exposure, pipUtil.getCcd(exposure[0]))
            mi = exposure.getMaskedImage()
            ds9.mtv(exposure, frame=frame, title=name)
            x0, y0 = mi.getX0(), mi.getY0()
        else:
            x0, y0 = 0, 0

        try:
            sources[0][0]
        except IndexError:              # empty list
            pass
        except TypeError:               # not a list of sets of sources
            sources = [sources]
            
        with ds9.Buffering():
            for i, ss in enumerate(sources):
                ctype = ctypes[i%len(ctypes)]
                ptype = ptypes[i%len(ptypes)]
                size = sizes[i%len(sizes)]

                for source in ss:
                    xc, yc = source.getXAstrom() - x0, source.getYAstrom() - y0
                    ds9.dot(ptype, xc, yc, size=size, frame=frame, ctype=ctype)
                    #try:
                    #    mag = 25-2.5*math.log10(source.getPsfFlux())
                    #    if mag > 15: continue
                    #except: continue
                    #ds9.dot("%.1f" % mag, xc, yc, frame=frame, ctype="red")
            nSourceSet = i + 1

        if matches:
            with ds9.Buffering():
                for first, second, d in matches:
                    i = nSourceSet      # counter for ptypes/ctypes

                    x1, y1 = first.getXAstrom() - x0, first.getYAstrom() - y0

                    ctype = ctypes[i%len(ctypes)]
                    ptype = ptypes[i%len(ptypes)]
                    size  = 2*sizes[i%len(sizes)]
                    ds9.dot(ptype, x1, y1, size=8, frame=frame, ctype=ctype)
                    i += 1

                    ctype = ctypes[i%len(ctypes)]
                    ptype = ptypes[i%len(ptypes)]
                    size  = 2*sizes[i%len(sizes)]
                    x2, y2 = second.getXAstrom() - x0, second.getYAstrom() - y0
                    ds9.dot(ptype, x2, y2, size=8, frame=frame, ctype=ctype)
                    i += 1

        if pause:
            if prompt is None:
                prompt = "%s: Enter or c to continue [chp]: " % name
            while True:
                ans = raw_input(prompt).lower()
                if ans in ("", "c",):
                    break
                if ans in ("p",):
                    import pdb; pdb.set_trace()
                elif ans in ("h", ):
                    print "h[elp] c[ontinue] p[db]"
    
    def _computeFullName(self, name):
        """Compute the full name of a subtask or metadata item, given its brief name
        
        @param name: brief name of subtask or metadata item
        """
        return "%s.%s" % (self._fullName, name)
