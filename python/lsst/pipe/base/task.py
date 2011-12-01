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
import lsstDebug
import lsst.pex.logging as pexLog
import lsst.daf.base as dafBase

__all__ = ["Task"]

class Task(object):
    """A data processing task
    
    Useful attributes include:
    * log: an lsst.pex.logging.Log; one log is shared among all tasks and subtasks
    * policy: task-specific Policy
    * metadata: an lsst.daf.data.PropertyList for collecting task-specific metadata,
        e.g. data quality and performance metrics. This is data that is only meant to be
        persisted, never to be used for data processing.
    
    Subclasses should create one or both of the following methods:
    * run: performs the main data processing. Takes appropriate task-specific arguments
    and returns a pipeBase.Struct with a named field for each item of output
    * runButler: un-persists input data using a data butler (provided as the first argument
    of runButler), processes the data and persists the results using the butler.
    """
    def __init__(self, policy=None, name=None, parentTask=None, log=None):
        """Create a Task
        
        @param policy: policy to configure this task
        @param name: brief name of task; ignored if parentTask=None
        @param parentTask: the parent task of this subtask, if any.
            If None then you should specify butler and policy.
            If not None then its fields are used as the defaults for all other arguments,
            though policy defaults to parentTask.policy.getPolicy(name).
        @param log: pexLog log
        @param metadata: a Python dict for metadata
        """
        self.metadata = dafBase.PropertyList()

        if parentTask != None:
            self._name = name
            self._fullName = parentTask._computeFullName(name)
            if policy == None:
                policy = parentTask.policy.getPolicy(name)
            if log == None:
                log = parentTask.log
            self._taskDict = parentTask._taskDict
        else:
            self._name = "main"
            self._fullName = self._name
            if log == None:
                log = pexLog.Log()
            self._taskDict = dict()
  
        self.policy = policy
        self.log = log
        self._display = lsstDebug.Info(__name__).display
        self._taskDict[self._fullName] = self
    
    def _computeFullName(self, name):
        """Compute the full name of a subtask or metadata item, given its brief name
        
        @param name: brief name of subtask or metadata item
        """
        return "%s.%s" % (self._fullName, name)
    
    def getFullName(self):
        """Return the full name of the task.

        The top level task is named "main" and subtasks use dotted notation.
        Thus subtask "foo" of subtask "bar" of the top level task has the full name "main.foo.bar".
        """
        return self._fullname

    def getName(self):
        """Return the brief name of the task.
        
        The top level task is named "main". Subtasks are named as specified in makeSubtask.
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
    
    def display(self, name, exposure=None, sources=[], matches=None, pause=None, prompt=None):
        """Display image and/or sources

        @param name Name of product to display
        @param exposure Exposure to display, or None
        @param sources list of sets of Sources to display, or []
        @param matches Matches to display, or None
        @param pause Pause execution?
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
            ds9.mtv(mi, frame=frame, title=name)
            x0, y0 = mi.getX0(), mi.getY0()
        else:
            x0, y0 = 0, 0

        try:
            sources[0][0]
        except IndexError:              # empty list
            pass
        except TypeError:               # not a list of sets of sources
            sources = [sources]
            
        ctypes = [ds9.GREEN, ds9.RED, ds9.BLUE]
        for i, ss in enumerate(sources):
            try:
                ds9.buffer()
            except AttributeError:
                ds9.cmdBuffer.pushSize()

            for source in ss:
                xc, yc = source.getXAstrom() - x0, source.getYAstrom() - y0
                ds9.dot("o", xc, yc, size=4, frame=frame, ctype=ctypes[i%len(ctypes)])
                #try:
                #    mag = 25-2.5*math.log10(source.getPsfFlux())
                #    if mag > 15: continue
                #except: continue
                #ds9.dot("%.1f" % mag, xc, yc, frame=frame, ctype="red")
            try:
                ds9.buffer(False)
            except AttributeError:
                ds9.cmdBuffer.popSize()

        if matches:
            try:
                ds9.buffer()
            except AttributeError:
                ds9.cmdBuffer.pushSize()

            for match in matches:
                first = match.first
                x1, y1 = first.getXAstrom() - x0, first.getYAstrom() - y0
                ds9.dot("+", x1, y1, size=8, frame=frame, ctype="yellow")
                second = match.second
                x2, y2 = second.getXAstrom() - x0, second.getYAstrom() - y0
                ds9.dot("x", x2, y2, size=8, frame=frame, ctype="red")
            try:
                ds9.buffer(False)
            except AttributeError:
                ds9.cmdBuffer.popSize()

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

    def timer(self, name):
        """Context manager to time the duration of an arbitrary block of code
        
        @param[in] name: name with which the data will be stored in metadata
        
        To use:
        self.timer(name):
            ...code to time...
        
        Writes the duration (in CPU seconds) to metadata using the specified name.
        """
        t1 = time.clock()
        yield
        duration = time.clock() - t1
        self.metadata.add(name, duration)
