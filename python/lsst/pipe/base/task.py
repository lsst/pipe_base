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

try:
    import lsst.afw.display.ds9 as ds9
except ImportError:
    # afw is above pipe_base in the class hierarchy, so we have to cope without it.
    # We'll warn on first use that it's unavailable, and then quietly swallow all
    # references to it.
    class Ds9Warning(object):
        """A null pattern which warns once that ds9 is not available"""
        def __init__(self):
            super(Ds9Warning, self).__setattr__("_warned", False)
        def __getattr__(self, name):
            if name in ("GREEN", "YELLOW", "RED", "BLUE"):
                # These are used in the Task.display definition, so don't warn when we use them
                return self
            if not super(Ds9Warning, self).__getattribute__("_warned"):
                print "WARNING: afw's ds9 is not available"
                super(Ds9Warning, self).__setattr__("_warned", True)
            return self
        def __setattr__(self, name, value):
            return self
        def __call__(self, *args, **kwargs):
            return self
    ds9 = Ds9Warning()

import lsst.pex.logging as pexLog
import lsst.daf.base as dafBase
from .timer import logInfo

__all__ = ["Task", "TaskError"]

class TaskError(Exception):
    """Use to report errors for which a traceback is not useful.
    
    Examples of such errors:
    - processCcd is asked to run detection, but not calibration, and no calexp is found.
    - coadd finds no valid images in the specified patch.
    """
    pass

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
    which configures this task. Subclasses should also have an attribute _DefaultName:
    the default name if there is no parent task. _DefaultName is required for subclasses of CmdLineTask
    and recommended for subclasses of Task because it simplifies construction (e.g. for unit tests).
    
    Pipeline tasks intended to be run as top-level tasks should be subclasses of CmdLineTask, not Task.
    """
    def __init__(self, config=None, name=None, parentTask=None, log=None):
        """Create a Task
        
        @param config: config to configure this task (task-specific subclass of pexConfig.Config);
            If parentTask specified then defaults to parentTask.config.<name>
            If parentTask is None then defaults to self.ConfigClass()
        @param name: brief name of task; defaults to the class name if parentTask=None
        @param parentTask: the parent task of this subtask, if any.
            If None (a top-level task) then you must specify config and name is ignored.
            If not None (a subtask) then you must specify name
        @param log: pexLog log; if None then the default is used;
            in either case a copy is made using the full task name.
        
        @raise RuntimeError if parentTask is None and config is None.
        @raise RuntimeError if parentTask is not None and name is None.
        """
        self.metadata = dafBase.PropertyList()

        if parentTask != None:
            if name is None:
                raise RuntimeError("name is required for a subtask")
            self._name = name
            self._fullName = parentTask._computeFullName(name)
            if config == None:
                config = getattr(parentTask.config, name)
            self._taskDict = parentTask._taskDict
        else:
            if name is None:
                name = getattr(self, "_DefaultName", None)
                if name is None:
                    raise RuntimeError("name is required for a task unless it has attribute _DefaultName")
                name = self._DefaultName
            self._name = name
            self._fullName = self._name
            if config == None:
                config = self.ConfigClass()
            self._taskDict = dict()
  
        self.config = config
        if log == None:
            log = pexLog.getDefaultLog()
        self.log = pexLog.Log(log, self._fullName)
        self._display = lsstDebug.Info(self.__module__).display
        self._taskDict[self._fullName] = self

    def emptyMetadata(self):
        """Empty metadata from this Task and all sub-Tasks."""
        for subtask in self._taskDict.itervalues():
            subtask.metadata = dafBase.PropertyList()

    def getSchemaCatalogs(self):
        """Return the schemas of catalogs generated by this task.

        The actual return value should be a dict of empty catalog objects that can be used to save
        all the schemas of all catalogs generated by this task.  Dict keys should be the butler
        dataset of the catalog, and values should be the correct lsst.afw.table catalog class.

        Subtasks should not be called recursively; instead use Task.getAllSchemaCatalogs() to return
        all schemas within a task hierarchy.

        This method may be called at any time after the Task is constructed, which means that
        all task schemas should be computed at construction time, *not* when data is actually
        processed.  This reflects the philosophy that the schema should not depend on the data.

        Returning catalogs rather than just schemas allows us to save e.g. slots for
        SourceCatalog as well.
        """
        return {}

    def getAllSchemaCatalogs(self):
        """Call getSchemaCatalogs() on self and all subtasks, combining the results into a single dict.
        """
        schemaDict = self.getSchemaCatalogs()
        for subtask in self._taskDict.itervalues():
            schemaDict.update(subtask.getSchemaCatalogs())
        return schemaDict

    def getFullMetadata(self):
        """Get metadata for all tasks
        
        @return metadata: an lsst.daf.base.PropertySet containing full task name: metadata
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        fullMetadata = dafBase.PropertySet()
        for fullName, task in self.getTaskDict().iteritems():
            fullMetadata.set(fullName.replace(".", ":"), task.metadata)
        return fullMetadata
    
    def getFullName(self):
        """Return the full name of the task.

        Subtasks use dotted notation. Thus subtask "foo" of subtask "bar" of the top level task
        has the full name "<top>.foo.bar" where <top> is the name of the top-level task
        (which defaults to the name of its class).
        """
        return self._fullName

    def getName(self):
        """Return the brief name of the task (the last field in the full name).
        """
        return self._name
    
    def getTaskDict(self):
        """Return a dictionary of all tasks as a shallow copy.
        
        @return taskDict: a dict containing full task name: task object
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        return self._taskDict.copy()
    
    def makeSubtask(self, name, **keyArgs):
        """Create a subtask as a new instance self.<name>
        
        The subtask must be defined by self.config.<name>, an instance of pex_config ConfigurableField.
        
        @param name: brief name of subtask
        @param **kwargs: extra keyword arguments used to construct the task.
            The following arguments are automatically provided and cannot be overridden:
            config, name and parentTask.
        """
        configurableField = getattr(self.config, name, None)
        if configurableField is None:
            raise KeyError("%s's config does not have field %r" % (self.getFullName, name))
        subtask = configurableField.apply(name=name, parentTask=self, **keyArgs)
        setattr(self, name, subtask)

    @contextlib.contextmanager
    def timer(self, name, logLevel = pexLog.Log.DEBUG):
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
    
    def display(self, name, exposure=None, sources=(), matches=None,
                ctypes=(ds9.GREEN, ds9.YELLOW, ds9.RED, ds9.BLUE,), ptypes=("o", "+", "x", "*",),
                sizes=(4,),
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
        if not self._display or not self._display.has_key(name) or \
                self._display < 0 or self._display in (False, None) or \
                self._display[name] < 0 or self._display[name] in (False, None):
            return

        if isinstance(self._display, int):
            frame = self._display
        elif isinstance(self._display, dict):
            frame = self._display[name]
        else:
            frame = 1

        if exposure:
            if isinstance(exposure, list):
                raise runtimeError("exposure may not be a list")
            mi = exposure.getMaskedImage()
            ds9.mtv(exposure, frame=frame, title=name)
            x0, y0 = mi.getX0(), mi.getY0()
        else:
            x0, y0 = 0, 0

        try:
            sources[0][0]
        except IndexError:              # empty list
            pass
        except (TypeError, NotImplementedError): # not a list of sets of sources
            sources = [sources]
            
        with ds9.Buffering():
            i = 0
            for i, ss in enumerate(sources):
                ctype = ctypes[i%len(ctypes)]
                ptype = ptypes[i%len(ptypes)]
                size = sizes[i%len(sizes)]

                for source in ss:
                    xc, yc = source.getX() - x0, source.getY() - y0
                    ds9.dot(ptype, xc, yc, size=size, frame=frame, ctype=ctype)
                    #try:
                    #    mag = 25-2.5*math.log10(source.getPsfFlux())
                    #    if mag > 15: continue
                    #except: continue
                    #ds9.dot("%.1f" % mag, xc, yc, frame=frame, ctype="red")

        if matches:
            with ds9.Buffering():
                for first, second, d in matches:
                    i = len(sources)    # counter for ptypes/ctypes

                    x1, y1 = first.getX() - x0, first.getY() - y0

                    ctype = ctypes[i%len(ctypes)]
                    ptype = ptypes[i%len(ptypes)]
                    size  = 2*sizes[i%len(sizes)]
                    ds9.dot(ptype, x1, y1, size=8, frame=frame, ctype=ctype)
                    i += 1

                    ctype = ctypes[i%len(ctypes)]
                    ptype = ptypes[i%len(ptypes)]
                    size  = 2*sizes[i%len(sizes)]
                    x2, y2 = second.getX() - x0, second.getY() - y0
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
