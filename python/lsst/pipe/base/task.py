from __future__ import absolute_import, division
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
from lsst.pex.config import ConfigurableField

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

## default ds9 colors for Task.display's ctypes argument
_DefaultDS9CTypes = (ds9.GREEN, ds9.YELLOW, ds9.RED, ds9.BLUE) 

## default ds9 point types for Task.display's ptypes argument
_DefaultDS9PTypes = ("o", "+", "x", "*")

class TaskError(Exception):
    """!Use to report errors for which a traceback is not useful.
    
    Examples of such errors:
    - processCcd is asked to run detection, but not calibration, and no calexp is found.
    - coadd finds no valid images in the specified patch.
    """
    pass

class Task(object):
    """!Base class for data processing tasks

    See \ref pipeBase_introduction "pipe_base introduction" to learn what tasks are,
    and \ref pipeTasks_writeTask "how to write a task" for more information about writing tasks.
    If the second link is broken (as it will be before the documentation is cross-linked)
    then look at the main page of pipe_tasks documentation for a link.
    
    Useful attributes include:
    * log: an lsst.pex.logging.Log
    * config: task-specific configuration; an instance of ConfigClass (see below)
    * metadata: an lsst.daf.base.PropertyList for collecting task-specific metadata,
        e.g. data quality and performance metrics. This is data that is only meant to be
        persisted, never to be used by the task.
    
    Subclasses typically have a method named "run" to perform the main data processing. Details:
    * run should process the minimum reasonable amount of data, typically a single CCD.
      Iteration, if desired, is performed by a caller of the run method. This is good design and allows
      multiprocessing without the run method having to support it directly.
    * If "run" can persist or unpersist data:
        * "run" should accept a butler data reference (or a collection of data references, if appropriate,
            e.g. coaddition).
        * There should be a way to run the task without persisting data. Typically the run method returns all
            data, even if it is persisted, and the task's config method offers a flag to disable persistence.

    \deprecated Tasks other than cmdLineTask.CmdLineTask%s should \em not accept a blob such as a butler data
    reference.  How we will handle data references is still TBD, so don't make changes yet! RHL 2014-06-27
    
    Subclasses must also have an attribute ConfigClass that is a subclass of lsst.pex.config.Config
    which configures the task. Subclasses should also have an attribute _DefaultName:
    the default name if there is no parent task. _DefaultName is required for subclasses of
    \ref cmdLineTask.CmdLineTask "CmdLineTask" and recommended for subclasses of Task because it simplifies
    construction (e.g. for unit tests).
    
    Tasks intended to be run from the command line should be subclasses of \ref cmdLineTask.CmdLineTask
    "CmdLineTask", not Task.
    """
    def __init__(self, config=None, name=None, parentTask=None, log=None):
        """!Create a Task
        
        @param[in] config       configuration for this task (an instance of self.ConfigClass,
            which is a task-specific subclass of lsst.pex.config.Config), or None. If None:
            - If parentTask specified then defaults to parentTask.config.\<name>
            - If parentTask is None then defaults to self.ConfigClass()
        @param[in] name         brief name of task, or None; if None then defaults to self._DefaultName
        @param[in] parentTask   the parent task of this subtask, if any.
            - If None (a top-level task) then you must specify config and name is ignored.
            - If not None (a subtask) then you must specify name
        @param[in] log          pexLog log; if None then the default is used;
            in either case a copy is made using the full task name.
        
        @throw RuntimeError if parentTask is None and config is None.
        @throw RuntimeError if parentTask is not None and name is None.
        @throw RuntimeError if name is None and _DefaultName does not exist.
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
        """!Empty (clear) the metadata for this Task and all sub-Tasks."""
        for subtask in self._taskDict.itervalues():
            subtask.metadata = dafBase.PropertyList()

    def getSchemaCatalogs(self):
        """!Return the schemas generated by this task

        @warning Subclasses the use schemas must override this method. The default implemenation
        returns an empty dict.

        @return a dict of butler dataset type: empty catalog (an instance of the appropriate
            lsst.afw.table Catalog type) for this task

        This method may be called at any time after the Task is constructed, which means that
        all task schemas should be computed at construction time, __not__ when data is actually
        processed. This reflects the philosophy that the schema should not depend on the data.

        Returning catalogs rather than just schemas allows us to save e.g. slots for SourceCatalog as well.

        See also Task.getAllSchemaCatalogs
        """
        return {}

    def getAllSchemaCatalogs(self):
        """!Call getSchemaCatalogs() on all tasks in the hiearchy, combining the results into a single dict.

        @return a dict of butler dataset type: empty catalog (an instance of the appropriate
            lsst.afw.table Catalog type) for all tasks in the hierarchy, from the top-level task down
            through all subtasks

        This method may be called on any task in the hierarchy; it will return the same answer, regardless.

        The default implementation should always suffice. If your subtask uses schemas the override
        Task.getSchemaCatalogs, not this method.
        """
        schemaDict = self.getSchemaCatalogs()
        for subtask in self._taskDict.itervalues():
            schemaDict.update(subtask.getSchemaCatalogs())
        return schemaDict

    def getFullMetadata(self):
        """!Get metadata for all tasks

        The returned metadata includes timing information (if \@timer.timeMethod is used)
        and any metadata set by the task. The name of each item consists of the full task name
        with "." replaced by ":", followed by "." and the name of the item, e.g.:
            topLeveltTaskName:subtaskName:subsubtaskName.itemName
        using ":" in the full task name disambiguates the rare situation that a task has a subtask
        and a metadata item with the same name.
        
        @return metadata: an lsst.daf.base.PropertySet containing full task name: metadata
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        fullMetadata = dafBase.PropertySet()
        for fullName, task in self.getTaskDict().iteritems():
            fullMetadata.set(fullName.replace(".", ":"), task.metadata)
        return fullMetadata
    
    def getFullName(self):
        """!Return the task name as a hierarchical name including parent task names

        The full name consists of the name of the parent task and each subtask separated by periods.
        For example:
        - The full name of top-level task "top" is simply "top"
        - The full name of subtask "sub" of top-level task "top" is "top.sub"
        - The full name of subtask "sub2" of subtask "sub" of top-level task "top" is "top.sub.sub2".
        """
        return self._fullName

    def getName(self):
        """!Return the name of the task

        See getFullName to get a hierarchical name including parent task names
        """
        return self._name
    
    def getTaskDict(self):
        """!Return a dictionary of all tasks as a shallow copy.
        
        @return taskDict: a dict containing full task name: task object
            for the top-level task and all subtasks, sub-subtasks, etc.
        """
        return self._taskDict.copy()
    
    def makeSubtask(self, name, **keyArgs):
        """!Create a subtask as a new instance self.\<name>
        
        The subtask must be defined by self.config.\<name>, an instance of pex_config ConfigurableField.
        
        @param name         brief name of subtask
        @param **keyArgs    extra keyword arguments used to construct the task.
            The following arguments are automatically provided and cannot be overridden:
            "config" and "parentTask".
        """
        configurableField = getattr(self.config, name, None)
        if configurableField is None:
            raise KeyError("%s's config does not have field %r" % (self.getFullName, name))
        subtask = configurableField.apply(name=name, parentTask=self, **keyArgs)
        setattr(self, name, subtask)

    @contextlib.contextmanager
    def timer(self, name, logLevel = pexLog.Log.DEBUG):
        """!Context manager to log performance data for an arbitrary block of code
        
        @param[in] name         name of code being timed;
            data will be logged using item name: \<name>Start\<item> and \<name>End\<item>
        @param[in] logLevel     one of the lsst.pex.logging.Log level constants
        
        Example of use:
        \code
        with self.timer("someCodeToTime"):
            ...code to time...
        \endcode

        See timer.logInfo for the information logged            
        """
        logInfo(obj = self, prefix = name + "Start", logLevel = logLevel)
        try:
            yield
        finally:
            logInfo(obj = self, prefix = name + "End",   logLevel = logLevel)
    
    def display(self, name, exposure=None, sources=(), matches=None,
                ctypes=_DefaultDS9CTypes, ptypes=_DefaultDS9PTypes,
                sizes=(4,),
                pause=None, prompt=None):
        """!Display an exposure and/or sources

        @warning This method is deprecated. New code should call lsst.afw.display.ds9 directly.

        @param[in] name         name of product to display
        @param[in] exposure     exposure to display (instance of lsst::afw::image::Exposure), or None
        @param[in] sources      list of Sources to display, as a single lsst.afw.table.SourceCatalog
                                or a list of lsst.afw.table.SourceCatalog,
                                or an empty list to not display sources
        @param[in] matches      list of source matches to display (instances of
            lsst.afw.table.ReferenceMatch), or None;
            if any matches are specified then exposure must be provided and have a lsst.afw.image.Wcs.
        @param[in] ctypes       array of colors to use on ds9 for displaying sources and matches
            (in that order).
            ctypes is indexed as follows, where ctypes is repeatedly cycled through, if necessary:
            - ctypes[i] is used to display sources[i]
            - ctypes[len(sources) + 2i] is used to display matches[i][0]
            - ctypes[len(sources) + 2i + 1] is used to display matches[i][1]
        @param[in] ptypes       array of ptypes to use on ds9 for displaying sources and matches;
            indexed like ctypes
        @param[in] sizes        array of sizes to use on ds9 for displaying sources and matches;
            indexed like ctypes
        @param[in] pause        pause execution?
        @param[in] prompt       prompt for user while paused (ignored if pause is False)

        @warning if matches are specified and exposure has no lsst.afw.image.Wcs then the matches are
        silently not shown.

        @throw Exception if matches specified and exposure is None
        """
        # N.b. doxygen will complain about parameters like ds9 and RED not being documented.  Bug ID 732356
        if not self._display or self._display < 0:
            return
        if isinstance(self._display, dict):
            if (name not in self._display) or not self._display[name] or self._display[name] < 0:
                return

        if isinstance(self._display, int):
            frame = self._display
        elif isinstance(self._display, dict):
            frame = self._display[name]
        else:
            frame = 1

        if exposure:
            if isinstance(exposure, list):
                raise RuntimeError("exposure may not be a list")
            mi = exposure.getMaskedImage()
            ds9.mtv(exposure, frame=frame, title=name)
            x0, y0 = mi.getX0(), mi.getY0()
        else:
            x0, y0 = 0, 0

        if len(sources) > 0 and hasattr(sources, "schema"):
            # user provided a single source catalog instead of a collection of catalogs
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

        if matches and exposure.getWcs() is not None:
            wcs = exposure.getWcs()
            with ds9.Buffering():
                for first, second, d in matches:
                    i = len(sources)    # counter for ptypes/ctypes, starting one after number of source lists
                    catPos = wcs.skyToPixel(first.getCoord())
                    x1, y1 = catPos.getX() - x0, catPos.getY() - y0

                    ctype = ctypes[i%len(ctypes)]
                    ptype = ptypes[i%len(ptypes)]
                    size  = 2*sizes[i%len(sizes)]
                    ds9.dot(ptype, x1, y1, size=size, frame=frame, ctype=ctype)
                    i += 1

                    ctype = ctypes[i%len(ctypes)]
                    ptype = ptypes[i%len(ptypes)]
                    size  = 2*sizes[i%len(sizes)]
                    x2, y2 = second.getX() - x0, second.getY() - y0
                    ds9.dot(ptype, x2, y2, size=size, frame=frame, ctype=ctype)
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

    @classmethod
    def makeField(cls, doc):
        """!Make an lsst.pex.config.ConfigurableField for this task

        Provides a convenient way to specify this task is a subtask of another task.
        Here is an example of use:
        \code
        class OtherTaskConfig(lsst.pex.config.Config)
            aSubtask = ATaskClass.makeField("a brief description of what this task does")
        \endcode

        @param[in] cls      this class
        @param[in] doc      help text for the field
        @return a lsst.pex.config.ConfigurableField for this task
        """
        return ConfigurableField(doc=doc, target=cls)

    def _computeFullName(self, name):
        """!Compute the full name of a subtask or metadata item, given its brief name

        For example: if the full name of this task is "top.sub.sub2"
        then _computeFullName("subname") returns "top.sub.sub2.subname".
        
        @param[in] name     brief name of subtask or metadata item
        @return the full name: the "name" argument prefixed by the full task name and a period.
        """
        return "%s.%s" % (self._fullName, name)
