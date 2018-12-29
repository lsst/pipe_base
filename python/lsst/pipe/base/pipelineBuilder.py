#
# LSST Data Management System
# Copyright 2018 AURA/LSST.
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
"""Module defining PipelineBuilder class and related methods.
"""

__all__ = ["PipelineBuilder"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .configOverrides import ConfigOverrides
from .pipeline import Pipeline, TaskDef
from . import pipeTools

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])

# ------------------------
#  Exported definitions --
# ------------------------


class PipelineBuilder(object):
    """PipelineBuilder class is responsible for building task pipeline.

    The class provides a set of methods to manipulate pipeline by adding,
    deleting, re-ordering tasks in pipeline and chaning their labels or
    configuration.

    Parameters
    ----------
    taskFactory : `TaskFactory`
        Factory object used to load/instantiate PipelineTasks
    pipeline : `Pipeline`, optional
        Initial pipeline to be modified, if `None` then new empty pipeline
        will be created.
    """
    def __init__(self, taskFactory, pipeline=None):
        if pipeline is None:
            pipeline = Pipeline()
        self._taskFactory = taskFactory
        self._pipeline = pipeline

    def pipeline(self, ordered=False):
        """Return updated pipeline instance.

        Pipeline will be checked for possible inconsistencies before
        returning.

        Parameters
        ----------
        ordered : `bool`, optional
            If `True` then order resulting pipeline according to Task data
            dependencies.

        Returns
        -------
        pipeline : `Pipeline`

        Raises
        ------
        Exception
            Raised if any inconsistencies are detected in pipeline definition,
            see `pipeTools.orderPipeline` for list of exception types.
        """
        # conditionally re-order pipeline if requested, but unconditionally
        # check for possible errors
        orderedPipeline = pipeTools.orderPipeline(self._pipeline, self._taskFactory)
        if ordered:
            return orderedPipeline
        else:
            return self._pipeline

    def addTask(self, taskName, label=None):
        """Append new task to a pipeline.

        Parameters
        ----------
        taskName : `str`
            Name of the new task, can be either full class name including
            package and module, or just a class name to be searched in
            known packages and modules.
        label : `str`, optional
            Label for new task, if `None` the n task class name is used as
            label.
        """
        # load task class, will throw on errors
        taskClass, taskName = self._taskFactory.loadTaskClass(taskName)

        # get label and check that it is unique
        if not label:
            label = taskName.rpartition('.')[2]
        if self._pipeline.labelIndex(label) >= 0:
            raise LookupError("Task label (or name) is not unique: " + label)

        # make config instance with defaults
        config = taskClass.ConfigClass()

        self._pipeline.append(TaskDef(taskName=taskName, config=config,
                                      taskClass=taskClass, label=label))

    def deleteTask(self, label):
        """Remove task from a pipeline.

        Parameters
        ----------
        label : `str`
            Label of the task to remove.
        """
        idx = self._pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        del self._pipeline[idx]

    def moveTask(self, label, newIndex):
        """Move task to a new position in a pipeline.

        Parameters
        ----------
        label : `str`
            Label of the task to move.
        newIndex : `int`
            New position.
        """
        idx = self._pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        self._pipeline.insert(newIndex, self._pipeline.pop(idx))

    def labelTask(self, label, newLabel):
        """Change task label.

        Parameters
        ----------
        label : `str`
            Existing label of the task.
        newLabel : `str`
            New label of the task.
        """
        idx = self._pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        # check that new one is unique
        if newLabel != label and self._pipeline.labelIndex(newLabel) >= 0:
            raise LookupError("New task label is not unique: " + label)
        self._pipeline[idx].label = newLabel

    def configOverride(self, label, value):
        """Apply single config override.

        Parameters
        ----------
        label : `str`
            Label of the task.
        value : `str`
            String in the form "param=value".
        """
        idx = self._pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        key, sep, val = value.partition('=')
        overrides = ConfigOverrides()
        overrides.addValueOverride(key, val)
        overrides.applyTo(self._pipeline[idx].config)

    def configOverrideFile(self, label, path):
        """Apply overrides from file.

        Parameters
        ----------
        label : `str`
            Label of the task.
        path : `str`
            Path to file with overrides.
        """
        idx = self._pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        overrides = ConfigOverrides()
        overrides.addFileOverride(path)
        overrides.applyTo(self._pipeline[idx].config)
