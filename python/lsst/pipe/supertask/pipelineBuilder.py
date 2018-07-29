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
"""
Module defining PipelineBuilder class and related methods.
"""

from __future__ import print_function
from builtins import object

__all__ = ['PipelineBuilder']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import os
import pickle

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .configOverrides import ConfigOverrides
from .pipeline import Pipeline, TaskDef
from . import pipeTools
import lsst.log as lsstLog
import lsst.utils

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class PipelineBuilder(object):
    """
    PipelineBuilder class is responsible for building task pipeline from
    a set of command line arguments.

    Parameters
    ----------
    taskFactory : `TaskFactory`
        Factory object used to load/instantiate PipelineTasks
    """

    def __init__(self, taskFactory):
        self.taskFactory = taskFactory

    def makePipeline(self, args):
        """Build pipeline from command line arguments.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command line.

        Returns
        -------
        `Pipeline` instance.

        Raises
        ------
        Exceptions will be raised for any kind of error conditions.
        """

        # need camera/package name to find overrides
        obsPkg = None
        camera = None
        if args.camera_overrides:
            # mapperClass = dafPersist.Butler.getMapperClass(args.input)
            # camera = mapperClass.getCameraName()
            # obsPkg = mapperClass.getPackageName()
            pass

        if args.pipeline:

            # load it from a pickle file, will raise on error
            with open(args.pipeline, 'rb') as pickleFile:
                pipeline = pickle.load(pickleFile)

            # check type
            if not isinstance(pipeline, Pipeline):
                msg = "Pickle file `{}' contains something other than Pipeline"
                raise TypeError(msg.format(args.pipeline))

        else:
            # start with empty one
            pipeline = Pipeline()

        # loop over all pipeline actions and apply them in order
        for action in args.pipeline_actions:

            if action.action == "new_task":

                self._newTask(pipeline, action.value, action.label, obsPkg, camera)

            elif action.action == "delete_task":

                self._dropTask(pipeline, action.label)

            elif action.action == "move_task":

                self._moveTask(pipeline, action.label, int(action.value))

            elif action.action == "relabel":

                self._labelTask(pipeline, action.label, action.value)

            elif action.action == "config":

                self._configOverride(pipeline, action.label, action.value)

            elif action.action == "configfile":

                self._configOverrideFile(pipeline, action.label, action.value)

        # re-order pipeline if requested, this also checks for possible errors
        if args.order_pipeline:
            pipeline = pipeTools.orderPipeline(pipeline, self.taskFactory)

        return pipeline

    def _newTask(self, pipeline, taskName, label=None, obsPkg=None, camera=None):
        """Append new task to a pipeline.

        Parameters
        ----------
        pipeline : py:class:`Pipeline`
            Pipeline instance.
        taskName : `str`
            Name of the new task, can be either full class name including
            package and module, or just a class name to be searched in
            known packages and modules.
        label : `str`, optional
            Label for new task, if `None` the n task class name is used as
            label.
        obsPkg : `str`, optional
            Name of the package to look for task overrides, if None then
            overrides are not used.
        camera : `str`, optional
            Camera name, used for camera-specific overrides an only if
            `obsPkg` is not `None`.
        """
        # load task class, will throw on errors
        taskClass, taskName = self.taskFactory.loadTaskClass(taskName)

        # get label and check that it is unique
        if not label:
            label = taskName.rpartition('.')[2]
        if pipeline.labelIndex(label) >= 0:
            raise LookupError("Task label (or name) is not unique: " + label)

        # make config instance with defaults
        config = taskClass.ConfigClass()

        # apply camera/package overrides
        if obsPkg:
            obsPkgDir = lsst.utils.getPackageDir(obsPkg)
            configName = taskClass._DefaultName
            fileName = configName + ".py"
            overrides = ConfigOverrides()
            for filePath in (
                os.path.join(obsPkgDir, "config", fileName),
                os.path.join(obsPkgDir, "config", camera, fileName),
            ):
                if os.path.exists(filePath):
                    lsstLog.info("Loading config overrride file %r", filePath)
                    overrides.addFileOverride(filePath)
                else:
                    lsstLog.debug("Config override file does not exist: %r", filePath)

            overrides.applyTo(config)

        pipeline.append(TaskDef(taskName=taskName, config=config,
                                taskClass=taskClass, label=label))

    def _dropTask(self, pipeline, label):
        """Remove task from a pipeline.

        Parameters
        ----------
        pipeline : py:class:`Pipeline`
            Pipeline instance.
        label : `str`
            Label of the task to remove.
        """
        idx = pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        del pipeline[idx]

    def _moveTask(self, pipeline, label, new_idx):
        """Move task to a new position in a pipeline.

        Parameters
        ----------
        pipeline : py:class:`Pipeline`
            Pipeline instance.
        label : `str`
            Label of the task to move.
        new_idx : `int`
            New position.
        """
        idx = pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        pipeline.insert(new_idx, pipeline.pop(idx))

    def _labelTask(self, pipeline, label, newLabel):
        """Change task label.

        Parameters
        ----------
        pipeline : py:class:`Pipeline`
            Pipeline instance.
        label : `str`
            Existing label of the task.
        newLabel : `str`
            New label of the task.
        """
        idx = pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        # check that new one is unique
        if newLabel != label and pipeline.labelIndex(newLabel) >= 0:
            raise LookupError("New task label is not unique: " + label)
        pipeline[idx].label = newLabel

    def _configOverride(self, pipeline, label, value):
        """Apply single config override.

        Parameters
        ----------
        pipeline : py:class:`Pipeline`
            Pipeline instance.
        label : `str`
            Label of the task.
        value : `str`
            String in the form "param = value".
        """
        idx = pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        key, sep, val = value.partition('=')
        overrides = ConfigOverrides()
        overrides.addValueOverride(key, val)
        overrides.applyTo(pipeline[idx].config)

    def _configOverrideFile(self, pipeline, label, value):
        """Apply overrides from file.

        Parameters
        ----------
        pipeline : py:class:`Pipeline`
            Pipeline instance.
        label : `str`
            Label of the task.
        value : `str`
            Path to file with overrides.
        """
        idx = pipeline.labelIndex(label)
        if idx < 0:
            raise LookupError("Task label is not found: " + label)
        key, sep, val = value.partition('=')
        overrides = ConfigOverrides()
        overrides.addFileOverride(value)
        overrides.applyTo(pipeline[idx].config)
