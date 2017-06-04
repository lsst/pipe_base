#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
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
Module defining GraphBuilder class and related methods.
"""

from __future__ import print_function
from builtins import object

__all__ = ['GraphBuilder']

#--------------------------------
#  Imports of standard modules --
#--------------------------------

#-----------------------------
# Imports for other modules --
#-----------------------------
import lsst.log as lsstLog
import lsst.obs.base.repodb.tests as repodbTest
from lsst.pipe.base.task import TaskError
from .taskFactory import TaskFactory
from .taskLoader import (TaskLoader, KIND_SUPERTASK)

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------


class GraphBuilder(object):
    """
    GraphBuilder class is responsible for building task execution graph from
    a Pipeline.

    Parameters
    ----------
    taskFactory : `TaskFactory`
        Factory object used to load/instantiate SuperTasks
    butler : `DataButler`
        Data butler instance.
    repoDb : `repodb.RepoDatabase`
        Data butler instance.
    repoQuery : `str`
        String which is passed to `RepoDatabase.makeGraph()`, should be empty
        or `None` if there is no restrictions on data selection.
    """

    def __init__(self, taskFactory, butler, repoDb, repoQuery):
        self.taskFactory = taskFactory
        self.butler = butler
        self.repoDb = repoDb
        self.repoQuery = repoQuery

    def makeGraph(self, pipeline):
        """Create execution graph for a pipeline.

        Parameters
        ----------
        pipeline : `supertask.Pipeline`
            Pipeline definition, task names/classes and their configs.

        Returns
        -------
        List of tuples (taskDef, quanta), `taskDef` is a `TaskDef` as present
        in pipeline, and `quanta` is a sequence of `Quantum` instances.

        Raises
        ------
        Exceptions will be raised
        """

        # make all task instances
        taskList = []
        for taskDef in pipeline:
            if taskDef.taskClass is None:
                # load task class, update task name to make sure it is fully-qualified,
                # do not update original taskDef in a Pipeline though
                tClass, tName = self.taskFactory.loadTaskClass(taskDef.taskName)
                taskDef = copy.copy(taskDef)
                taskDef.taskClass = tClass
                taskDef.taskName = tName

            task = self.taskFactory.makeTask(taskDef.taskClass, taskDef.config,
                                             None, self.butler)
            taskList += [(task, taskDef)]

        # build initial dataset graph
        inputClasses, outputClasses = self.buildIOClasses([task for task, _ in taskList])

        # make dataset graph
        repoGraph = self.makeRepoGraph(inputClasses, outputClasses)

        return self.makeGraphFromRepoGraph(taskList, repoGraph)

    def buildIOClasses(self, tasks):
        """Returns input and output dataset classes for all tasks.

        Parameters
        ----------
        tasks : sequence of `SuperTask`
            All tasks that form a pipeline.

        Returns
        -------
        inputClasses : set of `type`
            Dataset classes (subsclasses of `repodb.Dataset`) used as inputs
            by the pipeline.
        outputClasses : set of `type`
            Dataset classes produced by the pipeline.
        """
        # to build initial dataset graph we have to collect info about all
        # datasets to be used by this pipeline
        inputs = {}
        outputs = {}
        for task in tasks:
            taskInputs, taskOutputs = task.getDatasetClasses()
            if taskInputs:
                inputs.update(taskInputs)
            if taskOutputs:
                outputs.update(taskOutputs)

        # remove outputs from inputs
        inputClasses = set(inputs.values())
        outputClasses = set(outputs.values())
        inputClasses -= outputClasses

        return (inputClasses, outputClasses)

    def makeRepoGraph(self, inputClasses, ouputClasses):
        """Make initial dataset graph instance.

        Parameters
        ----------
        inputClasses : list of type
            List contains sub-classes (type objects) of Dataset which
            should already exist in input repository
        outputClasses : list of type
            List contains sub-classes (type objects) of Dataset which
            will be created by tasks

        Returns
        -------
        `repodb.graph.RepoGraph` instance.
        """
        repoGraph = self.repoDb.makeGraph(where=self.repoQuery,
                                          NeededDatasets=inputClasses,
                                          FutureDatasets=ouputClasses)
        return repoGraph

    def makeGraphFromRepoGraph(self, taskList, repoGraph):
        """Build execution graph for a pipeline.

        Parameters
        ----------
        taskList : list of tuples
            Each list item is a tuple (task, taskDef)
        repoGraph : `repodb.graph.RepoGraph`

        Returns
        -------
        Same value as `makeGraph()`.

        Raises
        ------
        Any exception raised in `defineQuanta()` is forwarded to caller.
        """
        # get all quanta from each task, add it to the list and say
        # that list is a graph (one of the million possible representations)
        graph = []
        for task, taskDef in taskList:

            # call task to make its quanta
            quanta = task.defineQuanta(repoGraph, self.butler)

            # undefined yet: dataset graph needs to be updated with the
            # outputs produced by this task. We can do it in the task
            # itself or we can do it here by scanning quanta outputs
            # and adding them to dataset graph
#             for quantum in quanta:
#                 for dsTypeName, datasets in quantum.outputs.items():
#                     existing = repoGraph.datasets.setdefault(dsTypeName, set())
#                     existing |= datasets

            # store in a graph
            graph.append((taskDef, quanta))

        return graph
