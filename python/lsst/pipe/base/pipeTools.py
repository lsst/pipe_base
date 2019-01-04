# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Module defining few methods to manipulate or query pipelines.
"""

# No one should do import * from this module
__all__ = ["isPipelineOrdered", "orderPipeline"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .pipeline import Pipeline

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------


def _loadTaskClass(taskDef, taskFactory):
    """Import task class if necessary.

    Raises
    ------
    `ImportError` is raised when task class cannot be imported.
    `MissingTaskFactoryError` is raised when TaskFactory is needed but not provided.
    """
    taskClass = taskDef.taskClass
    if not taskClass:
        if not taskFactory:
            raise MissingTaskFactoryError("Task class is not defined but task "
                                          "factory instance is not provided")
        taskClass = taskFactory.loadTaskClass(taskDef.taskName)
    return taskClass

# ------------------------
#  Exported definitions --
# ------------------------


class MissingTaskFactoryError(Exception):
    """Exception raised when client fails to provide TaskFactory instance.
    """
    pass


class DuplicateOutputError(Exception):
    """Exception raised when Pipeline has more than one task for the same
    output.
    """
    pass


class PipelineDataCycleError(Exception):
    """Exception raised when Pipeline has data dependency cycle.
    """
    pass


def isPipelineOrdered(pipeline, taskFactory=None):
    """Checks whether tasks in pipeline are correctly ordered.

    Pipeline is correctly ordered if for any DatasetType produced by a task
    in a pipeline all its consumer tasks are located after producer.

    Parameters
    ----------
    pipeline : `pipe.base.Pipeline`
        Pipeline description.
    taskFactory: `pipe.base.TaskFactory`, optional
        Instance of an object which knows how to import task classes. It is only
        used if pipeline task definitions do not define task classes.

    Returns
    -------
    True for correctly ordered pipeline, False otherwise.

    Raises
    ------
    `ImportError` is raised when task class cannot be imported.
    `DuplicateOutputError` is raised when there is more than one producer for a
    dataset type.
    `MissingTaskFactoryError` is raised when TaskFactory is needed but not
    provided.
    """
    # Build a map of DatasetType name to producer's index in a pipeline
    producerIndex = {}
    for idx, taskDef in enumerate(pipeline):

        # we will need task class for next operations, make sure it is loaded
        taskDef.taskClass = _loadTaskClass(taskDef, taskFactory)

        # get task output DatasetTypes, this can only be done via class method
        outputs = taskDef.taskClass.getOutputDatasetTypes(taskDef.config)
        for dsTypeDescr in outputs.values():
            dsType = dsTypeDescr.datasetType
            if dsType.name in producerIndex:
                raise DuplicateOutputError("DatasetType `{}' appears more than "
                                           "once as" " output".format(dsType.name))
            producerIndex[dsType.name] = idx

    # check all inputs that are also someone's outputs
    for idx, taskDef in enumerate(pipeline):

        # get task input DatasetTypes, this can only be done via class method
        inputs = taskDef.taskClass.getInputDatasetTypes(taskDef.config)
        for dsTypeDescr in inputs.values():
            dsType = dsTypeDescr.datasetType
            # all pre-existing datasets have effective index -1
            prodIdx = producerIndex.get(dsType.name, -1)
            if prodIdx >= idx:
                # not good, producer is downstream
                return False

    return True


def orderPipeline(pipeline, taskFactory=None):
    """Re-order tasks in pipeline to satisfy data dependencies.

    When possible new ordering keeps original relative order of the tasks.

    Parameters
    ----------
    pipeline : `pipe.base.Pipeline`
        Pipeline description.
    taskFactory: `pipe.base.TaskFactory`, optional
        Instance of an object which knows how to import task classes. It is only
        used if pipeline task definitions do not define task classes.

    Returns
    -------
    Correctly ordered pipeline (`pipe.base.Pipeline` instance).

    Raises
    ------
    `ImportError` is raised when task class cannot be imported.
    `DuplicateOutputError` is raised when there is more than one producer for a
    dataset type.
    `PipelineDataCycleError` is also raised when pipeline has dependency cycles.
    `MissingTaskFactoryError` is raised when TaskFactory is needed but not
    provided.
    """

    # This is a modified version of Kahn's algorithm that preserves order

    # build mapping of the tasks to their inputs and outputs
    inputs = {}   # maps task index to its input DatasetType names
    outputs = {}  # maps task index to its output DatasetType names
    allInputs = set()   # all inputs of all tasks
    allOutputs = set()  # all outputs of all tasks
    for idx, taskDef in enumerate(pipeline):

        # we will need task class for next operations, make sure it is loaded
        taskClass = _loadTaskClass(taskDef, taskFactory)

        # task outputs
        dsMap = taskClass.getOutputDatasetTypes(taskDef.config)
        for dsTypeDescr in dsMap.values():
            dsType = dsTypeDescr.datasetType
            if dsType.name in allOutputs:
                raise DuplicateOutputError("DatasetType `{}' appears more than "
                                           "once as" " output".format(dsType.name))
        outputs[idx] = set(dsTypeDescr.datasetType.name for dsTypeDescr in dsMap.values())
        allOutputs.update(outputs[idx])

        # task inputs
        dsMap = taskClass.getInputDatasetTypes(taskDef.config)
        inputs[idx] = set(dsTypeDescr.datasetType.name for dsTypeDescr in dsMap.values())
        allInputs.update(inputs[idx])

    # for simplicity add pseudo-node which is a producer for all pre-existing
    # inputs, its index is -1
    preExisting = allInputs - allOutputs
    outputs[-1] = preExisting

    # Set of nodes with no incoming edges, initially set to pseudo-node
    queue = [-1]
    result = []
    while queue:

        # move to final list, drop -1
        idx = queue.pop(0)
        if idx >= 0:
            result.append(idx)

        # remove task outputs from other tasks inputs
        thisTaskOutputs = outputs.get(idx, set())
        for taskInputs in inputs.values():
            taskInputs -= thisTaskOutputs

        # find all nodes with no incoming edges and move them to the queue
        topNodes = [key for key, value in inputs.items() if not value]
        queue += topNodes
        for key in topNodes:
            del inputs[key]

        # keep queue ordered
        queue.sort()

    # if there is something left it means cycles
    if inputs:
        # format it in usable way
        loops = []
        for idx, inputNames in inputs.items():
            taskName = pipeline[idx].label
            outputNames = outputs[idx]
            edge = "   {} -> {} -> {}".format(inputNames, taskName, outputNames)
            loops.append(edge)
        raise PipelineDataCycleError("Pipeline has data cycles:\n" + "\n".join(loops))

    return Pipeline(pipeline[idx] for idx in result)
