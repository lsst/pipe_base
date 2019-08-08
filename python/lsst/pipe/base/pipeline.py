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
from __future__ import annotations

"""Module defining Pipeline class and related methods.
"""

__all__ = ["Pipeline", "TaskDef", "TaskDatasetTypes", "PipelineDatasetTypes"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from dataclasses import dataclass
from types import MappingProxyType
from typing import FrozenSet, Mapping

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import DatasetType, DimensionUniverse
from .connections import PipelineTaskConnections, iterConnections

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class TaskDef:
    """TaskDef is a collection of information about task needed by Pipeline.

    The information includes task name, configuration object and optional
    task class. This class is just a collection of attributes and it exposes
    all of them so that attributes could potentially be modified in place
    (e.g. if configuration needs extra overrides).

    Attributes
    ----------
    taskName : `str`
        `PipelineTask` class name, currently it is not specified whether this
        is a fully-qualified name or partial name (e.g. ``module.TaskClass``).
        Framework should be prepared to handle all cases.
    config : `lsst.pex.config.Config`
        Instance of the configuration class corresponding to this task class,
        usually with all overrides applied.
    taskClass : `type` or ``None``
        `PipelineTask` class object, can be ``None``. If ``None`` then
        framework will have to locate and load class.
    label : `str`, optional
        Task label, usually a short string unique in a pipeline.
    """
    def __init__(self, taskName, config, taskClass=None, label=""):
        self.taskName = taskName
        self.config = config
        self.taskClass = taskClass
        self.label = label
        self.connections = config.connections.connectionsClass(config=config)

    def __str__(self):
        rep = "TaskDef(" + self.taskName
        if self.label:
            rep += ", label=" + self.label
        rep += ")"
        return rep


class Pipeline(list):
    """Pipeline is a sequence of `TaskDef` objects.

    Pipeline is given as one of the inputs to a supervising framework
    which builds execution graph out of it. Pipeline contains a sequence
    of `TaskDef` instances.

    Main purpose of this class is to provide a mechanism to pass pipeline
    definition from users to supervising framework. That mechanism is
    implemented using simple serialization and de-serialization via
    `pickle`. Note that pipeline serialization is not guaranteed to be
    compatible between different versions or releases.

    In current implementation Pipeline is a list (it inherits from `list`)
    and one can use all list methods on pipeline. Content of the pipeline
    can be modified, it is up to the client to verify that modifications
    leave pipeline in a consistent state. One could modify container
    directly by adding or removing its elements.

    Parameters
    ----------
    pipeline : iterable of `TaskDef` instances, optional
        Initial sequence of tasks.
    """
    def __init__(self, iterable=None):
        list.__init__(self, iterable or [])

    def labelIndex(self, label):
        """Return task index given its label.

        Parameters
        ----------
        label : `str`
            Task label.

        Returns
        -------
        index : `int`
            Task index, or -1 if label is not found.
        """
        for idx, taskDef in enumerate(self):
            if taskDef.label == label:
                return idx
        return -1

    def __str__(self):
        infos = [str(tdef) for tdef in self]
        return "Pipeline({})".format(", ".join(infos))


@dataclass(frozen=True)
class TaskDatasetTypes:
    """An immutable struct that extracts and classifies the dataset types used
    by a `PipelineTask`
    """

    initInputs: FrozenSet[DatasetType]
    """Dataset types that are needed as inputs in order to construct this Task.

    Task-level `initInputs` may be classified as either
    `~PipelineDatasetTypes.initInputs` or
    `~PipelineDatasetTypes.initIntermediates` at the Pipeline level.
    """

    initOutputs: FrozenSet[DatasetType]
    """Dataset types that may be written after constructing this Task.

    Task-level `initOutputs` may be classified as either
    `~PipelineDatasetTypes.initOutputs` or
    `~PipelineDatasetTypes.initIntermediates` at the Pipeline level.
    """

    inputs: FrozenSet[DatasetType]
    """Dataset types that are regular inputs to this Task.

    If an input dataset needed for a Quantum cannot be found in the input
    collection(s) or produced by another Task in the Pipeline, that Quantum
    (and all dependent Quanta) will not be produced.

    Task-level `inputs` may be classified as either
    `~PipelineDatasetTypes.inputs` or `~PipelineDatasetTypes.intermediates`
    at the Pipeline level.
    """

    prerequisites: FrozenSet[DatasetType]
    """Dataset types that are prerequisite inputs to this Task.

    Prerequisite inputs must exist in the input collection(s) before the
    pipeline is run, but do not constrain the graph - if a prerequisite is
    missing for a Quantum, `PrerequisiteMissingError` is raised.

    Prerequisite inputs are not resolved until the second stage of
    QuantumGraph generation.
    """

    outputs: FrozenSet[DatasetType]
    """Dataset types that are produced by this Task.

    Task-level `outputs` may be classified as either
    `~PipelineDatasetTypes.outputs` or `~PipelineDatasetTypes.intermediates`
    at the Pipeline level.
    """

    @classmethod
    def fromConnections(cls, connectionsInstance: PipelineTaskConnections, *,
                        universe: DimensionUniverse) -> TaskDatasetTypes:
        """Extract and classify the dataset types from a single `PipelineTask`.

        Parameters
        ----------
        connectionsInstance: `PipelineTaskConnections`
            An instance of a `PipelineTaskConnections` class for a particular
            `PipelineTask`.
        universe: `DimensionUniverse`
            Set of all known dimensions, used to construct normalized
            `DatasetType` objects.

        Returns
        -------
        types: `TaskDatasetTypes`
            The dataset types used by this task.
        """
        def makeDatasetTypesSet(connectionType):
            """Constructs a set of true `DatasetType` objects

            Parameters
            ----------
            connectionType : `str`
                Name of the connection type to produce a set for, corresponds
                to an attribute of type `list` on the connection class instance

            Returns
            -------
            datasetTypes : `frozenset`
                A set of all datasetTypes which correspond to the input
                connection type specified in the connection class of this
                `PipelineTask`

            Notes
            -----
            This function is a closure over the variables univers and
            connectionsInstnace
            """
            datasetTypes = []
            for c in iterConnections(connectionsInstance, connectionType):
                dimensions = getattr(c, 'dimensions', set())
                datasetTypes.append(DatasetType(c.name, universe.extract(dimensions), c.storageClass))
            return frozenset(datasetTypes)

        return cls(
            initInputs=makeDatasetTypesSet("initInputs"),
            initOutputs=makeDatasetTypesSet("initOutputs"),
            inputs=makeDatasetTypesSet("inputs"),
            prerequisites=makeDatasetTypesSet("prerequisiteInputs"),
            outputs=makeDatasetTypesSet("outputs"),
        )


@dataclass(frozen=True)
class PipelineDatasetTypes:
    """An immutable struct that classifies the dataset types used in a
    `Pipeline`.
    """

    initInputs: FrozenSet[DatasetType]
    """Dataset types that are needed as inputs in order to construct the Tasks
    in this Pipeline.

    This does not include dataset types that are produced when constructing
    other Tasks in the Pipeline (these are classified as `initIntermediates`).
    """

    initOutputs: FrozenSet[DatasetType]
    """Dataset types that may be written after constructing the Tasks in this
    Pipeline.

    This does not include dataset types that are also used as inputs when
    constructing other Tasks in the Pipeline (these are classified as
    `initIntermediates`).
    """

    initIntermediates: FrozenSet[DatasetType]
    """Dataset types that are both used when constructing one or more Tasks
    in the Pipeline and produced as a side-effect of constructing another
    Task in the Pipeline.
    """

    inputs: FrozenSet[DatasetType]
    """Dataset types that are regular inputs for the full pipeline.

    If an input dataset needed for a Quantum cannot be found in the input
    collection(s), that Quantum (and all dependent Quanta) will not be
    produced.
    """

    prerequisites: FrozenSet[DatasetType]
    """Dataset types that are prerequisite inputs for the full Pipeline.

    Prerequisite inputs must exist in the input collection(s) before the
    pipeline is run, but do not constrain the graph - if a prerequisite is
    missing for a Quantum, `PrerequisiteMissingError` is raised.

    Prerequisite inputs are not resolved until the second stage of
    QuantumGraph generation.
    """

    intermediates: FrozenSet[DatasetType]
    """Dataset types that are output by one Task in the Pipeline and consumed
    as inputs by one or more other Tasks in the Pipeline.
    """

    outputs: FrozenSet[DatasetType]
    """Dataset types that are output by a Task in the Pipeline and not consumed
    by any other Task in the Pipeline.
    """

    byTask: Mapping[str, TaskDatasetTypes]
    """Per-Task dataset types, keyed by label in the `Pipeline`.

    This is guaranteed to be zip-iterable with the `Pipeline` itself (assuming
    neither has been modified since the dataset types were extracted, of
    course).
    """

    @classmethod
    def fromPipeline(cls, pipeline: Pipeline, *, universe: DimensionUniverse) -> PipelineDatasetTypes:
        """Extract and classify the dataset types from all tasks in a
        `Pipeline`.

        Parameters
        ----------
        pipeline: `Pipeline`
            An ordered collection of tasks that can be run together.
        universe: `DimensionUniverse`
            Set of all known dimensions, used to construct normalized
            `DatasetType` objects.

        Returns
        -------
        types: `PipelineDatasetTypes`
            The dataset types used by this `Pipeline`.

        Raises
        ------
        ValueError
            Raised if Tasks are inconsistent about which datasets are marked
            prerequisite.  This indicates that the Tasks cannot be run as part
            of the same `Pipeline`.
        """
        allInputs = set()
        allOutputs = set()
        allInitInputs = set()
        allInitOutputs = set()
        prerequisites = set()
        byTask = dict()
        for taskDef in pipeline:
            thisTask = TaskDatasetTypes.fromConnections(taskDef.connections, universe=universe)
            allInitInputs.update(thisTask.initInputs)
            allInitOutputs.update(thisTask.initOutputs)
            allInputs.update(thisTask.inputs)
            prerequisites.update(thisTask.prerequisites)
            allOutputs.update(thisTask.outputs)
            byTask[taskDef.label] = thisTask
        if not prerequisites.isdisjoint(allInputs):
            raise ValueError("{} marked as both prerequisites and regular inputs".format(
                {dt.name for dt in allInputs & prerequisites}
            ))
        if not prerequisites.isdisjoint(allOutputs):
            raise ValueError("{} marked as both prerequisites and outputs".format(
                {dt.name for dt in allOutputs & prerequisites}
            ))
        # Make sure that components which are marked as inputs get treated as
        # intermediates if there is an output which produces the composite
        # containing the component
        intermediateComponents = set()
        intermediateComposites = set()
        outputNameMapping = {dsType.name: dsType for dsType in allOutputs}
        for dsType in allInputs:
            # get the name of a possible component
            name, component = dsType.nameAndComponent()
            # if there is a component name, that means this is a component
            # DatasetType, if there is an output which produces the parent of
            # this component, treat this input as an intermediate
            if component is not None:
                if name in outputNameMapping and outputNameMapping[name].dimensions == dsType.dimensions:
                    composite = DatasetType(name, dsType.dimensions, outputNameMapping[name].storageClass,
                                            universe=universe)
                    intermediateComponents.add(dsType)
                    intermediateComposites.add(composite)
        return cls(
            initInputs=frozenset(allInitInputs - allInitOutputs),
            initIntermediates=frozenset(allInitInputs & allInitOutputs),
            initOutputs=frozenset(allInitOutputs - allInitInputs),
            inputs=frozenset(allInputs - allOutputs - intermediateComponents),
            intermediates=frozenset(allInputs & allOutputs | intermediateComponents),
            outputs=frozenset(allOutputs - allInputs - intermediateComposites),
            prerequisites=frozenset(prerequisites),
            byTask=MappingProxyType(byTask),  # MappingProxyType -> frozen view of dict for immutability
        )
