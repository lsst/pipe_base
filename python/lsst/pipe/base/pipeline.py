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
from typing import Mapping, Union, Generator, TYPE_CHECKING

import copy
import os

# -----------------------------
#  Imports for other modules --
from lsst.daf.butler import DatasetType, NamedValueSet, Registry, SkyPixDimension
from lsst.utils import doImport
from .configOverrides import ConfigOverrides
from .connections import iterConnections
from .pipelineTask import PipelineTask

from . import pipelineIR
from . import pipeTools

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from lsst.obs.base.instrument import Instrument

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
        usually with all overrides applied. This config will be frozen.
    taskClass : `type` or ``None``
        `PipelineTask` class object, can be ``None``. If ``None`` then
        framework will have to locate and load class.
    label : `str`, optional
        Task label, usually a short string unique in a pipeline.
    """
    def __init__(self, taskName, config, taskClass=None, label=""):
        self.taskName = taskName
        config.freeze()
        self.config = config
        self.taskClass = taskClass
        self.label = label
        self.connections = config.connections.ConnectionsClass(config=config)

    @property
    def configDatasetName(self):
        """Name of a dataset type for configuration of this task (`str`)
        """
        return self.label + "_config"

    @property
    def metadataDatasetName(self):
        """Name of a dataset type for metadata of this task, `None` if
        metadata is not to be saved (`str`)
        """
        if self.config.saveMetadata:
            return self.label + "_metadata"
        else:
            return None

    def __str__(self):
        rep = "TaskDef(" + self.taskName
        if self.label:
            rep += ", label=" + self.label
        rep += ")"
        return rep

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskDef):
            return False
        return self.config == other.config and\
            self.taskClass == other.taskClass and\
            self.label == other.label

    def __hash__(self):
        return hash((self.taskClass, self.label))


class Pipeline:
    """A `Pipeline` is a representation of a series of tasks to run, and the
    configuration for those tasks.

    Parameters
    ----------
    description : `str`
        A description of that this pipeline does.
    """
    def __init__(self, description: str) -> Pipeline:
        pipeline_dict = {"description": description, "tasks": {}}
        self._pipelineIR = pipelineIR.PipelineIR(pipeline_dict)

    @classmethod
    def fromFile(cls, filename: str) -> Pipeline:
        """Load a pipeline defined in a pipeline yaml file.

        Parameters
        ----------
        filename: `str`
           A path that points to a pipeline defined in yaml format

        Returns
        -------
        pipeline: `Pipeline`
        """
        pipeline = cls.fromIR(pipelineIR.PipelineIR.from_file(filename))
        return pipeline

    @classmethod
    def fromString(cls, pipeline_string: str) -> Pipeline:
        """Create a pipeline from string formatted as a pipeline document.

        Parameters
        ----------
        pipeline_string : `str`
            A string that is formatted according like a pipeline document

        Returns
        -------
        pipeline: `Pipeline`
        """
        pipeline = cls.fromIR(pipelineIR.PipelineIR.from_string(pipeline_string))
        return pipeline

    @classmethod
    def fromIR(cls, deserialized_pipeline: pipelineIR.PipelineIR) -> Pipeline:
        """Create a pipeline from an already created `PipelineIR` object.

        Parameters
        ----------
        deserialized_pipeline: `PipelineIR`
            An already created pipeline intermediate representation object

        Returns
        -------
        pipeline: `Pipeline`
        """
        pipeline = cls.__new__(cls)
        pipeline._pipelineIR = deserialized_pipeline
        return pipeline

    @classmethod
    def fromPipeline(cls, pipeline: pipelineIR.PipelineIR) -> Pipeline:
        """Create a new pipeline by copying an already existing `Pipeline`.

        Parameters
        ----------
        pipeline: `Pipeline`
            An already created pipeline intermediate representation object

        Returns
        -------
        pipeline: `Pipeline`
        """
        return cls.fromIR(copy.deep_copy(pipeline._pipelineIR))

    def __str__(self) -> str:
        return str(self._pipelineIR)

    def addInstrument(self, instrument: Union[Instrument, str]):
        """Add an instrument to the pipeline, or replace an instrument that is
        already defined.

        Parameters
        ----------
        instrument : `~lsst.daf.butler.instrument.Instrument` or `str`
            Either a derived class object of a `lsst.daf.butler.instrument` or a
            string corresponding to a fully qualified
            `lsst.daf.butler.instrument` name.
        """
        if isinstance(instrument, str):
            pass
        else:
            # TODO: assume that this is a subclass of Instrument, no type checking
            instrument = f"{instrument.__module__}.{instrument.__qualname__}"
        self._pipelineIR.instrument = instrument

    def getInstrument(self):
        """Get the instrument from the pipeline.

        Returns
        -------
        instrument : `~lsst.daf.butler.instrument.Instrument`, `str`, or None
            A derived class object of a `lsst.daf.butler.instrument`, a string
            corresponding to a fully qualified `lsst.daf.butler.instrument`
            name, or None if the pipeline does not have an instrument.
        """
        return self._pipelineIR.instrument

    def addTask(self, task: Union[PipelineTask, str], label: str):
        """Add a new task to the pipeline, or replace a task that is already
        associated with the supplied label.

        Parameters
        ----------
        task: `PipelineTask` or `str`
            Either a derived class object of a `PipelineTask` or a string
            corresponding to a fully qualified `PipelineTask` name.
        label: `str`
            A label that is used to identify the `PipelineTask` being added
        """
        if isinstance(task, str):
            taskName = task
        elif issubclass(task, PipelineTask):
            taskName = f"{task.__module__}.{task.__qualname__}"
        else:
            raise ValueError("task must be either a child class of PipelineTask or a string containing"
                             " a fully qualified name to one")
        if not label:
            # in some cases (with command line-generated pipeline) tasks can
            # be defined without label which is not acceptable, use task
            # _DefaultName in that case
            if isinstance(task, str):
                task = doImport(task)
            label = task._DefaultName
        self._pipelineIR.tasks[label] = pipelineIR.TaskIR(label, taskName)

    def removeTask(self, label: str):
        """Remove a task from the pipeline.

        Parameters
        ----------
        label : `str`
            The label used to identify the task that is to be removed

        Raises
        ------
        KeyError
            If no task with that label exists in the pipeline

        """
        self._pipelineIR.tasks.pop(label)

    def addConfigOverride(self, label: str, key: str, value: object):
        """Apply single config override.

        Parameters
        ----------
        label : `str`
            Label of the task.
        key: `str`
            Fully-qualified field name.
        value : object
            Value to be given to a field.
        """
        self._addConfigImpl(label, pipelineIR.ConfigIR(rest={key: value}))

    def addConfigFile(self, label: str, filename: str):
        """Add overrides from a specified file.

        Parameters
        ----------
        label : `str`
            The label used to identify the task associated with config to
            modify
        filename : `str`
            Path to the override file.
        """
        self._addConfigImpl(label, pipelineIR.ConfigIR(file=[filename]))

    def addConfigPython(self, label: str, pythonString: str):
        """Add Overrides by running a snippet of python code against a config.

        Parameters
        ----------
        label : `str`
            The label used to identity the task associated with config to
            modify.
        pythonString: `str`
            A string which is valid python code to be executed. This is done
            with config as the only local accessible value.
        """
        self._addConfigImpl(label, pipelineIR.ConfigIR(python=pythonString))

    def _addConfigImpl(self, label: str, newConfig: pipelineIR.ConfigIR):
        if label not in self._pipelineIR.tasks:
            raise LookupError(f"There are no tasks labeled '{label}' in the pipeline")
        self._pipelineIR.tasks[label].add_or_update_config(newConfig)

    def toFile(self, filename: str):
        self._pipelineIR.to_file(filename)

    def toExpandedPipeline(self) -> Generator[TaskDef]:
        """Returns a generator of TaskDefs which can be used to create quantum
        graphs.

        Returns
        -------
        generator : generator of `TaskDef`
            The generator returned will be the sorted iterator of tasks which
            are to be used in constructing a quantum graph.

        Raises
        ------
        NotImplementedError
            If a dataId is supplied in a config block. This is in place for
            future use
        """
        taskDefs = []
        for label, taskIR in self._pipelineIR.tasks.items():
            taskClass = doImport(taskIR.klass)
            taskName = taskClass.__qualname__
            config = taskClass.ConfigClass()
            overrides = ConfigOverrides()
            if self._pipelineIR.instrument is not None:
                overrides.addInstrumentOverride(self._pipelineIR.instrument, taskClass._DefaultName)
            if taskIR.config is not None:
                for configIR in taskIR.config:
                    if configIR.dataId is not None:
                        raise NotImplementedError("Specializing a config on a partial data id is not yet "
                                                  "supported in Pipeline definition")
                    # only apply override if it applies to everything
                    if configIR.dataId is None:
                        if configIR.file:
                            for configFile in configIR.file:
                                overrides.addFileOverride(os.path.expandvars(configFile))
                        if configIR.python is not None:
                            overrides.addPythonOverride(configIR.python)
                        for key, value in configIR.rest.items():
                            overrides.addValueOverride(key, value)
            overrides.applyTo(config)
            # This may need to be revisited
            config.validate()
            taskDefs.append(TaskDef(taskName=taskName, config=config, taskClass=taskClass, label=label))

        # lets evaluate the contracts
        if self._pipelineIR.contracts is not None:
            label_to_config = {x.label: x.config for x in taskDefs}
            for contract in self._pipelineIR.contracts:
                # execute this in its own line so it can raise a good error message if there was problems
                # with the eval
                success = eval(contract.contract, None, label_to_config)
                if not success:
                    extra_info = f": {contract.msg}" if contract.msg is not None else ""
                    raise pipelineIR.ContractError(f"Contract(s) '{contract.contract}' were not "
                                                   f"satisfied{extra_info}")

        yield from pipeTools.orderPipeline(taskDefs)

    def __len__(self):
        return len(self._pipelineIR.tasks)

    def __eq__(self, other: "Pipeline"):
        if not isinstance(other, Pipeline):
            return False
        return self._pipelineIR == other._pipelineIR


@dataclass(frozen=True)
class TaskDatasetTypes:
    """An immutable struct that extracts and classifies the dataset types used
    by a `PipelineTask`
    """

    initInputs: NamedValueSet[DatasetType]
    """Dataset types that are needed as inputs in order to construct this Task.

    Task-level `initInputs` may be classified as either
    `~PipelineDatasetTypes.initInputs` or
    `~PipelineDatasetTypes.initIntermediates` at the Pipeline level.
    """

    initOutputs: NamedValueSet[DatasetType]
    """Dataset types that may be written after constructing this Task.

    Task-level `initOutputs` may be classified as either
    `~PipelineDatasetTypes.initOutputs` or
    `~PipelineDatasetTypes.initIntermediates` at the Pipeline level.
    """

    inputs: NamedValueSet[DatasetType]
    """Dataset types that are regular inputs to this Task.

    If an input dataset needed for a Quantum cannot be found in the input
    collection(s) or produced by another Task in the Pipeline, that Quantum
    (and all dependent Quanta) will not be produced.

    Task-level `inputs` may be classified as either
    `~PipelineDatasetTypes.inputs` or `~PipelineDatasetTypes.intermediates`
    at the Pipeline level.
    """

    prerequisites: NamedValueSet[DatasetType]
    """Dataset types that are prerequisite inputs to this Task.

    Prerequisite inputs must exist in the input collection(s) before the
    pipeline is run, but do not constrain the graph - if a prerequisite is
    missing for a Quantum, `PrerequisiteMissingError` is raised.

    Prerequisite inputs are not resolved until the second stage of
    QuantumGraph generation.
    """

    outputs: NamedValueSet[DatasetType]
    """Dataset types that are produced by this Task.

    Task-level `outputs` may be classified as either
    `~PipelineDatasetTypes.outputs` or `~PipelineDatasetTypes.intermediates`
    at the Pipeline level.
    """

    @classmethod
    def fromTaskDef(cls, taskDef: TaskDef, *, registry: Registry) -> TaskDatasetTypes:
        """Extract and classify the dataset types from a single `PipelineTask`.

        Parameters
        ----------
        taskDef: `TaskDef`
            An instance of a `TaskDef` class for a particular `PipelineTask`.
        registry: `Registry`
            Registry used to construct normalized `DatasetType` objects and
            retrieve those that are incomplete.

        Returns
        -------
        types: `TaskDatasetTypes`
            The dataset types used by this task.
        """
        def makeDatasetTypesSet(connectionType, freeze=True):
            """Constructs a set of true `DatasetType` objects

            Parameters
            ----------
            connectionType : `str`
                Name of the connection type to produce a set for, corresponds
                to an attribute of type `list` on the connection class instance
            freeze : `bool`, optional
                If `True`, call `NamedValueSet.freeze` on the object returned.

            Returns
            -------
            datasetTypes : `NamedValueSet`
                A set of all datasetTypes which correspond to the input
                connection type specified in the connection class of this
                `PipelineTask`

            Notes
            -----
            This function is a closure over the variables ``registry`` and
            ``taskDef``.
            """
            datasetTypes = NamedValueSet()
            for c in iterConnections(taskDef.connections, connectionType):
                dimensions = set(getattr(c, 'dimensions', set()))
                if "skypix" in dimensions:
                    try:
                        datasetType = registry.getDatasetType(c.name)
                    except LookupError as err:
                        raise LookupError(
                            f"DatasetType '{c.name}' referenced by "
                            f"{type(taskDef.connections).__name__} uses 'skypix' as a dimension "
                            f"placeholder, but does not already exist in the registry.  "
                            f"Note that reference catalog names are now used as the dataset "
                            f"type name instead of 'ref_cat'."
                        ) from err
                    rest1 = set(registry.dimensions.extract(dimensions - set(["skypix"])).names)
                    rest2 = set(dim.name for dim in datasetType.dimensions
                                if not isinstance(dim, SkyPixDimension))
                    if rest1 != rest2:
                        raise ValueError(f"Non-skypix dimensions for dataset type {c.name} declared in "
                                         f"connections ({rest1}) are inconsistent with those in "
                                         f"registry's version of this dataset ({rest2}).")
                else:
                    # Component dataset types are not explicitly in the
                    # registry.  This complicates consistency checks with
                    # registry and requires we work out the composite storage
                    # class.
                    registryDatasetType = None
                    try:
                        registryDatasetType = registry.getDatasetType(c.name)
                    except KeyError:
                        compositeName, componentName = DatasetType.splitDatasetTypeName(c.name)
                        parentStorageClass = DatasetType.PlaceholderParentStorageClass \
                            if componentName else None
                        datasetType = c.makeDatasetType(
                            registry.dimensions,
                            parentStorageClass=parentStorageClass
                        )
                        registryDatasetType = datasetType
                    else:
                        datasetType = c.makeDatasetType(
                            registry.dimensions,
                            parentStorageClass=registryDatasetType.parentStorageClass
                        )

                    if registryDatasetType and datasetType != registryDatasetType:
                        raise ValueError(f"Supplied dataset type ({datasetType}) inconsistent with "
                                         f"registry definition ({registryDatasetType}) "
                                         f"for {taskDef.label}.")
                datasetTypes.add(datasetType)
            if freeze:
                datasetTypes.freeze()
            return datasetTypes

        # optionally add output dataset for metadata
        outputs = makeDatasetTypesSet("outputs", freeze=False)
        if taskDef.metadataDatasetName is not None:
            # Metadata is supposed to be of the PropertySet type, its dimensions
            # correspond to a task quantum
            dimensions = registry.dimensions.extract(taskDef.connections.dimensions)
            outputs |= {DatasetType(taskDef.metadataDatasetName, dimensions, "PropertySet")}
        outputs.freeze()

        return cls(
            initInputs=makeDatasetTypesSet("initInputs"),
            initOutputs=makeDatasetTypesSet("initOutputs"),
            inputs=makeDatasetTypesSet("inputs"),
            prerequisites=makeDatasetTypesSet("prerequisiteInputs"),
            outputs=outputs,
        )


@dataclass(frozen=True)
class PipelineDatasetTypes:
    """An immutable struct that classifies the dataset types used in a
    `Pipeline`.
    """

    initInputs: NamedValueSet[DatasetType]
    """Dataset types that are needed as inputs in order to construct the Tasks
    in this Pipeline.

    This does not include dataset types that are produced when constructing
    other Tasks in the Pipeline (these are classified as `initIntermediates`).
    """

    initOutputs: NamedValueSet[DatasetType]
    """Dataset types that may be written after constructing the Tasks in this
    Pipeline.

    This does not include dataset types that are also used as inputs when
    constructing other Tasks in the Pipeline (these are classified as
    `initIntermediates`).
    """

    initIntermediates: NamedValueSet[DatasetType]
    """Dataset types that are both used when constructing one or more Tasks
    in the Pipeline and produced as a side-effect of constructing another
    Task in the Pipeline.
    """

    inputs: NamedValueSet[DatasetType]
    """Dataset types that are regular inputs for the full pipeline.

    If an input dataset needed for a Quantum cannot be found in the input
    collection(s), that Quantum (and all dependent Quanta) will not be
    produced.
    """

    prerequisites: NamedValueSet[DatasetType]
    """Dataset types that are prerequisite inputs for the full Pipeline.

    Prerequisite inputs must exist in the input collection(s) before the
    pipeline is run, but do not constrain the graph - if a prerequisite is
    missing for a Quantum, `PrerequisiteMissingError` is raised.

    Prerequisite inputs are not resolved until the second stage of
    QuantumGraph generation.
    """

    intermediates: NamedValueSet[DatasetType]
    """Dataset types that are output by one Task in the Pipeline and consumed
    as inputs by one or more other Tasks in the Pipeline.
    """

    outputs: NamedValueSet[DatasetType]
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
    def fromPipeline(cls, pipeline, *, registry: Registry) -> PipelineDatasetTypes:
        """Extract and classify the dataset types from all tasks in a
        `Pipeline`.

        Parameters
        ----------
        pipeline: `Pipeline`
            An ordered collection of tasks that can be run together.
        registry: `Registry`
            Registry used to construct normalized `DatasetType` objects and
            retrieve those that are incomplete.

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
        allInputs = NamedValueSet()
        allOutputs = NamedValueSet()
        allInitInputs = NamedValueSet()
        allInitOutputs = NamedValueSet()
        prerequisites = NamedValueSet()
        byTask = dict()
        if isinstance(pipeline, Pipeline):
            pipeline = pipeline.toExpandedPipeline()
        for taskDef in pipeline:
            thisTask = TaskDatasetTypes.fromTaskDef(taskDef, registry=registry)
            allInitInputs |= thisTask.initInputs
            allInitOutputs |= thisTask.initOutputs
            allInputs |= thisTask.inputs
            prerequisites |= thisTask.prerequisites
            allOutputs |= thisTask.outputs
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
        intermediateComponents = NamedValueSet()
        intermediateComposites = NamedValueSet()
        outputNameMapping = {dsType.name: dsType for dsType in allOutputs}
        for dsType in allInputs:
            # get the name of a possible component
            name, component = dsType.nameAndComponent()
            # if there is a component name, that means this is a component
            # DatasetType, if there is an output which produces the parent of
            # this component, treat this input as an intermediate
            if component is not None:
                if name in outputNameMapping:
                    if outputNameMapping[name].dimensions != dsType.dimensions:
                        raise ValueError(f"Component dataset type {dsType.name} has different "
                                         f"dimensions ({dsType.dimensions}) than its parent "
                                         f"({outputNameMapping[name].dimensions}).")
                    composite = DatasetType(name, dsType.dimensions, outputNameMapping[name].storageClass,
                                            universe=registry.dimensions)
                    intermediateComponents.add(dsType)
                    intermediateComposites.add(composite)

        def checkConsistency(a: NamedValueSet, b: NamedValueSet):
            common = a.names & b.names
            for name in common:
                if a[name] != b[name]:
                    raise ValueError(f"Conflicting definitions for dataset type: {a[name]} != {b[name]}.")

        checkConsistency(allInitInputs, allInitOutputs)
        checkConsistency(allInputs, allOutputs)
        checkConsistency(allInputs, intermediateComposites)
        checkConsistency(allOutputs, intermediateComposites)

        def frozen(s: NamedValueSet) -> NamedValueSet:
            s.freeze()
            return s

        return cls(
            initInputs=frozen(allInitInputs - allInitOutputs),
            initIntermediates=frozen(allInitInputs & allInitOutputs),
            initOutputs=frozen(allInitOutputs - allInitInputs),
            inputs=frozen(allInputs - allOutputs - intermediateComponents),
            intermediates=frozen(allInputs & allOutputs | intermediateComponents),
            outputs=frozen(allOutputs - allInputs - intermediateComposites),
            prerequisites=frozen(prerequisites),
            byTask=MappingProxyType(byTask),  # MappingProxyType -> frozen view of dict for immutability
        )
