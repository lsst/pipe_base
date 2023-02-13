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

__all__ = ["Pipeline", "TaskDef", "TaskDatasetTypes", "PipelineDatasetTypes", "LabelSpecifier"]

import copy
import logging
import os
import re
import urllib.parse

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from dataclasses import dataclass
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Callable,
    ClassVar,
    Dict,
    Generator,
    Iterable,
    Iterator,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

# -----------------------------
#  Imports for other modules --
from lsst.daf.butler import DatasetType, NamedValueSet, Registry, SkyPixDimension
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name

from . import pipelineIR, pipeTools
from ._task_metadata import TaskMetadata
from .config import PipelineTaskConfig
from .configOverrides import ConfigOverrides
from .connections import iterConnections
from .connectionTypes import Input
from .pipelineTask import PipelineTask
from .task import _TASK_METADATA_TYPE

if TYPE_CHECKING:  # Imports needed only for type annotations; may be circular.
    from lsst.obs.base import Instrument
    from lsst.pex.config import Config

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)

# ------------------------
#  Exported definitions --
# ------------------------


@dataclass
class LabelSpecifier:
    """A structure to specify a subset of labels to load

    This structure may contain a set of labels to be used in subsetting a
    pipeline, or a beginning and end point. Beginning or end may be empty,
    in which case the range will be a half open interval. Unlike python
    iteration bounds, end bounds are *INCLUDED*. Note that range based
    selection is not well defined for pipelines that are not linear in nature,
    and correct behavior is not guaranteed, or may vary from run to run.
    """

    labels: Optional[Set[str]] = None
    begin: Optional[str] = None
    end: Optional[str] = None

    def __post_init__(self) -> None:
        if self.labels is not None and (self.begin or self.end):
            raise ValueError(
                "This struct can only be initialized with a labels set or a begin (and/or) end specifier"
            )


class TaskDef:
    """TaskDef is a collection of information about task needed by Pipeline.

    The information includes task name, configuration object and optional
    task class. This class is just a collection of attributes and it exposes
    all of them so that attributes could potentially be modified in place
    (e.g. if configuration needs extra overrides).

    Attributes
    ----------
    taskName : `str`, optional
        The fully-qualified `PipelineTask` class name.  If not provided,
        ``taskClass`` must be.
    config : `lsst.pipe.base.config.PipelineTaskConfig`, optional
        Instance of the configuration class corresponding to this task class,
        usually with all overrides applied. This config will be frozen.  If
        not provided, ``taskClass`` must be provided and
        ``taskClass.ConfigClass()`` will be used.
    taskClass : `type`, optional
        `PipelineTask` class object; if provided and ``taskName`` is as well,
        the caller guarantees that they are consistent.  If not provided,
        ``taskName`` is used to import the type.
    label : `str`, optional
        Task label, usually a short string unique in a pipeline.  If not
        provided, ``taskClass`` must be, and ``taskClass._DefaultName`` will
        be used.
    """

    def __init__(
        self,
        taskName: Optional[str] = None,
        config: Optional[PipelineTaskConfig] = None,
        taskClass: Optional[Type[PipelineTask]] = None,
        label: Optional[str] = None,
    ):
        if taskName is None:
            if taskClass is None:
                raise ValueError("At least one of `taskName` and `taskClass` must be provided.")
            taskName = get_full_type_name(taskClass)
        elif taskClass is None:
            taskClass = doImportType(taskName)
        if config is None:
            if taskClass is None:
                raise ValueError("`taskClass` must be provided if `config` is not.")
            config = taskClass.ConfigClass()
        if label is None:
            if taskClass is None:
                raise ValueError("`taskClass` must be provided if `label` is not.")
            label = taskClass._DefaultName
        self.taskName = taskName
        try:
            config.validate()
        except Exception:
            _LOG.error("Configuration validation failed for task %s (%s)", label, taskName)
            raise
        config.freeze()
        self.config = config
        self.taskClass = taskClass
        self.label = label
        self.connections = config.connections.ConnectionsClass(config=config)

    @property
    def configDatasetName(self) -> str:
        """Name of a dataset type for configuration of this task (`str`)"""
        return self.label + "_config"

    @property
    def metadataDatasetName(self) -> Optional[str]:
        """Name of a dataset type for metadata of this task, `None` if
        metadata is not to be saved (`str`)
        """
        if self.config.saveMetadata:
            return self.makeMetadataDatasetName(self.label)
        else:
            return None

    @classmethod
    def makeMetadataDatasetName(cls, label: str) -> str:
        """Construct the name of the dataset type for metadata for a task.

        Parameters
        ----------
        label : `str`
            Label for the task within its pipeline.

        Returns
        -------
        name : `str`
            Name of the task's metadata dataset type.
        """
        return f"{label}_metadata"

    @property
    def logOutputDatasetName(self) -> Optional[str]:
        """Name of a dataset type for log output from this task, `None` if
        logs are not to be saved (`str`)
        """
        if cast(PipelineTaskConfig, self.config).saveLogOutput:
            return self.label + "_log"
        else:
            return None

    def __str__(self) -> str:
        rep = "TaskDef(" + self.taskName
        if self.label:
            rep += ", label=" + self.label
        rep += ")"
        return rep

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskDef):
            return False
        # This does not consider equality of configs when determining equality
        # as config equality is a difficult thing to define. Should be updated
        # after DM-27847
        return self.taskClass == other.taskClass and self.label == other.label

    def __hash__(self) -> int:
        return hash((self.taskClass, self.label))

    @classmethod
    def _unreduce(cls, taskName: str, config: PipelineTaskConfig, label: str) -> TaskDef:
        """Custom callable for unpickling.

        All arguments are forwarded directly to the constructor; this
        trampoline is only needed because ``__reduce__`` callables can't be
        called with keyword arguments.
        """
        return cls(taskName=taskName, config=config, label=label)

    def __reduce__(self) -> Tuple[Callable[[str, PipelineTaskConfig, str], TaskDef], Tuple[str, Config, str]]:
        return (self._unreduce, (self.taskName, self.config, self.label))


class Pipeline:
    """A `Pipeline` is a representation of a series of tasks to run, and the
    configuration for those tasks.

    Parameters
    ----------
    description : `str`
        A description of that this pipeline does.
    """

    def __init__(self, description: str):
        pipeline_dict = {"description": description, "tasks": {}}
        self._pipelineIR = pipelineIR.PipelineIR(pipeline_dict)

    @classmethod
    def fromFile(cls, filename: str) -> Pipeline:
        """Load a pipeline defined in a pipeline yaml file.

        Parameters
        ----------
        filename: `str`
           A path that points to a pipeline defined in yaml format. This
           filename may also supply additional labels to be used in
           subsetting the loaded Pipeline. These labels are separated from
           the path by a \\#, and may be specified as a comma separated
           list, or a range denoted as beginning..end. Beginning or end may
           be empty, in which case the range will be a half open interval.
           Unlike python iteration bounds, end bounds are *INCLUDED*. Note
           that range based selection is not well defined for pipelines that
           are not linear in nature, and correct behavior is not guaranteed,
           or may vary from run to run.

        Returns
        -------
        pipeline: `Pipeline`
            The pipeline loaded from specified location with appropriate (if
            any) subsetting

        Notes
        -----
        This method attempts to prune any contracts that contain labels which
        are not in the declared subset of labels. This pruning is done using a
        string based matching due to the nature of contracts and may prune more
        than it should.
        """
        return cls.from_uri(filename)

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> Pipeline:
        """Load a pipeline defined in a pipeline yaml file at a location
        specified by a URI.

        Parameters
        ----------
        uri: convertible to `ResourcePath`
            If a string is supplied this should be a URI path that points to a
            pipeline defined in yaml format, either as a direct path to the
            yaml file, or as a directory containing a "pipeline.yaml" file (the
            form used by `write_to_uri` with ``expand=True``). This uri may
            also supply additional labels to be used in subsetting the loaded
            Pipeline.  These labels are separated from the path by a \\#, and
            may be specified as a comma separated list, or a range denoted as
            beginning..end. Beginning or end may be empty, in which case the
            range will be a half open interval. Unlike python iteration bounds,
            end bounds are *INCLUDED*. Note that range based selection is not
            well defined for pipelines that are not linear in nature, and
            correct behavior is not guaranteed, or may vary from run to run.
            The same specifiers can be used with a `ResourcePath` object, by
            being the sole contents in the fragments attribute.

        Returns
        -------
        pipeline: `Pipeline`
            The pipeline loaded from specified location with appropriate (if
            any) subsetting

        Notes
        -----
        This method attempts to prune any contracts that contain labels which
        are not in the declared subset of labels. This pruning is done using a
        string based matching due to the nature of contracts and may prune more
        than it should.
        """
        # Split up the uri and any labels that were supplied
        uri, label_specifier = cls._parse_file_specifier(uri)
        pipeline: Pipeline = cls.fromIR(pipelineIR.PipelineIR.from_uri(uri))

        # If there are labels supplied, only keep those
        if label_specifier is not None:
            pipeline = pipeline.subsetFromLabels(label_specifier)
        return pipeline

    def subsetFromLabels(self, labelSpecifier: LabelSpecifier) -> Pipeline:
        """Subset a pipeline to contain only labels specified in labelSpecifier

        Parameters
        ----------
        labelSpecifier : `labelSpecifier`
            Object containing labels that describes how to subset a pipeline.

        Returns
        -------
        pipeline : `Pipeline`
            A new pipeline object that is a subset of the old pipeline

        Raises
        ------
        ValueError
            Raised if there is an issue with specified labels

        Notes
        -----
        This method attempts to prune any contracts that contain labels which
        are not in the declared subset of labels. This pruning is done using a
        string based matching due to the nature of contracts and may prune more
        than it should.
        """
        # Labels supplied as a set
        if labelSpecifier.labels:
            labelSet = labelSpecifier.labels
        # Labels supplied as a range, first create a list of all the labels
        # in the pipeline sorted according to task dependency. Then only
        # keep labels that lie between the supplied bounds
        else:
            # Create a copy of the pipeline to use when assessing the label
            # ordering. Use a dict for fast searching while preserving order.
            # Remove contracts so they do not fail in the expansion step. This
            # is needed because a user may only configure the tasks they intend
            # to run, which may cause some contracts to fail if they will later
            # be dropped
            pipeline = copy.deepcopy(self)
            pipeline._pipelineIR.contracts = []
            labels = {taskdef.label: True for taskdef in pipeline.toExpandedPipeline()}

            # Verify the bounds are in the labels
            if labelSpecifier.begin is not None:
                if labelSpecifier.begin not in labels:
                    raise ValueError(
                        f"Beginning of range subset, {labelSpecifier.begin}, not found in pipeline definition"
                    )
            if labelSpecifier.end is not None:
                if labelSpecifier.end not in labels:
                    raise ValueError(
                        f"End of range subset, {labelSpecifier.end}, not found in pipeline definition"
                    )

            labelSet = set()
            for label in labels:
                if labelSpecifier.begin is not None:
                    if label != labelSpecifier.begin:
                        continue
                    else:
                        labelSpecifier.begin = None
                labelSet.add(label)
                if labelSpecifier.end is not None and label == labelSpecifier.end:
                    break
        return Pipeline.fromIR(self._pipelineIR.subset_from_labels(labelSet))

    @staticmethod
    def _parse_file_specifier(uri: ResourcePathExpression) -> Tuple[ResourcePath, Optional[LabelSpecifier]]:
        """Split appart a uri and any possible label subsets"""
        if isinstance(uri, str):
            # This is to support legacy pipelines during transition
            uri, num_replace = re.subn("[:](?!\\/\\/)", "#", uri)
            if num_replace:
                raise ValueError(
                    f"The pipeline file {uri} seems to use the legacy :"
                    " to separate labels, please use # instead."
                )
            if uri.count("#") > 1:
                raise ValueError("Only one set of labels is allowed when specifying a pipeline to load")
        # Everything else can be converted directly to ResourcePath.
        uri = ResourcePath(uri)
        label_subset = uri.fragment or None

        specifier: Optional[LabelSpecifier]
        if label_subset is not None:
            label_subset = urllib.parse.unquote(label_subset)
            args: Dict[str, Union[Set[str], str, None]]
            # labels supplied as a list
            if "," in label_subset:
                if ".." in label_subset:
                    raise ValueError(
                        "Can only specify a list of labels or a rangewhen loading a Pipline not both"
                    )
                args = {"labels": set(label_subset.split(","))}
            # labels supplied as a range
            elif ".." in label_subset:
                # Try to de-structure the labelSubset, this will fail if more
                # than one range is specified
                begin, end, *rest = label_subset.split("..")
                if rest:
                    raise ValueError("Only one range can be specified when loading a pipeline")
                args = {"begin": begin if begin else None, "end": end if end else None}
            # Assume anything else is a single label
            else:
                args = {"labels": {label_subset}}

            # MyPy doesn't like how cavalier kwarg construction is with types.
            specifier = LabelSpecifier(**args)  # type: ignore
        else:
            specifier = None

        return uri, specifier

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
    def fromPipeline(cls, pipeline: Pipeline) -> Pipeline:
        """Create a new pipeline by copying an already existing `Pipeline`.

        Parameters
        ----------
        pipeline: `Pipeline`
            An already created pipeline intermediate representation object

        Returns
        -------
        pipeline: `Pipeline`
        """
        return cls.fromIR(copy.deepcopy(pipeline._pipelineIR))

    def __str__(self) -> str:
        return str(self._pipelineIR)

    def mergePipeline(self, pipeline: Pipeline) -> None:
        """Merge another in-memory `Pipeline` object into this one.

        This merges another pipeline into this object, as if it were declared
        in the import block of the yaml definition of this pipeline. This
        modifies this pipeline in place.

        Parameters
        ----------
        pipeline : `Pipeline`
            The `Pipeline` object that is to be merged into this object.
        """
        self._pipelineIR.merge_pipelines((pipeline._pipelineIR,))

    def addLabelToSubset(self, subset: str, label: str) -> None:
        """Add a task label from the specified subset.

        Parameters
        ----------
        subset : `str`
            The labeled subset to modify
        label : `str`
            The task label to add to the specified subset.

        Raises
        ------
        ValueError
            Raised if the specified subset does not exist within the pipeline.
            Raised if the specified label does not exist within the pipeline.
        """
        if label not in self._pipelineIR.tasks:
            raise ValueError(f"Label {label} does not appear within the pipeline")
        if subset not in self._pipelineIR.labeled_subsets:
            raise ValueError(f"Subset {subset} does not appear within the pipeline")
        self._pipelineIR.labeled_subsets[subset].subset.add(label)

    def removeLabelFromSubset(self, subset: str, label: str) -> None:
        """Remove a task label from the specified subset.

        Parameters
        ----------
        subset : `str`
            The labeled subset to modify
        label : `str`
            The task label to remove from the specified subset.

        Raises
        ------
        ValueError
            Raised if the specified subset does not exist in the pipeline.
            Raised if the specified label does not exist within the specified
            subset.
        """
        if subset not in self._pipelineIR.labeled_subsets:
            raise ValueError(f"Subset {subset} does not appear within the pipeline")
        if label not in self._pipelineIR.labeled_subsets[subset].subset:
            raise ValueError(f"Label {label} does not appear within the pipeline")
        self._pipelineIR.labeled_subsets[subset].subset.remove(label)

    def findSubsetsWithLabel(self, label: str) -> set[str]:
        """Find any subsets which may contain the specified label.

        This function returns the name of subsets which return the specified
        label. May return an empty set if there are no subsets, or no subsets
        containing the specified label.

        Parameters
        ----------
        label : `str`
            The task label to use in membership check

        Returns
        -------
        subsets : `set` of `str`
            Returns a set (possibly empty) of subsets names which contain the
            specified label.

        Raises
        ------
        ValueError
            Raised if the specified label does not exist within this pipeline.
        """
        results = set()
        if label not in self._pipelineIR.tasks:
            raise ValueError(f"Label {label} does not appear within the pipeline")
        for subset in self._pipelineIR.labeled_subsets.values():
            if label in subset.subset:
                results.add(subset.label)
        return results

    def addInstrument(self, instrument: Union[Instrument, str]) -> None:
        """Add an instrument to the pipeline, or replace an instrument that is
        already defined.

        Parameters
        ----------
        instrument : `~lsst.daf.butler.instrument.Instrument` or `str`
            Either a derived class object of a `lsst.daf.butler.instrument` or
            a string corresponding to a fully qualified
            `lsst.daf.butler.instrument` name.
        """
        if isinstance(instrument, str):
            pass
        else:
            # TODO: assume that this is a subclass of Instrument, no type
            # checking
            instrument = get_full_type_name(instrument)
        self._pipelineIR.instrument = instrument

    def getInstrument(self) -> Optional[str]:
        """Get the instrument from the pipeline.

        Returns
        -------
        instrument : `str`, or None
            The fully qualified name of a `lsst.obs.base.Instrument` subclass,
            name, or None if the pipeline does not have an instrument.
        """
        return self._pipelineIR.instrument

    def addTask(self, task: Union[Type[PipelineTask], str], label: str) -> None:
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
            taskName = get_full_type_name(task)
        else:
            raise ValueError(
                "task must be either a child class of PipelineTask or a string containing"
                " a fully qualified name to one"
            )
        if not label:
            # in some cases (with command line-generated pipeline) tasks can
            # be defined without label which is not acceptable, use task
            # _DefaultName in that case
            if isinstance(task, str):
                task_class = doImportType(task)
            label = task_class._DefaultName
        self._pipelineIR.tasks[label] = pipelineIR.TaskIR(label, taskName)

    def removeTask(self, label: str) -> None:
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

    def addConfigOverride(self, label: str, key: str, value: object) -> None:
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

    def addConfigFile(self, label: str, filename: str) -> None:
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

    def addConfigPython(self, label: str, pythonString: str) -> None:
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

    def _addConfigImpl(self, label: str, newConfig: pipelineIR.ConfigIR) -> None:
        if label == "parameters":
            if newConfig.rest.keys() - self._pipelineIR.parameters.mapping.keys():
                raise ValueError("Cannot override parameters that are not defined in pipeline")
            self._pipelineIR.parameters.mapping.update(newConfig.rest)
            if newConfig.file:
                raise ValueError("Setting parameters section with config file is not supported")
            if newConfig.python:
                raise ValueError("Setting parameters section using python block in unsupported")
            return
        if label not in self._pipelineIR.tasks:
            raise LookupError(f"There are no tasks labeled '{label}' in the pipeline")
        self._pipelineIR.tasks[label].add_or_update_config(newConfig)

    def write_to_uri(self, uri: ResourcePathExpression) -> None:
        """Write the pipeline to a file or directory.

        Parameters
        ----------
        uri : convertible to `ResourcePath`
            URI to write to; may have any scheme with `ResourcePath` write
            support or no scheme for a local file/directory.  Should have a
            ``.yaml``.
        """
        self._pipelineIR.write_to_uri(uri)

    def toExpandedPipeline(self) -> Generator[TaskDef, None, None]:
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
        for label in self._pipelineIR.tasks:
            taskDefs.append(self._buildTaskDef(label))

        # lets evaluate the contracts
        if self._pipelineIR.contracts is not None:
            label_to_config = {x.label: x.config for x in taskDefs}
            for contract in self._pipelineIR.contracts:
                # execute this in its own line so it can raise a good error
                # message if there was problems with the eval
                success = eval(contract.contract, None, label_to_config)
                if not success:
                    extra_info = f": {contract.msg}" if contract.msg is not None else ""
                    raise pipelineIR.ContractError(
                        f"Contract(s) '{contract.contract}' were not satisfied{extra_info}"
                    )

        taskDefs = sorted(taskDefs, key=lambda x: x.label)
        yield from pipeTools.orderPipeline(taskDefs)

    def _buildTaskDef(self, label: str) -> TaskDef:
        if (taskIR := self._pipelineIR.tasks.get(label)) is None:
            raise NameError(f"Label {label} does not appear in this pipeline")
        taskClass: Type[PipelineTask] = doImportType(taskIR.klass)
        taskName = get_full_type_name(taskClass)
        config = taskClass.ConfigClass()
        overrides = ConfigOverrides()
        if self._pipelineIR.instrument is not None:
            overrides.addInstrumentOverride(self._pipelineIR.instrument, taskClass._DefaultName)
        if taskIR.config is not None:
            for configIR in (configIr.formatted(self._pipelineIR.parameters) for configIr in taskIR.config):
                if configIR.dataId is not None:
                    raise NotImplementedError(
                        "Specializing a config on a partial data id is not yet "
                        "supported in Pipeline definition"
                    )
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
        return TaskDef(taskName=taskName, config=config, taskClass=taskClass, label=label)

    def __iter__(self) -> Generator[TaskDef, None, None]:
        return self.toExpandedPipeline()

    def __getitem__(self, item: str) -> TaskDef:
        return self._buildTaskDef(item)

    def __len__(self) -> int:
        return len(self._pipelineIR.tasks)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Pipeline):
            return False
        elif self._pipelineIR == other._pipelineIR:
            # Shortcut: if the IR is the same, the expanded pipeline must be
            # the same as well.  But the converse is not true.
            return True
        else:
            self_expanded = {td.label: (td.taskClass,) for td in self}
            other_expanded = {td.label: (td.taskClass,) for td in other}
            if self_expanded != other_expanded:
                return False
        # After DM-27847, we should compare configuration here, or better,
        # delegated to TaskDef.__eq__ after making that compare configurations.
        raise NotImplementedError(
            "Pipelines cannot be compared because config instances cannot be compared; see DM-27847."
        )


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

    queryConstraints: NamedValueSet[DatasetType]
    """Regular inputs that should not be used as constraints on the initial
    QuantumGraph generation data ID query, according to their tasks
    (`NamedValueSet`).
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
    def fromTaskDef(
        cls,
        taskDef: TaskDef,
        *,
        registry: Registry,
        include_configs: bool = True,
        storage_class_mapping: Optional[Mapping[str, str]] = None,
    ) -> TaskDatasetTypes:
        """Extract and classify the dataset types from a single `PipelineTask`.

        Parameters
        ----------
        taskDef: `TaskDef`
            An instance of a `TaskDef` class for a particular `PipelineTask`.
        registry: `Registry`
            Registry used to construct normalized `DatasetType` objects and
            retrieve those that are incomplete.
        include_configs : `bool`, optional
            If `True` (default) include config dataset types as
            ``initOutputs``.
        storage_class_mapping : `Mapping` of `str` to `StorageClass`, optional
            If a taskdef contains a component dataset type that is unknown
            to the registry, its parent StorageClass will be looked up in this
            mapping if it is supplied. If the mapping does not contain the
            composite dataset type, or the mapping is not supplied an exception
            will be raised.

        Returns
        -------
        types: `TaskDatasetTypes`
            The dataset types used by this task.

        Raises
        ------
        ValueError
            Raised if dataset type connection definition differs from
            registry definition.
        LookupError
            Raised if component parent StorageClass could not be determined
            and storage_class_mapping does not contain the composite type, or
            is set to None.
        """

        def makeDatasetTypesSet(
            connectionType: str,
            is_input: bool,
            freeze: bool = True,
        ) -> NamedValueSet[DatasetType]:
            """Constructs a set of true `DatasetType` objects

            Parameters
            ----------
            connectionType : `str`
                Name of the connection type to produce a set for, corresponds
                to an attribute of type `list` on the connection class instance
            is_input : `bool`
                These are input dataset types, else they are output dataset
                types.
            freeze : `bool`, optional
                If `True`, call `NamedValueSet.freeze` on the object returned.

            Returns
            -------
            datasetTypes : `NamedValueSet`
                A set of all datasetTypes which correspond to the input
                connection type specified in the connection class of this
                `PipelineTask`

            Raises
            ------
            ValueError
                Raised if dataset type connection definition differs from
                registry definition.
            LookupError
                Raised if component parent StorageClass could not be determined
                and storage_class_mapping does not contain the composite type,
                or is set to None.

            Notes
            -----
            This function is a closure over the variables ``registry`` and
            ``taskDef``, and ``storage_class_mapping``.
            """
            datasetTypes = NamedValueSet[DatasetType]()
            for c in iterConnections(taskDef.connections, connectionType):
                dimensions = set(getattr(c, "dimensions", set()))
                if "skypix" in dimensions:
                    try:
                        datasetType = registry.getDatasetType(c.name)
                    except LookupError as err:
                        raise LookupError(
                            f"DatasetType '{c.name}' referenced by "
                            f"{type(taskDef.connections).__name__} uses 'skypix' as a dimension "
                            "placeholder, but does not already exist in the registry.  "
                            "Note that reference catalog names are now used as the dataset "
                            "type name instead of 'ref_cat'."
                        ) from err
                    rest1 = set(registry.dimensions.extract(dimensions - set(["skypix"])).names)
                    rest2 = set(
                        dim.name for dim in datasetType.dimensions if not isinstance(dim, SkyPixDimension)
                    )
                    if rest1 != rest2:
                        raise ValueError(
                            f"Non-skypix dimensions for dataset type {c.name} declared in "
                            f"connections ({rest1}) are inconsistent with those in "
                            f"registry's version of this dataset ({rest2})."
                        )
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
                        if componentName:
                            if storage_class_mapping is None or compositeName not in storage_class_mapping:
                                raise LookupError(
                                    "Component parent class cannot be determined, and "
                                    "composite name was not in storage class mapping, or no "
                                    "storage_class_mapping was supplied"
                                )
                            else:
                                parentStorageClass = storage_class_mapping[compositeName]
                        else:
                            parentStorageClass = None
                        datasetType = c.makeDatasetType(
                            registry.dimensions, parentStorageClass=parentStorageClass
                        )
                        registryDatasetType = datasetType
                    else:
                        datasetType = c.makeDatasetType(
                            registry.dimensions, parentStorageClass=registryDatasetType.parentStorageClass
                        )

                    if registryDatasetType and datasetType != registryDatasetType:
                        # The dataset types differ but first check to see if
                        # they are compatible before raising.
                        if is_input:
                            # This DatasetType must be compatible on get.
                            is_compatible = datasetType.is_compatible_with(registryDatasetType)
                        else:
                            # Has to be able to be converted to expect type
                            # on put.
                            is_compatible = registryDatasetType.is_compatible_with(datasetType)
                        if is_compatible:
                            # For inputs we want the pipeline to use the
                            # pipeline definition, for outputs it should use
                            # the registry definition.
                            if not is_input:
                                datasetType = registryDatasetType
                            _LOG.debug(
                                "Dataset types differ (task %s != registry %s) but are compatible"
                                " for %s in %s.",
                                datasetType,
                                registryDatasetType,
                                "input" if is_input else "output",
                                taskDef.label,
                            )
                        else:
                            try:
                                # Explicitly check for storage class just to
                                # make more specific message.
                                _ = datasetType.storageClass
                            except KeyError:
                                raise ValueError(
                                    "Storage class does not exist for supplied dataset type "
                                    f"{datasetType} for {taskDef.label}."
                                ) from None
                            raise ValueError(
                                f"Supplied dataset type ({datasetType}) inconsistent with "
                                f"registry definition ({registryDatasetType}) "
                                f"for {taskDef.label}."
                            )
                datasetTypes.add(datasetType)
            if freeze:
                datasetTypes.freeze()
            return datasetTypes

        # optionally add initOutput dataset for config
        initOutputs = makeDatasetTypesSet("initOutputs", is_input=False, freeze=False)
        if include_configs:
            initOutputs.add(
                DatasetType(
                    taskDef.configDatasetName,
                    registry.dimensions.empty,
                    storageClass="Config",
                )
            )
        initOutputs.freeze()

        # optionally add output dataset for metadata
        outputs = makeDatasetTypesSet("outputs", is_input=False, freeze=False)
        if taskDef.metadataDatasetName is not None:
            # Metadata is supposed to be of the TaskMetadata type, its
            # dimensions correspond to a task quantum.
            dimensions = registry.dimensions.extract(taskDef.connections.dimensions)

            # Allow the storage class definition to be read from the existing
            # dataset type definition if present.
            try:
                current = registry.getDatasetType(taskDef.metadataDatasetName)
            except KeyError:
                # No previous definition so use the default.
                storageClass = "TaskMetadata" if _TASK_METADATA_TYPE is TaskMetadata else "PropertySet"
            else:
                storageClass = current.storageClass.name

            outputs.update({DatasetType(taskDef.metadataDatasetName, dimensions, storageClass)})
        if taskDef.logOutputDatasetName is not None:
            # Log output dimensions correspond to a task quantum.
            dimensions = registry.dimensions.extract(taskDef.connections.dimensions)
            outputs.update({DatasetType(taskDef.logOutputDatasetName, dimensions, "ButlerLogRecords")})

        outputs.freeze()

        inputs = makeDatasetTypesSet("inputs", is_input=True)
        queryConstraints = NamedValueSet(
            inputs[c.name]
            for c in cast(Iterable[Input], iterConnections(taskDef.connections, "inputs"))
            if not c.deferGraphConstraint
        )

        return cls(
            initInputs=makeDatasetTypesSet("initInputs", is_input=True),
            initOutputs=initOutputs,
            inputs=inputs,
            queryConstraints=queryConstraints,
            prerequisites=makeDatasetTypesSet("prerequisiteInputs", is_input=True),
            outputs=outputs,
        )


@dataclass(frozen=True)
class PipelineDatasetTypes:
    """An immutable struct that classifies the dataset types used in a
    `Pipeline`.
    """

    packagesDatasetName: ClassVar[str] = "packages"
    """Name of a dataset type used to save package versions.
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

    queryConstraints: NamedValueSet[DatasetType]
    """Regular inputs that should be used as constraints on the initial
    QuantumGraph generation data ID query, according to their tasks
    (`NamedValueSet`).
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
    def fromPipeline(
        cls,
        pipeline: Union[Pipeline, Iterable[TaskDef]],
        *,
        registry: Registry,
        include_configs: bool = True,
        include_packages: bool = True,
    ) -> PipelineDatasetTypes:
        """Extract and classify the dataset types from all tasks in a
        `Pipeline`.

        Parameters
        ----------
        pipeline: `Pipeline` or `Iterable` [ `TaskDef` ]
            A collection of tasks that can be run together.
        registry: `Registry`
            Registry used to construct normalized `DatasetType` objects and
            retrieve those that are incomplete.
        include_configs : `bool`, optional
            If `True` (default) include config dataset types as
            ``initOutputs``.
        include_packages : `bool`, optional
            If `True` (default) include the dataset type for software package
            versions in ``initOutputs``.

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
        allInputs = NamedValueSet[DatasetType]()
        allOutputs = NamedValueSet[DatasetType]()
        allInitInputs = NamedValueSet[DatasetType]()
        allInitOutputs = NamedValueSet[DatasetType]()
        prerequisites = NamedValueSet[DatasetType]()
        queryConstraints = NamedValueSet[DatasetType]()
        byTask = dict()
        if include_packages:
            allInitOutputs.add(
                DatasetType(
                    cls.packagesDatasetName,
                    registry.dimensions.empty,
                    storageClass="Packages",
                )
            )
        # create a list of TaskDefs in case the input is a generator
        pipeline = list(pipeline)

        # collect all the output dataset types
        typeStorageclassMap: Dict[str, str] = {}
        for taskDef in pipeline:
            for outConnection in iterConnections(taskDef.connections, "outputs"):
                typeStorageclassMap[outConnection.name] = outConnection.storageClass

        for taskDef in pipeline:
            thisTask = TaskDatasetTypes.fromTaskDef(
                taskDef,
                registry=registry,
                include_configs=include_configs,
                storage_class_mapping=typeStorageclassMap,
            )
            allInitInputs.update(thisTask.initInputs)
            allInitOutputs.update(thisTask.initOutputs)
            allInputs.update(thisTask.inputs)
            # Inputs are query constraints if any task considers them a query
            # constraint.
            queryConstraints.update(thisTask.queryConstraints)
            prerequisites.update(thisTask.prerequisites)
            allOutputs.update(thisTask.outputs)
            byTask[taskDef.label] = thisTask
        if not prerequisites.isdisjoint(allInputs):
            raise ValueError(
                "{} marked as both prerequisites and regular inputs".format(
                    {dt.name for dt in allInputs & prerequisites}
                )
            )
        if not prerequisites.isdisjoint(allOutputs):
            raise ValueError(
                "{} marked as both prerequisites and outputs".format(
                    {dt.name for dt in allOutputs & prerequisites}
                )
            )
        # Make sure that components which are marked as inputs get treated as
        # intermediates if there is an output which produces the composite
        # containing the component
        intermediateComponents = NamedValueSet[DatasetType]()
        intermediateComposites = NamedValueSet[DatasetType]()
        outputNameMapping = {dsType.name: dsType for dsType in allOutputs}
        for dsType in allInputs:
            # get the name of a possible component
            name, component = dsType.nameAndComponent()
            # if there is a component name, that means this is a component
            # DatasetType, if there is an output which produces the parent of
            # this component, treat this input as an intermediate
            if component is not None:
                # This needs to be in this if block, because someone might have
                # a composite that is a pure input from existing data
                if name in outputNameMapping:
                    intermediateComponents.add(dsType)
                    intermediateComposites.add(outputNameMapping[name])

        def checkConsistency(a: NamedValueSet, b: NamedValueSet) -> None:
            common = a.names & b.names
            for name in common:
                # Any compatibility is allowed. This function does not know
                # if a dataset type is to be used for input or output.
                if not (a[name].is_compatible_with(b[name]) or b[name].is_compatible_with(a[name])):
                    raise ValueError(f"Conflicting definitions for dataset type: {a[name]} != {b[name]}.")

        checkConsistency(allInitInputs, allInitOutputs)
        checkConsistency(allInputs, allOutputs)
        checkConsistency(allInputs, intermediateComposites)
        checkConsistency(allOutputs, intermediateComposites)

        def frozen(s: AbstractSet[DatasetType]) -> NamedValueSet[DatasetType]:
            assert isinstance(s, NamedValueSet)
            s.freeze()
            return s

        inputs = frozen(allInputs - allOutputs - intermediateComponents)

        return cls(
            initInputs=frozen(allInitInputs - allInitOutputs),
            initIntermediates=frozen(allInitInputs & allInitOutputs),
            initOutputs=frozen(allInitOutputs - allInitInputs),
            inputs=inputs,
            queryConstraints=frozen(queryConstraints & inputs),
            # If there are storage class differences in inputs and outputs
            # the intermediates have to choose priority. Here choose that
            # inputs to tasks much match the requested storage class by
            # applying the inputs over the top of the outputs.
            intermediates=frozen(allOutputs & allInputs | intermediateComponents),
            outputs=frozen(allOutputs - allInputs - intermediateComposites),
            prerequisites=frozen(prerequisites),
            byTask=MappingProxyType(byTask),  # MappingProxyType -> frozen view of dict for immutability
        )

    @classmethod
    def initOutputNames(
        cls,
        pipeline: Union[Pipeline, Iterable[TaskDef]],
        *,
        include_configs: bool = True,
        include_packages: bool = True,
    ) -> Iterator[str]:
        """Return the names of dataset types ot task initOutputs, Configs,
        and package versions for a pipeline.

        Parameters
        ----------
        pipeline: `Pipeline` or `Iterable` [ `TaskDef` ]
            A `Pipeline` instance or collection of `TaskDef` instances.
        include_configs : `bool`, optional
            If `True` (default) include config dataset types.
        include_packages : `bool`, optional
            If `True` (default) include the dataset type for package versions.

        Yields
        ------
        datasetTypeName : `str`
            Name of the dataset type.
        """
        if include_packages:
            # Package versions dataset type
            yield cls.packagesDatasetName

        if isinstance(pipeline, Pipeline):
            pipeline = pipeline.toExpandedPipeline()

        for taskDef in pipeline:
            # all task InitOutputs
            for name in taskDef.connections.initOutputs:
                attribute = getattr(taskDef.connections, name)
                yield attribute.name

            # config dataset name
            if include_configs:
                yield taskDef.configDatasetName
