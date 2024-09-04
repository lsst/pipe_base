# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

"""Module defining Pipeline class and related methods."""

from __future__ import annotations

__all__ = ["LabelSpecifier", "Pipeline", "TaskDef"]

import copy
import logging
import re
import urllib.parse

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from collections.abc import Callable, Set
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

# -----------------------------
#  Imports for other modules --
# -----------------------------
from lsst.daf.butler import DataCoordinate, DimensionUniverse, Registry
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name

from . import automatic_connection_constants as acc
from . import pipeline_graph, pipelineIR
from ._instrument import Instrument as PipeBaseInstrument
from .config import PipelineTaskConfig
from .connections import PipelineTaskConnections
from .pipelineTask import PipelineTask

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
    """A structure to specify a subset of labels to load.

    This structure may contain a set of labels to be used in subsetting a
    pipeline, or a beginning and end point. Beginning or end may be empty, in
    which case the range will be a half open interval. Unlike python iteration
    bounds, end bounds are *INCLUDED*.

    There are multiple potential definitions of range-based slicing for graphs
    that are not a simple linear sequence.  The definition used here is the
    intersection of the tasks downstream of ``begin`` and the tasks upstream of
    ``end``, i.e. tasks with no dependency relationship to a bounding task are
    not included.
    """

    labels: set[str] | None = None
    begin: str | None = None
    end: str | None = None

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

    Parameters
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
    connections : `PipelineTaskConnections`, optional
        Object that describes the dataset types used by the task.  If not
        provided, one will be constructed from the given configuration.  If
        provided, it is assumed that ``config`` has already been validated
        and frozen.
    """

    def __init__(
        self,
        taskName: str | None = None,
        config: PipelineTaskConfig | None = None,
        taskClass: type[PipelineTask] | None = None,
        label: str | None = None,
        connections: PipelineTaskConnections | None = None,
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
        if connections is None:
            # If we don't have connections yet, assume the config hasn't been
            # validated yet.
            try:
                config.validate()
            except Exception:
                _LOG.error("Configuration validation failed for task %s (%s)", label, taskName)
                raise
            config.freeze()
            connections = config.connections.ConnectionsClass(config=config)
        self.config = config
        self.taskClass = taskClass
        self.label = label
        self.connections = connections

    @property
    def configDatasetName(self) -> str:
        """Name of a dataset type for configuration of this task (`str`)."""
        return acc.CONFIG_INIT_OUTPUT_TEMPLATE.format(label=self.label)

    @property
    def metadataDatasetName(self) -> str:
        """Name of a dataset type for metadata of this task (`str`)."""
        return self.makeMetadataDatasetName(self.label)

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
        return acc.METADATA_OUTPUT_TEMPLATE.format(label=label)

    @property
    def logOutputDatasetName(self) -> str | None:
        """Name of a dataset type for log output from this task, `None` if
        logs are not to be saved (`str`).
        """
        if self.config.saveLogOutput:
            return acc.LOG_OUTPUT_TEMPLATE.format(label=self.label)
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
        """Unpickle pickle. Custom callable for unpickling.

        All arguments are forwarded directly to the constructor; this
        trampoline is only needed because ``__reduce__`` callables can't be
        called with keyword arguments.
        """
        return cls(taskName=taskName, config=config, label=label)

    def __reduce__(self) -> tuple[Callable[[str, PipelineTaskConfig, str], TaskDef], tuple[str, Config, str]]:
        return (self._unreduce, (self.taskName, self.config, self.label))


class Pipeline:
    """A `Pipeline` is a representation of a series of tasks to run, and the
    configuration for those tasks.

    Parameters
    ----------
    description : `str`
        A description of that this pipeline does.
    """

    PipelineSubsetCtrl = pipelineIR.PipelineSubsetCtrl

    def __init__(self, description: str):
        pipeline_dict = {"description": description, "tasks": {}}
        self._pipelineIR = pipelineIR.PipelineIR(pipeline_dict)

    @classmethod
    def fromFile(cls, filename: str) -> Pipeline:
        """Load a pipeline defined in a pipeline yaml file.

        Parameters
        ----------
        filename : `str`
            A path that points to a pipeline defined in yaml format. This
            filename may also supply additional labels to be used in
            subsetting the loaded Pipeline. These labels are separated from
            the path by a ``#``, and may be specified as a comma separated
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
            any) subsetting.

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
        uri : convertible to `~lsst.resources.ResourcePath`
            If a string is supplied this should be a URI path that points to a
            pipeline defined in yaml format, either as a direct path to the
            yaml file, or as a directory containing a ``pipeline.yaml`` file
            the form used by `write_to_uri` with ``expand=True``). This uri may
            also supply additional labels to be used in subsetting the loaded
            `Pipeline`. These labels are separated from the path by a ``#``,
            and may be specified as a comma separated list, or a range denoted
            as beginning..end. Beginning or end may be empty, in which case the
            range will be a half open interval. Unlike python iteration bounds,
            end bounds are *INCLUDED*. Note that range based selection is not
            well defined for pipelines that are not linear in nature, and
            correct behavior is not guaranteed, or may vary from run to run.
            The same specifiers can be used with a
            `~lsst.resources.ResourcePath` object, by being the sole contents
            in the fragments attribute.

        Returns
        -------
        pipeline : `Pipeline`
            The pipeline loaded from specified location with appropriate (if
            any) subsetting.

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

    def subsetFromLabels(
        self,
        labelSpecifier: LabelSpecifier,
        subsetCtrl: pipelineIR.PipelineSubsetCtrl = PipelineSubsetCtrl.DROP,
    ) -> Pipeline:
        """Subset a pipeline to contain only labels specified in
        ``labelSpecifier``.

        Parameters
        ----------
        labelSpecifier : `labelSpecifier`
            Object containing labels that describes how to subset a pipeline.
        subsetCtrl : `PipelineSubsetCtrl`
            Control object which decides how subsets with missing labels are
            handled. Setting to `PipelineSubsetCtrl.DROP` (the default) will
            cause any subsets that have labels which are not in the set of all
            task labels to be dropped. Setting to `PipelineSubsetCtrl.EDIT`
            will cause the subset to instead be edited to remove the
            nonexistent label.

        Returns
        -------
        pipeline : `Pipeline`
            A new pipeline object that is a subset of the old pipeline.

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
            graph = pipeline.to_graph()

            # Verify the bounds are in the labels
            if labelSpecifier.begin is not None and labelSpecifier.begin not in graph.tasks:
                raise ValueError(
                    f"Beginning of range subset, {labelSpecifier.begin}, not found in pipeline definition"
                )
            if labelSpecifier.end is not None and labelSpecifier.end not in graph.tasks:
                raise ValueError(
                    f"End of range subset, {labelSpecifier.end}, not found in pipeline definition"
                )

            labelSet = set(graph.tasks.between(labelSpecifier.begin, labelSpecifier.end))
        return Pipeline.fromIR(self._pipelineIR.subset_from_labels(labelSet, subsetCtrl))

    @staticmethod
    def _parse_file_specifier(uri: ResourcePathExpression) -> tuple[ResourcePath, LabelSpecifier | None]:
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

        specifier: LabelSpecifier | None
        if label_subset is not None:
            label_subset = urllib.parse.unquote(label_subset)
            args: dict[str, set[str] | str | None]
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
            A string that is formatted according like a pipeline document.

        Returns
        -------
        pipeline: `Pipeline`
            The new pipeline.
        """
        pipeline = cls.fromIR(pipelineIR.PipelineIR.from_string(pipeline_string))
        return pipeline

    @classmethod
    def fromIR(cls, deserialized_pipeline: pipelineIR.PipelineIR) -> Pipeline:
        """Create a pipeline from an already created `PipelineIR` object.

        Parameters
        ----------
        deserialized_pipeline : `PipelineIR`
            An already created pipeline intermediate representation object.

        Returns
        -------
        pipeline: `Pipeline`
            The new pipeline.
        """
        pipeline = cls.__new__(cls)
        pipeline._pipelineIR = deserialized_pipeline
        return pipeline

    @classmethod
    def fromPipeline(cls, pipeline: Pipeline) -> Pipeline:
        """Create a new pipeline by copying an already existing `Pipeline`.

        Parameters
        ----------
        pipeline : `Pipeline`
            An already created pipeline intermediate representation object.

        Returns
        -------
        pipeline: `Pipeline`
           The new pipeline.
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
            The labeled subset to modify.
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
            The labeled subset to modify.
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
            The task label to use in membership check.

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

    @property
    def task_labels(self) -> Set[str]:
        """Labels of all tasks in the pipelines.

        For simple pipelines with no imports, iteration over this set will
        match the order in which tasks are defined in the pipeline file.  In
        all other cases the order is unspecified but deterministic.  It is not
        dependency-ordered (use ``to_graph().tasks.keys()`` for that).
        """
        return self._pipelineIR.tasks.keys()

    @property
    def subsets(self) -> MappingProxyType[str, set]:
        """Returns a `MappingProxyType` where the keys are the labels of
        labeled subsets in the `Pipeline` and the values are the set of task
        labels contained within that subset.
        """
        return MappingProxyType(
            {label: subsetIr.subset for label, subsetIr in self._pipelineIR.labeled_subsets.items()}
        )

    def addLabeledSubset(self, label: str, description: str, taskLabels: set[str]) -> None:
        """Add a new labeled subset to the `Pipeline`.

        Parameters
        ----------
        label : `str`
            The label to assign to the subset.
        description : `str`
            A description of what the subset is for.
        taskLabels : `set` [`str`]
            The set of task labels to be associated with the labeled subset.

        Raises
        ------
        ValueError
            Raised if label already exists in the `Pipeline`.
            Raised if a task label is not found within the `Pipeline`.
        """
        if label in self._pipelineIR.labeled_subsets.keys():
            raise ValueError(f"Subset label {label} is already found within the Pipeline")
        if extra := (taskLabels - self._pipelineIR.tasks.keys()):
            raise ValueError(f"Task labels {extra} were not found within the Pipeline")
        self._pipelineIR.labeled_subsets[label] = pipelineIR.LabeledSubset(label, taskLabels, description)

    def removeLabeledSubset(self, label: str) -> None:
        """Remove a labeled subset from the `Pipeline`.

        Parameters
        ----------
        label : `str`
            The label of the subset to remove from the `Pipeline`.

        Raises
        ------
        ValueError
            Raised if the label is not found within the `Pipeline`.
        """
        if label not in self._pipelineIR.labeled_subsets.keys():
            raise ValueError(f"Subset label {label} was not found in the pipeline")
        self._pipelineIR.labeled_subsets.pop(label)

    def addInstrument(self, instrument: Instrument | str) -> None:
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

    def getInstrument(self) -> str | None:
        """Get the instrument from the pipeline.

        Returns
        -------
        instrument : `str`, or None
            The fully qualified name of a `lsst.obs.base.Instrument` subclass,
            name, or None if the pipeline does not have an instrument.
        """
        return self._pipelineIR.instrument

    def get_data_id(self, universe: DimensionUniverse) -> DataCoordinate:
        """Return a data ID with all dimension constraints embedded in the
        pipeline.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Object that defines all dimensions.

        Returns
        -------
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID with all dimension constraints embedded in the
            pipeline.
        """
        instrument_class_name = self._pipelineIR.instrument
        if instrument_class_name is not None:
            instrument_class = cast(PipeBaseInstrument, doImportType(instrument_class_name))
            if instrument_class is not None:
                return DataCoordinate.standardize(instrument=instrument_class.getName(), universe=universe)
        return DataCoordinate.make_empty(universe)

    def addTask(self, task: type[PipelineTask] | str, label: str) -> None:
        """Add a new task to the pipeline, or replace a task that is already
        associated with the supplied label.

        Parameters
        ----------
        task : `PipelineTask` or `str`
            Either a derived class object of a `PipelineTask` or a string
            corresponding to a fully qualified `PipelineTask` name.
        label : `str`
            A label that is used to identify the `PipelineTask` being added.
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
                task_class = cast(PipelineTask, doImportType(task))
                label = task_class._DefaultName
        self._pipelineIR.tasks[label] = pipelineIR.TaskIR(label, taskName)

    def removeTask(self, label: str) -> None:
        """Remove a task from the pipeline.

        Parameters
        ----------
        label : `str`
            The label used to identify the task that is to be removed.

        Raises
        ------
        KeyError
            If no task with that label exists in the pipeline.
        """
        self._pipelineIR.tasks.pop(label)

    def addConfigOverride(self, label: str, key: str, value: object) -> None:
        """Apply single config override.

        Parameters
        ----------
        label : `str`
            Label of the task.
        key : `str`
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
            modify.
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
        pythonString : `str`
            A string which is valid python code to be executed. This is done
            with config as the only local accessible value.
        """
        self._addConfigImpl(label, pipelineIR.ConfigIR(python=pythonString))

    def _addConfigImpl(self, label: str, newConfig: pipelineIR.ConfigIR) -> None:
        if label == "parameters":
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
        uri : convertible to `~lsst.resources.ResourcePath`
            URI to write to; may have any scheme with
            `~lsst.resources.ResourcePath` write support or no scheme for a
            local file/directory.  Should have a ``.yaml`` extension.
        """
        self._pipelineIR.write_to_uri(uri)

    def to_graph(
        self, registry: Registry | None = None, visualization_only: bool = False
    ) -> pipeline_graph.PipelineGraph:
        """Construct a pipeline graph from this pipeline.

        Constructing a graph applies all configuration overrides, freezes all
        configuration, checks all contracts, and checks for dataset type
        consistency between tasks (as much as possible without access to a data
        repository).  It cannot be reversed.

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`, optional
            Data repository client.  If provided, the graph's dataset types
            and dimensions will be resolved (see `PipelineGraph.resolve`).
        visualization_only : `bool`, optional
            Resolve the graph as well as possible even when dimensions and
            storage classes cannot really be determined.  This can include
            using the ``universe.commonSkyPix`` as the assumed dimensions of
            connections that use the "skypix" placeholder and using "<UNKNOWN>"
            as a storage class name (which will fail if the storage class
            itself is ever actually loaded).

        Returns
        -------
        graph : `pipeline_graph.PipelineGraph`
            Representation of the pipeline as a graph.
        """
        instrument_class_name = self._pipelineIR.instrument
        data_id = {}
        if instrument_class_name is not None:
            instrument_class: type[Instrument] = doImportType(instrument_class_name)
            if instrument_class is not None:
                data_id["instrument"] = instrument_class.getName()
        graph = pipeline_graph.PipelineGraph(data_id=data_id)
        graph.description = self._pipelineIR.description
        for label in self._pipelineIR.tasks:
            self._add_task_to_graph(label, graph)
        if self._pipelineIR.contracts is not None:
            label_to_config = {x.label: x.config for x in graph.tasks.values()}
            for contract in self._pipelineIR.contracts:
                # execute this in its own line so it can raise a good error
                # message if there was problems with the eval
                success = eval(contract.contract, None, label_to_config)
                if not success:
                    extra_info = f": {contract.msg}" if contract.msg is not None else ""
                    raise pipelineIR.ContractError(
                        f"Contract(s) '{contract.contract}' were not satisfied{extra_info}"
                    )
        for label, subset in self._pipelineIR.labeled_subsets.items():
            graph.add_task_subset(
                label, subset.subset, subset.description if subset.description is not None else ""
            )
        for step_ir in self._pipelineIR.steps:
            graph.steps.append(step_ir.label, step_ir.dimensions)
        if registry is not None or visualization_only:
            graph.resolve(registry=registry, visualization_only=visualization_only)
        else:
            graph.sort()
        return graph

    def _add_task_to_graph(self, label: str, graph: pipeline_graph.PipelineGraph) -> None:
        """Add a single task from this pipeline to a pipeline graph that is
        under construction.

        Parameters
        ----------
        label : `str`
            Label for the task to be added.
        graph : `pipeline_graph.PipelineGraph`
            Graph to add the task to.
        """
        if (taskIR := self._pipelineIR.tasks.get(label)) is None:
            raise NameError(f"Label {label} does not appear in this pipeline")
        taskClass: type[PipelineTask] = doImportType(taskIR.klass)
        config = taskClass.ConfigClass()
        instrument: PipeBaseInstrument | None = None
        if (instrumentName := self._pipelineIR.instrument) is not None:
            instrument_cls: type = doImportType(instrumentName)
            instrument = instrument_cls()
        config.applyConfigOverrides(
            instrument,
            getattr(taskClass, "_DefaultName", ""),
            taskIR.config,
            self._pipelineIR.parameters,
            label,
        )
        graph.add_task(label, taskClass, config)

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
            # Compare as much as we can (task classes and their edges).
            if self.to_graph().diff_tasks(other.to_graph()):
                return False
        # After DM-27847, we should compare configuration here.
        raise NotImplementedError(
            "Pipelines cannot be compared because config instances cannot be compared; see DM-27847."
        )
