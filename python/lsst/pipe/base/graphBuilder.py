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

"""Module defining GraphBuilder class and related methods.
"""

__all__ = ['GraphBuilder']

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import copy
import itertools
from collections import ChainMap
from dataclasses import dataclass
from typing import Set, List, Dict, Optional, Iterable
import logging

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .pipeline import PipelineDatasetTypes, TaskDatasetTypes, Pipeline, TaskDef
from .graph import QuantumGraph, QuantumGraphTaskNodes
from lsst.daf.butler import Quantum, DatasetRef, DimensionGraph, DataId, DimensionUniverse, DatasetType
from lsst.daf.butler.core.utils import NamedKeyDict
from lsst.daf.butler.sql import DataIdQueryBuilder, SingleDatasetQueryBuilder

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


@dataclass
class _DatasetScaffolding:
    """Helper class aggregating information about a `DatasetType`, used when
    constructing a `QuantumGraph`.

    `_DatasetScaffolding` does not hold the `DatasetType` instance itself
    because it is usually used as the value type in `_DatasetScaffoldingDict`,
    which uses `DatasetType` instances as keys.

    See `_PipelineScaffolding` for a top-down description of the full
    scaffolding data structure.

    Parameters
    ----------
    dimensions : `DimensionGraph`
        Dimensions of the `DatasetType`, expanded to include implied
        dependencies.
    """
    def __init__(self, dimensions: DimensionGraph):
        self.dimensions = dimensions
        self.producer = None
        self.consumers = {}
        self.dataIds = set()
        self.refs = []

    __slots__ = ("dimensions", "producer", "consumers", "dataIds", "refs")

    dimensions: DimensionGraph
    """The dimensions of the dataset type, expanded to included implied
    dependencies.

    Set during `_PipelineScaffolding` construction.
    """

    producer: Optional[_TaskScaffolding]
    """The scaffolding objects for the Task that produces this dataset.

    Set during `_PipelineScaffolding` construction.
    """

    consumers: Dict[str, _TaskScaffolding]
    """The scaffolding objects for the Tasks that consume this dataset,
    keyed by their label in the `Pipeline`.

    Set during `_PipelineScaffolding` construction.
    """

    dataIds: Set[DataId]
    """Data IDs for all instances of this dataset type in the graph.

    These data IDs cover the full set of implied-expanded dimensions (i.e.
    the `dimensions` attribute of this instance), which is a supserset of the
    dimensions used in `DatasetRef` instances (e.g. in ``refs``).

    Populated after construction by `_PipelineScaffolding.fillDataIds`.
    """

    refs: List[DatasetRef]
    """References for all instances of this dataset type in the graph.

    Populated after construction by `_PipelineScaffolding.fillDatasetRefs`.
    """


class _DatasetScaffoldingDict(NamedKeyDict):
    """Custom dictionary that maps `DatasetType` to `_DatasetScaffolding`.

    See `_PipelineScaffolding` for a top-down description of the full
    scaffolding data structure.

    Parameters
    ----------
    args
        Positional arguments are forwarded to the `dict` constructor.
    universe : `DimensionUniverse`
        Universe of all possible dimensions.
    """
    def __init__(self, *args, universe: DimensionGraph):
        super().__init__(*args)
        self.universe = universe

    @classmethod
    def fromDatasetTypes(cls, datasetTypes: Iterable[DatasetType], *,
                         universe: DimensionUniverse) -> _DatasetScaffoldingDict:
        """Construct a a dictionary from a flat iterable of `DatasetType` keys.

        Parameters
        ----------
        datasetTypes : `iterable` of `DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be
            constructed from the dimensions of the keys.
        universe : `DimensionUniverse`
            Universe of all possible dimensions.

        Returns
        -------
        dictionary : `_DatasetScaffoldingDict`
            A new dictionary instance.
        """
        return cls(((datasetType, _DatasetScaffolding(datasetType.dimensions.implied(only=False)))
                    for datasetType in datasetTypes),
                   universe=universe)

    @classmethod
    def fromSubset(cls, datasetTypes: Iterable[DatasetType], first: _DatasetScaffoldingDict,
                   *rest) -> _DatasetScaffoldingDict:
        """Return a new dictionary by extracting items corresponding to the
        given keys from one or more existing dictionaries.

        Parameters
        ----------
        datasetTypes : `iterable` of `DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be obtained
            by lookups against ``first`` and ``rest``.
        first : `_DatasetScaffoldingDict`
            Another dictionary from which to extract values.
        rest
            Additional dictionaries from which to extract values.

        Returns
        -------
        dictionary : `_DatasetScaffoldingDict`
            A new dictionary instance.
        """
        combined = ChainMap(first, *rest)
        return cls(((datasetType, combined[datasetType]) for datasetType in datasetTypes),
                   universe=first.universe)

    @property
    def dimensions(self) -> DimensionGraph:
        """The union of all dimensions used by all dataset types in this
        dictionary, including implied dependencies (`DimensionGraph`).
        """
        base = self.universe.empty
        if len(self) == 0:
            return base
        return base.union(*(scaffolding.dimensions for scaffolding in self.values()), implied=True)

    def unpackRefs(self) -> NamedKeyDict:
        """Unpack nested single-element `DatasetRef` lists into a new
        dictionary.

        This method assumes that each `_DatasetScaffolding.refs` list contains
        exactly one `DatasetRef`, as is the case for all "init" datasets.

        Returns
        -------
        dictionary : `NamedKeyDict`
            Dictionary mapping `DatasetType` to `DatasetRef`, with both
            `DatasetType` instances and string names usable as keys.
        """
        return NamedKeyDict((datasetType, scaffolding.refs[0]) for datasetType, scaffolding in self.items())


@dataclass
class _TaskScaffolding:
    """Helper class aggregating information about a `PipelineTask`, used when
    constructing a `QuantumGraph`.

    See `_PipelineScaffolding` for a top-down description of the full
    scaffolding data structure.

    Parameters
    ----------
    taskDef : `TaskDef`
        Data structure that identifies the task class and its config.
    parent : `_PipelineScaffolding`
        The parent data structure that will hold the instance being
        constructed.
    datasetTypes : `TaskDatasetTypes`
        Data structure that categorizes the dataset types used by this task.

    Raises
    ------
    GraphBuilderError
        Raised if the task's dimensions are not a subset of the union of the
        pipeline's dataset dimensions.
    """
    def __init__(self, taskDef: TaskDef, parent: _PipelineScaffolding, datasetTypes: TaskDatasetTypes):
        universe = parent.dimensions.universe
        self.taskDef = taskDef
        self.dimensions = universe.extract(taskDef.connections.dimensions, implied=True)
        if not self.dimensions.issubset(parent.dimensions):
            raise GraphBuilderError(f"Task with label '{taskDef.label}' has dimensions "
                                    f"{self.dimensions.toSet()} that are not a subset of "
                                    f"the pipeline dimensions {parent.dimensions.toSet()}.")
        # Initialize _DatasetScaffoldingDicts as subsets of the one or two
        # corresponding dicts in the parent _PipelineScaffolding.
        self.initInputs = _DatasetScaffoldingDict.fromSubset(datasetTypes.initInputs,
                                                             parent.initInputs, parent.initIntermediates)
        self.initOutputs = _DatasetScaffoldingDict.fromSubset(datasetTypes.initOutputs,
                                                              parent.initIntermediates, parent.initOutputs)
        self.inputs = _DatasetScaffoldingDict.fromSubset(datasetTypes.inputs,
                                                         parent.inputs, parent.intermediates)
        self.outputs = _DatasetScaffoldingDict.fromSubset(datasetTypes.outputs,
                                                          parent.intermediates, parent.outputs)
        self.prerequisites = _DatasetScaffoldingDict.fromSubset(datasetTypes.prerequisites,
                                                                parent.prerequisites)
        # Add backreferences to the _DatasetScaffolding objects that point to
        # this Task.
        for dataset in itertools.chain(self.initInputs.values(), self.inputs.values(),
                                       self.prerequisites.values()):
            dataset.consumers[self.taskDef.label] = self
        for dataset in itertools.chain(self.initOutputs.values(), self.outputs.values()):
            assert dataset.producer is None
            dataset.producer = self
        self.dataIds = set()
        self.quanta = []

    taskDef: TaskDef
    """Data structure that identifies the task class and its config
    (`TaskDef`).
    """

    dimensions: DimensionGraph
    """The dimensions of a single `Quantum` of this task, expanded to include
    implied dependencies (`DimensionGraph`).
    """

    initInputs: _DatasetScaffoldingDict
    """Dictionary containing information about datasets used to construct this
    task (`_DatasetScaffoldingDict`).
    """

    initOutputs: _DatasetScaffoldingDict
    """Dictionary containing information about datasets produced as a
    side-effect of constructing this task (`_DatasetScaffoldingDict`).
    """

    inputs: _DatasetScaffoldingDict
    """Dictionary containing information about datasets used as regular,
    graph-constraining inputs to this task (`_DatasetScaffoldingDict`).
    """

    outputs: _DatasetScaffoldingDict
    """Dictionary containing information about datasets produced by this task
    (`_DatasetScaffoldingDict`).
    """

    prerequisites: _DatasetScaffoldingDict
    """Dictionary containing information about input datasets that must be
    present in the repository before any Pipeline containing this task is run
    (`_DatasetScaffoldingDict`).
    """

    dataIds: Set[DataId]
    """Data IDs for all quanta for this task in the graph (`set` of `DataId`).

    Populated after construction by `_PipelineScaffolding.fillDataIds`.
    """

    quanta: List[Quantum]
    """All quanta for this task in the graph (`list` of `Quantum`).

    Populated after construction by `_PipelineScaffolding.fillQuanta`.
    """

    def addQuantum(self, quantum: Quantum):
        config = self.taskDef.config
        connectionClass = config.connections.connectionsClass
        connectionInstance = connectionClass(config=config)
        # This will raise if one of the check conditions is not met, which is the intended
        # behavior
        result = connectionInstance.adjustQuantum(quantum.predictedInputs)
        quantum._predictedInputs = NamedKeyDict(result)

        # If this function has reached this far add the quantum
        self.quanta.append(quantum)

    def makeQuantumGraphTaskNodes(self) -> QuantumGraphTaskNodes:
        """Create a `QuantumGraphTaskNodes` instance from the information in
        ``self``.

        Returns
        -------
        nodes : `QuantumGraphTaskNodes`
            The `QuantumGraph` elements corresponding to this task.
        """
        return QuantumGraphTaskNodes(
            taskDef=self.taskDef,
            quanta=self.quanta,
            initInputs=self.initInputs.unpackRefs(),
            initOutputs=self.initOutputs.unpackRefs(),
        )


@dataclass
class _PipelineScaffolding:
    """A helper data structure that organizes the information involved in
    constructing a `QuantumGraph` for a `Pipeline`.

    Parameters
    ----------
    pipeline : `Pipeline`
        Sequence of tasks from which a graph is to be constructed.  Must
        have nested task classes already imported.
    universe : `DimensionUniverse`
        Universe of all possible dimensions.

    Raises
    ------
    GraphBuilderError
        Raised if the task's dimensions are not a subset of the union of the
        pipeline's dataset dimensions.

    Notes
    -----
    The scaffolding data structure contains nested data structures for both
    tasks (`_TaskScaffolding`) and datasets (`_DatasetScaffolding`), with the
    latter held by `_DatasetScaffoldingDict`.  The dataset data structures are
    shared between the pipeline-level structure (which aggregates all datasets
    and categorizes them from the perspective of the complete pipeline) and the
    individual tasks that use them as inputs and outputs.

    `QuantumGraph` construction proceeds in five steps, with each corresponding
    to a different `_PipelineScaffolding` method:

    1. When `_PipelineScaffolding` is constructed, we extract and categorize
       the DatasetTypes used by the pipeline (delegating to
       `PipelineDatasetTypes.fromPipeline`), then use these to construct the
       nested `_TaskScaffolding` and `_DatasetScaffolding` objects.

    2. In `fillDataIds`, we construct and run the "Big Join Query", which
       returns related tuples of all dimensions used to identify any regular
       input, output, and intermediate datasets (not prerequisites).  We then
       iterate over these tuples of related dimensions, identifying the subsets
       that correspond to distinct data IDs for each task and dataset type.

    3. In `fillDatasetRefs`, we run follow-up queries against all of the
       dataset data IDs previously identified, populating the
       `_DatasetScaffolding.refs` lists - except for those for prerequisite
        datasets, which cannot be resolved until distinct quanta are
        identified.

    4. In `fillQuanta`, we extract subsets from the lists of `DatasetRef` into
       the inputs and outputs for each `Quantum` and search for prerequisite
       datasets, populating `_TaskScaffolding.quanta`.

    5. In `makeQuantumGraph`, we construct a `QuantumGraph` from the lists of
       per-task quanta identified in the previous step.
    """
    def __init__(self, pipeline, *, universe):
        self.tasks = []
        # Aggregate and categorize the DatasetTypes in the Pipeline.
        datasetTypes = PipelineDatasetTypes.fromPipeline(pipeline, universe=universe)
        # Construct dictionaries that map those DatasetTypes to structures
        # that will (later) hold addiitonal information about them.
        for attr in ("initInputs", "initIntermediates", "initOutputs",
                     "inputs", "intermediates", "outputs", "prerequisites"):
            setattr(self, attr, _DatasetScaffoldingDict.fromDatasetTypes(getattr(datasetTypes, attr),
                                                                         universe=universe))
        # Aggregate all dimensions for all non-init, non-prerequisite
        # DatasetTypes.  These are the ones we'll include in the big join query.
        self.dimensions = self.inputs.dimensions.union(self.inputs.dimensions,
                                                       self.intermediates.dimensions,
                                                       self.outputs.dimensions, implied=True)
        # Construct scaffolding nodes for each Task, and add backreferences
        # to the Task from each DatasetScaffolding node.
        # Note that there's only one scaffolding node for each DatasetType, shared by
        # _PipelineScaffolding and all _TaskScaffoldings that reference it.
        self.tasks = [_TaskScaffolding(taskDef=taskDef, parent=self, datasetTypes=taskDatasetTypes)
                      for taskDef, taskDatasetTypes in zip(pipeline, datasetTypes.byTask.values())]

    tasks: List[_TaskScaffolding]
    """Scaffolding data structures for each task in the pipeline
    (`list` of `_TaskScaffolding`).
    """

    initInputs: _DatasetScaffoldingDict
    """Datasets consumed but not produced when constructing the tasks in this
    pipeline (`_DatasetScaffoldingDict`).
    """

    initIntermediates: _DatasetScaffoldingDict
    """Datasets that are both consumed and produced when constructing the tasks
    in this pipeline (`_DatasetScaffoldingDict`).
    """

    initOutputs: _DatasetScaffoldingDict
    """Datasets produced but not consumed when constructing the tasks in this
    pipeline (`_DatasetScaffoldingDict`).
    """

    inputs: _DatasetScaffoldingDict
    """Datasets that are consumed but not produced when running this pipeline
    (`_DatasetScaffoldingDict`).
    """

    intermediates: _DatasetScaffoldingDict
    """Datasets that are both produced and consumed when running this pipeline
    (`_DatasetScaffoldingDict`).
    """

    outputs: _DatasetScaffoldingDict
    """Datasets produced but not consumed when when running this pipeline
    (`_DatasetScaffoldingDict`).
    """

    prerequisites: _DatasetScaffoldingDict
    """Datasets that are consumed when running this pipeline and looked up
    per-Quantum when generating the graph (`_DatasetScaffoldingDict`).
    """

    dimensions: DimensionGraph
    """All dimensions used by any regular input, intermediate, or output
    (not prerequisite) dataset; the set of dimension used in the "Big Join
    Query" (`DimensionGraph`).

    This is required to be a superset of all task quantum dimensions.
    """

    def fillDataIds(self, registry, originInfo, userQuery):
        """Query for the data IDs that connect nodes in the `QuantumGraph`.

        This method populates `_TaskScaffolding.dataIds` and
        `_DatasetScaffolding.dataIds` (except for those in `prerequisites`).

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        originInfo : `lsst.daf.butler.DatasetOriginInfo`
            Object holding the input and output collections for each
            `DatasetType`.
        userQuery : `str`, optional
            User-provided expression to limit the data IDs processed.
        """
        # Initialization datasets always have empty data IDs.
        emptyDataId = DataId(dimensions=registry.dimensions.empty)
        for scaffolding in itertools.chain(self.initInputs.values(),
                                           self.initIntermediates.values(),
                                           self.initOutputs.values()):
            scaffolding.dataIds.add(emptyDataId)
        # We'll run one big query for the data IDs for task dimensions and
        # regular input and outputs.
        query = DataIdQueryBuilder.fromDimensions(registry, self.dimensions)
        # Limit the query to only dimensions that are associated with the input
        # dataset types.
        for datasetType in self.inputs:
            query.requireDataset(datasetType, originInfo.getInputCollections(datasetType.name))
        # Add the user expression, if any
        if userQuery:
            query.whereParsedExpression(userQuery)
        # Execute the query and populate the data IDs in self
        # _TaskScaffolding.refs, extracting the subsets of the common data ID
        # from the query corresponding to the dimensions of each.  By using
        # sets, we remove duplicates caused by query rows in which the
        # dimensions that change are not relevant for that task or dataset
        # type.  For example, if the Big Join Query involves the dimensions
        # (instrument, visit, detector, skymap, tract, patch), we extract
        # "calexp" data IDs from the instrument, visit, and detector values
        # only, and rely on `set.add` to avoid duplications due to result rows
        # in which only skymap, tract, and patch are varying.
        # The Big Join Query is defined such that only visit+detector and
        # tract+patch combinations that represent spatial overlaps are included
        # in the results.
        for commonDataId in query.execute():
            for taskScaffolding in self.tasks:
                dataId = DataId(commonDataId, dimensions=taskScaffolding.dimensions)
                taskScaffolding.dataIds.add(dataId)
            for datasetType, scaffolding in itertools.chain(self.inputs.items(),
                                                            self.intermediates.items(),
                                                            self.outputs.items()):
                dataId = DataId(commonDataId, dimensions=scaffolding.dimensions)
                scaffolding.dataIds.add(dataId)

    def fillDatasetRefs(self, registry, originInfo, *, skipExisting=True, clobberExisting=False):
        """Perform follow up queries for each dataset data ID produced in
        `fillDataIds`.

        This method populates `_DatasetScaffolding.refs` (except for those in
        `prerequisites`).

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        originInfo : `lsst.daf.butler.DatasetOriginInfo`
            Object holding the input and output collections for each
            `DatasetType`.
        skipExisting : `bool`, optional
            If `True` (default), a Quantum is not created if all its outputs
            already exist.
        clobberExisting : `bool`, optional
            If `True`, overwrite any outputs that already exist.  Cannot be
            `True` if ``skipExisting`` is.

        Raises
        ------
        ValueError
            Raised if both `skipExisting` and `clobberExisting` are `True`.
        OutputExistsError
            Raised if an output dataset already exists in the output collection
            and both ``skipExisting`` and ``clobberExisting`` are `False`.  The
            case where some but not all of a quantum's outputs are present and
            ``skipExisting`` is `True` cannot be identified at this stage, and
            is handled by `fillQuanta` instead.
        """
        if clobberExisting and skipExisting:
            raise ValueError("clobberExisting and skipExisting cannot both be true.")
        # Look up input and initInput datasets in the input collection(s).
        for datasetType, scaffolding in itertools.chain(self.initInputs.items(), self.inputs.items()):
            for dataId in scaffolding.dataIds:
                # TODO: we only need to use SingleDatasetQueryBuilder here because
                # it provides multi-collection search support.  There should be a
                # way to do that directly with Registry, and it should probably
                # operate by just doing an unordered collection search and
                # resolving the order in Python.
                builder = SingleDatasetQueryBuilder.fromCollections(
                    registry, datasetType,
                    collections=originInfo.getInputCollections(datasetType.name)
                )
                builder.whereDataId(dataId)
                ref = builder.executeOne(expandDataId=True)
                if ref is None:
                    # Data IDs have been expanded to include implied
                    # dimensions, which is not what we want for the DatasetRef.
                    # Constructing a new DataID shrinks them back down.
                    ref = DatasetRef(datasetType, DataId(dataId, dimensions=datasetType.dimensions))
                scaffolding.refs.append(ref)
        # Look up [init] intermediate and output datasets in the output collection,
        # unless clobberExisting is True (in which case we don't care if these
        # already exist).
        for datasetType, scaffolding in itertools.chain(self.initIntermediates.items(),
                                                        self.initOutputs.items(),
                                                        self.intermediates.items(),
                                                        self.outputs.items()):
            collection = originInfo.getOutputCollection(datasetType.name)
            for dataId in scaffolding.dataIds:
                # TODO: we could easily support per-DatasetType clobberExisting
                # and skipExisting (it might make sense to put them in
                # originInfo), and I could imagine that being useful - it's
                # probably required in order to support writing initOutputs
                # before QuantumGraph generation.
                if clobberExisting:
                    ref = None
                else:
                    ref = registry.find(collection=collection, datasetType=datasetType, dataId=dataId)
                if ref is None:
                    # data IDs have been expanded to include implied dimensions,
                    # which is not what we want for the DatasetRef.
                    ref = DatasetRef(datasetType, DataId(dataId, dimensions=datasetType.dimensions))
                elif not skipExisting:
                    raise OutputExistsError(f"Output dataset {datasetType.name} already exists in "
                                            f"output collection {collection} with data ID {dataId}.")
                scaffolding.refs.append(ref)
        # Prerequisite dataset lookups are deferred until fillQuanta.

    def fillQuanta(self, registry, originInfo, *, skipExisting=True):
        """Define quanta for each task by splitting up the datasets associated
        with each task data ID.

        This method populates `_TaskScaffolding.quanta`.

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        originInfo : `lsst.daf.butler.DatasetOriginInfo`
            Object holding the input and output collections for each
            `DatasetType`.
        skipExisting : `bool`, optional
            If `True` (default), a Quantum is not created if all its outputs
            already exist.
        """
        for task in self.tasks:
            for quantumDataId in task.dataIds:
                # Identify the (regular) inputs that correspond to the Quantum
                # with this data ID.  These are those whose data IDs have the
                # same values for all dimensions they have in common.
                # We do this data IDs expanded to include implied dimensions,
                # which is why _DatasetScaffolding.dimensions is thus expanded
                # even though DatasetType.dimensions is not.
                inputs = NamedKeyDict()
                for datasetType, scaffolding in task.inputs.items():
                    inputs[datasetType] = [ref for ref, dataId in zip(scaffolding.refs, scaffolding.dataIds)
                                           if quantumDataId.matches(dataId)]
                # Same for outputs.
                outputs = NamedKeyDict()
                allOutputsPresent = True
                for datasetType, scaffolding in task.outputs.items():
                    outputs[datasetType] = []
                    for ref, dataId in zip(scaffolding.refs, scaffolding.dataIds):
                        if quantumDataId.matches(dataId):
                            if ref.id is None:
                                allOutputsPresent = False
                            else:
                                assert skipExisting, "Existing outputs should have already been identified."
                                if not allOutputsPresent:
                                    raise OutputExistsError(f"Output {datasetType.name} with data ID "
                                                            f"{dataId} already exists, but other outputs "
                                                            f"for task with label {task.taskDef.label} "
                                                            f"and data ID {quantumDataId} do not.")
                            outputs[datasetType].append(ref)
                if allOutputsPresent and skipExisting:
                    continue

                # Look up prerequisite datasets in the input collection(s).
                # These may have dimensions that extend beyond those we queried
                # for originally, because we want to permit those data ID
                # values to differ across quanta and dataset types.
                # For example, the same quantum may have a flat and bias with
                # a different calibration_label, or a refcat with a skypix
                # value that overlaps the quantum's data ID's region, but not
                # the user expression used for the initial query.
                for datasetType, scaffolding in task.prerequisites.items():
                    builder = SingleDatasetQueryBuilder.fromCollections(
                        registry, datasetType,
                        collections=originInfo.getInputCollections(datasetType.name)
                    )
                    if not datasetType.dimensions.issubset(quantumDataId.dimensions()):
                        builder.relateDimensions(quantumDataId.dimensions(), addResultColumns=False)
                    builder.whereDataId(quantumDataId)
                    refs = list(builder.execute(expandDataId=True))
                    if len(refs) == 0:
                        raise PrerequisiteMissingError(
                            f"No instances of prerequisite dataset {datasetType.name} found for task "
                            f"with label {task.taskDef.label} and quantum data ID {quantumDataId}."
                        )
                    inputs[datasetType] = refs
                task.addQuantum(
                    Quantum(
                        taskName=task.taskDef.taskName,
                        taskClass=task.taskDef.taskClass,
                        dataId=quantumDataId,
                        initInputs=task.initInputs.unpackRefs(),
                        predictedInputs=inputs,
                        outputs=outputs,
                    )
                )

    def makeQuantumGraph(self):
        """Create a `QuantumGraph` from the quanta already present in
        the scaffolding data structure.
        """
        graph = QuantumGraph(task.makeQuantumGraphTaskNodes() for task in self.tasks)
        graph.initInputs = self.initInputs.unpackRefs()
        graph.initOutputs = self.initOutputs.unpackRefs()
        graph.initIntermediates = self.initIntermediates.unpackRefs()
        return graph


# ------------------------
#  Exported definitions --
# ------------------------


class GraphBuilderError(Exception):
    """Base class for exceptions generated by graph builder.
    """
    pass


class OutputExistsError(GraphBuilderError):
    """Exception generated when output datasets already exist.
    """
    pass


class PrerequisiteMissingError(GraphBuilderError):
    """Exception generated when a prerequisite dataset does not exist.
    """
    pass


class GraphBuilder(object):
    """GraphBuilder class is responsible for building task execution graph from
    a Pipeline.

    Parameters
    ----------
    taskFactory : `TaskFactory`
        Factory object used to load/instantiate PipelineTasks
    registry : `~lsst.daf.butler.Registry`
        Data butler instance.
    skipExisting : `bool`, optional
        If `True` (default), a Quantum is not created if all its outputs
        already exist.
    clobberExisting : `bool`, optional
        If `True`, overwrite any outputs that already exist.  Cannot be
        `True` if ``skipExisting`` is.
    """

    def __init__(self, taskFactory, registry, skipExisting=True, clobberExisting=False):
        self.taskFactory = taskFactory
        self.registry = registry
        self.dimensions = registry.dimensions
        self.skipExisting = skipExisting
        self.clobberExisting = clobberExisting

    def _loadTaskClass(self, taskDef):
        """Make sure task class is loaded.

        Load task class, update task name to make sure it is fully-qualified,
        do not update original taskDef in a Pipeline though.

        Parameters
        ----------
        taskDef : `TaskDef`

        Returns
        -------
        `TaskDef` instance, may be the same as parameter if task class is
        already loaded.
        """
        if taskDef.taskClass is None:
            tClass, tName = self.taskFactory.loadTaskClass(taskDef.taskName)
            taskDef = copy.copy(taskDef)
            taskDef.taskClass = tClass
            taskDef.taskName = tName
        return taskDef

    def makeGraph(self, pipeline, originInfo, userQuery):
        """Create execution graph for a pipeline.

        Parameters
        ----------
        pipeline : `Pipeline`
            Pipeline definition, task names/classes and their configs.
        originInfo : `~lsst.daf.butler.DatasetOriginInfo`
            Object which provides names of the input/output collections.
        userQuery : `str`
            String which defunes user-defined selection for registry, should be
            empty or `None` if there is no restrictions on data selection.

        Returns
        -------
        graph : `QuantumGraph`

        Raises
        ------
        UserExpressionError
            Raised when user expression cannot be parsed.
        OutputExistsError
            Raised when output datasets already exist.
        Exception
            Other exceptions types may be raised by underlying registry
            classes.
        """
        # Make sure all task classes are loaded, creating a new Pipeline
        # to avoid modifying the input one.
        # TODO: in the future, it would be preferable for `Pipeline` to
        # guarantee that its Task classes have been imported to avoid this
        # sort of two-stage initialization.
        pipeline = Pipeline([self._loadTaskClass(taskDef) for taskDef in pipeline])

        scaffolding = _PipelineScaffolding(pipeline, universe=self.registry.dimensions)

        scaffolding.fillDataIds(self.registry, originInfo, userQuery)
        scaffolding.fillDatasetRefs(self.registry, originInfo,
                                    skipExisting=self.skipExisting,
                                    clobberExisting=self.clobberExisting)
        scaffolding.fillQuanta(self.registry, originInfo,
                               skipExisting=self.skipExisting)

        return scaffolding.makeQuantumGraph()
