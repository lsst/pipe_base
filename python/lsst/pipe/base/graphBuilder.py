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
import itertools
from collections import ChainMap
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Set
import logging


# -----------------------------
#  Imports for other modules --
# -----------------------------
from .connections import iterConnections
from .pipeline import PipelineDatasetTypes, TaskDatasetTypes, TaskDef, Pipeline
from .graph import QuantumGraph
from lsst.daf.butler import (
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionGraph,
    DimensionUniverse,
    NamedKeyDict,
    Quantum,
)
from lsst.utils import doImport

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


class _DatasetDict(NamedKeyDict[DatasetType, Dict[DataCoordinate, DatasetRef]]):
    """A custom dictionary that maps `DatasetType` to a nested dictionary of
    the known `DatasetRef` instances of that type.

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
                         universe: DimensionUniverse) -> _DatasetDict:
        """Construct a dictionary from a flat iterable of `DatasetType` keys.

        Parameters
        ----------
        datasetTypes : `iterable` of `DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be empty
            dictionaries.
        universe : `DimensionUniverse`
            Universe of all possible dimensions.

        Returns
        -------
        dictionary : `_DatasetDict`
            A new `_DatasetDict` instance.
        """
        return cls({datasetType: {} for datasetType in datasetTypes}, universe=universe)

    @classmethod
    def fromSubset(cls, datasetTypes: Iterable[DatasetType], first: _DatasetDict, *rest: _DatasetDict
                   ) -> _DatasetDict:
        """Return a new dictionary by extracting items corresponding to the
        given keys from one or more existing dictionaries.

        Parameters
        ----------
        datasetTypes : `iterable` of `DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be obtained
            by lookups against ``first`` and ``rest``.
        first : `_DatasetDict`
            Another dictionary from which to extract values.
        rest
            Additional dictionaries from which to extract values.

        Returns
        -------
        dictionary : `_DatasetDict`
            A new dictionary instance.
        """
        combined = ChainMap(first, *rest)
        return cls({datasetType: combined[datasetType] for datasetType in datasetTypes},
                   universe=first.universe)

    @property
    def dimensions(self) -> DimensionGraph:
        """The union of all dimensions used by all dataset types in this
        dictionary, including implied dependencies (`DimensionGraph`).
        """
        base = self.universe.empty
        if len(self) == 0:
            return base
        return base.union(*[datasetType.dimensions for datasetType in self.keys()])

    def unpackSingleRefs(self) -> NamedKeyDict[DatasetType, DatasetRef]:
        """Unpack nested single-element `DatasetRef` dicts into a new
        mapping with `DatasetType` keys and `DatasetRef` values.

        This method assumes that each nest contains exactly one item, as is the
        case for all "init" datasets.

        Returns
        -------
        dictionary : `NamedKeyDict`
            Dictionary mapping `DatasetType` to `DatasetRef`, with both
            `DatasetType` instances and string names usable as keys.
        """
        def getOne(refs: Dict[DataCoordinate, DatasetRef]) -> DatasetRef:
            ref, = refs.values()
            return ref
        return NamedKeyDict({datasetType: getOne(refs) for datasetType, refs in self.items()})

    def unpackMultiRefs(self) -> NamedKeyDict[DatasetType, DatasetRef]:
        """Unpack nested multi-element `DatasetRef` dicts into a new
        mapping with `DatasetType` keys and `set` of `DatasetRef` values.

        Returns
        -------
        dictionary : `NamedKeyDict`
            Dictionary mapping `DatasetType` to `DatasetRef`, with both
            `DatasetType` instances and string names usable as keys.
        """
        return NamedKeyDict({datasetType: list(refs.values()) for datasetType, refs in self.items()})

    def extract(self, datasetType: DatasetType, dataIds: Iterable[DataCoordinate]
                ) -> Iterator[DatasetRef]:
        """Iterate over the contained `DatasetRef` instances that match the
        given `DatasetType` and data IDs.

        Parameters
        ----------
        datasetType : `DatasetType`
            Dataset type to match.
        dataIds : `Iterable` [ `DataCoordinate` ]
            Data IDs to match.

        Returns
        -------
        refs : `Iterator` [ `DatasetRef` ]
            DatasetRef instances for which ``ref.datasetType == datasetType``
            and ``ref.dataId`` is in ``dataIds``.
        """
        refs = self[datasetType]
        return (refs[dataId] for dataId in dataIds)


class _QuantumScaffolding:
    """Helper class aggregating information about a `Quantum`, used when
    constructing a `QuantumGraph`.

    See `_PipelineScaffolding` for a top-down description of the full
    scaffolding data structure.

    Parameters
    ----------
    task : _TaskScaffolding
        Back-reference to the helper object for the `PipelineTask` this quantum
        represents an execution of.
    dataId : `DataCoordinate`
        Data ID for this quantum.
    """
    def __init__(self, task: _TaskScaffolding, dataId: DataCoordinate):
        self.task = task
        self.dataId = dataId
        self.inputs = _DatasetDict.fromDatasetTypes(task.inputs.keys(), universe=dataId.universe)
        self.outputs = _DatasetDict.fromDatasetTypes(task.outputs.keys(), universe=dataId.universe)
        self.prerequisites = _DatasetDict.fromDatasetTypes(task.prerequisites.keys(),
                                                           universe=dataId.universe)

    __slots__ = ("task", "dataId", "inputs", "outputs", "prerequisites")

    def __repr__(self):
        return f"_QuantumScaffolding(taskDef={self.task.taskDef}, dataId={self.dataId}, ...)"

    task: _TaskScaffolding
    """Back-reference to the helper object for the `PipelineTask` this quantum
    represents an execution of.
    """

    dataId: DataCoordinate
    """Data ID for this quantum.
    """

    inputs: _DatasetDict
    """Nested dictionary containing `DatasetRef` inputs to this quantum.

    This is initialized to map each `DatasetType` to an empty dictionary at
    construction.  Those nested dictionaries are populated (with data IDs as
    keys) with unresolved `DatasetRef` instances in
    `_PipelineScaffolding.connectDataIds`.
    """

    outputs: _DatasetDict
    """Nested dictionary containing `DatasetRef` outputs this quantum.
    """

    prerequisites: _DatasetDict
    """Nested dictionary containing `DatasetRef` prerequisite inputs to this
    quantum.
    """

    def makeQuantum(self) -> Quantum:
        """Transform the scaffolding object into a true `Quantum` instance.

        Returns
        -------
        quantum : `Quantum`
            An actual `Quantum` instance.
        """
        allInputs = self.inputs.unpackMultiRefs()
        allInputs.update(self.prerequisites.unpackMultiRefs())
        # Give the task's Connections class an opportunity to remove some
        # inputs, or complain if they are unacceptable.
        # This will raise if one of the check conditions is not met, which is
        # the intended behavior
        allInputs = self.task.taskDef.connections.adjustQuantum(allInputs)
        return Quantum(
            taskName=self.task.taskDef.taskName,
            taskClass=self.task.taskDef.taskClass,
            dataId=self.dataId,
            initInputs=self.task.initInputs.unpackSingleRefs(),
            inputs=allInputs,
            outputs=self.outputs.unpackMultiRefs(),
        )


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
    """
    def __init__(self, taskDef: TaskDef, parent: _PipelineScaffolding, datasetTypes: TaskDatasetTypes):
        universe = parent.dimensions.universe
        self.taskDef = taskDef
        self.dimensions = DimensionGraph(universe, names=taskDef.connections.dimensions)
        assert self.dimensions.issubset(parent.dimensions)
        # Initialize _DatasetDicts as subsets of the one or two
        # corresponding dicts in the parent _PipelineScaffolding.
        self.initInputs = _DatasetDict.fromSubset(datasetTypes.initInputs, parent.initInputs,
                                                  parent.initIntermediates)
        self.initOutputs = _DatasetDict.fromSubset(datasetTypes.initOutputs, parent.initIntermediates,
                                                   parent.initOutputs)
        self.inputs = _DatasetDict.fromSubset(datasetTypes.inputs, parent.inputs, parent.intermediates)
        self.outputs = _DatasetDict.fromSubset(datasetTypes.outputs, parent.intermediates, parent.outputs)
        self.prerequisites = _DatasetDict.fromSubset(datasetTypes.prerequisites, parent.prerequisites)
        self.dataIds = set()
        self.quanta = {}

    def __repr__(self):
        # Default dataclass-injected __repr__ gets caught in an infinite loop
        # because of back-references.
        return f"_TaskScaffolding(taskDef={self.taskDef}, ...)"

    taskDef: TaskDef
    """Data structure that identifies the task class and its config
    (`TaskDef`).
    """

    dimensions: DimensionGraph
    """The dimensions of a single `Quantum` of this task (`DimensionGraph`).
    """

    initInputs: _DatasetDict
    """Dictionary containing information about datasets used to construct this
    task (`_DatasetDict`).
    """

    initOutputs: _DatasetDict
    """Dictionary containing information about datasets produced as a
    side-effect of constructing this task (`_DatasetDict`).
    """

    inputs: _DatasetDict
    """Dictionary containing information about datasets used as regular,
    graph-constraining inputs to this task (`_DatasetDict`).
    """

    outputs: _DatasetDict
    """Dictionary containing information about datasets produced by this task
    (`_DatasetDict`).
    """

    prerequisites: _DatasetDict
    """Dictionary containing information about input datasets that must be
    present in the repository before any Pipeline containing this task is run
    (`_DatasetDict`).
    """

    quanta: Dict[DataCoordinate, _QuantumScaffolding]
    """Dictionary mapping data ID to a scaffolding object for the Quantum of
    this task with that data ID.
    """

    def makeQuantumSet(self) -> Set[Quantum]:
        """Create a `set` of `Quantum` from the information in ``self``.

        Returns
        -------
        nodes : `set` of `Quantum
            The `Quantum` elements corresponding to this task.
        """
        return set(q.makeQuantum() for q in self.quanta.values())


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

    Notes
    -----
    The scaffolding data structure contains nested data structures for both
    tasks (`_TaskScaffolding`) and datasets (`_DatasetDict`).  The dataset
    data structures are shared between the pipeline-level structure (which
    aggregates all datasets and categorizes them from the perspective of the
    complete pipeline) and the individual tasks that use them as inputs and
    outputs.

    `QuantumGraph` construction proceeds in four steps, with each corresponding
    to a different `_PipelineScaffolding` method:

    1. When `_PipelineScaffolding` is constructed, we extract and categorize
       the DatasetTypes used by the pipeline (delegating to
       `PipelineDatasetTypes.fromPipeline`), then use these to construct the
       nested `_TaskScaffolding` and `_DatasetDict` objects.

    2. In `connectDataIds`, we construct and run the "Big Join Query", which
       returns related tuples of all dimensions used to identify any regular
       input, output, and intermediate datasets (not prerequisites).  We then
       iterate over these tuples of related dimensions, identifying the subsets
       that correspond to distinct data IDs for each task and dataset type,
       and then create `_QuantumScaffolding` objects.

    3. In `resolveDatasetRefs`, we run follow-up queries against all of the
       dataset data IDs previously identified, transforming unresolved
       DatasetRefs into resolved DatasetRefs where appropriate.  We then look
       up prerequisite datasets for all quanta.

    4. In `makeQuantumGraph`, we construct a `QuantumGraph` from the lists of
       per-task `_QuantumScaffolding` objects.
    """
    def __init__(self, pipeline, *, registry):
        _LOG.debug("Initializing data structures for QuantumGraph generation.")
        self.tasks = []
        # Aggregate and categorize the DatasetTypes in the Pipeline.
        datasetTypes = PipelineDatasetTypes.fromPipeline(pipeline, registry=registry)
        # Construct dictionaries that map those DatasetTypes to structures
        # that will (later) hold addiitonal information about them.
        for attr in ("initInputs", "initIntermediates", "initOutputs",
                     "inputs", "intermediates", "outputs", "prerequisites"):
            setattr(self, attr, _DatasetDict.fromDatasetTypes(getattr(datasetTypes, attr),
                                                              universe=registry.dimensions))
        # Aggregate all dimensions for all non-init, non-prerequisite
        # DatasetTypes.  These are the ones we'll include in the big join
        # query.
        self.dimensions = self.inputs.dimensions.union(self.intermediates.dimensions,
                                                       self.outputs.dimensions)
        # Construct scaffolding nodes for each Task, and add backreferences
        # to the Task from each DatasetScaffolding node.
        # Note that there's only one scaffolding node for each DatasetType,
        # shared by _PipelineScaffolding and all _TaskScaffoldings that
        # reference it.
        if isinstance(pipeline, Pipeline):
            pipeline = pipeline.toExpandedPipeline()
        self.tasks = [_TaskScaffolding(taskDef=taskDef, parent=self, datasetTypes=taskDatasetTypes)
                      for taskDef, taskDatasetTypes in zip(pipeline,
                      datasetTypes.byTask.values())]

    def __repr__(self):
        # Default dataclass-injected __repr__ gets caught in an infinite loop
        # because of back-references.
        return f"_PipelineScaffolding(tasks={self.tasks}, ...)"

    tasks: List[_TaskScaffolding]
    """Scaffolding data structures for each task in the pipeline
    (`list` of `_TaskScaffolding`).
    """

    initInputs: _DatasetDict
    """Datasets consumed but not produced when constructing the tasks in this
    pipeline (`_DatasetDict`).
    """

    initIntermediates: _DatasetDict
    """Datasets that are both consumed and produced when constructing the tasks
    in this pipeline (`_DatasetDict`).
    """

    initOutputs: _DatasetDict
    """Datasets produced but not consumed when constructing the tasks in this
    pipeline (`_DatasetDict`).
    """

    inputs: _DatasetDict
    """Datasets that are consumed but not produced when running this pipeline
    (`_DatasetDict`).
    """

    intermediates: _DatasetDict
    """Datasets that are both produced and consumed when running this pipeline
    (`_DatasetDict`).
    """

    outputs: _DatasetDict
    """Datasets produced but not consumed when when running this pipeline
    (`_DatasetDict`).
    """

    prerequisites: _DatasetDict
    """Datasets that are consumed when running this pipeline and looked up
    per-Quantum when generating the graph (`_DatasetDict`).
    """

    dimensions: DimensionGraph
    """All dimensions used by any regular input, intermediate, or output
    (not prerequisite) dataset; the set of dimension used in the "Big Join
    Query" (`DimensionGraph`).

    This is required to be a superset of all task quantum dimensions.
    """

    @contextmanager
    def connectDataIds(self, registry, collections, userQuery, externalDataId):
        """Query for the data IDs that connect nodes in the `QuantumGraph`.

        This method populates `_TaskScaffolding.dataIds` and
        `_DatasetScaffolding.dataIds` (except for those in `prerequisites`).

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        collections
            Expressions representing the collections to search for input
            datasets.  May be any of the types accepted by
            `lsst.daf.butler.CollectionSearch.fromExpression`.
        userQuery : `str` or `None`
            User-provided expression to limit the data IDs processed.
        externalDataId : `DataCoordinate`
            Externally-provided data ID that should be used to restrict the
            results, just as if these constraints had been included via ``AND``
            in ``userQuery``.  This includes (at least) any instrument named
            in the pipeline definition.

        Returns
        -------
        commonDataIds : \
                `lsst.daf.butler.registry.queries.DataCoordinateQueryResults`
            An interface to a database temporary table containing all data IDs
            that will appear in this `QuantumGraph`.  Returned inside a
            context manager, which will drop the temporary table at the end of
            the `with` block in which this method is called.
        """
        _LOG.debug("Building query for data IDs.")
        # Initialization datasets always have empty data IDs.
        emptyDataId = DataCoordinate.makeEmpty(registry.dimensions)
        for datasetType, refs in itertools.chain(self.initInputs.items(),
                                                 self.initIntermediates.items(),
                                                 self.initOutputs.items()):
            refs[emptyDataId] = DatasetRef(datasetType, emptyDataId)
        # Run one big query for the data IDs for task dimensions and regular
        # inputs and outputs.  We limit the query to only dimensions that are
        # associated with the input dataset types, but don't (yet) try to
        # obtain the dataset_ids for those inputs.
        _LOG.debug("Submitting data ID query and materializing results.")
        with registry.queryDataIds(self.dimensions,
                                   datasets=list(self.inputs),
                                   collections=collections,
                                   where=userQuery,
                                   dataId=externalDataId,
                                   ).materialize() as commonDataIds:
            _LOG.debug("Expanding data IDs.")
            commonDataIds = commonDataIds.expanded()
            _LOG.debug("Iterating over query results to associate quanta with datasets.")
            # Iterate over query results, populating data IDs for datasets and
            # quanta and then connecting them to each other.
            n = 0
            for n, commonDataId in enumerate(commonDataIds):
                # Create DatasetRefs for all DatasetTypes from this result row,
                # noting that we might have created some already.
                # We remember both those that already existed and those that we
                # create now.
                refsForRow = {}
                for datasetType, refs in itertools.chain(self.inputs.items(), self.intermediates.items(),
                                                         self.outputs.items()):
                    datasetDataId = commonDataId.subset(datasetType.dimensions)
                    ref = refs.get(datasetDataId)
                    if ref is None:
                        ref = DatasetRef(datasetType, datasetDataId)
                        refs[datasetDataId] = ref
                    refsForRow[datasetType.name] = ref
                # Create _QuantumScaffolding objects for all tasks from this
                # result row, noting that we might have created some already.
                for task in self.tasks:
                    quantumDataId = commonDataId.subset(task.dimensions)
                    quantum = task.quanta.get(quantumDataId)
                    if quantum is None:
                        quantum = _QuantumScaffolding(task=task, dataId=quantumDataId)
                        task.quanta[quantumDataId] = quantum
                    # Whether this is a new quantum or an existing one, we can
                    # now associate the DatasetRefs for this row with it.  The
                    # fact that a Quantum data ID and a dataset data ID both
                    # came from the same result row is what tells us they
                    # should be associated.
                    # Many of these associates will be duplicates (because
                    # another query row that differed from this one only in
                    # irrelevant dimensions already added them), and we use
                    # sets to skip.
                    for datasetType in task.inputs:
                        ref = refsForRow[datasetType.name]
                        quantum.inputs[datasetType.name][ref.dataId] = ref
                    for datasetType in task.outputs:
                        ref = refsForRow[datasetType.name]
                        quantum.outputs[datasetType.name][ref.dataId] = ref
            _LOG.debug("Finished processing %d rows from data ID query.", n)
            yield commonDataIds

    def resolveDatasetRefs(self, registry, collections, run, commonDataIds, *, skipExisting=True):
        """Perform follow up queries for each dataset data ID produced in
        `fillDataIds`.

        This method populates `_DatasetScaffolding.refs` (except for those in
        `prerequisites`).

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        collections
            Expressions representing the collections to search for input
            datasets.  May be any of the types accepted by
            `lsst.daf.butler.CollectionSearch.fromExpression`.
        run : `str`, optional
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection for
            output datasets, if it already exists.
        commonDataIds : \
                `lsst.daf.butler.registry.queries.DataCoordinateQueryResults`
            Result of a previous call to `connectDataIds`.
        skipExisting : `bool`, optional
            If `True` (default), a Quantum is not created if all its outputs
            already exist in ``run``.  Ignored if ``run`` is `None`.

        Raises
        ------
        OutputExistsError
            Raised if an output dataset already exists in the output run
            and ``skipExisting`` is `False`.  The case where some but not all
            of a quantum's outputs are present and ``skipExisting`` is `True`
            cannot be identified at this stage, and is handled by `fillQuanta`
            instead.
        """
        # Look up [init] intermediate and output datasets in the output
        # collection, if there is an output collection.
        if run is not None:
            for datasetType, refs in itertools.chain(self.initIntermediates.items(),
                                                     self.initOutputs.items(),
                                                     self.intermediates.items(),
                                                     self.outputs.items()):
                _LOG.debug("Resolving %d datasets for intermediate and/or output dataset %s.",
                           len(refs), datasetType.name)
                isInit = datasetType in self.initIntermediates or datasetType in self.initOutputs
                resolvedRefQueryResults = commonDataIds.subset(
                    datasetType.dimensions,
                    unique=True
                ).findDatasets(
                    datasetType,
                    collections=run,
                    findFirst=True
                )
                for resolvedRef in resolvedRefQueryResults:
                    # TODO: we could easily support per-DatasetType
                    # skipExisting and I could imagine that being useful - it's
                    # probably required in order to support writing initOutputs
                    # before QuantumGraph generation.
                    assert resolvedRef.dataId in refs
                    if skipExisting or isInit:
                        refs[resolvedRef.dataId] = resolvedRef
                    else:
                        raise OutputExistsError(f"Output dataset {datasetType.name} already exists in "
                                                f"output RUN collection '{run}' with data ID"
                                                f" {resolvedRef.dataId}.")
        # Look up input and initInput datasets in the input collection(s).
        for datasetType, refs in itertools.chain(self.initInputs.items(), self.inputs.items()):
            _LOG.debug("Resolving %d datasets for input dataset %s.", len(refs), datasetType.name)
            resolvedRefQueryResults = commonDataIds.subset(
                datasetType.dimensions,
                unique=True
            ).findDatasets(
                datasetType,
                collections=collections,
                findFirst=True
            )
            dataIdsNotFoundYet = set(refs.keys())
            for resolvedRef in resolvedRefQueryResults:
                dataIdsNotFoundYet.discard(resolvedRef.dataId)
                refs[resolvedRef.dataId] = resolvedRef
            if dataIdsNotFoundYet:
                raise RuntimeError(
                    f"{len(dataIdsNotFoundYet)} dataset(s) of type "
                    f"'{datasetType.name}' was/were present in a previous "
                    f"query, but could not be found now."
                    f"This is either a logic bug in QuantumGraph generation "
                    f"or the input collections have been modified since "
                    f"QuantumGraph generation began."
                )
        # Copy the resolved DatasetRefs to the _QuantumScaffolding objects,
        # replacing the unresolved refs there, and then look up prerequisites.
        for task in self.tasks:
            _LOG.debug(
                "Applying resolutions and finding prerequisites for %d quanta of task with label '%s'.",
                len(task.quanta),
                task.taskDef.label
            )
            lookupFunctions = {
                c.name: c.lookupFunction
                for c in iterConnections(task.taskDef.connections, "prerequisiteInputs")
                if c.lookupFunction is not None
            }
            dataIdsToSkip = []
            for quantum in task.quanta.values():
                # Process outputs datasets only if there is a run to look for
                # outputs in and skipExisting is True.  Note that if
                # skipExisting is False, any output datasets that already exist
                # would have already caused an exception to be raised.
                # We never update the DatasetRefs in the quantum because those
                # should never be resolved.
                if run is not None and skipExisting:
                    resolvedRefs = []
                    unresolvedRefs = []
                    for datasetType, originalRefs in quantum.outputs.items():
                        for ref in task.outputs.extract(datasetType, originalRefs.keys()):
                            if ref.id is not None:
                                resolvedRefs.append(ref)
                            else:
                                unresolvedRefs.append(ref)
                    if resolvedRefs:
                        if unresolvedRefs:
                            raise OutputExistsError(
                                f"Quantum {quantum.dataId} of task with label "
                                f"'{quantum.task.taskDef.label}' has some outputs that exist "
                                f"({resolvedRefs}) "
                                f"and others that don't ({unresolvedRefs})."
                            )
                        else:
                            # All outputs are already present; skip this
                            # quantum and continue to the next.
                            dataIdsToSkip.append(quantum.dataId)
                            continue
                # Update the input DatasetRefs to the resolved ones we already
                # searched for.
                for datasetType, refs in quantum.inputs.items():
                    for ref in task.inputs.extract(datasetType, refs.keys()):
                        refs[ref.dataId] = ref
                # Look up prerequisite datasets in the input collection(s).
                # These may have dimensions that extend beyond those we queried
                # for originally, because we want to permit those data ID
                # values to differ across quanta and dataset types.
                for datasetType in task.prerequisites:
                    lookupFunction = lookupFunctions.get(datasetType.name)
                    if lookupFunction is not None:
                        # PipelineTask has provided its own function to do the
                        # lookup.  This always takes precedence.
                        refs = list(
                            lookupFunction(datasetType, registry, quantum.dataId, collections)
                        )
                    elif (datasetType.isCalibration()
                            and datasetType.dimensions <= quantum.dataId.graph
                            and quantum.dataId.graph.temporal):
                        # This is a master calibration lookup, which we have to
                        # handle specially because the query system can't do a
                        # temporal join on a non-dimension-based timespan yet.
                        timespan = quantum.dataId.timespan
                        try:
                            refs = [registry.findDataset(datasetType, quantum.dataId,
                                                         collections=collections,
                                                         timespan=timespan)]
                        except KeyError:
                            # This dataset type is not present in the registry,
                            # which just means there are no datasets here.
                            refs = []
                    else:
                        # Most general case.
                        refs = list(registry.queryDatasets(datasetType,
                                                           collections=collections,
                                                           dataId=quantum.dataId,
                                                           findFirst=True).expanded())
                    quantum.prerequisites[datasetType].update({ref.dataId: ref for ref in refs
                                                               if ref is not None})
            # Actually remove any quanta that we decided to skip above.
            if dataIdsToSkip:
                _LOG.debug("Pruning %d quanta for task with label '%s' because all of their outputs exist.",
                           len(dataIdsToSkip), task.taskDef.label)
                for dataId in dataIdsToSkip:
                    del task.quanta[dataId]

    def makeQuantumGraph(self):
        """Create a `QuantumGraph` from the quanta already present in
        the scaffolding data structure.

        Returns
        -------
        graph : `QuantumGraph`
            The full `QuantumGraph`.
        """
        graph = QuantumGraph({task.taskDef: task.makeQuantumSet() for task in self.tasks})
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
    registry : `~lsst.daf.butler.Registry`
        Data butler instance.
    skipExisting : `bool`, optional
        If `True` (default), a Quantum is not created if all its outputs
        already exist.
    """

    def __init__(self, registry, skipExisting=True):
        self.registry = registry
        self.dimensions = registry.dimensions
        self.skipExisting = skipExisting

    def makeGraph(self, pipeline, collections, run, userQuery):
        """Create execution graph for a pipeline.

        Parameters
        ----------
        pipeline : `Pipeline`
            Pipeline definition, task names/classes and their configs.
        collections
            Expressions representing the collections to search for input
            datasets.  May be any of the types accepted by
            `lsst.daf.butler.CollectionSearch.fromExpression`.
        run : `str`, optional
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection for
            output datasets, if it already exists.
        userQuery : `str`
            String which defines user-defined selection for registry, should be
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
        scaffolding = _PipelineScaffolding(pipeline, registry=self.registry)

        instrument = pipeline.getInstrument()
        if isinstance(instrument, str):
            instrument = doImport(instrument)
        if instrument is not None:
            dataId = DataCoordinate.standardize(instrument=instrument.getName(),
                                                universe=self.registry.dimensions)
        else:
            dataId = DataCoordinate.makeEmpty(self.registry.dimensions)
        with scaffolding.connectDataIds(self.registry, collections, userQuery, dataId) as commonDataIds:
            scaffolding.resolveDatasetRefs(self.registry, collections, run, commonDataIds,
                                           skipExisting=self.skipExisting)
        return scaffolding.makeQuantumGraph()
