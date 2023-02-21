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

__all__ = ["GraphBuilder"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import itertools
import logging
from collections import ChainMap, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Collection, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple, Union

from lsst.daf.butler import (
    CollectionType,
    DataCoordinate,
    DatasetIdGenEnum,
    DatasetRef,
    DatasetType,
    Datastore,
    DatastoreRecordData,
    DimensionGraph,
    DimensionUniverse,
    NamedKeyDict,
    NamedValueSet,
    Quantum,
    Registry,
)
from lsst.daf.butler.registry import MissingCollectionError, MissingDatasetTypeError
from lsst.daf.butler.registry.queries import DataCoordinateQueryResults
from lsst.daf.butler.registry.wildcards import CollectionWildcard
from lsst.utils import doImportType

from ._datasetQueryConstraints import DatasetQueryConstraintVariant
from ._status import NoWorkFound

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .connections import AdjustQuantumHelper, iterConnections
from .graph import QuantumGraph
from .pipeline import Pipeline, PipelineDatasetTypes, TaskDatasetTypes, TaskDef

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)


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

    def __init__(self, *args: Any, universe: DimensionUniverse):
        super().__init__(*args)
        self.universe = universe

    @classmethod
    def fromDatasetTypes(
        cls, datasetTypes: Iterable[DatasetType], *, universe: DimensionUniverse
    ) -> _DatasetDict:
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
    def fromSubset(
        cls, datasetTypes: Collection[DatasetType], first: _DatasetDict, *rest: _DatasetDict
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

        # Dataset types known to match immediately can be processed
        # without checks.
        matches = combined.keys() & set(datasetTypes)
        _dict = {k: combined[k] for k in matches}

        if len(_dict) < len(datasetTypes):
            # Work out which ones are missing.
            missing_datasetTypes = set(datasetTypes) - _dict.keys()

            # Get the known names for comparison.
            combined_by_name = {k.name: k for k in combined}

            missing = set()
            incompatible = {}
            for datasetType in missing_datasetTypes:
                # The dataset type is not found. It may not be listed
                # or it may be that it is there with the same name
                # but different definition.
                if datasetType.name in combined_by_name:
                    # This implies some inconsistency in definitions
                    # for connections. If there is support for storage
                    # class conversion we can let it slide.
                    # At this point we do not know
                    # where the inconsistency is but trust that down
                    # stream code will be more explicit about input
                    # vs output incompatibilities.
                    existing = combined_by_name[datasetType.name]
                    if existing.is_compatible_with(datasetType) or datasetType.is_compatible_with(existing):
                        _LOG.warning(
                            "Dataset type mismatch (%s != %s) but continuing since they are compatible",
                            datasetType,
                            existing,
                        )
                        _dict[datasetType] = combined[existing]
                    else:
                        incompatible[datasetType] = existing
                else:
                    missing.add(datasetType)

            if missing or incompatible:
                reasons = []
                if missing:
                    reasons.append(
                        "DatasetTypes {'.'.join(missing)} not present in list of known types: "
                        + ", ".join(d.name for d in combined)
                    )
                if incompatible:
                    for x, y in incompatible.items():
                        reasons.append(f"{x} incompatible with {y}")
                raise KeyError("Errors matching dataset types: " + " & ".join(reasons))

        return cls(_dict, universe=first.universe)

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
            (ref,) = refs.values()
            return ref

        return NamedKeyDict({datasetType: getOne(refs) for datasetType, refs in self.items()})

    def unpackMultiRefs(self) -> NamedKeyDict[DatasetType, List[DatasetRef]]:
        """Unpack nested multi-element `DatasetRef` dicts into a new
        mapping with `DatasetType` keys and `set` of `DatasetRef` values.

        Returns
        -------
        dictionary : `NamedKeyDict`
            Dictionary mapping `DatasetType` to `list` of `DatasetRef`, with
            both `DatasetType` instances and string names usable as keys.
        """
        return NamedKeyDict({datasetType: list(refs.values()) for datasetType, refs in self.items()})

    def extract(self, datasetType: DatasetType, dataIds: Iterable[DataCoordinate]) -> Iterator[DatasetRef]:
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
        self.prerequisites = _DatasetDict.fromDatasetTypes(
            task.prerequisites.keys(), universe=dataId.universe
        )

    __slots__ = ("task", "dataId", "inputs", "outputs", "prerequisites")

    def __repr__(self) -> str:
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

    def makeQuantum(self, datastore_records: Optional[Mapping[str, DatastoreRecordData]] = None) -> Quantum:
        """Transform the scaffolding object into a true `Quantum` instance.

        Parameters
        ----------
        datastore_records : `dict` [ `str`, `DatastoreRecordData` ], optional
            If not `None` then fill datastore records in each generated Quantum
            using the records from this structure.

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
        # the intended behavior.
        # If it raises NotWorkFound, there is a bug in the QG algorithm
        # or the adjustQuantum is incorrectly trying to make a prerequisite
        # input behave like a regular input; adjustQuantum should only raise
        # NoWorkFound if a regular input is missing, and it shouldn't be
        # possible for us to have generated ``self`` if that's true.
        helper = AdjustQuantumHelper(inputs=allInputs, outputs=self.outputs.unpackMultiRefs())
        helper.adjust_in_place(self.task.taskDef.connections, self.task.taskDef.label, self.dataId)
        initInputs = self.task.initInputs.unpackSingleRefs()
        quantum_records: Optional[Mapping[str, DatastoreRecordData]] = None
        if datastore_records is not None:
            quantum_records = {}
            input_refs = list(itertools.chain.from_iterable(helper.inputs.values()))
            input_refs += list(initInputs.values())
            input_ids = set(ref.id for ref in input_refs if ref.id is not None)
            for datastore_name, records in datastore_records.items():
                matching_records = records.subset(input_ids)
                if matching_records is not None:
                    quantum_records[datastore_name] = matching_records
        return Quantum(
            taskName=self.task.taskDef.taskName,
            taskClass=self.task.taskDef.taskClass,
            dataId=self.dataId,
            initInputs=initInputs,
            inputs=helper.inputs,
            outputs=helper.outputs,
            datastore_records=quantum_records,
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
        self.initInputs = _DatasetDict.fromSubset(
            datasetTypes.initInputs, parent.initInputs, parent.initIntermediates
        )
        self.initOutputs = _DatasetDict.fromSubset(
            datasetTypes.initOutputs, parent.initIntermediates, parent.initOutputs
        )
        self.inputs = _DatasetDict.fromSubset(datasetTypes.inputs, parent.inputs, parent.intermediates)
        self.outputs = _DatasetDict.fromSubset(datasetTypes.outputs, parent.intermediates, parent.outputs)
        self.prerequisites = _DatasetDict.fromSubset(datasetTypes.prerequisites, parent.prerequisites)
        self.dataIds: Set[DataCoordinate] = set()
        self.quanta = {}

    def __repr__(self) -> str:
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

    def makeQuantumSet(
        self,
        unresolvedRefs: Optional[Set[DatasetRef]] = None,
        datastore_records: Optional[Mapping[str, DatastoreRecordData]] = None,
    ) -> Set[Quantum]:
        """Create a `set` of `Quantum` from the information in ``self``.

        Parameters
        ----------
        unresolvedRefs : `set` [ `DatasetRef` ], optional
            Input dataset refs that have not been found.
        datastore_records : `dict`


        Returns
        -------
        nodes : `set` of `Quantum`
            The `Quantum` elements corresponding to this task.
        """
        if unresolvedRefs is None:
            unresolvedRefs = set()
        outputs = set()
        for q in self.quanta.values():
            try:
                tmpQuanta = q.makeQuantum(datastore_records)
                outputs.add(tmpQuanta)
            except (NoWorkFound, FileNotFoundError) as exc:
                refs = itertools.chain.from_iterable(self.inputs.unpackMultiRefs().values())
                if unresolvedRefs.intersection(refs):
                    # This means it is a node that is Known to be pruned
                    # later and should be left in even though some follow up
                    # queries fail. This allows the pruning to start from this
                    # quantum with known issues, and prune other nodes it
                    # touches
                    inputs = q.inputs.unpackMultiRefs()
                    inputs.update(q.prerequisites.unpackMultiRefs())
                    tmpQuantum = Quantum(
                        taskName=q.task.taskDef.taskName,
                        taskClass=q.task.taskDef.taskClass,
                        dataId=q.dataId,
                        initInputs=q.task.initInputs.unpackSingleRefs(),
                        inputs=inputs,
                        outputs=q.outputs.unpackMultiRefs(),
                    )
                    outputs.add(tmpQuantum)
                else:
                    raise exc
        return outputs


class _DatasetIdMaker:
    """Helper class which generates random dataset UUIDs for unresolved
    datasets.
    """

    def __init__(self, registry: Registry, run: str):
        self.datasetIdFactory = registry.datasetIdFactory
        self.run = run
        # Dataset IDs generated so far
        self.resolved: Dict[Tuple[DatasetType, DataCoordinate], DatasetRef] = {}

    def resolveRef(self, ref: DatasetRef) -> DatasetRef:
        if ref.id is not None:
            return ref

        # For components we need their parent dataset ID.
        if ref.isComponent():
            parent_ref = ref.makeCompositeRef()
            # Some basic check - parent should be resolved if this is an
            # existing input, or it should be in the cache already if it is
            # an intermediate.
            if parent_ref.id is None:
                key = parent_ref.datasetType, parent_ref.dataId
                if key not in self.resolved:
                    raise ValueError(f"Composite dataset is missing from cache: {parent_ref}")
                parent_ref = self.resolved[key]
            assert parent_ref.id is not None and parent_ref.run is not None, "parent ref must be resolved"
            return ref.resolved(parent_ref.id, parent_ref.run)

        key = ref.datasetType, ref.dataId
        if (resolved := self.resolved.get(key)) is None:
            resolved = self.datasetIdFactory.resolveRef(ref, self.run, DatasetIdGenEnum.UNIQUE)
            self.resolved[key] = resolved
        return resolved

    def resolveDict(self, refs: Dict[DataCoordinate, DatasetRef]) -> Dict[DataCoordinate, DatasetRef]:
        """Resolve all unresolved references in the provided dictionary."""
        return {dataId: self.resolveRef(ref) for dataId, ref in refs.items()}


@dataclass
class _PipelineScaffolding:
    """A helper data structure that organizes the information involved in
    constructing a `QuantumGraph` for a `Pipeline`.

    Parameters
    ----------
    pipeline : `Pipeline` or `Iterable` [ `TaskDef` ]
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

    def __init__(self, pipeline: Union[Pipeline, Iterable[TaskDef]], *, registry: Registry):
        _LOG.debug("Initializing data structures for QuantumGraph generation.")
        self.tasks = []
        # Aggregate and categorize the DatasetTypes in the Pipeline.
        datasetTypes = PipelineDatasetTypes.fromPipeline(pipeline, registry=registry)
        # Construct dictionaries that map those DatasetTypes to structures
        # that will (later) hold additional information about them.
        for attr in (
            "initInputs",
            "initIntermediates",
            "initOutputs",
            "inputs",
            "intermediates",
            "outputs",
            "prerequisites",
        ):
            setattr(
                self,
                attr,
                _DatasetDict.fromDatasetTypes(getattr(datasetTypes, attr), universe=registry.dimensions),
            )
        self.defaultDatasetQueryConstraints = datasetTypes.queryConstraints
        # Aggregate all dimensions for all non-init, non-prerequisite
        # DatasetTypes.  These are the ones we'll include in the big join
        # query.
        self.dimensions = self.inputs.dimensions.union(self.intermediates.dimensions, self.outputs.dimensions)
        # Construct scaffolding nodes for each Task, and add backreferences
        # to the Task from each DatasetScaffolding node.
        # Note that there's only one scaffolding node for each DatasetType,
        # shared by _PipelineScaffolding and all _TaskScaffoldings that
        # reference it.
        if isinstance(pipeline, Pipeline):
            pipeline = pipeline.toExpandedPipeline()
        self.tasks = [
            _TaskScaffolding(taskDef=taskDef, parent=self, datasetTypes=taskDatasetTypes)
            for taskDef, taskDatasetTypes in zip(pipeline, datasetTypes.byTask.values())
        ]

    def __repr__(self) -> str:
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

    defaultDatasetQueryConstraints: NamedValueSet[DatasetType]
    """Datasets that should be used as constraints in the initial query,
    according to tasks (`NamedValueSet`).
    """

    dimensions: DimensionGraph
    """All dimensions used by any regular input, intermediate, or output
    (not prerequisite) dataset; the set of dimension used in the "Big Join
    Query" (`DimensionGraph`).

    This is required to be a superset of all task quantum dimensions.
    """

    globalInitOutputs: _DatasetDict | None = None
    """Per-pipeline global output datasets (e.g. packages) (`_DatasetDict`)
    """

    @contextmanager
    def connectDataIds(
        self,
        registry: Registry,
        collections: Any,
        userQuery: Optional[str],
        externalDataId: DataCoordinate,
        datasetQueryConstraint: DatasetQueryConstraintVariant = DatasetQueryConstraintVariant.ALL,
        bind: Optional[Mapping[str, Any]] = None,
    ) -> Iterator[DataCoordinateQueryResults]:
        """Query for the data IDs that connect nodes in the `QuantumGraph`.

        This method populates `_TaskScaffolding.dataIds` and
        `_DatasetScaffolding.dataIds` (except for those in `prerequisites`).

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        collections
            Expressions representing the collections to search for input
            datasets.  See :ref:`daf_butler_ordered_collection_searches`.
        userQuery : `str` or `None`
            User-provided expression to limit the data IDs processed.
        externalDataId : `DataCoordinate`
            Externally-provided data ID that should be used to restrict the
            results, just as if these constraints had been included via ``AND``
            in ``userQuery``.  This includes (at least) any instrument named
            in the pipeline definition.
        datasetQueryConstraint : `DatasetQueryConstraintVariant`, optional
            The query constraint variant that should be used to constraint the
            query based on dataset existance, defaults to
            `DatasetQueryConstraintVariant.ALL`.
        bind : `Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``userQuery`` expression, keyed by the identifiers they replace.

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
        for datasetType, refs in itertools.chain(
            self.initInputs.items(), self.initIntermediates.items(), self.initOutputs.items()
        ):
            refs[emptyDataId] = DatasetRef(datasetType, emptyDataId)
        # Run one big query for the data IDs for task dimensions and regular
        # inputs and outputs.  We limit the query to only dimensions that are
        # associated with the input dataset types, but don't (yet) try to
        # obtain the dataset_ids for those inputs.
        _LOG.debug(
            "Submitting data ID query over dimensions %s and materializing results.",
            list(self.dimensions.names),
        )
        queryArgs: Dict[str, Any] = {
            "dimensions": self.dimensions,
            "where": userQuery,
            "dataId": externalDataId,
            "bind": bind,
        }
        if datasetQueryConstraint == DatasetQueryConstraintVariant.ALL:
            _LOG.debug(
                "Constraining graph query using default of %s.",
                list(self.defaultDatasetQueryConstraints.names),
            )
            queryArgs["datasets"] = list(self.defaultDatasetQueryConstraints)
            queryArgs["collections"] = collections
        elif datasetQueryConstraint == DatasetQueryConstraintVariant.OFF:
            _LOG.debug("Not using dataset existence to constrain query.")
        elif datasetQueryConstraint == DatasetQueryConstraintVariant.LIST:
            constraint = set(datasetQueryConstraint)
            inputs = {k.name: k for k in self.inputs.keys()}
            if remainder := constraint.difference(inputs.keys()):
                raise ValueError(
                    f"{remainder} dataset type(s) specified as a graph constraint, but"
                    f" do not appear as an input to the specified pipeline: {inputs.keys()}"
                )
            _LOG.debug(f"Constraining graph query using {constraint}")
            queryArgs["datasets"] = [typ for name, typ in inputs.items() if name in constraint]
            queryArgs["collections"] = collections
        else:
            raise ValueError(
                f"Unable to handle type {datasetQueryConstraint} given as datasetQueryConstraint."
            )

        if "datasets" in queryArgs:
            for i, dataset_type in enumerate(queryArgs["datasets"]):
                if dataset_type.isComponent():
                    queryArgs["datasets"][i] = dataset_type.makeCompositeDatasetType()

        with registry.queryDataIds(**queryArgs).materialize() as commonDataIds:
            _LOG.debug("Expanding data IDs.")
            commonDataIds = commonDataIds.expanded()
            _LOG.debug("Iterating over query results to associate quanta with datasets.")
            # Iterate over query results, populating data IDs for datasets and
            # quanta and then connecting them to each other.
            n = -1
            for n, commonDataId in enumerate(commonDataIds):
                # Create DatasetRefs for all DatasetTypes from this result row,
                # noting that we might have created some already.
                # We remember both those that already existed and those that we
                # create now.
                refsForRow = {}
                dataIdCacheForRow: Dict[DimensionGraph, DataCoordinate] = {}
                for datasetType, refs in itertools.chain(
                    self.inputs.items(), self.intermediates.items(), self.outputs.items()
                ):
                    datasetDataId: Optional[DataCoordinate]
                    if (datasetDataId := dataIdCacheForRow.get(datasetType.dimensions)) is None:
                        datasetDataId = commonDataId.subset(datasetType.dimensions)
                        dataIdCacheForRow[datasetType.dimensions] = datasetDataId
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
            if n < 0:
                _LOG.critical("Initial data ID query returned no rows, so QuantumGraph will be empty.")
                emptiness_explained = False
                for message in commonDataIds.explain_no_results():
                    _LOG.critical(message)
                    emptiness_explained = True
                if not emptiness_explained:
                    _LOG.critical(
                        "To reproduce this query for debugging purposes, run "
                        "Registry.queryDataIds with these arguments:"
                    )
                    # We could just repr() the queryArgs dict to get something
                    # the user could make sense of, but it's friendlier to
                    # put these args in an easier-to-construct equivalent form
                    # so they can read it more easily and copy and paste into
                    # a Python terminal.
                    _LOG.critical("  dimensions=%s,", list(queryArgs["dimensions"].names))
                    _LOG.critical("  dataId=%s,", queryArgs["dataId"].byName())
                    if queryArgs["where"]:
                        _LOG.critical("  where=%s,", repr(queryArgs["where"]))
                    if "datasets" in queryArgs:
                        _LOG.critical("  datasets=%s,", [t.name for t in queryArgs["datasets"]])
                    if "collections" in queryArgs:
                        _LOG.critical("  collections=%s,", list(queryArgs["collections"]))
            _LOG.debug("Finished processing %d rows from data ID query.", n)
            yield commonDataIds

    def resolveDatasetRefs(
        self,
        registry: Registry,
        collections: Any,
        run: Optional[str],
        commonDataIds: DataCoordinateQueryResults,
        *,
        skipExistingIn: Any = None,
        clobberOutputs: bool = True,
        constrainedByAllDatasets: bool = True,
        resolveRefs: bool = False,
    ) -> None:
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
            datasets.  See :ref:`daf_butler_ordered_collection_searches`.
        run : `str`, optional
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection for
            output datasets, if it already exists.
        commonDataIds : \
                `lsst.daf.butler.registry.queries.DataCoordinateQueryResults`
            Result of a previous call to `connectDataIds`.
        skipExistingIn
            Expressions representing the collections to search for existing
            output datasets that should be skipped.  See
            :ref:`daf_butler_ordered_collection_searches` for allowed types.
            `None` or empty string/sequence disables skipping.
        clobberOutputs : `bool`, optional
            If `True` (default), allow quanta to created even if outputs exist;
            this requires the same behavior behavior to be enabled when
            executing.  If ``skipExistingIn`` is not `None`, completed quanta
            (those with metadata, or all outputs if there is no metadata
            dataset configured) will be skipped rather than clobbered.
        constrainedByAllDatasets : `bool`, optional
            Indicates if the commonDataIds were generated with a constraint on
            all dataset types.
        resolveRefs : `bool`, optional
            If `True` then resolve all input references and generate random
            dataset IDs for all output and intermediate datasets. True value
            requires ``run`` collection to be specified.

        Raises
        ------
        OutputExistsError
            Raised if an output dataset already exists in the output run
            and ``skipExistingIn`` does not include output run, or if only
            some outputs are present and ``clobberOutputs`` is `False`.
        """
        # Run may be provided but it does not have to exist, in that case we
        # use it for resolving references but don't check it for existing refs.
        run_exists = False
        if run:
            try:
                run_exists = bool(registry.queryCollections(run))
            except MissingCollectionError:
                # Undocumented exception is raise if it does not exist
                pass

        skip_collections_wildcard: CollectionWildcard | None = None
        skipExistingInRun = False
        if skipExistingIn:
            skip_collections_wildcard = CollectionWildcard.from_expression(skipExistingIn)
            if run_exists:
                # as optimization check in the explicit list of names first
                skipExistingInRun = run in skip_collections_wildcard.strings
                if not skipExistingInRun:
                    # need to flatten it and check again
                    skipExistingInRun = run in registry.queryCollections(
                        skipExistingIn,
                        collectionTypes=CollectionType.RUN,
                    )

        idMaker: Optional[_DatasetIdMaker] = None
        if resolveRefs:
            assert run is not None, "run cannot be None when resolveRefs is True"
            idMaker = _DatasetIdMaker(registry, run)

        resolvedRefQueryResults: Iterable[DatasetRef]

        # Updating constrainedByAllDatasets here is not ideal, but we have a
        # few different code paths that each transfer different pieces of
        # information about what dataset query constraints were applied here,
        # and none of them has the complete picture until we get here.  We're
        # long overdue for a QG generation rewrite that will make this go away
        # entirely anyway.
        constrainedByAllDatasets = (
            constrainedByAllDatasets and self.defaultDatasetQueryConstraints == self.inputs.keys()
        )

        # Look up [init] intermediate and output datasets in the output
        # collection, if there is an output collection.
        if run_exists or skip_collections_wildcard is not None:
            for datasetType, refs in itertools.chain(
                self.initIntermediates.items(),
                self.initOutputs.items(),
                self.intermediates.items(),
                self.outputs.items(),
            ):
                _LOG.debug(
                    "Resolving %d datasets for intermediate and/or output dataset %s.",
                    len(refs),
                    datasetType.name,
                )
                isInit = datasetType in self.initIntermediates or datasetType in self.initOutputs
                subset = commonDataIds.subset(datasetType.dimensions, unique=True)
                # TODO: this assert incorrectly bans component inputs;
                # investigate on DM-33027.
                # assert not datasetType.isComponent(), \
                #     "Output datasets cannot be components."
                #
                # Instead we have to handle them manually to avoid a
                # deprecation warning, but it is at least confusing and
                # possibly a bug for components to appear here at all.
                if datasetType.isComponent():
                    parent_dataset_type = datasetType.makeCompositeDatasetType()
                    component = datasetType.component()
                else:
                    parent_dataset_type = datasetType
                    component = None

                # look at RUN collection first
                if run_exists:
                    try:
                        resolvedRefQueryResults = subset.findDatasets(
                            parent_dataset_type, collections=run, findFirst=True
                        )
                    except MissingDatasetTypeError:
                        resolvedRefQueryResults = []
                    for resolvedRef in resolvedRefQueryResults:
                        # TODO: we could easily support per-DatasetType
                        # skipExisting and I could imagine that being useful -
                        # it's probably required in order to support writing
                        # initOutputs before QuantumGraph generation.
                        assert resolvedRef.dataId in refs
                        if not (skipExistingInRun or isInit or clobberOutputs):
                            raise OutputExistsError(
                                f"Output dataset {datasetType.name} already exists in "
                                f"output RUN collection '{run}' with data ID"
                                f" {resolvedRef.dataId}."
                            )
                        # If we are going to resolve all outputs then we have
                        # to remember existing ones to avoid generating new
                        # dataset IDs for them.
                        if resolveRefs:
                            refs[resolvedRef.dataId] = (
                                resolvedRef.makeComponentRef(component)
                                if component is not None
                                else resolvedRef
                            )

                # And check skipExistingIn too, if RUN collection is in
                # it is handled above
                if skip_collections_wildcard is not None:
                    try:
                        resolvedRefQueryResults = subset.findDatasets(
                            parent_dataset_type, collections=skip_collections_wildcard, findFirst=True
                        )
                    except MissingDatasetTypeError:
                        resolvedRefQueryResults = []
                    for resolvedRef in resolvedRefQueryResults:
                        assert resolvedRef.dataId in refs
                        refs[resolvedRef.dataId] = (
                            resolvedRef.makeComponentRef(component) if component is not None else resolvedRef
                        )

        # Look up input and initInput datasets in the input collection(s).
        # container to accumulate unfound refs, if the common dataIs were not
        # constrained on dataset type existence.
        self.unfoundRefs = set()
        for datasetType, refs in itertools.chain(self.initInputs.items(), self.inputs.items()):
            _LOG.debug("Resolving %d datasets for input dataset %s.", len(refs), datasetType.name)
            if datasetType.isComponent():
                parent_dataset_type = datasetType.makeCompositeDatasetType()
                component = datasetType.component()
            else:
                parent_dataset_type = datasetType
                component = None
            try:
                resolvedRefQueryResults = commonDataIds.subset(
                    datasetType.dimensions, unique=True
                ).findDatasets(parent_dataset_type, collections=collections, findFirst=True)
            except MissingDatasetTypeError:
                resolvedRefQueryResults = []
            dataIdsNotFoundYet = set(refs.keys())
            for resolvedRef in resolvedRefQueryResults:
                dataIdsNotFoundYet.discard(resolvedRef.dataId)
                refs[resolvedRef.dataId] = (
                    resolvedRef if component is None else resolvedRef.makeComponentRef(component)
                )
            if dataIdsNotFoundYet:
                if constrainedByAllDatasets:
                    raise RuntimeError(
                        f"{len(dataIdsNotFoundYet)} dataset(s) of type "
                        f"'{datasetType.name}' was/were present in a previous "
                        "query, but could not be found now. "
                        "This is either a logic bug in QuantumGraph generation "
                        "or the input collections have been modified since "
                        "QuantumGraph generation began."
                    )
                elif not datasetType.dimensions:
                    raise RuntimeError(
                        f"Dataset {datasetType.name!r} (with no dimensions) could not be found in "
                        f"collections {collections}."
                    )
                else:
                    # if the common dataIds were not constrained using all the
                    # input dataset types, it is possible that some data ids
                    # found dont correspond to existing dataset types and they
                    # will be un-resolved. Mark these for later pruning from
                    # the quantum graph.
                    for k in dataIdsNotFoundYet:
                        self.unfoundRefs.add(refs[k])

        # Copy the resolved DatasetRefs to the _QuantumScaffolding objects,
        # replacing the unresolved refs there, and then look up prerequisites.
        for task in self.tasks:
            _LOG.debug(
                "Applying resolutions and finding prerequisites for %d quanta of task with label '%s'.",
                len(task.quanta),
                task.taskDef.label,
            )
            # The way iterConnections is designed makes it impossible to
            # annotate precisely enough to satisfy MyPy here.
            lookupFunctions = {
                c.name: c.lookupFunction  # type: ignore
                for c in iterConnections(task.taskDef.connections, "prerequisiteInputs")
                if c.lookupFunction is not None  # type: ignore
            }
            dataIdsFailed = []
            dataIdsSucceeded = []
            for quantum in task.quanta.values():
                # Process outputs datasets only if skipExistingIn is not None
                # or there is a run to look for outputs in and clobberOutputs
                # is True. Note that if skipExistingIn is None, any output
                # datasets that already exist would have already caused an
                # exception to be raised.  We never update the DatasetRefs in
                # the quantum because those should never be resolved.
                if skip_collections_wildcard is not None or (run_exists and clobberOutputs):
                    resolvedRefs = []
                    unresolvedRefs = []
                    haveMetadata = False
                    for datasetType, originalRefs in quantum.outputs.items():
                        for ref in task.outputs.extract(datasetType, originalRefs.keys()):
                            if ref.id is not None:
                                resolvedRefs.append(ref)
                                if datasetType.name == task.taskDef.metadataDatasetName:
                                    haveMetadata = True
                            else:
                                unresolvedRefs.append(ref)
                    if resolvedRefs:
                        if haveMetadata or not unresolvedRefs:
                            dataIdsSucceeded.append(quantum.dataId)
                            if skip_collections_wildcard is not None:
                                continue
                        else:
                            dataIdsFailed.append(quantum.dataId)
                            if not clobberOutputs:
                                raise OutputExistsError(
                                    f"Quantum {quantum.dataId} of task with label "
                                    f"'{quantum.task.taskDef.label}' has some outputs that exist "
                                    f"({resolvedRefs}) "
                                    f"and others that don't ({unresolvedRefs}), with no metadata output, "
                                    "and clobbering outputs was not enabled."
                                )
                # Update the input DatasetRefs to the resolved ones we already
                # searched for.
                for datasetType, input_refs in quantum.inputs.items():
                    for ref in task.inputs.extract(datasetType, input_refs.keys()):
                        input_refs[ref.dataId] = ref
                # Look up prerequisite datasets in the input collection(s).
                # These may have dimensions that extend beyond those we queried
                # for originally, because we want to permit those data ID
                # values to differ across quanta and dataset types.
                for datasetType in task.prerequisites:
                    if datasetType.isComponent():
                        parent_dataset_type = datasetType.makeCompositeDatasetType()
                        component = datasetType.component()
                    else:
                        parent_dataset_type = datasetType
                        component = None
                    lookupFunction = lookupFunctions.get(datasetType.name)
                    if lookupFunction is not None:
                        # PipelineTask has provided its own function to do the
                        # lookup.  This always takes precedence.
                        prereq_refs = list(lookupFunction(datasetType, registry, quantum.dataId, collections))
                    elif (
                        datasetType.isCalibration()
                        and datasetType.dimensions <= quantum.dataId.graph
                        and quantum.dataId.graph.temporal
                    ):
                        # This is a master calibration lookup, which we have to
                        # handle specially because the query system can't do a
                        # temporal join on a non-dimension-based timespan yet.
                        timespan = quantum.dataId.timespan
                        try:
                            prereq_ref = registry.findDataset(
                                parent_dataset_type,
                                quantum.dataId,
                                collections=collections,
                                timespan=timespan,
                            )
                            if prereq_ref is not None:
                                if component is not None:
                                    prereq_ref = prereq_ref.makeComponentRef(component)
                                prereq_refs = [prereq_ref]
                            else:
                                prereq_refs = []
                        except (KeyError, MissingDatasetTypeError):
                            # This dataset type is not present in the registry,
                            # which just means there are no datasets here.
                            prereq_refs = []
                    else:
                        # Most general case.
                        prereq_refs = [
                            prereq_ref if component is None else prereq_ref.makeComponentRef(component)
                            for prereq_ref in registry.queryDatasets(
                                parent_dataset_type,
                                collections=collections,
                                dataId=quantum.dataId,
                                findFirst=True,
                            ).expanded()
                        ]
                    prereq_refs_map = {ref.dataId: ref for ref in prereq_refs if ref is not None}
                    quantum.prerequisites[datasetType].update(prereq_refs_map)
                    task.prerequisites[datasetType].update(prereq_refs_map)

                # Resolve all quantum inputs and outputs.
                if idMaker:
                    for datasetDict in (quantum.inputs, quantum.outputs):
                        for refDict in datasetDict.values():
                            refDict.update(idMaker.resolveDict(refDict))

            # Resolve task initInputs and initOutputs.
            if idMaker:
                for datasetDict in (task.initInputs, task.initOutputs):
                    for refDict in datasetDict.values():
                        refDict.update(idMaker.resolveDict(refDict))

            # Actually remove any quanta that we decided to skip above.
            if dataIdsSucceeded:
                if skip_collections_wildcard is not None:
                    _LOG.debug(
                        "Pruning successful %d quanta for task with label '%s' because all of their "
                        "outputs exist or metadata was written successfully.",
                        len(dataIdsSucceeded),
                        task.taskDef.label,
                    )
                    for dataId in dataIdsSucceeded:
                        del task.quanta[dataId]
                elif clobberOutputs:
                    _LOG.info(
                        "Found %d successful quanta for task with label '%s' "
                        "that will need to be clobbered during execution.",
                        len(dataIdsSucceeded),
                        task.taskDef.label,
                    )
                else:
                    raise AssertionError("OutputExistsError should have already been raised.")
            if dataIdsFailed:
                if clobberOutputs:
                    _LOG.info(
                        "Found %d failed/incomplete quanta for task with label '%s' "
                        "that will need to be clobbered during execution.",
                        len(dataIdsFailed),
                        task.taskDef.label,
                    )
                else:
                    raise AssertionError("OutputExistsError should have already been raised.")

        # Collect initOutputs that do not belong to any task.
        global_dataset_types: set[DatasetType] = set(self.initOutputs)
        for task in self.tasks:
            global_dataset_types -= set(task.initOutputs)
        if global_dataset_types:
            self.globalInitOutputs = _DatasetDict.fromSubset(global_dataset_types, self.initOutputs)
            if idMaker is not None:
                for refDict in self.globalInitOutputs.values():
                    refDict.update(idMaker.resolveDict(refDict))

    def makeQuantumGraph(
        self,
        registry: Registry,
        metadata: Optional[Mapping[str, Any]] = None,
        datastore: Optional[Datastore] = None,
    ) -> QuantumGraph:
        """Create a `QuantumGraph` from the quanta already present in
        the scaffolding data structure.

        Parameters
        ---------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        metadata : Optional Mapping of `str` to primitives
            This is an optional parameter of extra data to carry with the
            graph.  Entries in this mapping should be able to be serialized in
            JSON.
        datastore : `Datastore`, optional
            If not `None` then fill datastore records in each generated
            Quantum.

        Returns
        -------
        graph : `QuantumGraph`
            The full `QuantumGraph`.
        """

        def _make_refs(dataset_dict: _DatasetDict) -> Iterable[DatasetRef]:
            """Extract all DatasetRefs from the dictionaries"""
            for ref_dict in dataset_dict.values():
                yield from ref_dict.values()

        datastore_records: Optional[Mapping[str, DatastoreRecordData]] = None
        if datastore is not None:
            datastore_records = datastore.export_records(
                itertools.chain(
                    _make_refs(self.inputs), _make_refs(self.initInputs), _make_refs(self.prerequisites)
                )
            )

        graphInput: Dict[TaskDef, Set[Quantum]] = {}
        for task in self.tasks:
            qset = task.makeQuantumSet(unresolvedRefs=self.unfoundRefs, datastore_records=datastore_records)
            graphInput[task.taskDef] = qset

        taskInitInputs = {task.taskDef: task.initInputs.unpackSingleRefs().values() for task in self.tasks}
        taskInitOutputs = {task.taskDef: task.initOutputs.unpackSingleRefs().values() for task in self.tasks}

        globalInitOutputs: list[DatasetRef] = []
        if self.globalInitOutputs is not None:
            for refs_dict in self.globalInitOutputs.values():
                globalInitOutputs.extend(refs_dict.values())

        graph = QuantumGraph(
            graphInput,
            metadata=metadata,
            pruneRefs=self.unfoundRefs,
            universe=self.dimensions.universe,
            initInputs=taskInitInputs,
            initOutputs=taskInitOutputs,
            globalInitOutputs=globalInitOutputs,
            registryDatasetTypes=self._get_registry_dataset_types(registry),
        )
        return graph

    def _get_registry_dataset_types(self, registry: Registry) -> Iterable[DatasetType]:
        """Make a list of all dataset types used by a graph as defined in
        registry.
        """
        chain = [
            self.initInputs,
            self.initIntermediates,
            self.initOutputs,
            self.inputs,
            self.intermediates,
            self.outputs,
            self.prerequisites,
        ]
        if self.globalInitOutputs is not None:
            chain.append(self.globalInitOutputs)

        # Collect names of all dataset types.
        all_names: set[str] = set(dstype.name for dstype in itertools.chain(*chain))
        dataset_types = {ds.name: ds for ds in registry.queryDatasetTypes(all_names)}

        # Check for types that do not exist in registry yet:
        # - inputs must exist
        # - intermediates and outputs may not exist, but there must not be
        #   more than one definition (e.g. differing in storage class)
        # - prerequisites may not exist, treat it the same as outputs here
        for dstype in itertools.chain(self.initInputs, self.inputs):
            if dstype.name not in dataset_types:
                raise MissingDatasetTypeError(f"Registry is missing an input dataset type {dstype}")

        new_outputs: dict[str, set[DatasetType]] = defaultdict(set)
        chain = [
            self.initIntermediates,
            self.initOutputs,
            self.intermediates,
            self.outputs,
            self.prerequisites,
        ]
        if self.globalInitOutputs is not None:
            chain.append(self.globalInitOutputs)
        for dstype in itertools.chain(*chain):
            if dstype.name not in dataset_types:
                new_outputs[dstype.name].add(dstype)
        for name, dstypes in new_outputs.items():
            if len(dstypes) > 1:
                raise ValueError(
                    "Pipeline contains multiple definitions for a dataset type "
                    f"which is not defined in registry yet: {dstypes}"
                )
            elif len(dstypes) == 1:
                dataset_types[name] = dstypes.pop()

        return dataset_types.values()


# ------------------------
#  Exported definitions --
# ------------------------


class GraphBuilderError(Exception):
    """Base class for exceptions generated by graph builder."""

    pass


class OutputExistsError(GraphBuilderError):
    """Exception generated when output datasets already exist."""

    pass


class PrerequisiteMissingError(GraphBuilderError):
    """Exception generated when a prerequisite dataset does not exist."""

    pass


class GraphBuilder:
    """GraphBuilder class is responsible for building task execution graph from
    a Pipeline.

    Parameters
    ----------
    registry : `~lsst.daf.butler.Registry`
        Data butler instance.
    skipExistingIn
        Expressions representing the collections to search for existing
        output datasets that should be skipped.  See
        :ref:`daf_butler_ordered_collection_searches`.
    clobberOutputs : `bool`, optional
        If `True` (default), allow quanta to created even if partial outputs
        exist; this requires the same behavior behavior to be enabled when
        executing.
    datastore : `Datastore`, optional
        If not `None` then fill datastore records in each generated Quantum.
    """

    def __init__(
        self,
        registry: Registry,
        skipExistingIn: Any = None,
        clobberOutputs: bool = True,
        datastore: Optional[Datastore] = None,
    ):
        self.registry = registry
        self.dimensions = registry.dimensions
        self.skipExistingIn = skipExistingIn
        self.clobberOutputs = clobberOutputs
        self.datastore = datastore

    def makeGraph(
        self,
        pipeline: Union[Pipeline, Iterable[TaskDef]],
        collections: Any,
        run: Optional[str],
        userQuery: Optional[str],
        datasetQueryConstraint: DatasetQueryConstraintVariant = DatasetQueryConstraintVariant.ALL,
        metadata: Optional[Mapping[str, Any]] = None,
        resolveRefs: bool = False,
        bind: Optional[Mapping[str, Any]] = None,
    ) -> QuantumGraph:
        """Create execution graph for a pipeline.

        Parameters
        ----------
        pipeline : `Pipeline` or `Iterable` [ `TaskDef` ]
            Pipeline definition, task names/classes and their configs.
        collections
            Expressions representing the collections to search for input
            datasets.  See :ref:`daf_butler_ordered_collection_searches`.
        run : `str`, optional
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection for
            output datasets. Collection does not have to exist and it will be
            created when graph is executed.
        userQuery : `str`
            String which defines user-defined selection for registry, should be
            empty or `None` if there is no restrictions on data selection.
        datasetQueryConstraint : `DatasetQueryConstraintVariant`, optional
            The query constraint variant that should be used to constraint the
            query based on dataset existance, defaults to
            `DatasetQueryConstraintVariant.ALL`.
        metadata : Optional Mapping of `str` to primitives
            This is an optional parameter of extra data to carry with the
            graph.  Entries in this mapping should be able to be serialized in
            JSON.
        resolveRefs : `bool`, optional
            If `True` then resolve all input references and generate random
            dataset IDs for all output and intermediate datasets. True value
            requires ``run`` collection to be specified.
        bind : `Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``userQuery`` expression, keyed by the identifiers they replace.

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
        if resolveRefs and run is None:
            raise ValueError("`resolveRefs` requires `run` parameter.")
        scaffolding = _PipelineScaffolding(pipeline, registry=self.registry)
        if not collections and (scaffolding.initInputs or scaffolding.inputs or scaffolding.prerequisites):
            raise ValueError("Pipeline requires input datasets but no input collections provided.")
        instrument_class: Optional[Any] = None
        if isinstance(pipeline, Pipeline):
            instrument_class_name = pipeline.getInstrument()
            if instrument_class_name is not None:
                instrument_class = doImportType(instrument_class_name)
            pipeline = list(pipeline.toExpandedPipeline())
        if instrument_class is not None:
            dataId = DataCoordinate.standardize(
                instrument=instrument_class.getName(), universe=self.registry.dimensions
            )
        else:
            dataId = DataCoordinate.makeEmpty(self.registry.dimensions)
        with scaffolding.connectDataIds(
            self.registry, collections, userQuery, dataId, datasetQueryConstraint, bind
        ) as commonDataIds:
            condition = datasetQueryConstraint == DatasetQueryConstraintVariant.ALL
            scaffolding.resolveDatasetRefs(
                self.registry,
                collections,
                run,
                commonDataIds,
                skipExistingIn=self.skipExistingIn,
                clobberOutputs=self.clobberOutputs,
                constrainedByAllDatasets=condition,
                resolveRefs=resolveRefs,
            )
        return scaffolding.makeQuantumGraph(
            registry=self.registry, metadata=metadata, datastore=self.datastore
        )
