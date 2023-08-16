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

"""Module defining GraphBuilder class and related methods.
"""

from __future__ import annotations

__all__ = ["GraphBuilder"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import contextlib
import itertools
import logging
from collections import ChainMap, defaultdict
from collections.abc import Collection, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from lsst.daf.butler import (
    CollectionType,
    DataCoordinate,
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
    SkyPixDimension,
)
from lsst.daf.butler.registry import MissingCollectionError, MissingDatasetTypeError
from lsst.daf.butler.registry.queries import DataCoordinateQueryResults
from lsst.daf.butler.registry.wildcards import CollectionWildcard
from lsst.sphgeom import PixelizationABC, RangeSet

# -----------------------------
#  Imports for other modules --
# -----------------------------
from . import automatic_connection_constants as acc
from ._datasetQueryConstraints import DatasetQueryConstraintVariant
from ._status import NoWorkFound
from .connections import AdjustQuantumHelper, iterConnections
from .graph import QuantumGraph
from .pipeline import Pipeline, PipelineDatasetTypes, TaskDatasetTypes, TaskDef

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)


@dataclass
class _RefHolder:
    r"""Placeholder for `~lsst.daf.butler.DatasetRef` representing a future
    resolved reference.

    As we eliminated unresolved `~lsst.daf.butler.DatasetRef`\s we now use
    `None` to represent a reference that is yet to be resolved. Information
    about its corresponding dataset type and coordinate is stored in
    `_DatasetDict` mapping.
    """

    dataset_type: DatasetType
    """Dataset type of the dataset to be created later. I need to store it here
    instead of inferring from `_DatasetDict` because `_RefHolder` can be shared
    between different compatible dataset types."""

    ref: DatasetRef | None = None
    """Dataset reference, initially `None`, created when all datasets are
    resolved.
    """

    @property
    def resolved_ref(self) -> DatasetRef:
        """Access resolved reference, should only be called after the
        reference is set (`~lsst.daf.butler.DatasetRef`).
        """
        assert self.ref is not None, "Dataset reference is not set."
        return self.ref


_Refs = TypeVar("_Refs")


class _DatasetDictBase(NamedKeyDict[DatasetType, _Refs]):
    """A custom dictionary that maps `~lsst.daf.butler.DatasetType` to a nested
    collection of the known `~lsst.daf.butler.DatasetRef` instances of that
    type.

    Parameters
    ----------
    args
        Positional arguments are forwarded to the `dict` constructor.
    universe : `~lsst.daf.butler.DimensionUniverse`
        Universe of all possible dimensions.
    """

    def __init__(self, *args: Any, universe: DimensionUniverse):
        super().__init__(*args)
        self.universe = universe

    @classmethod
    def _fromSubset(
        cls,
        datasetTypes: Collection[DatasetType],
        first: _DatasetDictBase,
        *rest: _DatasetDictBase,
    ) -> _DatasetDictBase:
        """Return a new dictionary by extracting items corresponding to the
        given keys from one or more existing dictionaries.

        Parameters
        ----------
        datasetTypes : `~collections.abc.Iterable` of \
                `~lsst.daf.butler.DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be obtained
            by lookups against ``first`` and ``rest``.
        first : `_DatasetDictBase`
            Another dictionary from which to extract values. Its actual type
            must be idedntical to the type of sub-class used to call this
            method.
        rest
            Additional dictionaries from which to extract values.

        Returns
        -------
        dictionary : `_DatasetDictBase`
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
                    convertible_to_existing = existing.is_compatible_with(datasetType)
                    convertible_from_existing = datasetType.is_compatible_with(existing)
                    if convertible_to_existing and convertible_from_existing:
                        _LOG.debug(
                            "Dataset type %s has multiple fully-compatible storage classes %s and %s",
                            datasetType.name,
                            datasetType.storageClass_name,
                            existing.storageClass_name,
                        )
                        _dict[datasetType] = combined[existing]
                    elif convertible_to_existing or convertible_from_existing:
                        # We'd need to refactor a fair amount to recognize
                        # whether this is an error or not, so I'm not going to
                        # bother until we need to do that for other reasons
                        # (it won't be too long).
                        _LOG.info(
                            "Dataset type %s is present with multiple only partially-compatible storage "
                            "classes %s and %s.",
                            datasetType.name,
                            datasetType.storageClass_name,
                            existing.storageClass_name,
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
                        f"DatasetTypes [{', '.join(d.name for d in missing)}] not present in list of known "
                        f"types: [{', '.join(d.name for d in combined)}]."
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
        return base.union(*[datasetType.dimensions for datasetType in self])

    def unpackSingleRefs(self, storage_classes: dict[str, str]) -> NamedKeyDict[DatasetType, DatasetRef]:
        """Unpack nested single-element `~lsst.daf.butler.DatasetRef` dicts
        into a new mapping with `~lsst.daf.butler.DatasetType` keys and
        `~lsst.daf.butler.DatasetRef` values.

        This method assumes that each nest contains exactly one item, as is the
        case for all "init" datasets.

        Parameters
        ----------
        storage_classes : `dict` [ `str`, `str` ]
            Mapping from dataset type name to the storage class to use for that
            dataset type.  These are typically the storage classes declared
            for a particular task, which may differ rom the data repository
            definitions.

        Returns
        -------
        dictionary : `~lsst.daf.butler.NamedKeyDict`
            Dictionary mapping `~lsst.daf.butler.DatasetType` to
            `~lsst.daf.butler.DatasetRef`, with both
            `~lsst.daf.butler.DatasetType` instances and string names usable
            as keys.
        """
        return NamedKeyDict(
            {datasetType: refs[0] for datasetType, refs in self.unpackMultiRefs(storage_classes).items()}
        )

    def unpackMultiRefs(self, storage_classes: dict[str, str]) -> NamedKeyDict[DatasetType, list[DatasetRef]]:
        """Unpack nested multi-element `~lsst.daf.butler.DatasetRef` dicts into
        a new mapping with `~lsst.daf.butler.DatasetType` keys and `list` of
        `~lsst.daf.butler.DatasetRef` values.

        Parameters
        ----------
        storage_classes : `dict` [ `str`, `str` ]
            Mapping from dataset type name to the storage class to use for that
            dataset type.  These are typically the storage classes declared
            for a particular task, which may differ rom the data repository
            definitions.

        Returns
        -------
        dictionary : `~lsst.daf.butler.NamedKeyDict`
            Dictionary mapping `~lsst.daf.butler.DatasetType` to `list` of
            `~lsst.daf.butler.DatasetRef`, with both
            `~lsst.daf.butler.DatasetType` instances and string names usable
            as keys.
        """
        raise NotImplementedError()


class _DatasetDict(_DatasetDictBase[dict[DataCoordinate, _RefHolder]]):
    """A custom dictionary that maps `~lsst.daf.butler.DatasetType` to a nested
    dictionary of the known `~lsst.daf.butler.DatasetRef` instances of that
    type.
    """

    @classmethod
    def fromDatasetTypes(
        cls, datasetTypes: Iterable[DatasetType], *, universe: DimensionUniverse
    ) -> _DatasetDict:
        """Construct a dictionary from a flat iterable of
        `~lsst.daf.butler.DatasetType` keys.

        Parameters
        ----------
        datasetTypes : `~collections.abc.Iterable` of \
                `~lsst.daf.butler.DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be empty
            dictionaries.
        universe : `~lsst.daf.butler.DimensionUniverse`
            Universe of all possible dimensions.

        Returns
        -------
        dictionary : `_DatasetDict`
            A new `_DatasetDict` instance.
        """
        return cls({datasetType: {} for datasetType in datasetTypes}, universe=universe)

    @classmethod
    def fromSubset(
        cls,
        datasetTypes: Collection[DatasetType],
        first: _DatasetDict,
        *rest: _DatasetDict,
    ) -> _DatasetDict:
        """Return a new dictionary by extracting items corresponding to the
        given keys from one or more existing dictionaries.

        Parameters
        ----------
        datasetTypes : `~collections.abc.Iterable` of \
                `~lsst.daf.butler.DatasetType`
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
        return cast(_DatasetDict, cls._fromSubset(datasetTypes, first, *rest))

    def unpackMultiRefs(self, storage_classes: dict[str, str]) -> NamedKeyDict[DatasetType, list[DatasetRef]]:
        # Docstring inherited.
        result = {}
        for dataset_type, holders in self.items():
            if (
                override := storage_classes.get(dataset_type.name, dataset_type.storageClass_name)
            ) != dataset_type.storageClass_name:
                dataset_type = dataset_type.overrideStorageClass(override)
                refs = [holder.resolved_ref.overrideStorageClass(override) for holder in holders.values()]
            else:
                refs = [holder.resolved_ref for holder in holders.values()]
            result[dataset_type] = refs
        return NamedKeyDict(result)

    def extract(
        self, datasetType: DatasetType, dataIds: Iterable[DataCoordinate]
    ) -> Iterator[tuple[DataCoordinate, DatasetRef | None]]:
        """Iterate over the contained `~lsst.daf.butler.DatasetRef` instances
        that match the given `~lsst.daf.butler.DatasetType` and data IDs.

        Parameters
        ----------
        datasetType : `~lsst.daf.butler.DatasetType`
            Dataset type to match.
        dataIds : `~collections.abc.Iterable` \
                [ `~lsst.daf.butler.DataCoordinate` ]
            Data IDs to match.

        Returns
        -------
        refs : `~collections.abc.Iterator` [ `~lsst.daf.butler.DatasetRef` ]
            DatasetRef instances for which ``ref.datasetType == datasetType``
            and ``ref.dataId`` is in ``dataIds``.
        """
        refs = self[datasetType]
        return ((dataId, refs[dataId].ref) for dataId in dataIds)

    def isdisjoint(self, other: _DatasetDict) -> bool:
        """Test whether ``self`` and ``other`` have any datasets in common.

        Datasets are considered in common if they have the same *parent*
        dataset type name and data ID; storage classes and components are not
        considered.
        """
        by_parent_name = {k.nameAndComponent()[0]: v.keys() for k, v in self.items()}
        for k, v in other.items():
            parent_name, _ = k.nameAndComponent()
            if not by_parent_name.get(parent_name, frozenset[DataCoordinate]()).isdisjoint(v.keys()):
                return False
        return True

    def iter_resolved_refs(self) -> Iterator[DatasetRef]:
        """Iterate over all DatasetRef instances held by this data structure,
        assuming that each `_RefHolder` already carries are resolved ref.
        """
        for holders_by_data_id in self.values():
            for holder in holders_by_data_id.values():
                yield holder.resolved_ref


class _DatasetDictMulti(_DatasetDictBase[defaultdict[DataCoordinate, list[_RefHolder]]]):
    """A custom dictionary that maps `~lsst.daf.butler.DatasetType` to a nested
    dictionary of the known `~lsst.daf.butler.DatasetRef` instances of that
    type. Nexted dictionary can contain multiple refs for the same data ID,
    suitable for use with calibration datasets.
    """

    @classmethod
    def fromDatasetTypes(
        cls, datasetTypes: Iterable[DatasetType], *, universe: DimensionUniverse
    ) -> _DatasetDictMulti:
        """Construct a dictionary from a flat iterable of
        `~lsst.daf.butler.DatasetType` keys.

        Parameters
        ----------
        datasetTypes : `~collections.abc.Iterable` of \
                `~lsst.daf.butler.DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be empty
            dictionaries.
        universe : `~lsst.daf.butler.DimensionUniverse`
            Universe of all possible dimensions.

        Returns
        -------
        dictionary : `_DatasetDictMulti`
            A new `_DatasetDictMulti` instance.
        """
        return cls({datasetType: defaultdict(list) for datasetType in datasetTypes}, universe=universe)

    @classmethod
    def fromSubset(
        cls,
        datasetTypes: Collection[DatasetType],
        first: _DatasetDictMulti,
        *rest: _DatasetDictMulti,
    ) -> _DatasetDictMulti:
        """Return a new dictionary by extracting items corresponding to the
        given keys from one or more existing dictionaries.

        Parameters
        ----------
        datasetTypes : `~collections.abc.Iterable` of \
                `~lsst.daf.butler.DatasetType`
            DatasetTypes to use as keys for the dict.  Values will be obtained
            by lookups against ``first`` and ``rest``.
        first : `_DatasetDictMulti`
            Another dictionary from which to extract values.
        rest
            Additional dictionaries from which to extract values.

        Returns
        -------
        dictionary : `_DatasetDictMulti`
            A new dictionary instance.
        """
        return cast(_DatasetDictMulti, cls._fromSubset(datasetTypes, first, *rest))

    def unpackMultiRefs(self, storage_classes: dict[str, str]) -> NamedKeyDict[DatasetType, list[DatasetRef]]:
        # Docstring inherited.
        result = {}
        for dataset_type, holder_map in self.items():
            if (
                override := storage_classes.get(dataset_type.name, dataset_type.storageClass_name)
            ) != dataset_type.storageClass_name:
                dataset_type = dataset_type.overrideStorageClass(override)
                refs = []
                for holder_list in holder_map.values():
                    refs += [holder.resolved_ref.overrideStorageClass(override) for holder in holder_list]
            else:
                refs = []
                for holder_list in holder_map.values():
                    refs += [holder.resolved_ref for holder in holder_list]
            result[dataset_type] = refs
        return NamedKeyDict(result)

    def iter_resolved_refs(self) -> Iterator[DatasetRef]:
        """Iterate over all DatasetRef instances held by this data structure,
        assuming that each `_RefHolder` already carries are resolved ref.
        """
        for holders_by_data_id in self.values():
            for holder_list in holders_by_data_id.values():
                for holder in holder_list:
                    yield holder.resolved_ref


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
    dataId : `~lsst.daf.butler.DataCoordinate`
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
    """Nested dictionary containing `~lsst.daf.butler.DatasetRef` inputs to
    this quantum.

    This is initialized to map each `~lsst.daf.butler.DatasetType` to an empty
    dictionary at construction.  Those nested dictionaries are populated
    (with data IDs as keys) with unresolved `~lsst.daf.butler.DatasetRef`
    instances in `_PipelineScaffolding.connectDataIds`.
    """

    outputs: _DatasetDict
    """Nested dictionary containing `~lsst.daf.butler.DatasetRef` outputs this
    quantum.
    """

    prerequisites: _DatasetDict
    """Nested dictionary containing `~lsst.daf.butler.DatasetRef` prerequisite
    inputs to this quantum.
    """

    def computeSpatialExtent(self, pixelization: PixelizationABC) -> RangeSet:
        """Return the spatial extent of this quantum's inputs and outputs in
        a skypix system.

        Parameters
        ----------
        pixelization : `lsst.sphgeom.PixelizationABC`
            Pixelization system.

        Returns
        -------
        extent : `lsst.sphgeom.RangeSet`
            Ranges of sky pixels that touch this quantum's inputs and outputs.
        """
        result = RangeSet()
        for dataset_type, datasets in itertools.chain(self.inputs.items(), self.outputs.items()):
            if dataset_type.dimensions.spatial:
                for data_id in datasets:
                    result |= pixelization.envelope(data_id.region)
        return result

    def makeQuantum(self, datastore_records: Mapping[str, DatastoreRecordData] | None = None) -> Quantum:
        """Transform the scaffolding object into a true `Quantum` instance.

        Parameters
        ----------
        datastore_records : `~collections.abc.Mapping` [ `str`, \
                `~lsst.daf.butler.DatastoreRecordData` ], optional
            If not `None` then fill datastore records in each generated Quantum
            using the records from this structure.

        Returns
        -------
        quantum : `Quantum`
            An actual `Quantum` instance.
        """
        allInputs = self.inputs.unpackMultiRefs(self.task.storage_classes)
        allInputs.update(self.prerequisites.unpackMultiRefs(self.task.storage_classes))
        # Give the task's Connections class an opportunity to remove some
        # inputs, or complain if they are unacceptable.
        # This will raise if one of the check conditions is not met, which is
        # the intended behavior.
        # If it raises NotWorkFound, there is a bug in the QG algorithm
        # or the adjustQuantum is incorrectly trying to make a prerequisite
        # input behave like a regular input; adjustQuantum should only raise
        # NoWorkFound if a regular input is missing, and it shouldn't be
        # possible for us to have generated ``self`` if that's true.
        helper = AdjustQuantumHelper(
            inputs=allInputs, outputs=self.outputs.unpackMultiRefs(self.task.storage_classes)
        )
        helper.adjust_in_place(self.task.taskDef.connections, self.task.taskDef.label, self.dataId)
        initInputs = self.task.initInputs.unpackSingleRefs(self.task.storage_classes)
        quantum_records: Mapping[str, DatastoreRecordData] | None = None
        if datastore_records is not None:
            quantum_records = {}
            input_refs = list(itertools.chain.from_iterable(helper.inputs.values()))
            input_refs += list(initInputs.values())
            input_ids = {ref.id for ref in input_refs}
            for datastore_name, records in datastore_records.items():
                matching_records = records.subset(input_ids)
                if matching_records is not None:
                    quantum_records[datastore_name] = matching_records
        # ignore the types because quantum really can take a sequence of inputs
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

    def __init__(
        self,
        taskDef: TaskDef,
        parent: _PipelineScaffolding,
        datasetTypes: TaskDatasetTypes,
    ):
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
        self.prerequisites = _DatasetDictMulti.fromSubset(datasetTypes.prerequisites, parent.prerequisites)
        self.dataIds: set[DataCoordinate] = set()
        self.quanta = {}
        self.storage_classes = {
            connection.name: connection.storageClass
            for connection in self.taskDef.connections.allConnections.values()
        }
        self.storage_classes[
            acc.CONFIG_INIT_OUTPUT_TEMPLATE.format(label=self.taskDef.label)
        ] = acc.CONFIG_INIT_OUTPUT_STORAGE_CLASS
        self.storage_classes[
            acc.LOG_OUTPUT_TEMPLATE.format(label=self.taskDef.label)
        ] = acc.LOG_OUTPUT_STORAGE_CLASS
        self.storage_classes[
            acc.METADATA_OUTPUT_TEMPLATE.format(label=self.taskDef.label)
        ] = acc.METADATA_OUTPUT_STORAGE_CLASS

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

    prerequisites: _DatasetDictMulti
    """Dictionary containing information about input datasets that must be
    present in the repository before any Pipeline containing this task is run
    (`_DatasetDictMulti`).
    """

    quanta: dict[DataCoordinate, _QuantumScaffolding]
    """Dictionary mapping data ID to a scaffolding object for the Quantum of
    this task with that data ID.
    """

    storage_classes: dict[str, str]
    """Mapping from dataset type name to storage class declared by this task.
    """

    def makeQuantumSet(
        self,
        missing: _DatasetDict,
        datastore_records: Mapping[str, DatastoreRecordData] | None = None,
    ) -> set[Quantum]:
        """Create a `set` of `Quantum` from the information in ``self``.

        Parameters
        ----------
        missing : `_DatasetDict`
            Input datasets that have not been found.
        datastore_records : `dict`
            Record from the datastore to export with quanta.

        Returns
        -------
        nodes : `set` of `Quantum`
            The `Quantum` elements corresponding to this task.
        """
        outputs = set()
        for q in self.quanta.values():
            try:
                tmpQuanta = q.makeQuantum(datastore_records)
                outputs.add(tmpQuanta)
            except (NoWorkFound, FileNotFoundError) as exc:
                if not missing.isdisjoint(q.inputs):
                    # This is a node that is known to be pruned later and
                    # should be left in even though some follow up queries
                    # fail. This allows the pruning to start from this quantum
                    # with known issues, and prune other nodes it touches.
                    inputs = q.inputs.unpackMultiRefs(self.storage_classes)
                    inputs.update(q.prerequisites.unpackMultiRefs(self.storage_classes))
                    tmpQuantum = Quantum(
                        taskName=q.task.taskDef.taskName,
                        taskClass=q.task.taskDef.taskClass,
                        dataId=q.dataId,
                        initInputs=q.task.initInputs.unpackSingleRefs(self.storage_classes),
                        inputs=inputs,
                        outputs=q.outputs.unpackMultiRefs(self.storage_classes),
                    )
                    outputs.add(tmpQuantum)
                else:
                    raise exc
        return outputs


class _DatasetIdMaker:
    """Helper class which generates random dataset UUIDs for unresolved
    datasets.
    """

    def __init__(self, run: str):
        self.run = run
        # Cache of dataset refs generated so far.
        self.resolved: dict[tuple[DatasetType, DataCoordinate], DatasetRef] = {}

    def resolveRef(self, dataset_type: DatasetType, data_id: DataCoordinate) -> DatasetRef:
        # For components we need their parent dataset ID.
        if dataset_type.isComponent():
            parent_type = dataset_type.makeCompositeDatasetType()
            # Parent should be resolved if this is an existing input, or it
            # should be in the cache already if it is an intermediate.
            key = parent_type, data_id
            if key not in self.resolved:
                raise ValueError(f"Composite dataset is missing from cache: {parent_type} {data_id}")
            parent_ref = self.resolved[key]
            return DatasetRef(dataset_type, data_id, id=parent_ref.id, run=parent_ref.run, conform=False)

        key = dataset_type, data_id
        if (resolved := self.resolved.get(key)) is None:
            resolved = DatasetRef(dataset_type, data_id, run=self.run, conform=False)
            self.resolved[key] = resolved
        return resolved

    def resolveDict(
        self, dataset_type: DatasetType, refs: dict[DataCoordinate, _RefHolder], is_output: bool
    ) -> None:
        """Resolve all unresolved references in the provided dictionary."""
        for data_id, holder in refs.items():
            if holder.ref is None or (is_output and holder.ref.run != self.run):
                holder.ref = self.resolveRef(holder.dataset_type, data_id)


@dataclass
class _PipelineScaffolding:
    """A helper data structure that organizes the information involved in
    constructing a `QuantumGraph` for a `Pipeline`.

    Parameters
    ----------
    pipeline : `Pipeline` or `~collections.abc.Iterable` [ `TaskDef` ]
        Sequence of tasks from which a graph is to be constructed.  Must
        have nested task classes already imported.
    universe : `~lsst.daf.butler.DimensionUniverse`
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

    def __init__(self, pipeline: Pipeline | Iterable[TaskDef], *, registry: Registry):
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
        ):
            setattr(
                self,
                attr,
                _DatasetDict.fromDatasetTypes(getattr(datasetTypes, attr), universe=registry.dimensions),
            )
        self.prerequisites = _DatasetDictMulti.fromDatasetTypes(
            datasetTypes.prerequisites, universe=registry.dimensions
        )
        self.missing = _DatasetDict(universe=registry.dimensions)
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
            for taskDef, taskDatasetTypes in zip(pipeline, datasetTypes.byTask.values(), strict=True)
        ]

    def __repr__(self) -> str:
        # Default dataclass-injected __repr__ gets caught in an infinite loop
        # because of back-references.
        return f"_PipelineScaffolding(tasks={self.tasks}, ...)"

    tasks: list[_TaskScaffolding]
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

    prerequisites: _DatasetDictMulti
    """Datasets that are consumed when running this pipeline and looked up
    per-Quantum when generating the graph (`_DatasetDictMulti`).
    """

    defaultDatasetQueryConstraints: NamedValueSet[DatasetType]
    """Datasets that should be used as constraints in the initial query,
    according to tasks (`~lsst.daf.butler.NamedValueSet`).
    """

    dimensions: DimensionGraph
    """All dimensions used by any regular input, intermediate, or output
    (not prerequisite) dataset; the set of dimension used in the "Big Join
    Query" (`~lsst.daf.butler.DimensionGraph`).

    This is required to be a superset of all task quantum dimensions.
    """

    missing: _DatasetDict
    """Datasets whose existence was originally predicted but were not
    actually found.

    Quanta that require these datasets as inputs will be pruned (recursively)
    when actually constructing a `QuantumGraph` object.

    These are currently populated only when the "initial dataset query
    constraint" does not include all overall-input dataset types, and hence the
    initial data ID query can include data IDs that it should not.
    """

    globalInitOutputs: _DatasetDict | None = None
    """Per-pipeline global output datasets (e.g. packages) (`_DatasetDict`)
    """

    @contextmanager
    def connectDataIds(
        self,
        registry: Registry,
        collections: Any,
        userQuery: str | None,
        externalDataId: DataCoordinate,
        datasetQueryConstraint: DatasetQueryConstraintVariant = DatasetQueryConstraintVariant.ALL,
        bind: Mapping[str, Any] | None = None,
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
        externalDataId : `~lsst.daf.butler.DataCoordinate`
            Externally-provided data ID that should be used to restrict the
            results, just as if these constraints had been included via ``AND``
            in ``userQuery``.  This includes (at least) any instrument named
            in the pipeline definition.
        datasetQueryConstraint : `DatasetQueryConstraintVariant`, optional
            The query constraint variant that should be used to constraint the
            query based on dataset existance, defaults to
            `DatasetQueryConstraintVariant.ALL`.
        bind : `~collections.abc.Mapping`, optional
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
            self.initInputs.items(),
            self.initIntermediates.items(),
            self.initOutputs.items(),
        ):
            refs[emptyDataId] = _RefHolder(datasetType)
        # Run one big query for the data IDs for task dimensions and regular
        # inputs and outputs.  We limit the query to only dimensions that are
        # associated with the input dataset types, but don't (yet) try to
        # obtain the dataset_ids for those inputs.
        _LOG.debug(
            "Submitting data ID query over dimensions %s and materializing results.",
            list(self.dimensions.names),
        )
        queryArgs: dict[str, Any] = {
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
            inputs = {k.name: k for k in self.inputs}
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
            n = 0  # Must count in loop since this is a lazy iterable.
            for commonDataId in commonDataIds:
                # Create DatasetRefs for all DatasetTypes from this result row,
                # noting that we might have created some already.
                # We remember both those that already existed and those that we
                # create now.
                n += 1
                refsForRow = {}
                dataIdCacheForRow: dict[DimensionGraph, DataCoordinate] = {}
                for datasetType, refs in itertools.chain(
                    self.inputs.items(),
                    self.intermediates.items(),
                    self.outputs.items(),
                ):
                    datasetDataId: DataCoordinate | None
                    if (datasetDataId := dataIdCacheForRow.get(datasetType.dimensions)) is None:
                        datasetDataId = commonDataId.subset(datasetType.dimensions)
                        dataIdCacheForRow[datasetType.dimensions] = datasetDataId
                    ref_holder = refs.get(datasetDataId)
                    if ref_holder is None:
                        ref_holder = _RefHolder(datasetType)
                        refs[datasetDataId] = ref_holder
                    refsForRow[datasetType.name] = ref_holder
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
                        dataId = dataIdCacheForRow[datasetType.dimensions]
                        ref_holder = refsForRow[datasetType.name]
                        quantum.inputs[datasetType.name][dataId] = ref_holder
                    for datasetType in task.outputs:
                        dataId = dataIdCacheForRow[datasetType.dimensions]
                        ref_holder = refsForRow[datasetType.name]
                        quantum.outputs[datasetType.name][dataId] = ref_holder
            if n == 0:
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
        run: str,
        commonDataIds: DataCoordinateQueryResults,
        *,
        skipExistingIn: Any = None,
        clobberOutputs: bool = True,
        constrainedByAllDatasets: bool = True,
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
        run : `str`
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
            with contextlib.suppress(MissingCollectionError):
                run_exists = bool(registry.queryCollections(run))

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

        idMaker = _DatasetIdMaker(run)

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
                        # To resolve all outputs we have to remember existing
                        # ones to avoid generating new dataset IDs for them.
                        refs[resolvedRef.dataId].ref = (
                            resolvedRef.makeComponentRef(component) if component is not None else resolvedRef
                        )

                # And check skipExistingIn too, if RUN collection is in
                # it is handled above
                if skip_collections_wildcard is not None:
                    try:
                        resolvedRefQueryResults = subset.findDatasets(
                            parent_dataset_type,
                            collections=skip_collections_wildcard,
                            findFirst=True,
                        )
                    except MissingDatasetTypeError:
                        resolvedRefQueryResults = []
                    for resolvedRef in resolvedRefQueryResults:
                        if resolvedRef.dataId not in refs:
                            continue
                        refs[resolvedRef.dataId].ref = (
                            resolvedRef.makeComponentRef(component) if component is not None else resolvedRef
                        )

        # Look up input and initInput datasets in the input collection(s). We
        # accumulate datasets in self.missing, if the common data IDs were not
        # constrained on dataset type existence.
        for datasetType, refs in itertools.chain(self.initInputs.items(), self.inputs.items()):
            _LOG.debug(
                "Resolving %d datasets for input dataset %s.",
                len(refs),
                datasetType.name,
            )
            if datasetType.isComponent():
                parent_dataset_type = datasetType.makeCompositeDatasetType()
                component = datasetType.component()
            else:
                parent_dataset_type = datasetType
                component = None
            missing_for_dataset_type: dict[DataCoordinate, _RefHolder] = {}
            try:
                resolvedRefQueryResults = commonDataIds.subset(
                    datasetType.dimensions, unique=True
                ).findDatasets(parent_dataset_type, collections=collections, findFirst=True)
            except MissingDatasetTypeError:
                resolvedRefQueryResults = []
            dataIdsNotFoundYet = set(refs.keys())
            for resolvedRef in resolvedRefQueryResults:
                dataIdsNotFoundYet.discard(resolvedRef.dataId)
                if resolvedRef.dataId not in refs:
                    continue
                refs[resolvedRef.dataId].ref = (
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
                    # If the common dataIds were not constrained using all the
                    # input dataset types, it is possible that some data ids
                    # found don't correspond to existing datasets. Mark these
                    # for later pruning from the quantum graph.
                    for k in dataIdsNotFoundYet:
                        missing_for_dataset_type[k] = refs[k]
            if missing_for_dataset_type:
                self.missing[datasetType] = missing_for_dataset_type

        # Resolve the missing refs, just so they look like all of the others;
        # in the end other code will make sure they never appear in the QG.
        for dataset_type, refDict in self.missing.items():
            idMaker.resolveDict(dataset_type, refDict, is_output=False)

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
                # exception to be raised.
                if skip_collections_wildcard is not None or (run_exists and clobberOutputs):
                    resolvedRefs = []
                    unresolvedDataIds = []
                    haveMetadata = False
                    for datasetType, originalRefs in quantum.outputs.items():
                        for dataId, ref in task.outputs.extract(datasetType, originalRefs.keys()):
                            if ref is not None:
                                resolvedRefs.append(ref)
                                originalRefs[dataId].ref = ref
                                if datasetType.name == task.taskDef.metadataDatasetName:
                                    haveMetadata = True
                            else:
                                unresolvedDataIds.append((datasetType, dataId))
                    if resolvedRefs:
                        if haveMetadata or not unresolvedDataIds:
                            dataIdsSucceeded.append(quantum.dataId)
                            if skip_collections_wildcard is not None:
                                continue
                        else:
                            dataIdsFailed.append(quantum.dataId)
                            if not clobberOutputs and run_exists:
                                raise OutputExistsError(
                                    f"Quantum {quantum.dataId} of task with label "
                                    f"'{quantum.task.taskDef.label}' has some outputs that exist "
                                    f"({resolvedRefs}) "
                                    f"and others that don't ({unresolvedDataIds}), with no metadata output, "
                                    "and clobbering outputs was not enabled."
                                )
                # Update the input DatasetRefs to the resolved ones we already
                # searched for.
                for datasetType, input_refs in quantum.inputs.items():
                    for data_id, ref in task.inputs.extract(datasetType, input_refs.keys()):
                        input_refs[data_id].ref = ref
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
                        where = ""
                        bind: dict[str, Any] = {}
                        if not quantum.dataId.graph.spatial:
                            # This has skypix dimensions (probably a reference
                            # catalog), but the quantum's data is not spatial
                            # (it's probably a full-survey sequence point).
                            # Try to limit the spatial extent to the union of
                            # the spatial extent of the inputs and outputs.
                            for dimension in datasetType.dimensions:
                                if isinstance(dimension, SkyPixDimension):
                                    extent = quantum.computeSpatialExtent(dimension.pixelization)
                                    pixels: list[int] = []
                                    for begin, end in extent:
                                        pixels.extend(range(begin, end))
                                    if not pixels:
                                        _LOG.warning(
                                            "Prerequisite input %r to task %r may be unbounded.",
                                            datasetType.name,
                                            quantum.task.taskDef.label,
                                        )
                                    else:
                                        bind["quantum_extent"] = pixels
                                        where = f"{dimension.name} IN (quantum_extent)"
                                    break
                        # Most general case.
                        prereq_refs = [
                            prereq_ref if component is None else prereq_ref.makeComponentRef(component)
                            for prereq_ref in registry.queryDatasets(
                                parent_dataset_type,
                                collections=collections,
                                dataId=quantum.dataId,
                                findFirst=True,
                                where=where,
                                bind=bind,
                            ).expanded()
                        ]

                    for ref in prereq_refs:
                        if ref is not None:
                            quantum.prerequisites[datasetType][ref.dataId] = _RefHolder(datasetType, ref)
                            task.prerequisites[datasetType][ref.dataId].append(_RefHolder(datasetType, ref))

                # Resolve all quantum inputs and outputs.
                for dataset_type, refDict in quantum.inputs.items():
                    idMaker.resolveDict(dataset_type, refDict, is_output=False)
                for dataset_type, refDict in quantum.outputs.items():
                    idMaker.resolveDict(dataset_type, refDict, is_output=True)

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
                elif clobberOutputs and run_exists:
                    _LOG.info(
                        "Found %d successful quanta for task with label '%s' "
                        "that will need to be clobbered during execution.",
                        len(dataIdsSucceeded),
                        task.taskDef.label,
                    )
            if dataIdsFailed:
                if clobberOutputs and run_exists:
                    _LOG.info(
                        "Found %d failed/incomplete quanta for task with label '%s' "
                        "that will need to be clobbered during execution.",
                        len(dataIdsFailed),
                        task.taskDef.label,
                    )

            # Resolve task initInputs and initOutputs.
            for dataset_type, refDict in task.initInputs.items():
                idMaker.resolveDict(dataset_type, refDict, is_output=False)
            if task.quanta:
                for dataset_type, refDict in task.initOutputs.items():
                    idMaker.resolveDict(dataset_type, refDict, is_output=True)
            else:
                # If there are no quanta for this task remaining (because they
                # all succeeded before and we're skipping those now), we do not
                # resolve the init outputs as outputs; instead we might want to
                # find them in the skip-existing-in collections instead, which
                # means from the pipeline perspective they're initInputs, not
                # initIntermediates.  They will be resolved by the tasks that
                # use them as inputs, or not at all.
                for dataset_type in task.initOutputs:
                    init_datasets = self.initIntermediates.pop(dataset_type, None)
                    if init_datasets is not None:
                        self.initInputs[dataset_type] = init_datasets
                    self.initOutputs.pop(dataset_type, None)
                # Removing the initInputs of this task from the scaffolding
                # data structures is trickier, because the same initInput may
                # be used by multiple tasks.
                # TODO: DM-38498: handle the above problem better.

        # Collect initOutputs that do not belong to any task.
        global_dataset_types: set[DatasetType] = set(self.initOutputs)
        for task in self.tasks:
            global_dataset_types -= set(task.initOutputs)
        if global_dataset_types:
            self.globalInitOutputs = _DatasetDict.fromSubset(global_dataset_types, self.initOutputs)
            for dataset_type, refDict in self.globalInitOutputs.items():
                idMaker.resolveDict(dataset_type, refDict, is_output=True)

    def makeQuantumGraph(
        self,
        registry: Registry,
        metadata: Mapping[str, Any] | None = None,
        datastore: Datastore | None = None,
    ) -> QuantumGraph:
        """Create a `QuantumGraph` from the quanta already present in
        the scaffolding data structure.

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry for the data repository; used for all data ID queries.
        metadata : `~collections.abc.Mapping` of `str` to primitives, optional
            This is an optional parameter of extra data to carry with the
            graph.  Entries in this mapping should be able to be serialized in
            JSON.
        datastore : `~lsst.daf.butler.Datastore`, optional
            If not `None` then fill datastore records in each generated
            Quantum.

        Returns
        -------
        graph : `QuantumGraph`
            The full `QuantumGraph`.
        """
        datastore_records: Mapping[str, DatastoreRecordData] | None = None
        if datastore is not None:
            datastore_records = datastore.export_records(
                ref.makeCompositeRef() if ref.isComponent() else ref
                for ref in itertools.chain(
                    self.inputs.iter_resolved_refs(),
                    self.initInputs.iter_resolved_refs(),
                    self.prerequisites.iter_resolved_refs(),
                )
            )

        graphInput: dict[TaskDef, set[Quantum]] = {}
        for task in self.tasks:
            if not task.quanta:
                continue
            qset = task.makeQuantumSet(missing=self.missing, datastore_records=datastore_records)
            graphInput[task.taskDef] = qset

        taskInitInputs = {
            task.taskDef: task.initInputs.unpackSingleRefs(task.storage_classes).values()
            for task in self.tasks
            if task.quanta
        }
        taskInitOutputs = {
            task.taskDef: task.initOutputs.unpackSingleRefs(task.storage_classes).values()
            for task in self.tasks
            if task.quanta
        }

        globalInitOutputs: list[DatasetRef] = []
        if self.globalInitOutputs is not None:
            for refs_dict in self.globalInitOutputs.values():
                globalInitOutputs.extend(holder.resolved_ref for holder in refs_dict.values())

        graph = QuantumGraph(
            graphInput,
            metadata=metadata,
            pruneRefs=list(self.missing.iter_resolved_refs()),
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
        chain: list[_DatasetDict | _DatasetDictMulti] = [
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
        all_names: set[str] = {dstype.name for dstype in itertools.chain(*chain)}
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
    datastore : `~lsst.daf.butler.Datastore`, optional
        If not `None` then fill datastore records in each generated Quantum.
    """

    def __init__(
        self,
        registry: Registry,
        skipExistingIn: Any = None,
        clobberOutputs: bool = True,
        datastore: Datastore | None = None,
    ):
        self.registry = registry
        self.dimensions = registry.dimensions
        self.skipExistingIn = skipExistingIn
        self.clobberOutputs = clobberOutputs
        self.datastore = datastore

    def makeGraph(
        self,
        pipeline: Pipeline | Iterable[TaskDef],
        collections: Any,
        run: str,
        userQuery: str | None,
        datasetQueryConstraint: DatasetQueryConstraintVariant = DatasetQueryConstraintVariant.ALL,
        metadata: Mapping[str, Any] | None = None,
        bind: Mapping[str, Any] | None = None,
        dataId: DataCoordinate | None = None,
    ) -> QuantumGraph:
        """Create execution graph for a pipeline.

        Parameters
        ----------
        pipeline : `Pipeline` or `~collections.abc.Iterable` [ `TaskDef` ]
            Pipeline definition, task names/classes and their configs.
        collections
            Expressions representing the collections to search for input
            datasets.  See :ref:`daf_butler_ordered_collection_searches`.
        run : `str`
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
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``userQuery`` expression, keyed by the identifiers they replace.
        dataId : `lsst.daf.butler.DataCoordinate`, optional
            Data ID that should also be included in the query constraint.

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
        if not collections and (scaffolding.initInputs or scaffolding.inputs or scaffolding.prerequisites):
            raise ValueError("Pipeline requires input datasets but no input collections provided.")
        if dataId is None:
            dataId = DataCoordinate.makeEmpty(self.registry.dimensions)
        if isinstance(pipeline, Pipeline):
            dataId = pipeline.get_data_id(self.registry.dimensions).union(dataId)
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
            )
        return scaffolding.makeQuantumGraph(
            registry=self.registry, metadata=metadata, datastore=self.datastore
        )
