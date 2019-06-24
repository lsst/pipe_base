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
from typing import Set, List, Dict, Optional
import logging

# -----------------------------
#  Imports for other modules --
# -----------------------------
from sqlalchemy.sql import and_

from .pipeline import PipelineDatasetTypes, Pipeline, TaskDef
from .graph import QuantumGraph, QuantumGraphTaskNodes
from lsst.daf.butler import Quantum, DatasetRef, DimensionGraph, DataId
from lsst.daf.butler.core.utils import NamedKeyDict
from lsst.daf.butler.sql import DataIdQueryBuilder, SingleDatasetQueryBuilder

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__.partition(".")[2])


@dataclass
class _DatasetScaffolding:

    __slots__ = ("dimensions", "producer", "consumers", "dataIds", "refs")

    dimensions: DimensionGraph
    producer: Optional[_TaskScaffolding]
    consumers: Dict[str, _TaskScaffolding]
    dataIds: Set[DataId]
    refs: List[DatasetRef]

    def __init__(self, dimensions, *, producer=None, consumers=None, refs=()):
        self.dimensions = dimensions
        self.producer = producer
        self.consumers = consumers if consumers is not None else {}
        self.dataIds = set()
        self.refs = []


class _DatasetScaffoldingDict(NamedKeyDict):
    """Custom dictionary that maps DatasetType to _DatasetScaffolding.

    _DatasetScaffoldingDict can report the aggregate dimensions of all held
    dataset types.
    """

    def __init__(self, *args, universe):
        super().__init__(*args)
        self.universe = universe

    @classmethod
    def fromDatasetTypes(cls, datasetTypes, *args, universe):
        return cls(((datasetType, _DatasetScaffolding(datasetType.dimensions.implied(only=False)))
                    for datasetType in datasetTypes),
                   universe=universe)

    @classmethod
    def fromSubset(cls, datasetTypes, first, *rest):
        combined = ChainMap(first, *rest)
        return cls(((datasetType, combined[datasetType]) for datasetType in datasetTypes),
                   universe=first.universe)

    @property
    def dimensions(self):
        base = self.universe.empty
        if len(self) == 0:
            return base
        return base.union(*(scaffolding.dimensions for scaffolding in self.values()), implied=True)

    def unpackRefs(self):
        return NamedKeyDict((datasetType, scaffolding.refs[0]) for datasetType, scaffolding in self.items())


@dataclass
class _TaskScaffolding:
    taskDef: TaskDef
    dimensions: DimensionGraph
    initInputs: _DatasetScaffoldingDict
    initOutputs: _DatasetScaffoldingDict
    inputs: _DatasetScaffoldingDict
    outputs: _DatasetScaffoldingDict
    prerequisites: _DatasetScaffoldingDict
    dataIds: Set[DataId]
    quanta: List[Quantum]

    def __init__(self, taskDef, parent, datasetTypes):
        universe = parent.dimensions.universe
        self.taskDef = taskDef
        self.dimensions = universe.extract(taskDef.config.quantum.dimensions, implied=True)
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

    def makeQuantumGraphTaskNodes(self):
        return QuantumGraphTaskNodes(
            taskDef=self.taskDef,
            quanta=self.quanta,
            initInputs=self.initInputs.unpackRefs(),
            initOutputs=self.initOutputs.unpackRefs(),
        )


@dataclass
class _PipelineScaffolding:

    initInputs: _DatasetScaffoldingDict
    initIntermediates: _DatasetScaffoldingDict
    initOutputs: _DatasetScaffoldingDict
    inputs: _DatasetScaffoldingDict
    intermediates: _DatasetScaffoldingDict
    outputs: _DatasetScaffoldingDict
    prerequisites: _DatasetScaffoldingDict
    dimensions: DimensionGraph
    tasks: List[_TaskScaffolding]

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

    def fillDataIds(self, registry, originInfo, userQuery):
        # Initialization datasets always have empty data IDs.
        emptyDataId = DataId(dimensions=registry.dimensions.empty)
        for datasetType, scaffolding in itertools.chain(self.initInputs.items(),
                                                        self.initIntermediates.items(),
                                                        self.initOutputs.items()):
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
        # dimensions that change are not relevant.
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
        if clobberExisting and skipExisting:
            raise ValueError("clobberExisting and skipExisting cannot both be true.")
        # Look up input and initInput datasets in the input collection(s).
        for datasetType, scaffolding in itertools.chain(self.initInputs.items(), self.inputs.items()):
            # TODO: we only need to use SingleDatasetQueryBuilder here because
            # it provides multi-collection search support.  There should be a
            # way to do that directly with Registry, and it should probably
            # operate by just doing an unordered collection search and
            # resolving the order in Python.
            builder = SingleDatasetQueryBuilder.fromCollections(
                registry, datasetType,
                collections=originInfo.getInputCollections(datasetType.name)
            )
            linkColumns = {link: builder.findSelectableForLink(link).columns[link]
                           for link in datasetType.dimensions.links()}
            for dataId in scaffolding.dataIds:
                ref = builder.executeOne(
                    whereSql=and_(*[col == dataId[link] for link, col in linkColumns.items()]),
                    expandDataId=True
                )
                if ref is None:
                    # data IDs have been expanded to include implied dimensions,
                    # which is not what we want for the DatasetRef.
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
                # and skipExisting (it would make sense to put them in
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

    def fillQuanta(self, registry, originInfo, *, skipExisting=True, clobberExisting=False):
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
                for datasetType, scaffolding in task.outputs.items():
                    outputs[datasetType] = [ref for ref, dataId in zip(scaffolding.refs, scaffolding.dataIds)
                                            if quantumDataId.matches(dataId)]
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
                    links = quantumDataId.dimensions().links() & datasetType.dimensions.links()
                    if not datasetType.dimensions.issubset(quantumDataId.dimensions()):
                        links |= builder.relateDimensions(quantumDataId.dimensions(),
                                                          addResultColumns=False)
                    linkColumns = {link: builder.findSelectableForLink(link).columns[link] for link in links}
                    inputs[datasetType] = list(
                        builder.execute(
                            whereSql=and_(*[col == quantumDataId[link] for link, col in linkColumns.items()]),
                            expandDataId=True
                        )
                    )
                task.quanta.append(
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
    skipExisting : `bool`
        If `True` (default), a Quantum is not created if all its outputs
        already exist.
    clobberExisting : `bool`
        If `True`, overwrite any outputs that already exist.  Cannot be
        `True` if ``skipExisting`` is.
    """

    def __init__(self, taskFactory, registry, skipExisting=True, clobberExisting=False):
        self.taskFactory = taskFactory
        self.registry = registry
        self.dimensions = registry.dimensions
        self.skipExisting = skipExisting
        self.clobberExisting = clobberExisting

    ALLOWED_QUANTUM_DIMENSIONS = frozenset(["instrument", "visit", "exposure", "detector",
                                            "physical_filter", "abstract_filter",
                                            "skymap", "tract", "patch"])

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
                               skipExisting=self.skipExisting,
                               clobberExisting=self.clobberExisting)

        return scaffolding.makeQuantumGraph()
