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

"""Bunch of common classes and methods for use in unit tests.
"""
from __future__ import annotations

__all__ = ["AddTaskConfig", "AddTask", "AddTaskFactoryMock"]

import itertools
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union, cast

import lsst.daf.butler.tests as butlerTests
import lsst.pex.config as pexConfig
import numpy
from lsst.daf.butler import Butler, Config, DataId, DatasetRef, DatasetType, Formatter, LimitedButler
from lsst.daf.butler.core.logging import ButlerLogRecords
from lsst.resources import ResourcePath
from lsst.utils import doImportType

from .. import connectionTypes as cT
from .._instrument import Instrument
from ..config import PipelineTaskConfig
from ..connections import PipelineTaskConnections
from ..graph import QuantumGraph
from ..graphBuilder import DatasetQueryConstraintVariant as DSQVariant
from ..graphBuilder import GraphBuilder
from ..pipeline import Pipeline, TaskDatasetTypes, TaskDef
from ..pipelineTask import PipelineTask
from ..struct import Struct
from ..task import _TASK_FULL_METADATA_TYPE
from ..taskFactory import TaskFactory

if TYPE_CHECKING:
    from lsst.daf.butler import Registry

_LOG = logging.getLogger(__name__)


class SimpleInstrument(Instrument):
    def __init__(self, *args: Any, **kwargs: Any):
        pass

    @staticmethod
    def getName() -> str:
        return "INSTRU"

    def getRawFormatter(self, dataId: DataId) -> Type[Formatter]:
        return Formatter

    def register(self, registry: Registry, *, update: bool = False) -> None:
        pass


class AddTaskConnections(
    PipelineTaskConnections,
    dimensions=("instrument", "detector"),
    defaultTemplates={"in_tmpl": "_in", "out_tmpl": "_out"},
):
    """Connections for AddTask, has one input and two outputs,
    plus one init output.
    """

    input = cT.Input(
        name="add_dataset{in_tmpl}",
        dimensions=["instrument", "detector"],
        storageClass="NumpyArray",
        doc="Input dataset type for this task",
    )
    output = cT.Output(
        name="add_dataset{out_tmpl}",
        dimensions=["instrument", "detector"],
        storageClass="NumpyArray",
        doc="Output dataset type for this task",
    )
    output2 = cT.Output(
        name="add2_dataset{out_tmpl}",
        dimensions=["instrument", "detector"],
        storageClass="NumpyArray",
        doc="Output dataset type for this task",
    )
    initout = cT.InitOutput(
        name="add_init_output{out_tmpl}",
        storageClass="NumpyArray",
        doc="Init Output dataset type for this task",
    )


class AddTaskConfig(PipelineTaskConfig, pipelineConnections=AddTaskConnections):
    """Config for AddTask."""

    addend = pexConfig.Field[int](doc="amount to add", default=3)


class AddTask(PipelineTask):
    """Trivial PipelineTask for testing, has some extras useful for specific
    unit tests.
    """

    ConfigClass = AddTaskConfig
    _DefaultName = "add_task"

    initout = numpy.array([999])
    """InitOutputs for this task"""

    taskFactory: Optional[AddTaskFactoryMock] = None
    """Factory that makes instances"""

    def run(self, input: int) -> Struct:  # type: ignore
        if self.taskFactory:
            # do some bookkeeping
            if self.taskFactory.stopAt == self.taskFactory.countExec:
                raise RuntimeError("pretend something bad happened")
            self.taskFactory.countExec += 1

        self.config = cast(AddTaskConfig, self.config)
        self.metadata.add("add", self.config.addend)
        output = input + self.config.addend
        output2 = output + self.config.addend
        _LOG.info("input = %s, output = %s, output2 = %s", input, output, output2)
        return Struct(output=output, output2=output2)


class AddTaskFactoryMock(TaskFactory):
    """Special task factory that instantiates AddTask.

    It also defines some bookkeeping variables used by AddTask to report
    progress to unit tests.
    """

    def __init__(self, stopAt: int = -1):
        self.countExec = 0  # incremented by AddTask
        self.stopAt = stopAt  # AddTask raises exception at this call to run()

    def makeTask(
        self, taskDef: TaskDef, butler: LimitedButler, initInputRefs: Iterable[DatasetRef] | None
    ) -> PipelineTask:
        taskClass = taskDef.taskClass
        assert taskClass is not None
        task = taskClass(config=taskDef.config, initInputs=None, name=taskDef.label)
        task.taskFactory = self  # type: ignore
        return task


def registerDatasetTypes(registry: Registry, pipeline: Union[Pipeline, Iterable[TaskDef]]) -> None:
    """Register all dataset types used by tasks in a registry.

    Copied and modified from `PreExecInit.initializeDatasetTypes`.

    Parameters
    ----------
    registry : `~lsst.daf.butler.Registry`
        Registry instance.
    pipeline : `typing.Iterable` of `TaskDef`
        Iterable of TaskDef instances, likely the output of the method
        toExpandedPipeline on a `~lsst.pipe.base.Pipeline` object
    """
    for taskDef in pipeline:
        configDatasetType = DatasetType(
            taskDef.configDatasetName, {}, storageClass="Config", universe=registry.dimensions
        )
        storageClass = "Packages"
        packagesDatasetType = DatasetType(
            "packages", {}, storageClass=storageClass, universe=registry.dimensions
        )
        datasetTypes = TaskDatasetTypes.fromTaskDef(taskDef, registry=registry)
        for datasetType in itertools.chain(
            datasetTypes.initInputs,
            datasetTypes.initOutputs,
            datasetTypes.inputs,
            datasetTypes.outputs,
            datasetTypes.prerequisites,
            [configDatasetType, packagesDatasetType],
        ):
            _LOG.info("Registering %s with registry", datasetType)
            # this is a no-op if it already exists and is consistent,
            # and it raises if it is inconsistent. But components must be
            # skipped
            if not datasetType.isComponent():
                registry.registerDatasetType(datasetType)


def makeSimplePipeline(nQuanta: int, instrument: Optional[str] = None) -> Pipeline:
    """Make a simple Pipeline for tests.

    This is called by ``makeSimpleQGraph`` if no pipeline is passed to that
    function. It can also be used to customize the pipeline used by
    ``makeSimpleQGraph`` function by calling this first and passing the result
    to it.

    Parameters
    ----------
    nQuanta : `int`
        The number of quanta to add to the pipeline.
    instrument : `str` or `None`, optional
        The importable name of an instrument to be added to the pipeline or
        if no instrument should be added then an empty string or `None`, by
        default None

    Returns
    -------
    pipeline : `~lsst.pipe.base.Pipeline`
        The created pipeline object.
    """
    pipeline = Pipeline("test pipeline")
    # make a bunch of tasks that execute in well defined order (via data
    # dependencies)
    for lvl in range(nQuanta):
        pipeline.addTask(AddTask, f"task{lvl}")
        pipeline.addConfigOverride(f"task{lvl}", "connections.in_tmpl", lvl)
        pipeline.addConfigOverride(f"task{lvl}", "connections.out_tmpl", lvl + 1)
    if instrument:
        pipeline.addInstrument(instrument)
    return pipeline


def makeSimpleButler(root: str, run: str = "test", inMemory: bool = True) -> Butler:
    """Create new data butler instance.

    Parameters
    ----------
    root : `str`
        Path or URI to the root location of the new repository.
    run : `str`, optional
        Run collection name.
    inMemory : `bool`, optional
        If true make in-memory repository.

    Returns
    -------
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    """
    root_path = ResourcePath(root, forceDirectory=True)
    if not root_path.isLocal:
        raise ValueError(f"Only works with local root not {root_path}")
    config = Config()
    if not inMemory:
        config["registry", "db"] = f"sqlite:///{root_path.ospath}/gen3.sqlite"
        config["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
    repo = butlerTests.makeTestRepo(str(root_path), {}, config=config)
    butler = Butler(butler=repo, run=run)
    return butler


def populateButler(
    pipeline: Pipeline, butler: Butler, datasetTypes: Dict[Optional[str], List[str]] | None = None
) -> None:
    """Populate data butler with data needed for test.

    Initializes data butler with a bunch of items:
    - registers dataset types which are defined by pipeline
    - create dimensions data for (instrument, detector)
    - adds datasets based on ``datasetTypes`` dictionary, if dictionary is
      missing then a single dataset with type "add_dataset0" is added

    All datasets added to butler have ``dataId={instrument=instrument,
    detector=0}`` where ``instrument`` is extracted from pipeline, "INSTR" is
    used if pipeline is missing instrument definition. Type of the dataset is
    guessed from dataset type name (assumes that pipeline is made of `AddTask`
    tasks).

    Parameters
    ----------
    pipeline : `~lsst.pipe.base.Pipeline`
        Pipeline instance.
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    datasetTypes : `dict` [ `str`, `list` ], optional
        Dictionary whose keys are collection names and values are lists of
        dataset type names. By default a single dataset of type "add_dataset0"
        is added to a ``butler.run`` collection.
    """

    # Add dataset types to registry
    taskDefs = list(pipeline.toExpandedPipeline())
    registerDatasetTypes(butler.registry, taskDefs)

    instrument = pipeline.getInstrument()
    if instrument is not None:
        instrument_class = doImportType(instrument)
        instrumentName = instrument_class.getName()
    else:
        instrumentName = "INSTR"

    # Add all needed dimensions to registry
    butler.registry.insertDimensionData("instrument", dict(name=instrumentName))
    butler.registry.insertDimensionData("detector", dict(instrument=instrumentName, id=0, full_name="det0"))

    taskDefMap = dict((taskDef.label, taskDef) for taskDef in taskDefs)
    # Add inputs to butler
    if not datasetTypes:
        datasetTypes = {None: ["add_dataset0"]}
    for run, dsTypes in datasetTypes.items():
        if run is not None:
            butler.registry.registerRun(run)
        for dsType in dsTypes:
            if dsType == "packages":
                # Version is intentionally inconsistent.
                # Dict is convertible to Packages if Packages is installed.
                data: Any = {"python": "9.9.99"}
                butler.put(data, dsType, run=run)
            else:
                if dsType.endswith("_config"):
                    # find a config from matching task name or make a new one
                    taskLabel, _, _ = dsType.rpartition("_")
                    taskDef = taskDefMap.get(taskLabel)
                    if taskDef is not None:
                        data = taskDef.config
                    else:
                        data = AddTaskConfig()
                elif dsType.endswith("_metadata"):
                    data = _TASK_FULL_METADATA_TYPE()
                elif dsType.endswith("_log"):
                    data = ButlerLogRecords.from_records([])
                else:
                    data = numpy.array([0.0, 1.0, 2.0, 5.0])
                butler.put(data, dsType, run=run, instrument=instrumentName, detector=0)


def makeSimpleQGraph(
    nQuanta: int = 5,
    pipeline: Optional[Pipeline] = None,
    butler: Optional[Butler] = None,
    root: Optional[str] = None,
    callPopulateButler: bool = True,
    run: str = "test",
    skipExistingIn: Any = None,
    inMemory: bool = True,
    userQuery: str = "",
    datasetTypes: Optional[Dict[Optional[str], List[str]]] = None,
    datasetQueryConstraint: DSQVariant = DSQVariant.ALL,
    makeDatastoreRecords: bool = False,
    resolveRefs: bool = False,
    bind: Optional[Mapping[str, Any]] = None,
) -> Tuple[Butler, QuantumGraph]:
    """Make simple QuantumGraph for tests.

    Makes simple one-task pipeline with AddTask, sets up in-memory registry
    and butler, fills them with minimal data, and generates QuantumGraph with
    all of that.

    Parameters
    ----------
    nQuanta : `int`
        Number of quanta in a graph, only used if ``pipeline`` is None.
    pipeline : `~lsst.pipe.base.Pipeline`
        If `None` then a pipeline is made with `AddTask` and default
        `AddTaskConfig`.
    butler : `~lsst.daf.butler.Butler`, optional
        Data butler instance, if None then new data butler is created by
        calling `makeSimpleButler`.
    callPopulateButler : `bool`, optional
        If True insert datasets into the butler prior to building a graph.
        If False butler argument must not be None, and must be pre-populated.
        Defaults to True.
    root : `str`
        Path or URI to the root location of the new repository. Only used if
        ``butler`` is None.
    run : `str`, optional
        Name of the RUN collection to add to butler, only used if ``butler``
        is None.
    skipExistingIn
        Expressions representing the collections to search for existing
        output datasets that should be skipped.  See
        :ref:`daf_butler_ordered_collection_searches`.
    inMemory : `bool`, optional
        If true make in-memory repository, only used if ``butler`` is `None`.
    userQuery : `str`, optional
        The user query to pass to ``makeGraph``, by default an empty string.
    datasetTypes : `dict` [ `str`, `list` ], optional
        Dictionary whose keys are collection names and values are lists of
        dataset type names. By default a single dataset of type "add_dataset0"
        is added to a ``butler.run`` collection.
    datasetQueryQConstraint : `DatasetQueryConstraintVariant`
        The query constraint variant that should be used to constrain the
        query based on dataset existence, defaults to
        `DatasetQueryConstraintVariant.ALL`.
    makeDatastoreRecords : `bool`, optional
        If `True` then add datstore records to generated quanta.
    resolveRefs : `bool`, optional
        If `True` then resolve all input references and generate random dataset
        IDs for all output and intermediate datasets.
    bind : `Mapping`, optional
        Mapping containing literal values that should be injected into the
        ``userQuery`` expression, keyed by the identifiers they replace.

    Returns
    -------
    butler : `~lsst.daf.butler.Butler`
        Butler instance
    qgraph : `~lsst.pipe.base.QuantumGraph`
        Quantum graph instance
    """

    if pipeline is None:
        pipeline = makeSimplePipeline(nQuanta=nQuanta)

    if butler is None:
        if root is None:
            raise ValueError("Must provide `root` when `butler` is None")
        if callPopulateButler is False:
            raise ValueError("populateButler can only be False when butler is supplied as an argument")
        butler = makeSimpleButler(root, run=run, inMemory=inMemory)

    if callPopulateButler:
        populateButler(pipeline, butler, datasetTypes=datasetTypes)

    # Make the graph
    _LOG.debug("Instantiating GraphBuilder, skipExistingIn=%s", skipExistingIn)
    builder = GraphBuilder(
        registry=butler.registry,
        skipExistingIn=skipExistingIn,
        datastore=butler.datastore if makeDatastoreRecords else None,
    )
    _LOG.debug(
        "Calling GraphBuilder.makeGraph, collections=%r, run=%r, userQuery=%r bind=%s",
        butler.collections,
        run or butler.run,
        userQuery,
        bind,
    )
    qgraph = builder.makeGraph(
        pipeline,
        collections=butler.collections,
        run=run or butler.run,
        userQuery=userQuery,
        datasetQueryConstraint=datasetQueryConstraint,
        resolveRefs=resolveRefs,
        bind=bind,
    )

    return butler, qgraph
