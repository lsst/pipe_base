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

"""Bunch of common classes and methods for use in unit tests."""

from __future__ import annotations

__all__ = ["AddTask", "AddTaskConfig", "AddTaskFactoryMock"]

import logging
from collections.abc import Iterable, Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, cast

import numpy

import lsst.daf.butler.tests as butlerTests
import lsst.pex.config as pexConfig
from lsst.daf.butler import Butler, Config, DataId, DatasetRef, DatasetType, Formatter, LimitedButler
from lsst.daf.butler.logging import ButlerLogRecords
from lsst.resources import ResourcePath
from lsst.utils import doImportType
from lsst.utils.introspection import get_full_type_name

from .. import connectionTypes as cT
from .._instrument import Instrument
from ..all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from ..all_dimensions_quantum_graph_builder import DatasetQueryConstraintVariant as DSQVariant
from ..automatic_connection_constants import PACKAGES_INIT_OUTPUT_NAME, PACKAGES_INIT_OUTPUT_STORAGE_CLASS
from ..config import PipelineTaskConfig
from ..connections import PipelineTaskConnections
from ..graph import QuantumGraph
from ..pipeline import Pipeline
from ..pipeline_graph import PipelineGraph, TaskNode
from ..pipelineTask import PipelineTask
from ..struct import Struct
from ..task import _TASK_FULL_METADATA_TYPE
from ..taskFactory import TaskFactory

if TYPE_CHECKING:
    from lsst.daf.butler import Registry

_LOG = logging.getLogger(__name__)


class SimpleInstrument(Instrument):
    """Simple instrument class suitable for testing.

    Parameters
    ----------
    *args : `~typing.Any`
        Ignore parameters.
    **kwargs : `~typing.Any`
        Unused keyword arguments.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        pass

    @staticmethod
    def getName() -> str:
        return "INSTRU"

    def getRawFormatter(self, dataId: DataId) -> type[Formatter]:
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

    taskFactory: AddTaskFactoryMock | None = None
    """Factory that makes instances"""

    def run(self, input: int) -> Struct:
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

    Parameters
    ----------
    stopAt : `int`, optional
        Number of times to call `run` before stopping.
    """

    def __init__(self, stopAt: int = -1):
        self.countExec = 0  # incremented by AddTask
        self.stopAt = stopAt  # AddTask raises exception at this call to run()

    def makeTask(
        self,
        task_node: TaskNode,
        /,
        butler: LimitedButler,
        initInputRefs: Iterable[DatasetRef] | None,
    ) -> PipelineTask:
        task = task_node.task_class(config=task_node.config, initInputs=None, name=task_node.label)
        task.taskFactory = self  # type: ignore
        return task


def registerDatasetTypes(registry: Registry, pipeline: Pipeline | PipelineGraph) -> None:
    """Register all dataset types used by tasks in a registry.

    Copied and modified from `PreExecInit.initializeDatasetTypes`.

    Parameters
    ----------
    registry : `~lsst.daf.butler.Registry`
        Registry instance.
    pipeline : `.Pipeline`, or `.pipeline_graph.PipelineGraph`
        The pipeline whose dataset types should be registered, as a `.Pipeline`
        instance or `.PipelineGraph` instance.
    """
    match pipeline:
        case PipelineGraph() as pipeline_graph:
            pass
        case Pipeline():
            pipeline_graph = pipeline.to_graph()
        case _:
            raise TypeError(f"Unexpected pipeline argument value: {pipeline}.")
    pipeline_graph.resolve(registry)
    dataset_types = [node.dataset_type for node in pipeline_graph.dataset_types.values()]
    dataset_types.append(
        DatasetType(
            PACKAGES_INIT_OUTPUT_NAME,
            {},
            storageClass=PACKAGES_INIT_OUTPUT_STORAGE_CLASS,
            universe=registry.dimensions,
        )
    )
    for dataset_type in dataset_types:
        _LOG.info("Registering %s with registry", dataset_type)
        registry.registerDatasetType(dataset_type)


def makeSimplePipeline(nQuanta: int, instrument: str | None = None) -> Pipeline:
    """Make a simple Pipeline for tests.

    This is called by `makeSimpleQGraph()` if no pipeline is passed to that
    function. It can also be used to customize the pipeline used by
    `makeSimpleQGraph()` function by calling this first and passing the result
    to it.

    Parameters
    ----------
    nQuanta : `int`
        The number of quanta to add to the pipeline.
    instrument : `str` or `None`, optional
        The importable name of an instrument to be added to the pipeline or
        if no instrument should be added then an empty string or `None`, by
        default `None`.

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


def makeSimpleButler(
    root: str, run: str = "test", inMemory: bool = True, config: Config | str | None = None
) -> Butler:
    """Create new data butler instance.

    Parameters
    ----------
    root : `str`
        Path or URI to the root location of the new repository.
    run : `str`, optional
        Run collection name.
    inMemory : `bool`, optional
        If true make in-memory repository.
    config : `~lsst.daf.butler.Config`, optional
        Configuration to use for new Butler, if `None` then default
        configuration is used. If ``inMemory`` is `True` then configuration
        is updated to use SQLite registry and file datastore in ``root``.

    Returns
    -------
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    """
    root_path = ResourcePath(root, forceDirectory=True)
    if not root_path.isLocal:
        raise ValueError(f"Only works with local root not {root_path}")
    butler_config = Config()
    if config:
        butler_config.update(Config(config))
    if not inMemory:
        butler_config["registry", "db"] = f"sqlite:///{root_path.ospath}/gen3.sqlite"
        butler_config["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
    repo = butlerTests.makeTestRepo(str(root_path), {}, config=butler_config)
    butler = Butler.from_config(butler=repo, run=run)
    return butler


def populateButler(
    pipeline: Pipeline | PipelineGraph,
    butler: Butler,
    datasetTypes: dict[str | None, list[str]] | None = None,
    instrument: str | None = None,
) -> None:
    """Populate data butler with data needed for test.

    Initializes data butler with a bunch of items:
    - registers dataset types which are defined by pipeline
    - create dimensions data for (instrument, detector)
    - adds datasets based on ``datasetTypes`` dictionary, if dictionary is
      missing then a single dataset with type "add_dataset0" is added.

    All datasets added to butler have ``dataId={instrument=instrument,
    detector=0}`` where ``instrument`` is extracted from pipeline, "INSTR" is
    used if pipeline is missing instrument definition. Type of the dataset is
    guessed from dataset type name (assumes that pipeline is made of `AddTask`
    tasks).

    Parameters
    ----------
    pipeline : `.Pipeline` or `.pipeline_graph.PipelineGraph`
        Pipeline or pipeline graph instance.
    butler : `~lsst.daf.butler.Butler`
        Data butler instance.
    datasetTypes : `dict` [ `str`, `list` ], optional
        Dictionary whose keys are collection names and values are lists of
        dataset type names. By default a single dataset of type "add_dataset0"
        is added to a ``butler.run`` collection.
    instrument : `str`, optional
        Fully-qualified name of the instrumemnt class (as it appears in a
        pipeline).  This is needed to propagate the instrument from the
        original pipeline if a pipeline graph is passed instead.
    """
    # Add dataset types to registry
    match pipeline:
        case PipelineGraph() as pipeline_graph:
            pass
        case Pipeline():
            pipeline_graph = pipeline.to_graph()
            if instrument is None:
                instrument = pipeline.getInstrument()
        case _:
            raise TypeError(f"Unexpected pipeline object: {pipeline!r}.")

    registerDatasetTypes(butler.registry, pipeline_graph)

    if instrument is not None:
        instrument_class = doImportType(instrument)
        instrumentName = cast(Instrument, instrument_class).getName()
        instrumentClass = get_full_type_name(instrument_class)
    else:
        instrumentName = "INSTR"
        instrumentClass = None

    # Add all needed dimensions to registry
    butler.registry.insertDimensionData("instrument", dict(name=instrumentName, class_name=instrumentClass))
    butler.registry.insertDimensionData("detector", dict(instrument=instrumentName, id=0, full_name="det0"))

    # Add inputs to butler.
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
                    task_node = pipeline_graph.tasks.get(taskLabel)
                    if task_node is not None:
                        data = task_node.config
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
    pipeline: Pipeline | PipelineGraph | None = None,
    butler: Butler | None = None,
    root: str | None = None,
    callPopulateButler: bool = True,
    run: str = "test",
    instrument: str | None = None,
    skipExistingIn: Any = None,
    inMemory: bool = True,
    userQuery: str = "",
    datasetTypes: dict[str | None, list[str]] | None = None,
    datasetQueryConstraint: DSQVariant = DSQVariant.ALL,
    makeDatastoreRecords: bool = False,
    bind: Mapping[str, Any] | None = None,
    metadata: MutableMapping[str, Any] | None = None,
) -> tuple[Butler, QuantumGraph]:
    """Make simple `QuantumGraph` for tests.

    Makes simple one-task pipeline with AddTask, sets up in-memory registry
    and butler, fills them with minimal data, and generates QuantumGraph with
    all of that.

    Parameters
    ----------
    nQuanta : `int`
        Number of quanta in a graph, only used if ``pipeline`` is None.
    pipeline : `.Pipeline` or `.pipeline_graph.PipelineGraph`
        Pipeline or pipeline graph to build the graph from. If `None`, a
        pipeline is made with `AddTask` and default `AddTaskConfig`.
    butler : `~lsst.daf.butler.Butler`, optional
        Data butler instance, if None then new data butler is created by
        calling `makeSimpleButler`.
    root : `str`
        Path or URI to the root location of the new repository. Only used if
        ``butler`` is None.
    callPopulateButler : `bool`, optional
        If True insert datasets into the butler prior to building a graph.
        If False butler argument must not be None, and must be pre-populated.
        Defaults to True.
    run : `str`, optional
        Name of the RUN collection to add to butler, only used if ``butler``
        is None.
    instrument : `str` or `None`, optional
        The importable name of an instrument to be added to the pipeline or
        if no instrument should be added then an empty string or `None`, by
        default `None`. Only used if ``pipeline`` is `None`.
    skipExistingIn : `~typing.Any`
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
    datasetQueryConstraint : `DatasetQueryConstraintVariant`
        The query constraint variant that should be used to constrain the
        query based on dataset existence, defaults to
        `DatasetQueryConstraintVariant.ALL`.
    makeDatastoreRecords : `bool`, optional
        If `True` then add datastore records to generated quanta.
    bind : `~collections.abc.Mapping`, optional
        Mapping containing literal values that should be injected into the
        ``userQuery`` expression, keyed by the identifiers they replace.
    metadata : `~collections.abc.Mapping`, optional
        Optional graph metadata.

    Returns
    -------
    butler : `~lsst.daf.butler.Butler`
        Butler instance.
    qgraph : `~lsst.pipe.base.QuantumGraph`
        Quantum graph instance.
    """
    match pipeline:
        case PipelineGraph() as pipeline_graph:
            pass
        case Pipeline():
            pipeline_graph = pipeline.to_graph()
            if instrument is None:
                instrument = pipeline.getInstrument()
        case None:
            pipeline_graph = makeSimplePipeline(nQuanta=nQuanta, instrument=instrument).to_graph()
        case _:
            raise TypeError(f"Unexpected pipeline object: {pipeline!r}.")

    if butler is None:
        if root is None:
            raise ValueError("Must provide `root` when `butler` is None")
        if callPopulateButler is False:
            raise ValueError("populateButler can only be False when butler is supplied as an argument")
        butler = makeSimpleButler(root, run=run, inMemory=inMemory)

    if callPopulateButler:
        populateButler(pipeline_graph, butler, datasetTypes=datasetTypes, instrument=instrument)

    # Make the graph
    _LOG.debug(
        "Instantiating QuantumGraphBuilder, "
        "skip_existing_in=%s, input_collections=%r, output_run=%r, where=%r, bind=%s.",
        skipExistingIn,
        butler.collections.defaults,
        run,
        userQuery,
        bind,
    )
    if not run:
        assert butler.run is not None, "Butler must have run defined"
        run = butler.run
    builder = AllDimensionsQuantumGraphBuilder(
        pipeline_graph,
        butler,
        skip_existing_in=skipExistingIn if skipExistingIn is not None else [],
        input_collections=butler.collections.defaults,
        output_run=run,
        where=userQuery,
        bind=bind,
        dataset_query_constraint=datasetQueryConstraint,
    )
    _LOG.debug("Calling QuantumGraphBuilder.build.")
    if not metadata:
        metadata = {}
    metadata["output_run"] = run

    qgraph = builder.build(metadata=metadata, attach_datastore_records=makeDatastoreRecords)

    return butler, qgraph
