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

__all__ = ["AddTaskConfig", "AddTask", "AddTaskFactoryMock"]

import itertools
import logging
import numpy

from lsst.base import Packages
from lsst.daf.base import PropertySet
from lsst.daf.butler import Butler, ButlerURI, Config, DatasetType
import lsst.daf.butler.tests as butlerTests
from lsst.daf.butler.core.logging import ButlerLogRecords
import lsst.pex.config as pexConfig
from lsst.utils import doImport
from ... import base as pipeBase
from .. import connectionTypes as cT

_LOG = logging.getLogger(__name__)


# SimpleInstrument has an instrument-like API as needed for unit testing, but
# can not explicitly depend on Instrument because pipe_base does not explicitly
# depend on obs_base.
class SimpleInstrument:

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def getName():
        return "INSTRU"

    def applyConfigOverrides(self, name, config):
        pass


class AddTaskConnections(pipeBase.PipelineTaskConnections,
                         dimensions=("instrument", "detector"),
                         defaultTemplates={"in_tmpl": "_in", "out_tmpl": "_out"}):
    """Connections for AddTask, has one input and two outputs,
    plus one init output.
    """
    input = cT.Input(name="add_dataset{in_tmpl}",
                     dimensions=["instrument", "detector"],
                     storageClass="NumpyArray",
                     doc="Input dataset type for this task")
    output = cT.Output(name="add_dataset{out_tmpl}",
                       dimensions=["instrument", "detector"],
                       storageClass="NumpyArray",
                       doc="Output dataset type for this task")
    output2 = cT.Output(name="add2_dataset{out_tmpl}",
                        dimensions=["instrument", "detector"],
                        storageClass="NumpyArray",
                        doc="Output dataset type for this task")
    initout = cT.InitOutput(name="add_init_output{out_tmpl}",
                            storageClass="NumpyArray",
                            doc="Init Output dataset type for this task")


class AddTaskConfig(pipeBase.PipelineTaskConfig,
                    pipelineConnections=AddTaskConnections):
    """Config for AddTask.
    """
    addend = pexConfig.Field(doc="amount to add", dtype=int, default=3)


class AddTask(pipeBase.PipelineTask):
    """Trivial PipelineTask for testing, has some extras useful for specific
    unit tests.
    """

    ConfigClass = AddTaskConfig
    _DefaultName = "add_task"

    initout = numpy.array([999])
    """InitOutputs for this task"""

    taskFactory = None
    """Factory that makes instances"""

    def run(self, input):

        if self.taskFactory:
            # do some bookkeeping
            if self.taskFactory.stopAt == self.taskFactory.countExec:
                raise RuntimeError("pretend something bad happened")
            self.taskFactory.countExec += 1

        self.metadata.add("add", self.config.addend)
        output = input + self.config.addend
        output2 = output + self.config.addend
        _LOG.info("input = %s, output = %s, output2 = %s", input, output, output2)
        return pipeBase.Struct(output=output, output2=output2)


class AddTaskFactoryMock(pipeBase.TaskFactory):
    """Special task factory that instantiates AddTask.

    It also defines some bookkeeping variables used by AddTask to report
    progress to unit tests.
    """
    def __init__(self, stopAt=-1):
        self.countExec = 0  # incremented by AddTask
        self.stopAt = stopAt  # AddTask raises exception at this call to run()

    def makeTask(self, taskClass, name, config, overrides, butler):
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        task = taskClass(config=config, initInputs=None, name=name)
        task.taskFactory = self
        return task


def registerDatasetTypes(registry, pipeline):
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
        configDatasetType = DatasetType(taskDef.configDatasetName, {},
                                        storageClass="Config",
                                        universe=registry.dimensions)
        packagesDatasetType = DatasetType("packages", {},
                                          storageClass="Packages",
                                          universe=registry.dimensions)
        datasetTypes = pipeBase.TaskDatasetTypes.fromTaskDef(taskDef, registry=registry)
        for datasetType in itertools.chain(datasetTypes.initInputs, datasetTypes.initOutputs,
                                           datasetTypes.inputs, datasetTypes.outputs,
                                           datasetTypes.prerequisites,
                                           [configDatasetType, packagesDatasetType]):
            _LOG.info("Registering %s with registry", datasetType)
            # this is a no-op if it already exists and is consistent,
            # and it raises if it is inconsistent. But components must be
            # skipped
            if not datasetType.isComponent():
                registry.registerDatasetType(datasetType)


def makeSimplePipeline(nQuanta, instrument=None):
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
    pipeline = pipeBase.Pipeline("test pipeline")
    # make a bunch of tasks that execute in well defined order (via data
    # dependencies)
    for lvl in range(nQuanta):
        pipeline.addTask(AddTask, f"task{lvl}")
        pipeline.addConfigOverride(f"task{lvl}", "connections.in_tmpl", lvl)
        pipeline.addConfigOverride(f"task{lvl}", "connections.out_tmpl", lvl+1)
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
    root = ButlerURI(root, forceDirectory=True)
    if not root.isLocal:
        raise ValueError(f"Only works with local root not {root}")
    config = Config()
    if not inMemory:
        config["registry", "db"] = f"sqlite:///{root.ospath}/gen3.sqlite"
        config["datastore", "cls"] = "lsst.daf.butler.datastores.fileDatastore.FileDatastore"
    repo = butlerTests.makeTestRepo(root, {}, config=config)
    butler = Butler(butler=repo, run=run)
    return butler


def populateButler(pipeline, butler, datasetTypes=None):
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
        if isinstance(instrument, str):
            instrument = doImport(instrument)
        instrumentName = instrument.getName()
    else:
        instrumentName = "INSTR"

    # Add all needed dimensions to registry
    butler.registry.insertDimensionData("instrument", dict(name=instrumentName))
    butler.registry.insertDimensionData("detector", dict(instrument=instrumentName,
                                                         id=0, full_name="det0"))

    taskDefMap = dict((taskDef.label, taskDef) for taskDef in taskDefs)
    # Add inputs to butler
    if not datasetTypes:
        datasetTypes = {None: ["add_dataset0"]}
    for run, dsTypes in datasetTypes.items():
        if run is not None:
            butler.registry.registerRun(run)
        for dsType in dsTypes:
            if dsType == "packages":
                # Version is intentionally inconsistent
                data = Packages({"python": "9.9.99"})
                butler.put(data, dsType, run=run)
            else:
                if dsType.endswith("_config"):
                    # find a confing from matching task name or make a new one
                    taskLabel, _, _ = dsType.rpartition("_")
                    taskDef = taskDefMap.get(taskLabel)
                    if taskDef is not None:
                        data = taskDef.config
                    else:
                        data = AddTaskConfig()
                elif dsType.endswith("_metadata"):
                    data = PropertySet()
                elif dsType.endswith("_log"):
                    data = ButlerLogRecords.from_records([])
                else:
                    data = numpy.array([0., 1., 2., 5.])
                butler.put(data, dsType, run=run, instrument=instrumentName, detector=0)


def makeSimpleQGraph(nQuanta=5, pipeline=None, butler=None, root=None, run="test",
                     skipExistingIn=None, inMemory=True, userQuery="",
                     datasetTypes=None):
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
    root : `str`
        Path or URI to the root location of the new repository. Only used if
        ``butler`` is None.
    run : `str`, optional
        Name of the RUN collection to add to butler, only used if ``butler``
        is None.
    skipExistingIn
        Expressions representing the collections to search for existing
        output datasets that should be skipped.  May be any of the types
        accepted by `lsst.daf.butler.CollectionSearch.fromExpression`.
    inMemory : `bool`, optional
        If true make in-memory repository, only used if ``butler`` is `None`.
    userQuery : `str`, optional
        The user query to pass to ``makeGraph``, by default an empty string.
    datasetTypes : `dict` [ `str`, `list` ], optional
        Dictionary whose keys are collection names and values are lists of
        dataset type names. By default a single dataset of type "add_dataset0"
        is added to a ``butler.run`` collection.

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
        butler = makeSimpleButler(root, run=run, inMemory=inMemory)

    populateButler(pipeline, butler, datasetTypes=datasetTypes)

    # Make the graph
    _LOG.debug("Instantiating GraphBuilder, skipExistingIn=%s", skipExistingIn)
    builder = pipeBase.GraphBuilder(registry=butler.registry, skipExistingIn=skipExistingIn)
    _LOG.debug("Calling GraphBuilder.makeGraph, collections=%r, run=%r, userQuery=%r",
               butler.collections, run or butler.run, userQuery)
    qgraph = builder.makeGraph(
        pipeline,
        collections=butler.collections,
        run=run or butler.run,
        userQuery=userQuery
    )

    return butler, qgraph
