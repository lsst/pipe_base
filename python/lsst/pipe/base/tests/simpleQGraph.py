# This file is part of ctrl_mpexec.
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

from lsst.daf.butler import (Butler, Config, DatasetType, CollectionSearch)
import lsst.daf.butler.tests as butlerTests
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
from lsst.pipe.base import connectionTypes as cT

_LOG = logging.getLogger(__name__)


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

    def loadTaskClass(self, taskName):
        if taskName == "AddTask":
            return AddTask, "AddTask"

    def makeTask(self, taskClass, config, overrides, butler):
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        task = taskClass(config=config, initInputs=None)
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


def makeSimpleQGraph(nQuanta=5, pipeline=None, butler=None, root=None, skipExisting=False, inMemory=True):
    """Make simple QuantumGraph for tests.

    Makes simple one-task pipeline with AddTask, sets up in-memory
    registry and butler, fills them with minimal data, and generates
    QuantumGraph with all of that.

    Parameters
    ----------
    nQuanta : `int`
        Number of quanta in a graph.
    pipeline : `~lsst.pipe.base.Pipeline`
        If `None` then one-task pipeline is made with `AddTask` and
        default `AddTaskConfig`.
    butler : `~lsst.daf.butler.Butler`, optional
        Data butler instance, this should be an instance returned from a
        previous call to this method.
    root : `str`
        Path or URI to the root location of the new repository. Only used if
        ``butler`` is None.
    skipExisting : `bool`, optional
        If `True` (default), a Quantum is not created if all its outputs
        already exist.
    inMemory : `bool`, optional
        If true make in-memory repository.

    Returns
    -------
    butler : `~lsst.daf.butler.Butler`
        Butler instance
    qgraph : `~lsst.pipe.base.QuantumGraph`
        Quantum graph instance
    """

    if pipeline is None:
        pipeline = pipeBase.Pipeline("test pipeline")
        # make a bunch of tasks that execute in well defined order (via data
        # dependencies)
        for lvl in range(nQuanta):
            pipeline.addTask(AddTask, f"task{lvl}")
            pipeline.addConfigOverride(f"task{lvl}", "connections.in_tmpl", f"{lvl}")
            pipeline.addConfigOverride(f"task{lvl}", "connections.out_tmpl", f"{lvl+1}")

    if butler is None:

        if root is None:
            raise ValueError("Must provide `root` when `butler` is None")

        config = Config()
        if not inMemory:
            config["registry", "db"] = f"sqlite:///{root}/gen3.sqlite"
            config["datastore", "cls"] = "lsst.daf.butler.datastores.posixDatastore.PosixDatastore"
        repo = butlerTests.makeTestRepo(root, {}, config=config)
        collection = "test"
        butler = Butler(butler=repo, run=collection)

        # Add dataset types to registry
        registerDatasetTypes(butler.registry, pipeline.toExpandedPipeline())

        # Add all needed dimensions to registry
        butler.registry.insertDimensionData("instrument", dict(name="INSTR"))
        butler.registry.insertDimensionData("detector", dict(instrument="INSTR", id=0, full_name="det0"))

        # Add inputs to butler
        data = numpy.array([0., 1., 2., 5.])
        butler.put(data, "add_dataset0", instrument="INSTR", detector=0)

    # Make the graph
    builder = pipeBase.GraphBuilder(registry=butler.registry, skipExisting=skipExisting)
    qgraph = builder.makeGraph(
        pipeline,
        collections=CollectionSearch.fromExpression(butler.run),
        run=butler.run,
        userQuery=""
    )

    return butler, qgraph
