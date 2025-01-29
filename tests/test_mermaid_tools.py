# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Simple unit test for Pipeline visualization."""

import io
import unittest

import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.pipe.base import Pipeline, PipelineTask, PipelineTaskConfig, PipelineTaskConnections
from lsst.pipe.base.mermaid_tools import pipeline2mermaid


class ExamplePipelineTaskConnections(PipelineTaskConnections, dimensions=("visit", "detector")):
    """Connections class used for testing.

    Parameters
    ----------
    config : `PipelineTaskConfig`
        The config to use for this connections class.
    """

    input1 = cT.PrerequisiteInput(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExampleStorageClass",
        doc="Input for this task",
    )
    input2 = cT.Input(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExampleStorageClass",
        doc="Input for this task",
    )
    output1 = cT.Output(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExampleStorageClass",
        doc="Output for this task",
    )
    output2 = cT.Output(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExampleStorageClass",
        doc="Output for this task",
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        if not config.connections.input2:
            self.inputs.remove("input2")
        if not config.connections.output2:
            self.outputs.remove("output2")


class ExamplePipelineTaskConfig(PipelineTaskConfig, pipelineConnections=ExamplePipelineTaskConnections):
    """Example config used for testing."""


def _makeConfig(inputName, outputName, pipeline, label):
    """Add config overrides.

    Factory method for config instances.

    inputName and outputName can be either string or tuple of strings
    with two items max.
    """
    if isinstance(inputName, tuple):
        pipeline.addConfigOverride(label, "connections.input1", inputName[0])
        pipeline.addConfigOverride(label, "connections.input2", inputName[1] if len(inputName) > 1 else "")
    else:
        pipeline.addConfigOverride(label, "connections.input1", inputName)

    if isinstance(outputName, tuple):
        pipeline.addConfigOverride(label, "connections.output1", outputName[0])
        pipeline.addConfigOverride(label, "connections.output2", outputName[1] if len(outputName) > 1 else "")
    else:
        pipeline.addConfigOverride(label, "connections.output1", outputName)


class ExamplePipelineTask(PipelineTask):
    """Example pipeline task used for testing."""

    ConfigClass = ExamplePipelineTaskConfig
    _DefaultName = "examplePipelineTask"


def _makePipeline(tasks):
    """Generate Pipeline instance.

    Parameters
    ----------
    tasks : list of tuples
        Each tuple in the list has 3 or 4 items:
        - input DatasetType name(s), string or tuple of strings
        - output DatasetType name(s), string or tuple of strings
        - task label, string
        - optional task class object, can be None

    Returns
    -------
    Pipeline instance
    """
    pipe = Pipeline("test pipeline")
    for task in tasks:
        inputs = task[0]
        outputs = task[1]
        label = task[2]
        klass = task[3] if len(task) > 3 else ExamplePipelineTask
        pipe.addTask(klass, label)
        _makeConfig(inputs, outputs, pipe, label)
    return list(pipe.to_graph()._iter_task_defs())


class MermaidToolsTestCase(unittest.TestCase):
    """A test case for Mermaid tools."""

    def test_pipeline2mermaid(self):
        """Tests for mermaid_tools.pipeline2mermaid method."""
        pipeline = _makePipeline(
            [
                ("A", ("B", "C"), "task0"),
                ("B", "E", "task1"),
                ("B", ("D", "F"), "task2"),
                ("D", "G", "task3"),
                (("C", "F"), "H", "task4"),
                ("task4_metadata", "I", "task5"),
            ]
        )
        file = io.StringIO()
        pipeline2mermaid(pipeline, file, expand_dimensions=True)

        # It's hard to validate complete output, just checking few basic
        # things, even that is not terribly stable.
        fileValue = file.getvalue()
        lines = fileValue.strip().split("\n")
        nClassDefs = 2
        nTasks = 6
        nTaskClass = 6
        nDatasets = 10
        nDatasetClass = 10
        nEdges = 16
        nLinkStyles = 2  # For the default and pre-requisite edges
        nExtra = 1  # 'flowchart' opening line

        self.assertEqual(
            len(lines),
            nClassDefs + nTasks + nTaskClass + nDatasets + nDatasetClass + nEdges + nLinkStyles + nExtra,
        )

        # Make sure the 'flowchart' definition is at the beginning of the file.
        self.assertTrue(fileValue.startswith("flowchart TD"))

        # Make sure components are connected appropriately.
        self.assertIn("DATASET_B --> TASK_1", fileValue)
        self.assertIn("TASK_2 --> DATASET_F", fileValue)

        # Make sure class definitions are created for datasets.
        self.assertIn("class DATASET_E ds;", fileValue)

        # Make sure there is a connection created for metadata if someone tries
        # to read it in.
        self.assertIn("TASK_4 --> DATASET_task4_metadata", fileValue)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Generic file handle leak check."""


def setup_module(module):
    """Set up the module for pytest.

    Parameters
    ----------
    module : `~types.ModuleType`
        Module to set up.
    """
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
