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
from unittest.mock import MagicMock, patch

import lsst.pipe.base.connectionTypes as cT
import lsst.pipe.base.pipeline_graph.visualization as vis
import lsst.utils.tests
from lsst.pipe.base import Pipeline, PipelineTask, PipelineTaskConfig, PipelineTaskConnections
from lsst.pipe.base.mermaid_tools import pipeline2mermaid

# Save the original __init__ method of Mermaid.
_originalMermaidInit = vis.Mermaid.__init__


def _mockedMermaidInit(self, graph, width=None, height=None, scale=None):
    # Call the original initializer to set things up.
    _originalMermaidInit(self, graph, width=width, height=height, scale=scale)

    # Override only the svg_response and img_response attributes.
    self.svg_response = MagicMock(content=b"<svg>Mocked SVG Content</svg>")
    self.img_response = MagicMock(content=b"\x89PNG\r\n\x1a\nMocked PNG Content")


class ExamplePipelineTaskConnections(PipelineTaskConnections, dimensions=("visit", "detector")):
    """Connections class used for testing.

    Parameters
    ----------
    config : `PipelineTaskConfig`
        The config to use for this connections class.
    """

    input1 = cT.Input(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExposureF",
        doc="Input for this task",
    )
    input1Prerequisite = cT.PrerequisiteInput(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExposureF",
        doc="Prerequisite input for this task",
    )
    input2 = cT.Input(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExposureF",
        doc="Input for this task",
    )
    input2Prerequisite = cT.PrerequisiteInput(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExposureF",
        doc="Prerequisite input for this task",
    )
    output1 = cT.Output(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExposureF",
        doc="Output for this task",
    )
    output2 = cT.Output(
        name="",
        dimensions=["visit", "detector"],
        storageClass="ExposureF",
        doc="Output for this task",
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)
        for x in config.connections:
            # Avoid complaints about incorrect storage class for metadata.
            if getattr(config.connections, x).endswith("_metadata"):
                getattr(self, x).__dict__["storageClass"] = "TaskMetadata"

            # Remove inputs and outputs that are not used.
            if x.startswith("input"):
                if not getattr(config.connections, x):
                    if x in self.inputs:
                        self.inputs.remove(x)
                    elif x in self.prerequisiteInputs:
                        self.prerequisiteInputs.remove(x)
            elif x.startswith("output"):
                if not getattr(config.connections, x):
                    if x in self.outputs:
                        self.outputs.remove(x)


class ExamplePipelineTaskConfig(PipelineTaskConfig, pipelineConnections=ExamplePipelineTaskConnections):
    """Example config used for testing."""


def _makeConfig(inputNames, outputNames, pipeline, label, inputTypes=None):
    """Configure pipeline input/output connections by adding config overrides.

    Parameters
    ----------
    inputNames : list or tuple of str
        A list or tuple containing up to two input names.
    outputNames : list or tuple of str
        A list or tuple containing up to two output names.
    pipeline : object
        The pipeline object where configuration overrides will be applied.
    label : str
        The label associated with the configuration.
    inputTypes : list or tuple of str, optional
        A list or tuple specifying input types. Elements can be 'Prerequisite'
        or '' (the default, indicating a regular input).
    """
    # Ensure at least two elements, truncate excess.
    inputTypes = (inputTypes or ["", ""])[:2]

    for i in (0, 1):
        inputName = inputNames[i] if i < len(inputNames) else ""
        isPrerequisite = inputTypes[i] == "Prerequisite" if i < len(inputNames) else False

        pipeline.addConfigOverride(label, f"connections.input{i + 1}", "" if isPrerequisite else inputName)
        pipeline.addConfigOverride(
            label, f"connections.input{i + 1}Prerequisite", inputName if isPrerequisite else ""
        )

        outputName = outputNames[i] if i < len(outputNames) else ""
        pipeline.addConfigOverride(label, f"connections.output{i + 1}", outputName)


class ExamplePipelineTask(PipelineTask):
    """Example pipeline task used for testing."""

    ConfigClass = ExamplePipelineTaskConfig
    _DefaultName = "examplePipelineTask"


def _makePipeline(tasks, inputTypeOverrides=None):
    """Generate a Pipeline instance and return its task definitions.

    Parameters
    ----------
    tasks : list of tuples
        Each tuple in the list has 3 or 4 items:
        - input DatasetType name(s), string or tuple of strings
        - output DatasetType name(s), string or tuple of strings
        - task label, string
        - optional task class object, can be None
    inputTypeOverrides : dict, optional
        Dictionary of input type overrides.

    Returns
    -------
    pipe: `lsst.pipe.base.Pipeline`
        The pipeline instance.
    task_defs: list
        List of task definitions.
    """
    pipe = Pipeline("test pipeline")
    for task in tasks:
        inputs = task[0]
        outputs = task[1]
        label = task[2]
        klass = task[3] if len(task) > 3 else ExamplePipelineTask
        pipe.addTask(klass, label)
        if not isinstance(inputs, tuple | list):
            inputs = (inputs,)
        if not isinstance(outputs, tuple | list):
            outputs = (outputs,)
        if inputTypeOverrides:
            inputTypes = [inputTypeOverrides.get(x) for x in inputs]
        _makeConfig(inputs, outputs, pipe, label, inputTypes)
    taskDefs = list(pipe.to_graph()._iter_task_defs())
    return pipe, taskDefs


class MermaidTestCase(unittest.TestCase):
    """A test case for Mermaid pipeline visualization."""

    def setUp(self):
        self.pipeline, self.pipelineTaskDefs = _makePipeline(
            [
                ("A", ("B", "C"), "task0"),
                ("B", "E", "task1"),
                ("B", ("D", "F"), "task2"),
                ("D", "G", "task3"),
                (("C", "F"), "H", "task4"),
                ("task4_metadata", "I", "task5"),
            ],
            inputTypeOverrides={"A": "Prerequisite"},
        )

    def validateMermaidSource(self, file):
        """Analyze the mermaid source for correctness.

        Parameters
        ----------
        file : `io.StringIO`
            The in-memory file-like object to analyze.
        """
        # It's hard to validate the complete output, just checking a few basic
        # things, but even that is not terribly stable.
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

        breakpoint()
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

    def test_pipeline2mermaid(self):
        """Test mermaid_tools.pipeline2mermaid method."""
        file = io.StringIO()
        pipeline2mermaid(self.pipeline, file, expand_dimensions=True)
        self.validateMermaidSource(file)

        file.seek(0)
        pipeline2mermaid(self.pipelineTaskDefs, file, expand_dimensions=True)
        self.validateMermaidSource(file)

    @patch.object(vis.Mermaid, "__init__", new=_mockedMermaidInit)
    def test_show_mermaid(self):
        """Test the output of pipeline_graph.visualization.show_mermaid method
        in different formats.

        Notes
        -----
        For generating image outputs, the show_mermaid method invokes a remote
        rendering service (mermaid.ink), which can lead to timeouts at times.
        To bypass this instability, we patch Mermaid's __init__ method to
        override its svg_response and img_response attributes, allowing us to
        return predictable, mocked output.
        """
        # Create a pipeline graph for visualization.
        graph = self.pipeline.to_graph(visualization_only=True)

        # 1. Test Mermaid source output (mmd format) using a text stream.
        file = io.StringIO()
        vis.show_mermaid(graph, file, output_format="mmd", dataset_types=True, task_classes="full")
        # self.validateMermaidSource(file)

        # 2. Test SVG output using a binary stream.
        file = io.BytesIO()
        vis.show_mermaid(graph, file, output_format="svg", dataset_types=True, task_classes="full")
        file.seek(0)  # Read from the beginning of the file.
        self.assertEqual(file.read(), b"<svg>Mocked SVG Content</svg>")

        # 3. Test PNG output using a binary stream.
        file = io.BytesIO()
        vis.show_mermaid(graph, file, output_format="png", dataset_types=True, task_classes="full")
        file.seek(0)
        self.assertEqual(file.read(), b"\x89PNG\r\n\x1a\nMocked PNG Content")


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
