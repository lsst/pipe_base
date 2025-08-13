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

"""Simple unit test for Pipeline visualization using Mermaid."""

import io
import unittest
from unittest.mock import MagicMock, patch

import lsst.pipe.base.connectionTypes as cT
import lsst.pipe.base.pipeline_graph.visualization as vis
import lsst.utils.tests
from lsst.pipe.base import Pipeline, PipelineTask, PipelineTaskConfig, PipelineTaskConnections
from lsst.pipe.base.mermaid_tools import pipeline2mermaid

MERMAID_AVAILABLE = vis._mermaid.MERMAID_AVAILABLE

if MERMAID_AVAILABLE:
    # Since we are patching Mermaid’s `__init__`, calling
    # `vis.Mermaid.__init__` inside the mock would cause infinite recursion.
    # Using a reference to the original initializer prevents this.
    _originalMermaidInit = vis._mermaid.Mermaid.__init__

    # Mocked content for SVG and PNG files.
    MOCKED_SVG_CONTENT = b"<svg>Mocked SVG Content</svg>"
    MOCKED_PNG_CONTENT = b"\x89PNG\r\n\x1a\nMocked PNG Content"

    def _mockMermaidInit(self, *args, **kwargs):
        # Call the original initializer to set things up.
        _originalMermaidInit(self, *args, **kwargs)

        # Override only the `svg_response` and `img_response` attributes.
        self.svg_response = MagicMock(content=MOCKED_SVG_CONTENT)
        self.img_response = MagicMock(content=MOCKED_PNG_CONTENT)


class ExamplePipelineTaskConnections(PipelineTaskConnections, dimensions=("visit", "detector")):
    """Connections class used for testing.

    Parameters
    ----------
    config : `~lsst.pipe.base.PipelineTaskConfig`
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
    """Configure pipeline connections by adding config overrides.

    Parameters
    ----------
    inputNames : `list` or `tuple` of `str`
        A list or tuple containing up to two input names.
    outputNames : `list` or `tuple` of `str`
        A list or tuple containing up to two output names.
    pipeline : `~lsst.pipe.base.Pipeline`
        The pipeline object where configuration overrides will be applied.
    label : `str`
        The label associated with the configuration.
    inputTypes : `list` or `tuple` of `str`, optional
        A list or tuple specifying input types. Elements can be 'Prerequisite'
        or empty strings (the default, indicating a regular input).
    """
    # Ensure at least two elements, default to empty strings.
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
    """Generate a Pipeline instance and return it along with task definitions.

    Parameters
    ----------
    tasks : `list` of `tuple`
        Each tuple in the list has 3 or 4 items:
        - input DatasetType name(s), string or tuple of strings
        - output DatasetType name(s), string or tuple of strings
        - task label, string
        - optional task class object, can be None
    inputTypeOverrides : `dict`, optional
        Dictionary of input type overrides.

    Returns
    -------
    pipe: `~lsst.pipe.base.Pipeline`
        The pipeline instance.
    task_defs: `list` of `~lsst.pipe.base.TaskDef`
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
        # Create a pipeline with example tasks and dataset types.
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

        # Create a pipeline graph for processing.
        self.graph = self.pipeline.to_graph(visualization_only=True)

    def validateMermaidSource(self, file):
        """Validate the Mermaid source output for basic components.

        Parameters
        ----------
        file : `~io.StringIO`
            The in-memory file-like object containing the Mermaid source.
        """
        # It's hard to validate the complete output, just checking some basic
        # things, but even that is not terribly stable.
        fileValue = file.getvalue()
        lines = fileValue.strip().split("\n")
        nClassDefs = 2
        nTasks = 6
        nTaskClasses = 6
        nDatasets = 10
        nDatasetClasses = 10
        nEdges = 16
        nLinkStyles = 2  # For the default and pre-requisite edges
        nExtra = 1  # 'flowchart' opening line

        self.assertEqual(
            len(lines),
            nClassDefs + nTasks + nTaskClasses + nDatasets + nDatasetClasses + nEdges + nLinkStyles + nExtra,
        )

        # Confirm Mermaid syntax begins with a top-down flowchart declaration.
        self.assertTrue(fileValue.startswith("flowchart TD"))

        # Select the reference set based on naming style.
        namingRef = {
            "edge1": "B:0 --> task1:2",
            "edge2": "task2:2 --> F:0",
            "classDef": "class E:0 dsType;",
            "metadata": "task4:2 --> task4_metadata:0",
        }

        # Make sure components are connected appropriately.
        self.assertIn(namingRef["edge1"], fileValue)
        self.assertIn(namingRef["edge2"], fileValue)

        # Make sure class definitions are created for datasets.
        self.assertIn(namingRef["classDef"], fileValue)

        # Make sure there is a connection created for metadata.
        self.assertIn(namingRef["metadata"], fileValue)

    def test_pipeline2mermaid(self):
        """Validate Mermaid syntax generated by `pipeline2mermaid`."""
        for source in [self.pipeline, self.pipelineTaskDefs]:
            file = io.StringIO()
            pipeline2mermaid(source, file, expand_dimensions=False)
            self.validateMermaidSource(file)

    def test_show_mermaid_source(self):
        """Test generating Mermaid source using `show_mermaid`."""
        # Validate Mermaid source output (mmd format) in a text stream.
        file = io.StringIO()
        vis.show_mermaid(self.graph, file, dataset_types=True, task_classes="full")
        self.validateMermaidSource(file)

    @unittest.skipIf(not MERMAID_AVAILABLE, "Skipping image rendering tests since `mermaid-py` is missing.")
    def test_show_mermaid_image(self):
        """Test the output of the `show_mermaid` method in png and svg formats.

        For generating image outputs, the `show_mermaid` method invokes a
        remote rendering service (`mermaid.ink`), which can lead to timeouts at
        times. To bypass this instability, we patch Mermaid's `__init__` method
        to override its `svg_response` and `img_response` attributes, allowing
        us to return predictable, mocked contents instead.
        """
        # Test image rendering formats with mocked responses in binary streams.
        with patch.object(vis._mermaid.Mermaid, "__init__", new=_mockMermaidInit):
            for fmt, expected in [("svg", MOCKED_SVG_CONTENT), ("png", MOCKED_PNG_CONTENT)]:
                file = io.BytesIO()
                vis.show_mermaid(self.graph, file, output_format=fmt, dataset_types=True, task_classes="full")
                file.seek(0)  # Read from the beginning.
                self.assertEqual(file.read(), expected)


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
