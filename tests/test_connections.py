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

"""Simple unit test for PipelineTaskConnections."""

import unittest
import warnings

import pytest

import lsst.pipe.base as pipeBase
import lsst.utils.tests
from lsst.pex.config import Field


class TestConnectionsClass(unittest.TestCase):
    """Test connection classes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Test dimensions
        self.test_dims = ("a", "b")

    def testConnectionsDeclaration(self):
        """Tests the declaration of a Connections Class."""
        with pytest.raises(TypeError):
            # This should raise because this Connections class is created with
            # no dimensions
            class TestConnections(pipeBase.PipelineTaskConnections):
                pass

        with pytest.raises(TypeError):
            # This should raise because this Connections class is created with
            # out template defaults
            class TestConnectionsTemplate(pipeBase.PipelineTaskConnections, dimensions=self.test_dims):
                field = pipeBase.connectionTypes.Input(
                    doc="Test", name="{template}test", dimensions=self.test_dims, storageClass="Dummy"
                )

        # This declaration should raise no exceptions
        class TestConnectionsWithDimensions(pipeBase.PipelineTaskConnections, dimensions=self.test_dims):
            pass

        # This should not raise
        class TestConnectionsWithTemplate(
            pipeBase.PipelineTaskConnections,
            dimensions=self.test_dims,
            defaultTemplates={"template": "working"},
        ):
            field = pipeBase.connectionTypes.Input(
                doc="Test", name="{template}test", dimensions=self.test_dims, storageClass="Dummy"
            )

    def testConnectionsOnConnectionsClass(self):
        class TestConnections(pipeBase.PipelineTaskConnections, dimensions=self.test_dims):
            initInput1 = pipeBase.connectionTypes.InitInput(
                doc="Test Init input", name="init_input", storageClass="Dummy"
            )
            initInput2 = pipeBase.connectionTypes.InitInput(
                doc="Test Init input", name="init_input2", storageClass="Dummy"
            )

            initOutput1 = pipeBase.connectionTypes.InitOutput(
                doc="Test Init output", name="init_output1", storageClass="Dummy"
            )
            initOutput2 = pipeBase.connectionTypes.InitOutput(
                doc="Test Init output", name="init_output2", storageClass="Dummy"
            )

            input1 = pipeBase.connectionTypes.Input(
                doc="test input", name="input2", dimensions=self.test_dims, storageClass="Dummy"
            )
            input2 = pipeBase.connectionTypes.Input(
                doc="test input", name="input2", dimensions=self.test_dims, storageClass="Dummy"
            )

            prereqInputs1 = pipeBase.connectionTypes.PrerequisiteInput(
                doc="test input", name="pre_input1", dimensions=self.test_dims, storageClass="Dummy"
            )
            prereqInputs2 = pipeBase.connectionTypes.PrerequisiteInput(
                doc="test input", name="pre_input2", dimensions=self.test_dims, storageClass="Dummy"
            )

            output1 = pipeBase.connectionTypes.Output(
                doc="test output", name="output", dimensions=self.test_dims, storageClass="Dummy"
            )
            output2 = pipeBase.connectionTypes.Output(
                doc="test output", name="output", dimensions=self.test_dims, storageClass="Dummy"
            )

        self.assertEqual(TestConnections.initInputs, frozenset(["initInput1", "initInput2"]))
        self.assertEqual(TestConnections.initOutputs, frozenset(["initOutput1", "initOutput2"]))
        self.assertEqual(TestConnections.inputs, frozenset(["input1", "input2"]))
        self.assertEqual(TestConnections.prerequisiteInputs, frozenset(["prereqInputs1", "prereqInputs2"]))
        self.assertEqual(TestConnections.outputs, frozenset(["output1", "output2"]))

    def buildTestConnections(self):
        class TestConnectionsWithTemplate(
            pipeBase.PipelineTaskConnections,
            dimensions=self.test_dims,
            defaultTemplates={"template": "working"},
        ):
            field = pipeBase.connectionTypes.Input(
                doc="Test", name="{template}test", dimensions=self.test_dims, storageClass="Dummy"
            )
            field2 = pipeBase.connectionTypes.Output(
                doc="Test", name="field2Type", dimensions=self.test_dims, storageClass="Dummy", multiple=True
            )

            def adjustQuantum(self, datasetRefMap):
                if len(datasetRefMap.field) < 2:
                    raise ValueError("This connection should have more than one entry")

        class TestConfig(pipeBase.PipelineTaskConfig, pipelineConnections=TestConnectionsWithTemplate):
            pass

        config = TestConfig()
        config.connections.template = "fromConfig"
        config.connections.field2 = "field2FromConfig"

        connections = TestConnectionsWithTemplate(config=config)
        return connections

    def testConnectionsInstantiation(self):
        connections = self.buildTestConnections()
        self.assertEqual(connections.field.name, "fromConfigtest")
        self.assertEqual(connections.field2.name, "field2FromConfig")
        self.assertEqual(connections.allConnections["field"].name, "fromConfigtest")
        self.assertEqual(connections.allConnections["field2"].name, "field2FromConfig")

    def testBuildDatasetRefs(self):
        connections = self.buildTestConnections()

        mockQuantum = pipeBase.Struct(
            inputs={"fromConfigtest": ["a"]}, outputs={"field2FromConfig": ["b", "c"]}
        )

        inputRefs, outputRefs = connections.buildDatasetRefs(mockQuantum)
        self.assertEqual(inputRefs.field, "a")
        self.assertEqual(outputRefs.field2, ["b", "c"])

    def testAdjustQuantum(self):
        connections = self.buildTestConnections()
        mockQuantum = pipeBase.Struct(
            inputs={"fromConfigtest": ["a"]}, outputs={"field2FromConfig": ["b", "c"]}
        )
        inputRefs, outputRefs = connections.buildDatasetRefs(mockQuantum)
        with self.assertRaises(ValueError):
            connections.adjustQuantum(inputRefs)

    def testDimensionCheck(self):
        with self.assertRaises(TypeError):

            class TestConnectionsWithBrokenDimensionsStr(pipeBase.PipelineTask, dimensions={"a"}):
                pass

        with self.assertRaises(TypeError):

            class TestConnectionsWithBrokenDimensionsIter(pipeBase.PipelineTask, dimensions=2):
                pass

        with self.assertRaises(TypeError):
            pipeBase.connectionTypes.Output(
                Doc="mock doc", dimensions={"a"}, name="output", storageClass="mock"
            )

        with self.assertRaises(TypeError):
            pipeBase.connectionTypes.Output(Doc="mock doc", dimensions=1, name="output", storageClass="mock")

    def test_deprecation(self) -> None:
        """Test support for deprecating connections."""

        class TestConnections(
            pipeBase.PipelineTaskConnections,
            dimensions=self.test_dims,
            defaultTemplates={"t1": "dataset_type_1"},
            deprecatedTemplates={"t1": "Deprecated in v600, will be removed after v601."},
        ):
            input1 = pipeBase.connectionTypes.Input(
                doc="Docs for input1",
                name="input1_{t1}",
                storageClass="StructuredDataDict",
                deprecated="Deprecated in v50000, will be removed after v50001.",
            )

            def __init__(self, config):
                if config.drop_input1:
                    del self.input1

        class TestConfig(pipeBase.PipelineTaskConfig, pipelineConnections=TestConnections):
            drop_input1 = Field("Remove the 'input1' connection if True", dtype=bool, default=False)

        config = TestConfig()
        with self.assertWarns(FutureWarning):
            config.connections.input1 = "dataset_type_2"
        with self.assertWarns(FutureWarning):
            config.connections.t1 = "dataset_type_3"

        with self.assertWarns(FutureWarning):
            TestConnections(config=config)

        config.drop_input1 = True

        with warnings.catch_warnings():
            warnings.simplefilter("error", FutureWarning)
            TestConnections(config=config)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module):
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
