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

import os
import tempfile
import textwrap
import unittest

import lsst.utils.tests
from lsst.pipe.base.pipelineIR import ConfigIR, PipelineIR, PipelineSubsetCtrl

# Find where the test pipelines exist and store it in an environment variable.
os.environ["TESTDIR"] = os.path.dirname(__file__)


class ConfigIRTestCase(unittest.TestCase):
    """A test case for ConfigIR Objects.

    ConfigIR contains a method that is not exercised by the PipelineIR task,
    so it should be tested here.
    """

    def testMergeConfig(self):
        # Create some configs to merge
        config1 = ConfigIR(
            python="config.foo=6", dataId={"visit": 7}, file=["test1.py"], rest={"a": 1, "b": 2}
        )
        config2 = ConfigIR(python=None, dataId=None, file=["test2.py"], rest={"c": 1, "d": 2})
        config3 = ConfigIR(python="config.bar=7", dataId=None, file=["test3.py"], rest={"c": 1, "d": 2})
        config4 = ConfigIR(python=None, dataId=None, file=["test4.py"], rest={"c": 3, "e": 4})
        config5 = ConfigIR(rest={"f": 5, "g": 6})
        config6 = ConfigIR(rest={"h": 7, "i": 8})
        config7 = ConfigIR(rest={"h": 9})

        # Merge configs with different dataIds, this should yield two elements
        self.assertEqual(list(config1.maybe_merge(config2)), [config1, config2])

        # Merge configs with python blocks defined, this should yield two
        # elements
        self.assertEqual(list(config1.maybe_merge(config3)), [config1, config3])

        # Merge configs with file defined, this should yield two elements
        self.assertEqual(list(config2.maybe_merge(config4)), [config2, config4])

        # merge config2 into config1
        merge_result = list(config5.maybe_merge(config6))
        self.assertEqual(len(merge_result), 1)
        self.assertEqual(config5.rest, {"f": 5, "g": 6, "h": 7, "i": 8})

        # Cant merge configs with shared keys
        self.assertEqual(list(config6.maybe_merge(config7)), [config6, config7])


class PipelineIRTestCase(unittest.TestCase):
    """A test case for PipelineIR objects."""

    def testPipelineIRInitChecks(self):
        # Missing description
        pipeline_str = """
        tasks:
          a: module.A
        """
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Missing tasks
        pipeline_str = """
        description: Test Pipeline
        """
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # This should raise a FileNotFoundError, as there are imported defined
        # so the __init__ method should pass but the imported file does not
        # exist
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports: /dummy_pipeline.yaml
        """
        )

        with self.assertRaises(FileNotFoundError):
            PipelineIR.from_string(pipeline_str)

    def testTaskParsing(self):
        # Should be able to parse a task defined both ways
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(list(pipeline.tasks.keys()), ["modA", "modB"])
        self.assertEqual([t.klass for t in pipeline.tasks.values()], ["test.modA", "test.modB"])

    def testImportParsing(self):
        # This should raise, as the two pipelines, both define the same label
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline1.yaml
            - $TESTDIR/testPipeline2.yaml
        """
        )
        # "modA" is the duplicated label, and it should appear in the error.
        with self.assertRaisesRegex(ValueError, "modA"):
            PipelineIR.from_string(pipeline_str)

        # This should pass, as the conflicting task is excluded
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - location: $TESTDIR/testPipeline1.yaml
              exclude: modA
            - $TESTDIR/testPipeline2.yaml
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(set(pipeline.tasks.keys()), {"modA", "modB"})

        # This should pass, as the conflicting task is no in includes
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - location: $TESTDIR/testPipeline1.yaml
              include: modB
              labeledSubsetModifyMode: DROP
            - $TESTDIR/testPipeline2.yaml
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(set(pipeline.tasks.keys()), {"modA", "modB"})

        # Test that you cant include and exclude a task
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - location: $TESTDIR/testPipeline1.yaml
              exclude: modA
              include: modB
              labeledSubsetModifyMode: EDIT
            - $TESTDIR/testPipeline2.yaml
        """
        )

        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Test unknown labeledSubsetModifyModes raise
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - location: $TESTDIR/testPipeline1.yaml
              exclude: modA
              include: modB
              labeledSubsetModifyMode: WRONG
            - $TESTDIR/testPipeline2.yaml
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Test that contracts are imported
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline1.yaml
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.contracts[0].contract, "modA.b == modA.c")

        # Test that contracts are not imported
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - location: $TESTDIR/testPipeline1.yaml
              importContracts: False
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.contracts, [])

        # Test that configs are imported when defining the same task again
        # with the same label
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline2.yaml
        tasks:
          modA:
            class: "test.moduleA"
            config:
                value2: 2
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"value1": 1, "value2": 2})

        # Test that configs are not imported when redefining the task
        # associated with a label
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline2.yaml
        tasks:
          modA:
            class: "test.moduleAReplace"
            config:
                value2: 2
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"value2": 2})

        # Test that named subsets are imported
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline2.yaml
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.labeled_subsets.keys(), {"modSubset"})
        self.assertEqual(pipeline.labeled_subsets["modSubset"].subset, {"modA"})

        # Test that imported and redeclaring a named subset works
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline2.yaml
        tasks:
          modE: "test.moduleE"
        subsets:
          modSubset:
            - modE
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.labeled_subsets.keys(), {"modSubset"})
        self.assertEqual(pipeline.labeled_subsets["modSubset"].subset, {"modE"})

        # Test that imported from two pipelines that both declare a named
        # subset with the same name fails
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline2.yaml
            - $TESTDIR/testPipeline3.yaml
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Test that imported a named subset that duplicates a label declared
        # in this pipeline fails
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline2.yaml
        tasks:
          modSubset: "test.moduleE"
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Test that imported fails if a named subset and task label conflict
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline2.yaml
            - $TESTDIR/testPipeline4.yaml
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Test that importing Pipelines with different step definitions fails
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - $TESTDIR/testPipeline5.yaml
        steps:
            - label: sub1
              sharding_dimensions: ['a', 'e']
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Test that it does not fail if steps are excluded
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - location: $TESTDIR/testPipeline5.yaml
              importSteps: false
        steps:
            - label: sub1
              sharding_dimensions: ['a', 'e']
        """
        )
        PipelineIR.from_string(pipeline_str)

        # Test that importing does work
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
            - location: $TESTDIR/testPipeline5.yaml
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(set(step.label for step in pipeline.steps), {"sub1", "sub2"})

    def testSteps(self):
        # Test that steps definitions are created
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: "test.moduleA"
            modB: "test.moduleB"
        subsets:
            sub1:
                subset:
                - modA
                - modB
            sub2:
                subset:
                - modA
        steps:
            - label: sub1
              sharding_dimensions: ['a', 'b']
            - label: sub2
              sharding_dimensions: ['a', 'b']
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(set(step.label for step in pipeline.steps), {"sub1", "sub2"})

        # Test that steps definitions must be unique
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: "test.moduleA"
            modB: "test.moduleB"
        subsets:
            sub1:
                subset:
                - modA
                - modB
            sub2:
                subset:
                - modA
        steps:
            - label: sub1
              sharding_dimensions: ['a', 'b']
            - label: sub1
              sharding_dimensions: ['a', 'b']
        """
        )
        with self.assertRaises(ValueError):
            pipeline = PipelineIR.from_string(pipeline_str)

    def testReadParameters(self):
        # verify that parameters section are read in from a pipeline
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        parameters:
          value1: A
          value2: B
        tasks:
          modA: ModuleA
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.parameters.mapping, {"value1": "A", "value2": "B"})

    def testTaskParameterLabel(self):
        # verify that "parameters" cannot be used as a task label
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
          parameters: modA
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

    def testParameterImporting(self):
        # verify that importing parameters happens correctly
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
          - $TESTDIR/testPipeline1.yaml
          - location: $TESTDIR/testPipeline2.yaml
            exclude:
              - modA

        parameters:
          value4: valued
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(
            pipeline.parameters.mapping,
            {"value4": "valued", "value1": "valueNew", "value2": "valueB", "value3": "valueC"},
        )

    def testImportingInstrument(self):
        # verify an instrument is imported, or ignored, (Or otherwise modified
        # for potential future use)
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
          - $TESTDIR/testPipeline1.yaml
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.instrument, "test.instrument")

        # verify that an imported pipeline can have its instrument set to None
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
          - location: $TESTDIR/testPipeline1.yaml
            instrument: None
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.instrument, None)

        # verify that an imported pipeline can have its instrument modified
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        imports:
          - location: $TESTDIR/testPipeline1.yaml
            instrument: new.instrument
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.instrument, "new.instrument")

        # Test that multiple instruments can't be defined,
        # and that the error message tells you what instruments were found.
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        instrument: new.instrument
        imports:
          - location: $TESTDIR/testPipeline1.yaml
        """
        )
        with self.assertRaisesRegex(ValueError, "new.instrument .* test.instrument."):
            PipelineIR.from_string(pipeline_str)

    def testParameterConfigFormatting(self):
        # verify that a config properly is formatted with parameters
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        parameters:
          value1: A
        tasks:
          modA:
            class: ModuleA
            config:
              testKey: parameters.value1
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        newConfig = pipeline.tasks["modA"].config[0].formatted(pipeline.parameters)
        self.assertEqual(newConfig.rest["testKey"], "A")

    def testReadContracts(self):
        # Verify that contracts are read in from a pipeline
        location = "$TESTDIR/testPipeline1.yaml"
        pipeline = PipelineIR.from_uri(location)
        self.assertEqual(pipeline.contracts[0].contract, "modA.b == modA.c")

        # Verify that a contract message is loaded
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
        contracts:
            - contract: modA.foo == modB.Bar
              msg: "Test message"
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.contracts[0].msg, "Test message")

    def testReadNamedSubsets(self):
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
            modC: test.modC
            modD: test.modD
        subsets:
            subset1:
              - modA
              - modB
            subset2:
              subset:
                - modC
                - modD
              description: "A test named subset"
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.labeled_subsets.keys(), {"subset1", "subset2"})

        self.assertEqual(pipeline.labeled_subsets["subset1"].subset, {"modA", "modB"})
        self.assertEqual(pipeline.labeled_subsets["subset1"].description, None)

        self.assertEqual(pipeline.labeled_subsets["subset2"].subset, {"modC", "modD"})
        self.assertEqual(pipeline.labeled_subsets["subset2"].description, "A test named subset")

        # verify that forgetting a subset key is an error
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
            modC: test.modC
            modD: test.modD
        subsets:
            subset2:
              sub:
                - modC
                - modD
              description: "A test named subset"
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # verify putting a label in a named subset that is not in the task is
        # an error
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
            modC: test.modC
            modD: test.modD
        subsets:
            subset2:
              - modC
              - modD
              - modE
        """
        )
        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

    def testSubsettingPipeline(self):
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
            modC: test.modC
            modD: test.modD
        subsets:
            subset1:
              - modA
              - modB
            subset2:
              subset:
                - modC
                - modD
              description: "A test named subset"
        """
        )
        pipeline = PipelineIR.from_string(pipeline_str)
        # verify that creating a pipeline subset with the default drop behavior
        # removes any labeled subset that contains a label not in the set of
        # all task labels.
        pipelineSubset1 = pipeline.subset_from_labels({"modA", "modB", "modC"})
        self.assertEqual(pipelineSubset1.labeled_subsets.keys(), {"subset1"})
        # verify that creating a pipeline subset with the edit behavior
        # edits any labeled subset that contains a label not in the set of
        # all task labels.
        pipelineSubset2 = pipeline.subset_from_labels({"modA", "modB", "modC"}, PipelineSubsetCtrl.EDIT)
        self.assertEqual(pipelineSubset2.labeled_subsets.keys(), {"subset1", "subset2"})
        self.assertEqual(pipelineSubset2.labeled_subsets["subset2"].subset, {"modC"})

    def testInstrument(self):
        # Verify that if instrument is defined it is parsed out
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        instrument: dummyCam
        tasks:
            modA: test.moduleA
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.instrument, "dummyCam")

    def testReadTaskConfig(self):
        # Verify that a task with a config is read in correctly
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA:
                class: test.moduleA
                config:
                    propertyA: 6
                    propertyB: 7
                    file: testfile.py
                    python: "config.testDict['a'] = 9"
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].file, ["testfile.py"])
        self.assertEqual(pipeline.tasks["modA"].config[0].python, "config.testDict['a'] = 9")
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"propertyA": 6, "propertyB": 7})

        # Verify that multiple files are read fine
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA:
                class: test.moduleA
                config:
                    file:
                        - testfile.py
                        - otherFile.py
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].file, ["testfile.py", "otherFile.py"])

        # Test reading multiple Config entries
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA:
                class: test.moduleA
                config:
                    - propertyA: 6
                      propertyB: 7
                      dataId: {"visit": 6}
                    - propertyA: 8
                      propertyB: 9
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"propertyA": 6, "propertyB": 7})
        self.assertEqual(pipeline.tasks["modA"].config[0].dataId, {"visit": 6})
        self.assertEqual(pipeline.tasks["modA"].config[1].rest, {"propertyA": 8, "propertyB": 9})
        self.assertEqual(pipeline.tasks["modA"].config[1].dataId, None)

    def testSerialization(self):
        # Test creating a pipeline, writing it to a file, reading the file
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        instrument: dummyCam
        imports:
          - location: $TESTDIR/testPipeline1.yaml
            instrument: None
        tasks:
            modC:
                class: test.moduleC
                config:
                    - propertyA: 6
                      propertyB: 7
                      dataId: {"visit": 6}
                    - propertyA: 8
                      propertyB: 9
            modD: test.moduleD
        contracts:
            - modA.foo == modB.bar
        subsets:
            subA:
               - modA
               - modC
        """
        )

        pipeline = PipelineIR.from_string(pipeline_str)

        # Create the temp file, write and read
        with tempfile.NamedTemporaryFile() as tf:
            pipeline.write_to_uri(tf.name)
            loaded_pipeline = PipelineIR.from_uri(tf.name)
        self.assertEqual(pipeline, loaded_pipeline)

    def testPipelineYamlLoader(self):
        # Tests that an exception is thrown in the case a key is used multiple
        # times in a given scope within a pipeline file
        pipeline_str = textwrap.dedent(
            """
        description: Test Pipeline
        tasks:
            modA: test1
            modB: test2
            modA: test3
        """
        )
        self.assertRaises(KeyError, PipelineIR.from_string, pipeline_str)

    def testMultiLineStrings(self):
        """Test that multi-line strings in pipelines are written with
        '|' continuation-syntax instead of explicit newlines.
        """
        pipeline_ir = PipelineIR({"description": "Line 1\nLine2\n", "tasks": {"modA": "task1"}})
        string = str(pipeline_ir)
        self.assertIn("|", string)
        self.assertNotIn(r"\n", string)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module):
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
