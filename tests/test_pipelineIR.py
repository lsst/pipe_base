
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

import os
import tempfile
import textwrap
import unittest

from lsst.pipe.base.pipelineIR import PipelineIR, ConfigIR
import lsst.utils.tests


class ConfigIRTestCase(unittest.TestCase):
    """A test case for ConfigIR Objects

    ConfigIR contains a method that is not exercised by the PipelineIR task,
    so it should be tested here
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testMergeConfig(self):
        # Create some configs to merge
        config1 = ConfigIR(python="config.foo=6", dataId={"visit": 7}, file=["test1.py"],
                           rest={"a": 1, "b": 2})
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
    """A test case for PipelineIR objects
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

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

        # This should raise a FileNotFoundError, as there are inherits defined
        # so the __init__ method should pass but the inherited file does not
        # exist
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits: /dummy_pipeline.yaml
        """)

        with self.assertRaises(FileNotFoundError):
            PipelineIR.from_string(pipeline_str)

    def testTaskParsing(self):
        # Should be able to parse a task defined both ways
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(list(pipeline.tasks.keys()), ["modA", "modB"])
        self.assertEqual([t.klass for t in pipeline.tasks.values()], ["test.modA", "test.modB"])

    def testInheritParsing(self):
        # This should raise, as the two pipelines, both define the same label
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - $PIPE_BASE_DIR/tests/testPipeline1.yaml
            - $PIPE_BASE_DIR/tests/testPipeline2.yaml
        """)

        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # This should pass, as the conflicting task is excluded
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - location: $PIPE_BASE_DIR/tests/testPipeline1.yaml
              exclude: modA
            - $PIPE_BASE_DIR/tests/testPipeline2.yaml
        """)
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(set(pipeline.tasks.keys()), set(["modA", "modB"]))

        # This should pass, as the conflicting task is no in includes
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - location: $PIPE_BASE_DIR/tests/testPipeline1.yaml
              include: modB
            - $PIPE_BASE_DIR/tests/testPipeline2.yaml
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(set(pipeline.tasks.keys()), set(["modA", "modB"]))

        # Test that you cant include and exclude a task
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - location: $PIPE_BASE_DIR/tests/testPipeline1.yaml
              exclude: modA
              include: modB
            - $PIPE_BASE_DIR/tests/testPipeline2.yaml
        """)

        with self.assertRaises(ValueError):
            PipelineIR.from_string(pipeline_str)

        # Test that contracts are inherited
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - $PIPE_BASE_DIR/tests/testPipeline1.yaml
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.contracts[0].contract, "modA.b == modA.c")

        # Test that contracts are not inherited
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - location: $PIPE_BASE_DIR/tests/testPipeline1.yaml
              importContracts: False
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.contracts, [])

        # Test that configs are inherited when defining the same task again
        # with the same label
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - $PIPE_BASE_DIR/tests/testPipeline2.yaml
        tasks:
          modA:
            class: "test.moduleA"
            config:
                value2: 2
        """)
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"value1": 1, "value2": 2})

        # Test that configs are not inherited when redefining the task
        # associated with a label
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        inherits:
            - $PIPE_BASE_DIR/tests/testPipeline2.yaml
        tasks:
          modA:
            class: "test.moduleAReplace"
            config:
                value2: 2
        """)
        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"value2": 2})

    def testReadContracts(self):
        # Verify that contracts are read in from a pipeline
        location = os.path.expandvars("$PIPE_BASE_DIR/tests/testPipeline1.yaml")
        pipeline = PipelineIR.from_file(location)
        self.assertEqual(pipeline.contracts[0].contract, "modA.b == modA.c")

        # Verify that a contract message is loaded
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        tasks:
            modA: test.modA
            modB:
              class: test.modB
        contracts:
            - contract: modA.foo == modB.Bar
              msg: "Test message"
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.contracts[0].msg, "Test message")

    def testInstrument(self):
        # Verify that if instrument is defined it is parsed out
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        instrument: dummyCam
        tasks:
            modA: test.moduleA
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.instrument, "dummyCam")

    def testReadTaskConfig(self):
        # Verify that a task with a config is read in correctly
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        tasks:
            modA:
                class: test.moduleA
                config:
                    propertyA: 6
                    propertyB: 7
                    file: testfile.py
                    python: "config.testDict['a'] = 9"
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].file, ["testfile.py"])
        self.assertEqual(pipeline.tasks["modA"].config[0].python, "config.testDict['a'] = 9")
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"propertyA": 6, "propertyB": 7})

        # Verify that multiple files are read fine
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        tasks:
            modA:
                class: test.moduleA
                config:
                    file:
                        - testfile.py
                        - otherFile.py
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].file, ["testfile.py", "otherFile.py"])

        # Test reading multiple Config entries
        pipeline_str = textwrap.dedent("""
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
        """)

        pipeline = PipelineIR.from_string(pipeline_str)
        self.assertEqual(pipeline.tasks["modA"].config[0].rest, {"propertyA": 6, "propertyB": 7})
        self.assertEqual(pipeline.tasks["modA"].config[0].dataId, {"visit": 6})
        self.assertEqual(pipeline.tasks["modA"].config[1].rest, {"propertyA": 8, "propertyB": 9})
        self.assertEqual(pipeline.tasks["modA"].config[1].dataId, None)

    def testSerialization(self):
        # Test creating a pipeline, writing it to a file, reading the file
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        instrument: dummyCam
        inherits: $PIPE_BASE_DIR/tests/testPipeline1.yaml
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
        """)

        pipeline = PipelineIR.from_string(pipeline_str)

        # Create the temp file, write and read
        with tempfile.NamedTemporaryFile() as tf:
            pipeline.to_file(tf.name)
            loaded_pipeline = PipelineIR.from_file(tf.name)
        self.assertEqual(pipeline, loaded_pipeline)

    def testPipelineYamlLoader(self):
        # Tests that an exception is thrown in the case a key is used multiple
        # times in a given scope within a pipeline file
        pipeline_str = textwrap.dedent("""
        description: Test Pipeline
        tasks:
            modA: test1
            modB: test2
            modA: test3
        """)
        self.assertRaises(KeyError, PipelineIR.from_string, pipeline_str)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
