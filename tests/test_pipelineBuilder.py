#
# LSST Data Management System
# Copyright 2018 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""Simple unit test for PipelineBuilder.
"""

import os
import unittest
from tempfile import NamedTemporaryFile

import lsst.utils.tests
import lsst.pex.config as pexConfig
from lsst.pipe.base import (PipelineTask, PipelineTaskConfig, PipelineBuilder)


class SimpleConfig(PipelineTaskConfig):
    field = pexConfig.Field(dtype=str, doc="arbitrary string")


class TaskOne(PipelineTask):
    ConfigClass = SimpleConfig
    _DefaultName = "taskOne"


class TaskTwo(PipelineTask):
    ConfigClass = SimpleConfig
    _DefaultName = "taskTwo"


class TaskFactoryMock(object):
    def loadTaskClass(self, taskName):
        if taskName == "TaskOne":
            return TaskOne, "TaskOne"
        elif taskName == "TaskTwo":
            return TaskTwo, "TaskTwo"

    def makeTask(self, taskClass, config, overrides, butler):
        if config is None:
            config = taskClass.ConfigClass()
            if overrides:
                overrides.applyTo(config)
        return taskClass(config=config, butler=butler)


class PipelineBuilderTestCase(unittest.TestCase):
    """A test case for PipelineBuilder class
    """

    def test_AddTasks(self):
        """Simple test case adding tasks to a pipeline
        """
        # create a task with default label
        builder = PipelineBuilder(TaskFactoryMock())
        builder.addTask("TaskOne")
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 1)
        self.assertEqual(pipeline[0].taskName, "TaskOne")
        self.assertEqual(pipeline[0].label, "TaskOne")
        self.assertIs(pipeline[0].taskClass, TaskOne)

        # create a task, give it a label
        builder = PipelineBuilder(TaskFactoryMock())
        builder.addTask("TaskOne", "label")
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 1)
        self.assertEqual(pipeline[0].taskName, "TaskOne")
        self.assertEqual(pipeline[0].label, "label")
        self.assertIs(pipeline[0].taskClass, TaskOne)

        # two different tasks
        builder = PipelineBuilder(TaskFactoryMock())
        builder.addTask("TaskOne")
        builder.addTask("TaskTwo")
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(pipeline[0].taskName, "TaskOne")
        self.assertEqual(pipeline[0].label, "TaskOne")
        self.assertIs(pipeline[0].taskClass, TaskOne)
        self.assertEqual(pipeline[1].taskName, "TaskTwo")
        self.assertEqual(pipeline[1].label, "TaskTwo")
        self.assertIs(pipeline[1].taskClass, TaskTwo)

        # more than one instance of each class
        builder = PipelineBuilder(TaskFactoryMock())
        tasks = [("TaskOne",),
                 ("TaskTwo",),
                 ("TaskOne", "label"),
                 ("TaskTwo", "label2")]
        for task in tasks:
            builder.addTask(*task)
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 4)
        self.assertEqual(pipeline[0].taskName, "TaskOne")
        self.assertEqual(pipeline[0].label, "TaskOne")
        self.assertEqual(pipeline[1].taskName, "TaskTwo")
        self.assertEqual(pipeline[1].label, "TaskTwo")
        self.assertEqual(pipeline[2].taskName, "TaskOne")
        self.assertEqual(pipeline[2].label, "label")
        self.assertEqual(pipeline[3].taskName, "TaskTwo")
        self.assertEqual(pipeline[3].label, "label2")

    def test_LabelExceptions(self):
        """Simple test case adding tasks with identical labels
        """
        builder = PipelineBuilder(TaskFactoryMock())
        builder.addTask("TaskOne")
        with self.assertRaises(LookupError):
            builder.addTask("TaskOne")

        builder = PipelineBuilder(TaskFactoryMock())
        builder.addTask("TaskOne", "label")
        with self.assertRaises(LookupError):
            builder.addTask("TaskTwo", "label")

    def test_DeleteTask(self):
        """Simple test case removing tasks
        """
        builder = PipelineBuilder(TaskFactoryMock())
        # make short pipeline
        tasks = [("TaskOne",),
                 ("TaskTwo",),
                 ("TaskOne", "label"),
                 ("TaskTwo", "label2")]
        for task in tasks:
            builder.addTask(*task)
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 4)

        builder.deleteTask("label")
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 3)

        builder.deleteTask("TaskTwo")
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 2)

        builder.deleteTask("TaskOne")
        builder.deleteTask("label2")
        pipeline = builder.pipeline()
        self.assertEqual(len(pipeline), 0)

        # unknown label should raise LookupError
        builder.addTask("TaskOne")
        builder.addTask("TaskTwo")
        with self.assertRaises(LookupError):
            builder.deleteTask("label2")

    def test_MoveTask(self):
        """Simple test case moving tasks
        """
        builder = PipelineBuilder(TaskFactoryMock())
        # make short pipeline
        tasks = [("TaskOne",),
                 ("TaskTwo",),
                 ("TaskOne", "label"),
                 ("TaskTwo", "label2")]
        for task in tasks:
            builder.addTask(*task)
        pipeline = builder.pipeline()
        self.assertEqual([t.label for t in pipeline],
                         ["TaskOne", "TaskTwo", "label", "label2"])

        builder.moveTask("label", 1)
        pipeline = builder.pipeline()
        self.assertEqual([t.label for t in pipeline],
                         ["TaskOne", "label", "TaskTwo", "label2"])

        # negatives are accepted but may be non-intuitive
        builder.moveTask("TaskOne", -1)
        pipeline = builder.pipeline()
        self.assertEqual([t.label for t in pipeline],
                         ["label", "TaskTwo", "TaskOne", "label2"])

        # position larger than list size moves to end
        builder.moveTask("label", 100)
        pipeline = builder.pipeline()
        self.assertEqual([t.label for t in pipeline],
                         ["TaskTwo", "TaskOne", "label2", "label"])

        builder.moveTask("label", 0)
        pipeline = builder.pipeline()
        self.assertEqual([t.label for t in pipeline],
                         ["label", "TaskTwo", "TaskOne", "label2"])

        # unknown label should raise LookupError
        with self.assertRaises(LookupError):
            builder.moveTask("unlabel", 0)

    def test_LabelTask(self):
        """Simple test case re-labeling tasks
        """
        builder = PipelineBuilder(TaskFactoryMock())
        # make short pipeline
        tasks = [("TaskOne",),
                 ("TaskTwo",),
                 ("TaskOne", "label"),
                 ("TaskTwo", "label2")]
        for task in tasks:
            builder.addTask(*task)
        pipeline = builder.pipeline()
        self.assertEqual([t.label for t in pipeline],
                         ["TaskOne", "TaskTwo", "label", "label2"])

        builder.labelTask("TaskOne", "label0")
        pipeline = builder.pipeline()
        self.assertEqual([t.label for t in pipeline],
                         ["label0", "TaskTwo", "label", "label2"])

        # unknown label should raise LookupError
        with self.assertRaises(LookupError):
            builder.labelTask("unlabel", "label3")

        # duplicate label should raise LookupError
        with self.assertRaises(LookupError):
            builder.labelTask("label2", "label0")

    def test_ConfigTask(self):
        """Simple test case for config overrides
        """
        builder = PipelineBuilder(TaskFactoryMock())
        # make simple pipeline
        builder.addTask("TaskOne", "task")
        pipeline = builder.pipeline()
        self.assertIsInstance(pipeline[0].config, SimpleConfig)
        self.assertIsNone(pipeline[0].config.field)

        builder.configOverride("task", "field=value")
        pipeline = builder.pipeline()
        self.assertEqual(pipeline[0].config.field, "value")

        # unknown label should raise LookupError
        with self.assertRaises(LookupError):
            builder.configOverride("label", "field=value")

    def test_ConfigFileTask(self):
        """Simple test case for config overrides in file
        """
        builder = PipelineBuilder(TaskFactoryMock())
        # make simple pipeline
        builder.addTask("TaskOne", "task")
        builder.configOverride("task", "field=value")
        pipeline = builder.pipeline()
        self.assertEqual(pipeline[0].config.field, "value")

        # save config to file for next test
        overrides = NamedTemporaryFile(mode="wt", delete=False)
        pipeline[0].config.saveToStream(overrides)
        fname = overrides.name
        del overrides

        builder = PipelineBuilder(TaskFactoryMock())
        builder.addTask("TaskOne", "task")
        builder.configOverrideFile("task", fname)
        pipeline = builder.pipeline()
        self.assertEqual(pipeline[0].config.field, "value")

        os.unlink(fname)

        # unknown label should raise LookupError
        with self.assertRaises(LookupError):
            builder.configOverrideFile("label", "/dev/null")


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
