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

from __future__ import absolute_import, division, print_function

from argparse import Namespace
from tempfile import NamedTemporaryFile
import os
import unittest
from builtins import object

import lsst.utils.tests
from lsst.log import Log
import lsst.pex.config as pexConfig
from lsst.pipe.supertask import PipelineBuilder, SuperTask, SuperTaskConfig, parser


class SimpleConfig(SuperTaskConfig):
    field = pexConfig.Field(dtype=str, doc="arbitrary string")


class TaskOne(SuperTask):
    ConfigClass = SimpleConfig
    _DefaultName = "taskOne"
    def __init__(self, **kwargs):
        OneToOneTask.__init__(self, Dataset1, Dataset2, **kwargs)


class TaskTwo(SuperTask):
    ConfigClass = SimpleConfig
    _DefaultName = "taskTwo"
    def __init__(self, **kwargs):
        OneToOneTask.__init__(self, Dataset2, Dataset3, **kwargs)


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

    def setUp(self):
        self.builder = PipelineBuilder(TaskFactoryMock())

    def tearDown(self):
        del self.builder

    def test_AddTasks(self):
        """Simple test case adding tasks to a pipeline
        """
        # create a task with default label
        actions = [parser._ACTION_ADD_TASK("TaskOne")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(len(pipeline), 1)
        self.assertEqual(pipeline[0].taskName, "TaskOne")
        self.assertEqual(pipeline[0].label, "TaskOne")
        self.assertIs(pipeline[0].taskClass, TaskOne)

        # create a task, give it a label
        actions = [parser._ACTION_ADD_TASK("TaskOne:label")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(len(pipeline), 1)
        self.assertEqual(pipeline[0].taskName, "TaskOne")
        self.assertEqual(pipeline[0].label, "label")
        self.assertIs(pipeline[0].taskClass, TaskOne)

        # two different tasks
        actions = [parser._ACTION_ADD_TASK("TaskOne"),
                   parser._ACTION_ADD_TASK("TaskTwo")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(pipeline[0].taskName, "TaskOne")
        self.assertEqual(pipeline[0].label, "TaskOne")
        self.assertIs(pipeline[0].taskClass, TaskOne)
        self.assertEqual(pipeline[1].taskName, "TaskTwo")
        self.assertEqual(pipeline[1].label, "TaskTwo")
        self.assertIs(pipeline[1].taskClass, TaskTwo)

        # more than one instance of each class
        actions = [parser._ACTION_ADD_TASK("TaskOne"),
                   parser._ACTION_ADD_TASK("TaskTwo"),
                   parser._ACTION_ADD_TASK("TaskOne:label"),
                   parser._ACTION_ADD_TASK("TaskTwo:label2")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
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
        actions = [parser._ACTION_ADD_TASK("TaskOne"),
                   parser._ACTION_ADD_TASK("TaskOne")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)

        args.pipeline_actions = [parser._ACTION_ADD_TASK("TaskOne:label"),
                                 parser._ACTION_ADD_TASK("TaskTwo:label")]
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)

    def test_DeleteTask(self):
        """Simple test case removing tasks
        """
        # make short pipeline
        actions = [parser._ACTION_ADD_TASK("TaskOne"),
                   parser._ACTION_ADD_TASK("TaskTwo"),
                   parser._ACTION_ADD_TASK("TaskOne:label"),
                   parser._ACTION_ADD_TASK("TaskTwo:label2")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(len(pipeline), 4)

        args.pipeline_actions += [parser._ACTION_DELETE_TASK("label")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(len(pipeline), 3)

        args.pipeline_actions += [parser._ACTION_DELETE_TASK("TaskTwo")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(len(pipeline), 2)

        args.pipeline_actions += [parser._ACTION_DELETE_TASK("TaskOne"),
                                  parser._ACTION_DELETE_TASK("label2")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(len(pipeline), 0)

        # unknown label should raise LookupError
        args.pipeline_actions = [parser._ACTION_ADD_TASK("TaskOne"),
                                 parser._ACTION_ADD_TASK("TaskTwo"),
                                 parser._ACTION_DELETE_TASK("label2")]
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)

    def test_MoveTask(self):
        """Simple test case moving tasks
        """
        # make short pipeline
        actions = [parser._ACTION_ADD_TASK("TaskOne"),
                   parser._ACTION_ADD_TASK("TaskTwo"),
                   parser._ACTION_ADD_TASK("TaskOne:label"),
                   parser._ACTION_ADD_TASK("TaskTwo:label2")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertEqual([t.label for t in pipeline],
                         ["TaskOne", "TaskTwo", "label", "label2"])

        args.pipeline_actions += [parser._ACTION_MOVE_TASK("label:1")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual([t.label for t in pipeline],
                         ["TaskOne", "label", "TaskTwo", "label2"])

        # negatives are accepted but may be non-intuitive
        args.pipeline_actions += [parser._ACTION_MOVE_TASK("TaskOne:-1")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual([t.label for t in pipeline],
                         ["label", "TaskTwo", "TaskOne", "label2"])

        # position larger than list size moves to end
        args.pipeline_actions += [parser._ACTION_MOVE_TASK("label:100")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual([t.label for t in pipeline],
                         ["TaskTwo", "TaskOne", "label2", "label"])

        args.pipeline_actions += [parser._ACTION_MOVE_TASK("label:0")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual([t.label for t in pipeline],
                         ["label", "TaskTwo", "TaskOne", "label2"])

        # unknown label should raise LookupError
        args.pipeline_actions += [parser._ACTION_MOVE_TASK("unlabel:0")]
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)

    def test_LabelTask(self):
        """Simple test case re-labeling tasks
        """
        # make short pipeline
        actions = [parser._ACTION_ADD_TASK("TaskOne"),
                   parser._ACTION_ADD_TASK("TaskTwo"),
                   parser._ACTION_ADD_TASK("TaskOne:label"),
                   parser._ACTION_ADD_TASK("TaskTwo:label2")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertEqual([t.label for t in pipeline],
                         ["TaskOne", "TaskTwo", "label", "label2"])

        args.pipeline_actions += [parser._ACTION_LABEL_TASK("TaskOne:label0")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual([t.label for t in pipeline],
                         ["label0", "TaskTwo", "label", "label2"])

        # unknown label should raise LookupError
        args.pipeline_actions += [parser._ACTION_LABEL_TASK("unlabel:label3")]
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)

        # duplicate label should raise LookupError
        args.pipeline_actions += [parser._ACTION_LABEL_TASK("label2:label0")]
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)

    def test_ConfigTask(self):
        """Simple test case for config overrides
        """
        # make simple pipeline
        actions = [parser._ACTION_ADD_TASK("TaskOne:task")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertIsInstance(pipeline[0].config, SimpleConfig)
        self.assertIsNone(pipeline[0].config.field)

        args.pipeline_actions += [parser._ACTION_CONFIG("task.field=value")]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(pipeline[0].config.field, "value")

        # unknown label should raise LookupError
        args.pipeline_actions += [parser._ACTION_CONFIG("label.field=value")]
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)

    def test_ConfigFileTask(self):
        """Simple test case for config overrides in file
        """
        # make simple pipeline
        actions = [parser._ACTION_ADD_TASK("TaskOne:task"),
                   parser._ACTION_CONFIG("task.field=value")]
        args = Namespace(camera_overrides=False,
                         pipeline=None,
                         pipeline_actions=actions,
                         order_pipeline=False)
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(pipeline[0].config.field, "value")

        # save config to file for next test
        overrides = NamedTemporaryFile(mode="wt", delete=False)
        pipeline[0].config.saveToStream(overrides)
        fname = overrides.name
        del overrides

        args.pipeline_actions = [parser._ACTION_ADD_TASK("TaskOne:task"),
                                 parser._ACTION_CONFIG_FILE("task:" + fname)]
        pipeline = self.builder.makePipeline(args)
        self.assertEqual(pipeline[0].config.field, "value")

        os.unlink(fname)

        # unknown label should raise LookupError
        args.pipeline_actions = [parser._ACTION_ADD_TASK("TaskOne:task"),
                                 parser._ACTION_CONFIG_FILE("label:/dev/null")]
        with self.assertRaises(LookupError):
            self.builder.makePipeline(args)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    actions = []
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
