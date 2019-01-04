#
# LSST Data Management System
# Copyright 2017 AURA/LSST.
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

"""Simple unit test for Pipeline.
"""

import unittest
import pickle

import lsst.pex.config as pexConfig
from lsst.pipe.base import Struct, PipelineTask, PipelineTaskConfig, Pipeline, TaskDef
import lsst.utils.tests


class AddConfig(PipelineTaskConfig):
    addend = pexConfig.Field(doc="amount to add", dtype=float, default=3.1)


class AddTask(PipelineTask):
    ConfigClass = AddConfig

    def run(self, val):
        self.metadata.add("add", self.config.addend)
        return Struct(
            val=val + self.config.addend,
        )


class MultConfig(PipelineTaskConfig):
    multiplicand = pexConfig.Field(doc="amount by which to multiply", dtype=float, default=2.5)


class MultTask(PipelineTask):
    ConfigClass = MultConfig

    def run(self, val):
        self.metadata.add("mult", self.config.multiplicand)
        return Struct(
            val=val * self.config.multiplicand,
        )


class TaskTestCase(unittest.TestCase):
    """A test case for Task
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testTaskDef(self):
        """Tests for TaskDef structure
        """
        task1 = TaskDef("lsst.pipe.base.tests.Add", AddConfig())
        self.assertEqual(task1.taskName, "lsst.pipe.base.tests.Add")
        self.assertIsInstance(task1.config, AddConfig)
        self.assertIsNone(task1.taskClass)
        self.assertEqual(task1.label, "")

        task2 = TaskDef("lsst.pipe.base.tests.Mult", MultConfig(), MultTask, "mult_task")
        self.assertEqual(task2.taskName, "lsst.pipe.base.tests.Mult")
        self.assertIsInstance(task2.config, MultConfig)
        self.assertIs(task2.taskClass, MultTask)
        self.assertEqual(task2.label, "mult_task")

    def testEmpty(self):
        """Creating empty pipeline
        """
        pipeline = Pipeline()
        self.assertEqual(len(pipeline), 0)

    def testAppend(self):
        """Testing append() method
        """
        pipeline = Pipeline()
        self.assertEqual(len(pipeline), 0)
        pipeline.append(TaskDef("lsst.pipe.base.tests.Add", AddConfig()))
        pipeline.append(TaskDef("lsst.pipe.base.tests.Mult", MultConfig()))
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(pipeline[0].taskName, "lsst.pipe.base.tests.Add")
        self.assertEqual(pipeline[1].taskName, "lsst.pipe.base.tests.Mult")

    def testInitial(self):
        """Testing constructor with initial data
        """
        pipeline = Pipeline([TaskDef("lsst.pipe.base.tests.Add", AddConfig()),
                             TaskDef("lsst.pipe.base.tests.Mult", MultConfig())])
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(pipeline[0].taskName, "lsst.pipe.base.tests.Add")
        self.assertEqual(pipeline[1].taskName, "lsst.pipe.base.tests.Mult")

    def testPickle(self):
        """Test pickling/unpickling.
        """
        pipeline = Pipeline([TaskDef("lsst.pipe.base.tests.Add", AddConfig()),
                             TaskDef("lsst.pipe.base.tests.Mult", MultConfig())])
        blob = pickle.dumps(pipeline)
        pipeline = pickle.loads(blob)
        self.assertIsInstance(pipeline, Pipeline)
        self.assertEqual(len(pipeline), 2)
        self.assertEqual(pipeline[0].taskName, "lsst.pipe.base.tests.Add")
        self.assertEqual(pipeline[1].taskName, "lsst.pipe.base.tests.Mult")


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
