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

"""Simple unit test for ResourceConfig.
"""

import unittest

import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.utils.tests
from lsst.daf.butler import StorageClass, StorageClassFactory


class NullConnections(pipeBase.PipelineTaskConnections, dimensions=()):
    pass


class NoResourceTask(pipeBase.PipelineTask):
    _DefaultName = "no_resource_task"
    ConfigClass = pipeBase.PipelineTaskConfig


class OneConfig(pipeBase.PipelineTaskConfig, pipelineConnections=NullConnections):
    resources = pexConfig.ConfigField(dtype=pipeBase.ResourceConfig, doc="Resource configuration")


class OneTask(pipeBase.PipelineTask):
    _DefaultName = "one_task"
    ConfigClass = OneConfig


class TwoConfig(pipeBase.PipelineTaskConfig, pipelineConnections=NullConnections):
    resources = pexConfig.ConfigField(dtype=pipeBase.ResourceConfig, doc="Resource configuration")

    def setDefaults(self):
        self.resources.minMemoryMB = 1024
        self.resources.minNumCores = 32


class TwoTask(pipeBase.PipelineTask):
    _DefaultName = "two_task"
    ConfigClass = TwoConfig


class TaskTestCase(unittest.TestCase):
    """A test case for Task"""

    @classmethod
    def setUpClass(cls):
        for name in ("SCA", "SCB", "SCC", "SCX", "SCY"):
            StorageClassFactory().registerStorageClass(StorageClass(name))

    def testNoResource(self):
        """Test for a task without resource config"""
        task = NoResourceTask()
        res_config = task.getResourceConfig()
        self.assertIs(res_config, None)

    def testOneResource(self):
        """Test for a task with resource config"""
        task = OneTask()
        res_config = task.getResourceConfig()
        self.assertIsNot(res_config, None)
        self.assertIs(res_config.minMemoryMB, None)
        self.assertEqual(res_config.minNumCores, 1)

    def testTwoResource(self):
        """Test for a task with resource config and special defaults"""
        task = TwoTask()
        res_config = task.getResourceConfig()
        self.assertIsNot(res_config, None)
        self.assertEqual(res_config.minMemoryMB, 1024)
        self.assertEqual(res_config.minNumCores, 32)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
