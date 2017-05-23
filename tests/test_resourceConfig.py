"""Simple unit test for ResourceConfig.
"""
#
# LSST Data Management System
# Copyright 2016 LSST Corporation.
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

import unittest

import lsst.utils.tests
import lsst.pex.config as pexConfig
from lsst.pipe import supertask


class NoResourceTask(supertask.SuperTask):
    _DefaultName = "no_resource_task"
    ConfigClass = pexConfig.Config


class OneConfig(supertask.ConfigWithResource):
    pass


class OneTask(supertask.SuperTask):
    _DefaultName = "one_task"
    ConfigClass = OneConfig


class TwoConfig(pexConfig.Config):
    resources = pexConfig.ConfigField(dtype=supertask.ResourceConfig, doc=supertask.ResourceConfig.__doc__)


class TwoTask(supertask.SuperTask):
    _DefaultName = "two_task"
    ConfigClass = OneConfig


class ThreeConfig(supertask.ConfigWithResource):

    def setDefaults(self):
        self.resources.min_memory_MB = 1024
        self.resources.min_num_cores = 32


class ThreeTask(supertask.SuperTask):
    _DefaultName = "three_task"
    ConfigClass = ThreeConfig


class TaskTestCase(unittest.TestCase):
    """A test case for Task
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testNoResource(self):
        """Test for a task without resource config
        """
        task = NoResourceTask()
        res_config = task.get_resource_config()
        self.assertIs(res_config, None)

    def testOneResource(self):
        """Test for a task with resource config
        """
        task = OneTask()
        res_config = task.get_resource_config()
        self.assertIsNot(res_config, None)
        self.assertIs(res_config.min_memory_MB, None)
        self.assertEqual(res_config.min_num_cores, 1)

    def testTwoResource(self):
        """Test for a task with resource config
        """
        task = TwoTask()
        res_config = task.get_resource_config()
        self.assertIsNot(res_config, None)
        self.assertIs(res_config.min_memory_MB, None)
        self.assertEqual(res_config.min_num_cores, 1)

    def testThreeResource(self):
        """Test for a task with resource config and special defaults
        """
        task = ThreeTask()
        res_config = task.get_resource_config()
        self.assertIsNot(res_config, None)
        self.assertEqual(res_config.min_memory_MB, 1024)
        self.assertEqual(res_config.min_num_cores, 32)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
