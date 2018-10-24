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

import lsst.utils.tests
import lsst.pex.config as pexConfig
from lsst.daf.butler import StorageClass, StorageClassFactory
import lsst.pipe.base as pipeBase


class NoResourceTask(pipeBase.PipelineTask):
    _DefaultName = "no_resource_task"
    ConfigClass = pexConfig.Config


class OneConfig(pexConfig.Config):
    resources = pexConfig.ConfigField(dtype=pipeBase.ResourceConfig,
                                      doc="Resource configuration")


class OneTask(pipeBase.PipelineTask):
    _DefaultName = "one_task"
    ConfigClass = OneConfig


class TwoConfig(pexConfig.Config):
    resources = pexConfig.ConfigField(dtype=pipeBase.ResourceConfig,
                                      doc="Resource configuration")

    def setDefaults(self):
        self.resources.minMemoryMB = 1024
        self.resources.minNumCores = 32


class TwoTask(pipeBase.PipelineTask):
    _DefaultName = "two_task"
    ConfigClass = TwoConfig


class ConfigWithDatasets(pexConfig.Config):
    input1 = pipeBase.InputDatasetField(name="in1",
                                        units=["UnitA"],
                                        storageClass="SCA",
                                        doc="")
    input2 = pipeBase.InputDatasetField(name="in2",
                                        units=["UnitA", "UnitB"],
                                        storageClass="SCB",
                                        scalar=True,
                                        doc="")
    output = pipeBase.OutputDatasetField(name="out",
                                         units=["UnitB", "UnitC"],
                                         storageClass="SCC",
                                         scalar=False,
                                         doc="")
    initInput = pipeBase.InitInputDatasetField(name="init_input",
                                               storageClass="SCX",
                                               doc="")
    initOutput = pipeBase.InitOutputDatasetField(name="init_output",
                                                 storageClass="SCY",
                                                 doc="")


class TaskTestCase(unittest.TestCase):
    """A test case for Task
    """

    @classmethod
    def setUpClass(cls):
        for name in ("SCA", "SCB", "SCC", "SCX", "SCY"):
            StorageClassFactory().registerStorageClass(StorageClass(name))

    def testNoResource(self):
        """Test for a task without resource config
        """
        task = NoResourceTask()
        res_config = task.getResourceConfig()
        self.assertIs(res_config, None)

    def testOneResource(self):
        """Test for a task with resource config
        """
        task = OneTask()
        res_config = task.getResourceConfig()
        self.assertIsNot(res_config, None)
        self.assertIs(res_config.minMemoryMB, None)
        self.assertEqual(res_config.minNumCores, 1)

    def testTwoResource(self):
        """Test for a task with resource config and special defaults
        """
        task = TwoTask()
        res_config = task.getResourceConfig()
        self.assertIsNot(res_config, None)
        self.assertEqual(res_config.minMemoryMB, 1024)
        self.assertEqual(res_config.minNumCores, 32)

    def testEmptyDatasetConfig(self):
        """Test for a config without datasets
        """
        config = pexConfig.Config()
        self.assertEqual(pipeBase.PipelineTask.getInputDatasetTypes(config), {})
        self.assertEqual(pipeBase.PipelineTask.getOutputDatasetTypes(config), {})
        self.assertEqual(pipeBase.PipelineTask.getInitInputDatasetTypes(config), {})
        self.assertEqual(pipeBase.PipelineTask.getInitOutputDatasetTypes(config), {})

    def testDatasetConfig(self):
        """Test for a config with datasets
        """
        config = ConfigWithDatasets()

        descriptors = pipeBase.PipelineTask.getInputDatasetTypes(config)
        self.assertCountEqual(descriptors.keys(), ["input1", "input2"])
        descriptor = descriptors["input1"]
        self.assertEqual(descriptor.datasetType.name, config.input1.name)
        self.assertCountEqual(descriptor.datasetType.dataUnits, config.input1.units)
        self.assertEqual(descriptor.datasetType.storageClass.name, config.input1.storageClass)
        self.assertFalse(descriptor.scalar)
        descriptor = descriptors["input2"]
        self.assertEqual(descriptor.datasetType.name, config.input2.name)
        self.assertCountEqual(descriptor.datasetType.dataUnits, config.input2.units)
        self.assertEqual(descriptor.datasetType.storageClass.name, config.input2.storageClass)
        self.assertTrue(descriptor.scalar)

        descriptors = pipeBase.PipelineTask.getOutputDatasetTypes(config)
        self.assertCountEqual(descriptors.keys(), ["output"])
        descriptor = descriptors["output"]
        self.assertEqual(descriptor.datasetType.name, config.output.name)
        self.assertCountEqual(descriptor.datasetType.dataUnits, config.output.units)
        self.assertEqual(descriptor.datasetType.storageClass.name, config.output.storageClass)
        self.assertFalse(descriptor.scalar)

        descriptors = pipeBase.PipelineTask.getInitInputDatasetTypes(config)
        self.assertCountEqual(descriptors.keys(), ["initInput"])
        descriptor = descriptors["initInput"]
        self.assertEqual(descriptor.datasetType.name, config.initInput.name)
        self.assertEqual(len(descriptor.datasetType.dataUnits), 0)
        self.assertEqual(descriptor.datasetType.storageClass.name, config.initInput.storageClass)
        self.assertTrue(descriptor.scalar)

        descriptors = pipeBase.PipelineTask.getInitOutputDatasetTypes(config)
        self.assertCountEqual(descriptors.keys(), ["initOutput"])
        descriptor = descriptors["initOutput"]
        self.assertEqual(descriptor.datasetType.name, config.initOutput.name)
        self.assertEqual(len(descriptor.datasetType.dataUnits), 0)
        self.assertEqual(descriptor.datasetType.storageClass.name, config.initOutput.storageClass)
        self.assertTrue(descriptor.scalar)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
