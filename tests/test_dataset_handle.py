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

import unittest

from lsst.daf.butler import DataCoordinate, DimensionUniverse, StorageClassConfig, StorageClassFactory
from lsst.daf.butler.tests import MetricsExample
from lsst.pipe.base import InMemoryDatasetHandle

storageClasses = """
Integer:
  pytype: int
StructuredDataTestDict:
  pytype: dict
StructuredDataTestList:
  pytype: list
  delegate: lsst.daf.butler.tests.ListDelegate
  parameters:
    - slice
  derivedComponents:
    counter: Integer
StructuredDataTest:
  # Data from a simple Python class
  pytype: lsst.daf.butler.tests.MetricsExample
  delegate: lsst.daf.butler.tests.MetricsDelegate
  # Use YAML formatter by default
  components:
    # Components are those supported by get.
    summary: StructuredDataTestDict
    output: StructuredDataTestDict
    data: StructuredDataTestList
  parameters:
    - slice
  derivedComponents:
    counter: Integer
MetricsConversion:
  # Special storage class to test conversions.
  pytype: lsst.daf.butler.tests.MetricsExampleModel
  delegate: lsst.daf.butler.tests.MetricsDelegate
  converters:
    lsst.daf.butler.tests.MetricsExample: lsst.daf.butler.tests.MetricsExampleModel.from_metrics
StructuredDataTestListSet:
  pytype: set
  converters:
    list: builtins.set
"""


class SpecialThing:
    """Class known not to have associated StorageClass"""


class TestDatasetHandle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = StorageClassConfig.fromYaml(storageClasses)
        cls.factory = StorageClassFactory()
        cls.factory.addFromConfig(config)

    def test_dataset_handle_basic(self):
        inMemoryDataset = 42
        hdl = InMemoryDatasetHandle(inMemoryDataset)

        self.assertEqual(hdl.get(), inMemoryDataset)

    def test_dataset_handle_unknown(self):
        inMemoryDataset = SpecialThing()
        hdl = InMemoryDatasetHandle(inMemoryDataset)

        self.assertEqual(hdl.get(), inMemoryDataset)

        with self.assertRaises(KeyError):
            # Will not be able to find a matching StorageClass.
            hdl.get(parameters={"key": "value"})

    def test_dataset_handle_none(self):
        hdl = InMemoryDatasetHandle(None)
        self.assertIsNone(hdl.get())
        self.assertIsNone(hdl.get(component="comp"))
        self.assertIsNone(hdl.get(parameters={"something": 42}))

    def test_dataset_handle_dataid(self):
        hdl = InMemoryDatasetHandle(42)
        self.assertEqual(dict(hdl.dataId), {})

        dataId = DataCoordinate.makeEmpty(DimensionUniverse())
        hdl = InMemoryDatasetHandle(42, dataId=dataId)
        self.assertIs(hdl.dataId, dataId)

    def test_dataset_handle_metric(self):
        metric = MetricsExample(summary={"a": 1, "b": 2}, output={"c": {"d": 5}}, data=[1, 2, 3, 4])

        # First with explicit storage class.
        hdl = InMemoryDatasetHandle(metric, storageClass="StructuredDataTest")
        retrieved = hdl.get()
        self.assertEqual(retrieved, metric)

        data = hdl.get(component="data")
        self.assertEqual(data, metric.data)

        # Now with implicit storage class.
        hdl = InMemoryDatasetHandle(metric)
        data = hdl.get(component="data")
        self.assertEqual(data, metric.data)

        # Parameters.
        data = hdl.get(parameters={"slice": slice(2)})
        self.assertEqual(data.summary, metric.summary)
        self.assertEqual(data.data, [1, 2])

        data = hdl.get(parameters={"slice": slice(2)}, component="data")
        self.assertEqual(data, [1, 2])

        # Use parameters in constructor and also override.
        hdl = InMemoryDatasetHandle(metric, storageClass="StructuredDataTest", parameters={"slice": slice(3)})
        self.assertEqual(hdl.get(component="data"), [1, 2, 3])
        self.assertEqual(hdl.get(component="counter"), 3)
        self.assertEqual(hdl.get(component="data", parameters={"slice": slice(1, 3)}), [2, 3])
        self.assertEqual(hdl.get(component="counter", parameters={"slice": slice(1, 3)}), 2)

        # Ensure the original has not been modified.
        self.assertEqual(len(metric.data), 4)

    def test_handle_conversion(self):
        metric = MetricsExample(summary={"a": 1, "b": 2}, output={"c": {"d": 5}}, data=[1, 2, 3, 4])

        # Test conversion with no components or parameters.
        hdl = InMemoryDatasetHandle(metric)
        retrieved = hdl.get()  # Reset the reference.
        converted = hdl.get(storageClass="MetricsConversion")
        self.assertIsNot(type(converted), type(retrieved))
        self.assertEqual(retrieved, converted)

        # Again with a full storage class.
        sc = self.factory.getStorageClass("MetricsConversion")
        converted2 = hdl.get(storageClass=sc)
        self.assertEqual(converted2, converted)

        # Conversion of component.
        data = hdl.get(component="data", storageClass="StructuredDataTestListSet")
        self.assertIsInstance(data, set)
        self.assertEqual(data, set(converted.data))


if __name__ == "__main__":
    unittest.main()
