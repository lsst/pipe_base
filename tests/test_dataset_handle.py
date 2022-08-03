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

try:
    import numpy as np
    import pandas as pd
except ImportError:
    pd = None

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
"""


class SpecialThing:
    """Class known not to have associated StorageClass"""


class TestDatasetHandle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = StorageClassConfig.fromYaml(storageClasses)
        factory = StorageClassFactory()
        factory.addFromConfig(config)

    def test_dataset_handle_basic(self):
        inMemoryDataset = 42
        hdl = InMemoryDatasetHandle(inMemoryDataset)

        self.assertEqual(hdl.get(), inMemoryDataset)

    def test_dataset_handle_unknown(self):
        inMemoryDataset = SpecialThing()
        hdl = InMemoryDatasetHandle(inMemoryDataset)

        self.assertEqual(hdl.get(), inMemoryDataset)

        with self.assertRaises(ValueError):
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


@unittest.skipUnless(pd is not None, "Cannot test InMemoryDataset(DataFrame) without pandas.")
class TestDataFrameDatasetHandle(unittest.TestCase):
    def test_single_index_dataframe(self):
        data = np.zeros(5, dtype=[("index", "i4"), ("a", "f8"), ("b", "f8"), ("c", "f8"), ("ddd", "f8")])
        data["index"][:] = np.arange(5)
        data["a"] = np.random.randn(5)
        data["b"] = np.random.randn(5)
        data["c"] = np.random.randn(5)
        data["ddd"] = np.random.randn(5)
        df1 = pd.DataFrame(data)
        df1 = df1.set_index("index")
        all_columns = df1.columns.append(pd.Index(df1.index.names))

        hdl = InMemoryDatasetHandle(df1)

        # Test component
        columns = hdl.get(component="columns")
        self.assertTrue(all_columns.equals(columns))

        with self.assertRaises(KeyError):
            hdl.get(component="none")

        # Test parameters
        df2 = hdl.get()
        self.assertTrue(df1.equals(df2))
        df3 = hdl.get(parameters={"columns": ["a", "c"]})
        self.assertTrue(df1.loc[:, ["a", "c"]].equals(df3))
        df4 = hdl.get(parameters={"columns": "a"})
        self.assertTrue(df1.loc[:, ["a"]].equals(df4))
        df5 = hdl.get(parameters={"columns": ["index", "a"]})
        self.assertTrue(df1.loc[:, ["a"]].equals(df5))
        df6 = hdl.get(parameters={"columns": "ddd"})
        self.assertTrue(df1.loc[:, ["ddd"]].equals(df6))
        # Passing an unrecognized column should be a ValueError.
        with self.assertRaises(ValueError):
            hdl.get(parameters={"columns": ["e"]})

    def test_multi_index_dataframe(self):
        columns1 = pd.MultiIndex.from_tuples(
            [
                ("g", "a"),
                ("g", "b"),
                ("g", "c"),
                ("r", "a"),
                ("r", "b"),
                ("r", "c"),
            ],
            names=["filter", "column"],
        )
        df1 = pd.DataFrame(np.random.randn(5, 6), index=np.arange(5, dtype=int), columns=columns1)

        hdl = InMemoryDatasetHandle(df1)

        # Read the whole DataFrame.
        df2 = hdl.get()
        self.assertTrue(df1.equals(df2))
        # Read just the column descriptions.
        columns2 = hdl.get(component="columns")
        self.assertTrue(df1.columns.equals(columns2))

        # Read just some columns a few different ways.
        with self.assertRaises(NotImplementedError):
            hdl.get(parameters={"columns": {"filter": "g"}})
        with self.assertRaises(NotImplementedError):
            hdl.get(parameters={"columns": {"filter": ["r"], "column": "a"}})


if __name__ == "__main__":
    unittest.main()
