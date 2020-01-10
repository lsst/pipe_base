# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Unit tests for `lsst.pipe.base.tests`, a library for testing
PipelineTask subclasses.
"""

import shutil
import tempfile
import unittest

import lsst.utils.tests
import lsst.daf.butler

from lsst.pipe.base.tests import makeTestButler, makeDatasetType


class ButlerUtilsTestSuite(lsst.utils.tests.TestCase):
    @classmethod
    def setUpClass(cls):
        # Butler or collection should be re-created for each test case, but
        # this has a prohibitive run-time cost at present
        cls.root = tempfile.mkdtemp()

        dataIds = {
            "instrument": ["notACam", "dummyCam"],
            "physical_filter": ["k2020", "l2019"],
            "visit": [101, 102],
            "detector": [5]
        }
        cls.butler = makeTestButler(cls.root, dataIds)

        makeDatasetType(cls.butler, "DataType1", {"instrument"}, "NumpyArray")
        makeDatasetType(cls.butler, "DataType2", {"instrument", "visit", "detector"}, "NumpyArray")

    @classmethod
    def tearDownClass(cls):
        # TODO: use addClassCleanup rather than tearDownClass in Python 3.8
        #    to keep the addition and removal together
        shutil.rmtree(cls.root, ignore_errors=True)

    def testButlerValid(self):
        self.butler.validateConfiguration()

    def _checkButlerDimension(self, dimensions, query, expected):
        result = [id for id in self.butler.registry.queryDimensions(
            dimensions,
            where=query,
            expand=False)]
        self.assertEqual(len(result), 1)
        self.assertIn(dict(result[0]), expected)

    def testButlerDimensions(self):
        self. _checkButlerDimension({"instrument"},
                                    "instrument='notACam'",
                                    [{"instrument": "notACam"}, {"instrument": "dummyCam"}])
        self. _checkButlerDimension({"visit", "instrument"},
                                    "visit=101",
                                    [{"instrument": "notACam", "visit": 101},
                                     {"instrument": "dummyCam", "visit": 101}])
        self. _checkButlerDimension({"visit", "instrument"},
                                    "visit=102",
                                    [{"instrument": "notACam", "visit": 102},
                                     {"instrument": "dummyCam", "visit": 102}])
        self. _checkButlerDimension({"detector", "instrument"},
                                    "detector=5",
                                    [{"instrument": "notACam", "detector": 5},
                                     {"instrument": "dummyCam", "detector": 5}])

    def testDatasets(self):
        self.assertEqual(len(self.butler.registry.getAllDatasetTypes()), 2)

        # Testing the DatasetType objects is not practical, because all tests need a DimensionUniverse
        # So just check that we have the dataset types we expect
        self.butler.registry.getDatasetType("DataType1")
        self.butler.registry.getDatasetType("DataType2")

        with self.assertRaises(ValueError):
            makeDatasetType(self.butler, "DataType3", {"4thDimension"}, "NumpyArray")
        with self.assertRaises(ValueError):
            makeDatasetType(self.butler, "DataType3", {"instrument"}, "UnstorableType")


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
