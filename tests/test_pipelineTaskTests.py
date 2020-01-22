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

import collections.abc
import shutil
import tempfile
import unittest

import numpy as np

import lsst.utils.tests
import lsst.pex.config
import lsst.daf.butler

from lsst.pipe.base import Struct, PipelineTask, PipelineTaskConfig, PipelineTaskConnections, connectionTypes
from lsst.pipe.base.tests import (makeTestRepo, makeUniqueButler, makeDatasetType, expandUniqueId, runQuantum,
                                  makeQuantum, validateOutputConnections)


class ButlerUtilsTestSuite(lsst.utils.tests.TestCase):
    @classmethod
    def setUpClass(cls):
        # Repository should be re-created for each test case, but
        # this has a prohibitive run-time cost at present
        cls.root = tempfile.mkdtemp()

        dataIds = {
            "instrument": ["notACam", "dummyCam"],
            "physical_filter": ["k2020", "l2019"],
            "visit": [101, 102],
            "detector": [5]
        }
        butler = makeTestRepo(cls.root, dataIds)

        makeDatasetType(butler, "DataType1", {"instrument"}, "NumpyArray")
        makeDatasetType(butler, "DataType2", {"instrument", "visit", "detector"}, "NumpyArray")

    @classmethod
    def tearDownClass(cls):
        # TODO: use addClassCleanup rather than tearDownClass in Python 3.8
        #    to keep the addition and removal together
        shutil.rmtree(cls.root, ignore_errors=True)

    def setUp(self):
        self.butler = makeUniqueButler(self.root)

    def testButlerValid(self):
        self.butler.validateConfiguration()

    def testButlerUninitialized(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(OSError):
                makeUniqueButler(temp)

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

    def testUniqueButler(self):
        dataId = {"instrument": "notACam"}
        self.butler.put(np.array([1, 2, 3]), "DataType1", dataId)
        self.assertTrue(self.butler.datasetExists("DataType1", dataId))

        newButler = makeUniqueButler(self.root)
        with self.assertRaises(LookupError):
            newButler.datasetExists("DataType1", dataId)

    def testExpandUniqueId(self):
        self.assertEqual(dict(expandUniqueId(self.butler, {"instrument": "notACam"})),
                         {"instrument": "notACam"})
        self.assertIn(dict(expandUniqueId(self.butler, {"visit": 101})),
                      [{"instrument": "notACam", "visit": 101},
                       {"instrument": "dummyCam", "visit": 101}])
        self.assertIn(dict(expandUniqueId(self.butler, {"detector": 5})),
                      [{"instrument": "notACam", "detector": 5},
                       {"instrument": "dummyCam", "detector": 5}])
        self.assertIn(dict(expandUniqueId(self.butler, {"physical_filter": "k2020"})),
                      [{"instrument": "notACam", "physical_filter": "k2020"},
                       {"instrument": "notACam", "physical_filter": "k2020"}])
        with self.assertRaises(ValueError):
            expandUniqueId(self.butler, {"tract": 42})


class VisitConnections(PipelineTaskConnections, dimensions={"instrument", "visit"}):
    a = connectionTypes.Input(
        name="VisitA",
        storageClass="NumpyArray",
        multiple=False,
        dimensions={"instrument", "visit"},
    )
    b = connectionTypes.Input(
        name="VisitB",
        storageClass="NumpyArray",
        multiple=False,
        dimensions={"instrument", "visit"},
    )
    outA = connectionTypes.Output(
        name="VisitOutA",
        storageClass="NumpyArray",
        multiple=False,
        dimensions={"instrument", "visit"},
    )
    outB = connectionTypes.Output(
        name="VisitOutB",
        storageClass="NumpyArray",
        multiple=False,
        dimensions={"instrument", "visit"},
    )


class PatchConnections(PipelineTaskConnections, dimensions={"skymap", "tract"}):
    a = connectionTypes.Input(
        name="PatchA",
        storageClass="NumpyArray",
        multiple=True,
        dimensions={"skymap", "tract", "patch"},
    )
    b = connectionTypes.Input(
        name="PatchB",
        storageClass="NumpyArray",
        multiple=False,
        dimensions={"skymap", "tract"},
    )
    out = connectionTypes.Output(
        name="PatchOut",
        storageClass="NumpyArray",
        multiple=True,
        dimensions={"skymap", "tract", "patch"},
    )

    def __init__(self, *, config=None):
        super().__init__(config=config)

        if not config.doUseB:
            self.inputs.remove("b")


class VisitConfig(PipelineTaskConfig, pipelineConnections=VisitConnections):
    pass


class PatchConfig(PipelineTaskConfig, pipelineConnections=PatchConnections):
    doUseB = lsst.pex.config.Field(default=True, dtype=bool, doc="")


class VisitTask(PipelineTask):
    ConfigClass = VisitConfig
    _DefaultName = "visit"

    def run(self, a, b):
        outA = a + b
        outB = a - b
        return Struct(outA=outA, outB=outB)


class PatchTask(PipelineTask):
    ConfigClass = PatchConfig
    _DefaultName = "patch"

    def run(self, a, b=None):
        if self.config.doUseB:
            out = [oneA + b for oneA in a]
        else:
            out = a
        return Struct(out=out)


class PipelineTaskTestSuite(lsst.utils.tests.TestCase):
    @classmethod
    def setUpClass(cls):
        # Butler or collection should be re-created for each test case, but
        # this has a prohibitive run-time cost at present
        cls.root = tempfile.mkdtemp()

        dataIds = {
            "instrument": ["notACam"],
            "physical_filter": ["k2020"],  # needed for expandUniqueId(visit)
            "visit": [101, 102],
            "skymap": ["sky"],
            "tract": [42],
            "patch": [0, 1],
        }
        butler = makeTestRepo(cls.root, dataIds)

        for typeName in {"VisitA", "VisitB", "VisitOutA", "VisitOutB"}:
            makeDatasetType(butler, typeName, {"instrument", "visit"}, "NumpyArray")
        for typeName in {"PatchA", "PatchOut"}:
            makeDatasetType(butler, typeName, {"skymap", "tract", "patch"}, "NumpyArray")
        makeDatasetType(butler, "PatchB", {"skymap", "tract"}, "NumpyArray")

    def setUp(self):
        self.butler = makeUniqueButler(self.root)

    def testMakeQuantumNoSuchDatatype(self):
        config = VisitConfig()
        config.connections.a = "Visit"
        task = VisitTask(config=config)
        dataId = expandUniqueId(self.butler, {"visit": 102})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        self.butler.put(inA, "VisitA", dataId)
        self.butler.put(inB, "VisitB", dataId)

        with self.assertRaises(ValueError):
            makeQuantum(task, self.butler, {key: dataId for key in {"a", "b", "outA", "outB"}})

    def testMakeQuantumInvalidDimension(self):
        config = VisitConfig()
        config.connections.a = "PatchA"
        task = VisitTask(config=config)
        dataIdV = expandUniqueId(self.butler, {"visit": 102})
        dataIdP = expandUniqueId(self.butler, {"patch": 0})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        self.butler.put(inA, "VisitA", dataIdV)
        self.butler.put(inA, "PatchA", dataIdP)
        self.butler.put(inB, "VisitB", dataIdV)

        with self.assertRaises(ValueError):
            makeQuantum(task, self.butler, {
                "a": dataIdP,
                "b": dataIdV,
                "outA": dataIdV,
                "outB": dataIdV,
            })

    def testMakeQuantumMissingMultiple(self):
        task = PatchTask()
        dataId = expandUniqueId(self.butler, {"tract": 42})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        for patch in {0, 1}:
            self.butler.put(inA + patch, "PatchA", dataId, patch=patch)
        self.butler.put(inB, "PatchB", dataId)

        with self.assertRaises(ValueError):
            makeQuantum(task, self.butler, {
                "a": dict(dataId, patch=0),
                "b": dataId,
                "out": [dict(dataId, patch=patch) for patch in {0, 1}],
            })

    def testMakeQuantumExtraMultiple(self):
        task = PatchTask()
        dataId = expandUniqueId(self.butler, {"tract": 42})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        for patch in {0, 1}:
            self.butler.put(inA + patch, "PatchA", dataId, patch=patch)
        self.butler.put(inB, "PatchB", dataId)

        with self.assertRaises(ValueError):
            makeQuantum(task, self.butler, {
                "a": [dict(dataId, patch=patch) for patch in {0, 1}],
                "b": [dataId],
                "out": [dict(dataId, patch=patch) for patch in {0, 1}],
            })

    def testMakeQuantumMissingDataId(self):
        task = VisitTask()
        dataId = expandUniqueId(self.butler, {"visit": 102})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        self.butler.put(inA, "VisitA", dataId)
        self.butler.put(inB, "VisitB", dataId)

        with self.assertRaises(ValueError):
            makeQuantum(task, self.butler, {key: dataId for key in {"a", "outA", "outB"}})
        with self.assertRaises(ValueError):
            makeQuantum(task, self.butler, {key: dataId for key in {"a", "b", "outB"}})

    def testMakeQuantumCorruptedDataId(self):
        task = VisitTask()
        dataId = expandUniqueId(self.butler, {"visit": 102})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        self.butler.put(inA, "VisitA", dataId)
        self.butler.put(inB, "VisitB", dataId)

        with self.assertRaises(ValueError):
            # third argument should be a mapping keyed by component name
            makeQuantum(task, self.butler, dataId)

    def testRunQuantumVisitWithRun(self):
        task = VisitTask()
        dataId = expandUniqueId(self.butler, {"visit": 102})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        self.butler.put(inA, "VisitA", dataId)
        self.butler.put(inB, "VisitB", dataId)

        quantum = makeQuantum(task, self.butler, {key: dataId for key in {"a", "b", "outA", "outB"}})
        runQuantum(task, self.butler, quantum, mockRun=False)

        # Can we use runQuantum to verify that task.run got called with correct inputs/outputs?
        self.assertTrue(self.butler.datasetExists("VisitOutA", dataId))
        self.assertFloatsAlmostEqual(self.butler.get("VisitOutA", dataId), inA + inB)
        self.assertTrue(self.butler.datasetExists("VisitOutB", dataId))
        self.assertFloatsAlmostEqual(self.butler.get("VisitOutB", dataId), inA - inB)

    def testRunQuantumPatchWithRun(self):
        task = PatchTask()
        dataId = expandUniqueId(self.butler, {"tract": 42})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        for patch in {0, 1}:
            self.butler.put(inA + patch, "PatchA", dataId, patch=patch)
        self.butler.put(inB, "PatchB", dataId)

        quantum = makeQuantum(task, self.butler, {
            "a": [dict(dataId, patch=patch) for patch in {0, 1}],
            "b": dataId,
            "out": [dict(dataId, patch=patch) for patch in {0, 1}],
        })
        runQuantum(task, self.butler, quantum, mockRun=False)

        # Can we use runQuantum to verify that task.run got called with correct inputs/outputs?
        for patch in {0, 1}:
            self.assertTrue(self.butler.datasetExists("PatchOut", dataId, patch=patch))
            self.assertFloatsAlmostEqual(self.butler.get("PatchOut", dataId, patch=patch),
                                         inA + inB + patch)

    def testRunQuantumVisitMockRun(self):
        task = VisitTask()
        dataId = expandUniqueId(self.butler, {"visit": 102})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        self.butler.put(inA, "VisitA", dataId)
        self.butler.put(inB, "VisitB", dataId)

        quantum = makeQuantum(task, self.butler, {key: dataId for key in {"a", "b", "outA", "outB"}})
        run = runQuantum(task, self.butler, quantum, mockRun=True)

        # Can we use the mock to verify that task.run got called with the correct inputs?
        # Can't use assert_called_once_with because of how Numpy handles ==
        run.assert_called_once()
        self.assertFalse(run.call_args[0])
        kwargs = run.call_args[1]
        self.assertEqual(kwargs.keys(), {"a", "b"})
        np.testing.assert_array_equal(kwargs["a"], inA)
        np.testing.assert_array_equal(kwargs["b"], inB)

    def testRunQuantumPatchMockRun(self):
        task = PatchTask()
        dataId = expandUniqueId(self.butler, {"tract": 42})

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        for patch in {0, 1}:
            self.butler.put(inA + patch, "PatchA", dataId, patch=patch)
        self.butler.put(inB, "PatchB", dataId)

        quantum = makeQuantum(task, self.butler, {
            # Use lists, not sets, to ensure order agrees with test assertion
            "a": [dict(dataId, patch=patch) for patch in [0, 1]],
            "b": dataId,
            "out": [dict(dataId, patch=patch) for patch in [0, 1]],
        })
        run = runQuantum(task, self.butler, quantum, mockRun=True)

        # Can we use the mock to verify that task.run got called with the correct inputs?
        # Can't use assert_called_once_with because of how Numpy handles ==
        run.assert_called_once()
        self.assertFalse(run.call_args[0])
        kwargs = run.call_args[1]
        self.assertEqual(kwargs.keys(), {"a", "b"})
        self.assertIsInstance(kwargs["a"], collections.abc.Sequence)
        for actual, expected in zip(kwargs["a"], (inA + patch for patch in [0, 1])):
            np.testing.assert_array_almost_equal(actual, expected)
        np.testing.assert_array_equal(kwargs["b"], inB)

    def testRunQuantumPatchOptionalInput(self):
        config = PatchConfig()
        config.doUseB = False
        task = PatchTask(config=config)
        dataId = expandUniqueId(self.butler, {"tract": 42})

        inA = np.array([1, 2, 3])
        for patch in {0, 1}:
            self.butler.put(inA + patch, "PatchA", dataId, patch=patch)

        quantum = makeQuantum(task, self.butler, {
            "a": [dict(dataId, patch=patch) for patch in {0, 1}],
            "out": [dict(dataId, patch=patch) for patch in {0, 1}],
        })
        run = runQuantum(task, self.butler, quantum, mockRun=True)

        # Can we use the mock to verify that task.run got called with the correct inputs?
        # Can't use assert_called_once_with because of how Numpy handles ==
        run.assert_called_once()
        self.assertFalse(run.call_args[0])
        kwargs = run.call_args[1]
        self.assertEqual(kwargs.keys(), {"a"})

    def testValidateOutputConnectionsPass(self):
        task = VisitTask()

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        result = task.run(inA, inB)

        self.assertTrue(validateOutputConnections(task, result))

    def testValidateOutputConnectionsMissing(self):
        task = VisitTask()

        def run(a, b):
            return Struct(a=a)
        task.run = run

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        result = task.run(inA, inB)

        self.assertFalse(validateOutputConnections(task, result))

    def testValidateOutputConnectionsSingle(self):
        task = PatchTask()

        def run(a, b):
            return Struct(out=b)
        task.run = run

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        result = task.run([inA], inB)

        self.assertFalse(validateOutputConnections(task, result))

    def testValidateOutputConnectionsMultiple(self):
        task = VisitTask()

        def run(a, b):
            return Struct(a=[a])
        task.run = run

        inA = np.array([1, 2, 3])
        inB = np.array([4, 0, 1])
        result = task.run(inA, inB)

        self.assertFalse(validateOutputConnections(task, result))


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
