#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
import json
import logging
import numbers
import time
import unittest

import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase
import lsst.utils.tests
import yaml

# Whilst in transition the test can't tell which type is
# going to be used for metadata.
from lsst.pipe.base.task import _TASK_METADATA_TYPE
from lsst.utils.timer import timeMethod


class AddConfig(pexConfig.Config):
    addend = pexConfig.Field(doc="amount to add", dtype=float, default=3.1)


class AddTask(pipeBase.Task):
    ConfigClass = AddConfig

    @timeMethod
    def run(self, val):
        self.metadata.add("add", self.config.addend)
        return pipeBase.Struct(
            val=val + self.config.addend,
        )


class MultConfig(pexConfig.Config):
    multiplicand = pexConfig.Field(doc="amount by which to multiply", dtype=float, default=2.5)


class MultTask(pipeBase.Task):
    ConfigClass = MultConfig

    @timeMethod
    def run(self, val):
        self.metadata.add("mult", self.config.multiplicand)
        return pipeBase.Struct(
            val=val * self.config.multiplicand,
        )


# prove that registry fields can also be used to hold subtasks
# by using a registry to hold MultTask
multRegistry = pexConfig.makeRegistry("Registry for Mult-like tasks")
multRegistry.register("stdMult", MultTask)


class AddMultConfig(pexConfig.Config):
    add = AddTask.makeField("add task")
    mult = multRegistry.makeField("mult task", default="stdMult")


class AddMultTask(pipeBase.Task):
    ConfigClass = AddMultConfig
    _DefaultName = "addMult"
    _add_module_logger_prefix = False

    """First add, then multiply"""

    def __init__(self, **keyArgs):
        pipeBase.Task.__init__(self, **keyArgs)
        self.makeSubtask("add")
        self.makeSubtask("mult")

    @timeMethod
    def run(self, val):
        with self.timer("context"):
            addRet = self.add.run(val)
            multRet = self.mult.run(addRet.val)
            self.metadata.add("addmult", multRet.val)
            return pipeBase.Struct(
                val=multRet.val,
            )

    @timeMethod
    def failDec(self):
        """A method that fails with a decorator"""
        raise RuntimeError("failDec intentional error")

    def failCtx(self):
        """A method that fails inside a context manager"""
        with self.timer("failCtx"):
            raise RuntimeError("failCtx intentional error")


class AddMultTask2(AddMultTask):
    """Subclass that gets an automatic logger prefix."""

    _add_module_logger_prefix = True


class AddTwiceTask(AddTask):
    """Variant of AddTask that adds twice the addend"""

    def run(self, val):
        addend = self.config.addend
        return pipeBase.Struct(val=val + (2 * addend))


class TaskTestCase(unittest.TestCase):
    """A test case for Task"""

    def setUp(self):
        self.valDict = dict()

    def tearDown(self):
        self.valDict = None

    def testBasics(self):
        """Test basic construction and use of a task"""
        for addend in (1.1, -3.5):
            for multiplicand in (0.9, -45.0):
                config = AddMultTask.ConfigClass()
                config.add.addend = addend
                config.mult["stdMult"].multiplicand = multiplicand
                # make sure both ways of accessing the registry work and give
                # the same result
                self.assertEqual(config.mult.active.multiplicand, multiplicand)
                addMultTask = AddMultTask(config=config)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + addend) * multiplicand)

    def testNames(self):
        """Test getName() and getFullName()"""
        addMultTask = AddMultTask()
        self.assertEqual(addMultTask.getName(), "addMult")
        self.assertEqual(addMultTask.add.getName(), "add")
        self.assertEqual(addMultTask.mult.getName(), "mult")

        self.assertEqual(addMultTask._name, "addMult")
        self.assertEqual(addMultTask.add._name, "add")
        self.assertEqual(addMultTask.mult._name, "mult")

        self.assertEqual(addMultTask.getFullName(), "addMult")
        self.assertEqual(addMultTask.add.getFullName(), "addMult.add")
        self.assertEqual(addMultTask.mult.getFullName(), "addMult.mult")

        self.assertEqual(addMultTask._fullName, "addMult")
        self.assertEqual(addMultTask.add._fullName, "addMult.add")
        self.assertEqual(addMultTask.mult._fullName, "addMult.mult")

    def testLog(self):
        """Test the Task's logger"""
        addMultTask = AddMultTask()
        self.assertEqual(addMultTask.log.name, "addMult")
        self.assertEqual(addMultTask.add.log.name, "addMult.add")

        log = logging.getLogger("tester")
        addMultTask = AddMultTask(log=log)
        self.assertEqual(addMultTask.log.name, "tester.addMult")
        self.assertEqual(addMultTask.add.log.name, "tester.addMult.add")

        addMultTask2 = AddMultTask2()
        self.assertEqual(addMultTask2.log.name, f"{__name__}.addMult")

    def testGetFullMetadata(self):
        """Test getFullMetadata()"""
        addMultTask = AddMultTask()
        addMultTask.run(val=1.234)  # Add some metadata
        fullMetadata = addMultTask.getFullMetadata()
        self.assertIsInstance(fullMetadata["addMult"], _TASK_METADATA_TYPE)
        self.assertIsInstance(fullMetadata["addMult:add"], _TASK_METADATA_TYPE)
        self.assertIsInstance(fullMetadata["addMult:mult"], _TASK_METADATA_TYPE)
        self.assertEqual(set(fullMetadata), {"addMult", "addMult:add", "addMult:mult"})

        all_names = fullMetadata.names(topLevelOnly=False)
        self.assertIn("addMult", all_names)
        self.assertIn("addMult.runStartUtc", all_names)

        param_names = fullMetadata.paramNames(topLevelOnly=True)
        # No top level keys without hierarchy
        self.assertEqual(set(param_names), set())

        param_names = fullMetadata.paramNames(topLevelOnly=False)
        self.assertNotIn("addMult", param_names)
        self.assertIn("addMult.runStartUtc", param_names)
        self.assertIn("addMult:add.runStartCpuTime", param_names)

    def testEmptyMetadata(self):
        task = AddMultTask()
        task.run(val=1.2345)
        task.emptyMetadata()
        fullMetadata = task.getFullMetadata()
        self.assertEqual(len(fullMetadata["addMult"]), 0)
        self.assertEqual(len(fullMetadata["addMult:add"]), 0)
        self.assertEqual(len(fullMetadata["addMult:mult"]), 0)

    def testReplace(self):
        """Test replacing one subtask with another"""
        for addend in (1.1, -3.5):
            for multiplicand in (0.9, -45.0):
                config = AddMultTask.ConfigClass()
                config.add.retarget(AddTwiceTask)
                config.add.addend = addend
                config.mult["stdMult"].multiplicand = multiplicand
                addMultTask = AddMultTask(config=config)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + (2 * addend)) * multiplicand)

    def testFail(self):
        """Test timers when the code they are timing fails"""
        addMultTask = AddMultTask()
        try:
            addMultTask.failDec()
            self.fail("Expected RuntimeError")
        except RuntimeError:
            self.assertIn("failDecEndCpuTime", addMultTask.metadata)
        try:
            addMultTask.failCtx()
            self.fail("Expected RuntimeError")
        except RuntimeError:
            self.assertIn("failCtxEndCpuTime", addMultTask.metadata)

    def testTimeMethod(self):
        """Test that the timer is adding the right metadata"""
        addMultTask = AddMultTask()

        # Run twice to ensure we are additive.
        addMultTask.run(val=1.1)
        addMultTask.run(val=2.0)
        # Check existence and type
        for key, keyType in (
            ("Utc", str),
            ("CpuTime", float),
            ("UserTime", float),
            ("SystemTime", float),
            ("MaxResidentSetSize", numbers.Integral),
            ("MinorPageFaults", numbers.Integral),
            ("MajorPageFaults", numbers.Integral),
            ("BlockInputs", numbers.Integral),
            ("BlockOutputs", numbers.Integral),
            ("VoluntaryContextSwitches", numbers.Integral),
            ("InvoluntaryContextSwitches", numbers.Integral),
        ):
            for when in ("Start", "End"):
                for method in ("run", "context"):
                    name = method + when + key
                    self.assertIn(name, addMultTask.metadata, name + " is missing from task metadata")
                    self.assertIsInstance(
                        addMultTask.metadata.getScalar(name),
                        keyType,
                        f"{name} is not of the right type "
                        f"({keyType} vs {type(addMultTask.metadata.getScalar(name))})",
                    )
        # Some basic sanity checks
        currCpuTime = time.process_time()
        self.assertLessEqual(
            addMultTask.metadata.getScalar("runStartCpuTime"),
            addMultTask.metadata.getScalar("runEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.metadata.getScalar("runEndCpuTime"), currCpuTime)
        self.assertLessEqual(
            addMultTask.metadata.getScalar("contextStartCpuTime"),
            addMultTask.metadata.getScalar("contextEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.metadata.getScalar("contextEndCpuTime"), currCpuTime)
        self.assertLessEqual(
            addMultTask.add.metadata.getScalar("runStartCpuTime"),
            addMultTask.metadata.getScalar("runEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.add.metadata.getScalar("runEndCpuTime"), currCpuTime)

        # Add some explicit values for serialization test.
        addMultTask.metadata["comment"] = "A comment"
        addMultTask.metadata["integer"] = 5
        addMultTask.metadata["float"] = 3.14
        addMultTask.metadata["bool"] = False
        addMultTask.metadata.add("commentList", "comment1")
        addMultTask.metadata.add("commentList", "comment1")
        addMultTask.metadata.add("intList", 6)
        addMultTask.metadata.add("intList", 7)
        addMultTask.metadata.add("boolList", False)
        addMultTask.metadata.add("boolList", True)
        addMultTask.metadata.add("floatList", 6.6)
        addMultTask.metadata.add("floatList", 7.8)

        # TaskMetadata can serialize to JSON but not YAML
        # and PropertySet can serialize to YAML and not JSON.
        if hasattr(addMultTask.metadata, "json"):
            j = addMultTask.metadata.json()
            new_meta = pipeBase.TaskMetadata.parse_obj(json.loads(j))
        else:
            y = yaml.dump(addMultTask.metadata)
            new_meta = yaml.safe_load(y)
        self.assertEqual(new_meta, addMultTask.metadata)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
