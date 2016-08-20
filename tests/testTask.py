#!/usr/bin/env python
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
import time
import unittest
import numbers

from past.builtins import basestring

import lsst.utils.tests
import lsst.daf.base as dafBase
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase


class AddConfig(pexConfig.Config):
    addend = pexConfig.Field(doc="amount to add", dtype=float, default=3.1)


class AddTask(pipeBase.Task):
    ConfigClass = AddConfig

    @pipeBase.timeMethod
    def run(self, val):
        self.metadata.add("add", self.config.addend)
        return pipeBase.Struct(
            val=val + self.config.addend,
        )


class MultConfig(pexConfig.Config):
    multiplicand = pexConfig.Field(doc="amount by which to multiply", dtype=float, default=2.5)


class MultTask(pipeBase.Task):
    ConfigClass = MultConfig

    @pipeBase.timeMethod
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

    """First add, then multiply"""

    def __init__(self, **keyArgs):
        pipeBase.Task.__init__(self, **keyArgs)
        self.makeSubtask("add")
        self.makeSubtask("mult")

    @pipeBase.timeMethod
    def run(self, val):
        with self.timer("context"):
            addRet = self.add.run(val)
            multRet = self.mult.run(addRet.val)
            self.metadata.add("addmult", multRet.val)
            return pipeBase.Struct(
                val=multRet.val,
            )

    @pipeBase.timeMethod
    def failDec(self):
        """A method that fails with a decorator
        """
        raise RuntimeError("failDec intentional error")

    def failCtx(self):
        """A method that fails inside a context manager
        """
        with self.timer("failCtx"):
            raise RuntimeError("failCtx intentional error")


class AddTwiceTask(AddTask):
    """Variant of AddTask that adds twice the addend"""

    def run(self, val):
        addend = self.config.addend
        return pipeBase.Struct(val=val + (2 * addend))


class TaskTestCase(unittest.TestCase):
    """A test case for Task
    """

    def setUp(self):
        self.valDict = dict()

    def tearDown(self):
        self.valDict = None

    def testBasics(self):
        """Test basic construction and use of a task
        """
        for addend in (1.1, -3.5):
            for multiplicand in (0.9, -45.0):
                config = AddMultTask.ConfigClass()
                config.add.addend = addend
                config.mult["stdMult"].multiplicand = multiplicand
                # make sure both ways of accessing the registry work and give the same result
                self.assertEqual(config.mult.active.multiplicand, multiplicand)
                addMultTask = AddMultTask(config=config)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + addend) * multiplicand)

    def testNames(self):
        """Test getName() and getFullName()
        """
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

    def testGetFullMetadata(self):
        """Test getFullMetadata()
        """
        addMultTask = AddMultTask()
        fullMetadata = addMultTask.getFullMetadata()
        self.assertIsInstance(fullMetadata.getPropertySet("addMult"), dafBase.PropertySet)
        self.assertIsInstance(fullMetadata.getPropertySet("addMult:add"), dafBase.PropertySet)
        self.assertIsInstance(fullMetadata.getPropertySet("addMult:mult"), dafBase.PropertySet)

    def testEmptyMetadata(self):
        task = AddMultTask()
        task.run(val=1.2345)
        task.emptyMetadata()
        fullMetadata = task.getFullMetadata()
        self.assertEqual(fullMetadata.getPropertySet("addMult").nameCount(), 0)
        self.assertEqual(fullMetadata.getPropertySet("addMult:add").nameCount(), 0)
        self.assertEqual(fullMetadata.getPropertySet("addMult:mult").nameCount(), 0)

    def testReplace(self):
        """Test replacing one subtask with another
        """
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
        """Test timers when the code they are timing fails
        """
        addMultTask = AddMultTask()
        try:
            addMultTask.failDec()
            self.fail("Expected RuntimeError")
        except RuntimeError:
            self.assertIsNotNone(addMultTask.metadata.get("failDecEndCpuTime", None))
        try:
            addMultTask.failCtx()
            self.fail("Expected RuntimeError")
        except RuntimeError:
            self.assertIsNotNone(addMultTask.metadata.get("failCtxEndCpuTime", None))

    def testTimeMethod(self):
        """Test that the timer is adding the right metadata
        """
        addMultTask = AddMultTask()
        addMultTask.run(val=1.1)
        # Check existence and type
        for key, keyType in (("Utc", basestring),
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
                    self.assertIn(name, addMultTask.metadata.names(),
                                  name + " is missing from task metadata")
                    self.assertIsInstance(addMultTask.metadata.get(name), keyType,
                                          "%s is not of the right type (%s vs %s)" %
                                          (name, keyType, type(addMultTask.metadata.get(name))))
        # Some basic sanity checks
        currCpuTime = time.clock()
        self.assertLessEqual(
            addMultTask.metadata.get("runStartCpuTime"),
            addMultTask.metadata.get("runEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.metadata.get("runEndCpuTime"), currCpuTime)
        self.assertLessEqual(
            addMultTask.metadata.get("contextStartCpuTime"),
            addMultTask.metadata.get("contextEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.metadata.get("contextEndCpuTime"), currCpuTime)
        self.assertLessEqual(
            addMultTask.add.metadata.get("runStartCpuTime"),
            addMultTask.metadata.get("runEndCpuTime"),
        )
        self.assertLessEqual(addMultTask.add.metadata.get("runEndCpuTime"), currCpuTime)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
