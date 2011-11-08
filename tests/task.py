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
import unittest

import lsst.utils.tests as utilsTests
import lsst.pex.policy as pexPolicy
import lsst.pipe.base as pipeBase

class AddTask(pipeBase.Task):
    @pipeBase.timeit
    def run(self, val):
        addend = self.policy.get("addend")
        return pipeBase.Struct(val = val + addend)
    
    @staticmethod
    def getPolicy(addend=3.1):
        policy = pexPolicy.Policy()
        policy.set("addend", addend)
        return policy

class MultTask(pipeBase.Task):
    @pipeBase.timeit
    def run(self, val):
        multiplicand = self.policy.get("multiplicand")
        return pipeBase.Struct(val = val * multiplicand)
    
    @staticmethod
    def getPolicy(multiplicand=2.5):
        policy = pexPolicy.Policy()
        policy.set("multiplicand", multiplicand)
        return policy

class AddMultTask(pipeBase.Task):
    """First add, then multiply"""
    def __init__(self, **keyArgs):
        pipeBase.Task.__init__(self, **keyArgs)
        self.makeSubtask("add", AddTask)
        self.makeSubtask("mult", MultTask)

    @pipeBase.timeit
    def run(self, val):
        addRet = self.add.run(val)
        multRet = self.mult.run(addRet.val)
        return pipeBase.Struct(val = multRet.val)
    
    @staticmethod
    def getPolicy(addend=3.1, multiplicand=2.5):
        policy = pexPolicy.Policy()
        addPolicy = AddTask.getPolicy(addend=addend)
        multPolicy = MultTask.getPolicy(multiplicand=multiplicand)
        policy.set("add", addPolicy)
        policy.set("mult", multPolicy)
        return policy

class AddTwiceTask(AddTask):
    """Variant of AddTask that adds twice the addend"""
    def run(self, val):
        addend = self.policy.get("addend")
        return pipeBase.Struct(val = val + (2 * addend))


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
                policy = AddMultTask.getPolicy(addend=addend, multiplicand=multiplicand)
                addMultTask = AddMultTask(policy=policy)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + addend) * multiplicand)
    
    def testReplace(self):
        """Test replacing one subtask with another
        """
        for addend in (1.1, -3.5):
            for multiplicand in (0.9, -45.0):
                policy = AddMultTask.getPolicy(addend=addend, multiplicand=multiplicand)
                addMultTask = AddMultTask(policy=policy)
                addMultTask.makeSubtask("add", AddTwiceTask)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + (2 * addend)) * multiplicand)
    
    def testNames(self):
        """Test task names
        """
        policy = AddMultTask.getPolicy()
        addMultTask = AddMultTask(policy=policy)
        self.assertEquals(addMultTask._name, "main")
        self.assertEquals(addMultTask.add._name, "add")
        self.assertEquals(addMultTask.mult._name, "mult")
        self.assertEquals(addMultTask._fullName, "main")
        self.assertEquals(addMultTask.add._fullName, "main.add")
        self.assertEquals(addMultTask.mult._fullName, "main.mult")
    
    def testTimeIt(self):
        """Test that the timer is adding the right metadata
        """
        policy = AddMultTask.getPolicy()
        addMultTask = AddMultTask(policy=policy)
        addMultTask.run(val=1.1)
        metadata = addMultTask.getMetadata()
        self.assert_(float(metadata["main.run_time"]) < 0.1)
        self.assert_(float(metadata["main.add.run_time"]) < 0.1)
        self.assert_(float(metadata["main.mult.run_time"]) < 0.1)
 

def suite():
    """Return a suite containing all the test cases in this module.
    """
    utilsTests.init()

    suites = []

    suites += unittest.makeSuite(TaskTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)

    return unittest.TestSuite(suites)


def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
