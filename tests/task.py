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
import lsst.pex.config as pexPolicy
import lsst.pipe.base as pipeBase

class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()

class AddTask(pipeBase.Task):
    @pipeBase.timeMethod
    def run(self, val):
        addend = self.config.get("addend")
        return pipeBase.Struct(val = val + addend)
    
    @classproperty
    def ConfigClass(cls):
        config = pexPolicy.Policy()
        config.set("addend", 3.1)
        return config

class MultTask(pipeBase.Task):
    @pipeBase.timeMethod
    def run(self, val):
        multiplicand = self.config.get("multiplicand")
        return pipeBase.Struct(val = val * multiplicand)
    
    @classproperty
    def ConfigClass(cls):
        config = pexPolicy.Policy()
        config.set("multiplicand", 2.5)
        return config

class AddMultTask(pipeBase.Task):
    """First add, then multiply"""
    def __init__(self, **keyArgs):
        pipeBase.Task.__init__(self, **keyArgs)
        self.makeSubtask("add", AddTask)
        self.makeSubtask("mult", MultTask)

    @pipeBase.timeMethod
    def run(self, val):
        with self.timer("context"):
            addRet = self.add.run(val)
            multRet = self.mult.run(addRet.val)
            return pipeBase.Struct(val = multRet.val)
    
    @classproperty
    def ConfigClass(cls):
        config = pexPolicy.Policy()
        addPolicy = AddTask.ConfigClass()
        multPolicy = MultTask.ConfigClass()
        config.set("addPolicy", addPolicy)
        config.set("multPolicy", multPolicy)
        return config

class AddTwiceTask(AddTask):
    """Variant of AddTask that adds twice the addend"""
    def run(self, val):
        addend = self.config.get("addend")
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
                config = AddMultTask.ConfigClass()
                config.getPolicy("addPolicy").set("addend", addend)
                config.getPolicy("multPolicy").set("multiplicand", multiplicand)
                addMultTask = AddMultTask(config=config)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + addend) * multiplicand)
    
    def testReplace(self):
        """Test replacing one subtask with another
        """
        for addend in (1.1, -3.5):
            for multiplicand in (0.9, -45.0):
                config = AddMultTask.ConfigClass()
                config.getPolicy("addPolicy").set("addend", addend)
                config.getPolicy("multPolicy").set("multiplicand", multiplicand)
                addMultTask = AddMultTask(config=config)
                addMultTask.makeSubtask("add", AddTwiceTask)
                for val in (-1.0, 0.0, 17.5):
                    ret = addMultTask.run(val=val)
                    self.assertAlmostEqual(ret.val, (val + (2 * addend)) * multiplicand)
    
    def testNames(self):
        """Test task names
        """
        config = AddMultTask.ConfigClass()
        addMultTask = AddMultTask(config=config)
        self.assertEquals(addMultTask._name, "main")
        self.assertEquals(addMultTask.add._name, "add")
        self.assertEquals(addMultTask.mult._name, "mult")
        self.assertEquals(addMultTask._fullName, "main")
        self.assertEquals(addMultTask.add._fullName, "main.add")
        self.assertEquals(addMultTask.mult._fullName, "main.mult")
    
    def testTimeMethod(self):
        """Test that the timer is adding the right metadata
        """
        config = AddMultTask.ConfigClass()
        addMultTask = AddMultTask(config=config)
        addMultTask.run(val=1.1)
        self.assertLess(addMultTask.metadata.get("runDuration"), 0.1)
        self.assertLess(addMultTask.metadata.get("contextDuration"), 0.1)
        self.assertLess(addMultTask.add.metadata.get("runDuration"), 0.1)
        self.assertLess(addMultTask.mult.metadata.get("runDuration"), 0.1)
 

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
