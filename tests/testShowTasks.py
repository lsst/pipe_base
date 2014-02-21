#!/usr/bin/env python
# 
# LSST Data Management System
# Copyright 2013 LSST Corporation.
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
import sys
import StringIO
import unittest

import lsst.utils.tests as utilsTests
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

class SimpleTaskConfig(pexConfig.Config):
    ff3 = pexConfig.Field(doc="float 1", dtype=float, default=3.1)
    sf3 = pexConfig.Field(doc="str 1", dtype=str, default="default for sf1")

class SimpleTask(pipeBase.Task):
    ConfigClass = SimpleTaskConfig

class TaskWithSubtasksConfig(pexConfig.Config):
    sst1 = SimpleTask.makeField(doc="sub-subtask 1")
    sst2 = SimpleTask.makeField(doc="sub-subtask 2")
    ff2 = pexConfig.Field(doc="float 1", dtype=float, default=3.1)
    sf2 = pexConfig.Field(doc="str 1", dtype=str, default="default for sf1")

class TaskWithSubtasks(pipeBase.Task):
    ConfigClass = TaskWithSubtasksConfig

class MainTaskConfig(pexConfig.Config):
    st1 = TaskWithSubtasks.makeField(doc="subtask 1")
    st2 = TaskWithSubtasks.makeField(doc="subtask 2")
    ff1 = pexConfig.Field(doc="float 2", dtype=float, default=3.1)
    sf1 = pexConfig.Field(doc="str 2", dtype=str, default="default for strField")


class MainTask(pipeBase.Task):
    ConfigClass = MainTaskConfig

c = MainTaskConfig()


class ShowTasksTestCase(unittest.TestCase):
    """A test case for the code that implements ArgumentParser's --show config option
    """
    def testBasicShowTaskHierarchy(self):
        """Test basic usage of show
        """
        config = MainTaskConfig()
        expectedData = """Subtasks:
st1: __main__.TaskWithSubtasks
st1.sst1: __main__.SimpleTask
st1.sst2: __main__.SimpleTask
st2: __main__.TaskWithSubtasks
st2.sst1: __main__.SimpleTask
st2.sst2: __main__.SimpleTask
"""
        tempStdOut = StringIO.StringIO()
        savedStdOut, sys.stdout = sys.stdout, tempStdOut
        try:
            pipeBase.argumentParser.showTaskHierarchy(config)
        finally:
            sys.stdout = savedStdOut
        
        self.assertEqual(tempStdOut.getvalue(), expectedData)

def suite():
    """Return a suite containing all the test cases in this module.
    """
    utilsTests.init()

    suites = []

    suites += unittest.makeSuite(ShowTasksTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)

    return unittest.TestSuite(suites)


def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
