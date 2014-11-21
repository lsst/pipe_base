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
import os
import shutil
import unittest
import tempfile

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.logging as pexLog
import lsst.pipe.base as pipeBase
from lsst.obs.test import TestConfig

ObsTestDir = eups.productDir("obs_test")
if not ObsTestDir:
    print "Warning: you must setup obs_test to run these tests"
else:
    DataPath = os.path.join(ObsTestDir, "data", "input")

__all__ = ["TestConfig"]

class TestTask(pipeBase.CmdLineTask):
    ConfigClass = TestConfig
    _DefaultName = "test"
    def __init__(self, *args, **kwargs):
        pipeBase.CmdLineTask.__init__(self, *args, **kwargs)
        self.dataRefList = []
        self.numProcessed = 0
        self.metadata.set("numProcessed", self.numProcessed)

    @pipeBase.timeMethod
    def run(self, dataRef):
        if self.config.doFail:
            raise pipeBase.TaskError("Failed by request: config.doFail is true")
        self.dataRefList.append(dataRef)
        self.numProcessed += 1
        self.metadata.set("numProcessed", self.numProcessed)
        return pipeBase.Struct(
            numProcessed = self.numProcessed,
        )

class CannotConstructTask(TestTask):
    """A task that cannot be constructed; used to test error handling
    """
    def __init__(self, *args, **kwargs):
        raise RuntimeError("This task cannot be constructed")

class NoMultiprocessTask(TestTask):
    """Version of TestTask that does not support multiprocessing"""
    canMultiprocess = False


class CmdLineTaskTestCase(unittest.TestCase):
    """A test case for CmdLineTask
    """
    def setUp(self):
        os.environ.pop("PIPE_INPUT_ROOT", None)
        os.environ.pop("PIPE_CALIB_ROOT", None)
        os.environ.pop("PIPE_OUTPUT_ROOT", None)
        self.outPath = tempfile.mkdtemp()
    
    def tearDown(self):
        try:
            shutil.rmtree(self.outPath)
        except Exception:
            print "WARNING: failed to remove temporary dir %r" % (self.outPath,)
        del self.outPath

    def testBasics(self):
        """Test basic construction and use of a command-line task
        """
        retVal = TestTask.parseAndRun(args=[DataPath, "--output", self.outPath, 
            "--id", "visit=1"])
        self.assertEqual(retVal.resultList, [None])
        task = TestTask(config=retVal.parsedCmd.config)
        parsedCmd = retVal.parsedCmd
        self.assertEqual(len(parsedCmd.id.refList), 1)
        dataRef = parsedCmd.id.refList[0]
        dataId = dataRef.dataId
        self.assertEqual(dataId["visit"], 1)
        self.assertEqual(task.getName(), "test")
        config = dataRef.get("test_config", immediate=True)
        self.assertEqual(config, task.config)
        metadata = dataRef.get("test_metadata", immediate=True)
        self.assertEqual(metadata.get("test.numProcessed"), 1)
    
    def testOverrides(self):
        """Test config and log override
        """
        config = TestTask.ConfigClass()
        config.floatField = -99.9
        defLog = pexLog.getDefaultLog()
        log = pexLog.Log(defLog, "cmdLineTask")
        retVal = TestTask.parseAndRun(
            args=[DataPath, "--output", self.outPath, "--id", "visit=2"],
            config = config,
            log = log
        )
        self.assertEquals(retVal.parsedCmd.config.floatField, -99.9)
        self.assertTrue(retVal.parsedCmd.log is log)
    
    def testDoReturnResults(self):
        """Test the doReturnResults flag
        """
        retVal = TestTask.parseAndRun(args=[DataPath, "--output", self.outPath, 
            "--id", "visit=3", "filter=r"], doReturnResults=True)
        self.assertEqual(len(retVal.resultList), 1)
        result = retVal.resultList[0]
        self.assertEqual(result.metadata.get("numProcessed"), 1)
        self.assertEqual(result.result.numProcessed, 1)

    def testDoReturnResultsOnFailure(self):
        retVal = TestTask.parseAndRun(args=[DataPath, "--output", self.outPath, 
            "--id", "visit=3", "filter=r", "--config", "doFail=True", "--clobber-config"], doReturnResults=True)
        self.assertEqual(len(retVal.resultList), 1)
        result = retVal.resultList[0]
        self.assertEqual(result.metadata.get("numProcessed"), 0)
        self.assertEqual(retVal.resultList[0].result, None)

    def testMultiprocess(self):
        """Test multiprocessing at a very minimal level
        """
        for TaskClass in (TestTask, NoMultiprocessTask):
            result = TaskClass.parseAndRun(args=[DataPath, "--output", self.outPath, 
                "-j", "5", "--id", "visit=2", "filter=r"])
            self.assertEqual(result.taskRunner.numProcesses, 5 if TaskClass.canMultiprocess else 1)
    
    def testCannotConstructTask(self):
        """Test error handling when a task cannot be constructed
        """
        for doRaise in (False, True):
            args=[DataPath, "--output", self.outPath, "--id", "visit=1"]
            if doRaise:
                args.append("--doraise")
            self.assertRaises(RuntimeError, CannotConstructTask.parseAndRun, args=args)


class TestMultipleIdTaskRunner(pipeBase.TaskRunner):
    """TaskRunner to get multiple identifiers down into a Task"""
    @staticmethod
    def getTargetList(parsedCmd):
        """We want our Task to process one dataRef from each identifier at a time"""
        return zip(parsedCmd.one.refList, parsedCmd.two.refList)

    def __call__(self, target):
        """Send results from the Task back so we can inspect

        For this test case with obs_test, we know that the results are picklable
        and small, so returning something is not a problem.
        """
        task = self.TaskClass(config=self.config, log=self.log)
        return task.run(target)

class TestMultipleIdTask(pipeBase.CmdLineTask):
    _DefaultName = "test"
    ConfigClass = TestConfig
    RunnerClass = TestMultipleIdTaskRunner

    @classmethod
    def _makeArgumentParser(cls):
        """We want an argument parser that has multiple identifiers"""
        parser = pipeBase.ArgumentParser(name=cls._DefaultName)
        parser.add_id_argument("--one", "raw", "data identifier one", level="sensor")
        parser.add_id_argument("--two", "raw", "data identifier two", level="sensor")
        return parser

    def run(self, data):
        """Our Task just spits back what's in the dataRefs."""
        oneRef = data[0]
        twoRef = data[1]
        return oneRef.get("raw", snap=0, channel="0,0"), twoRef.get("raw", snap=0, channel="0,0")


class MultipleIdTaskTestCase(unittest.TestCase):
    """A test case for CmdLineTask using multiple identifiers

    Tests implementation of ticket 2144, and demonstrates how
    to get results from multiple identifiers down into a Task.
    """
    def setUp(self):
        os.environ.pop("PIPE_INPUT_ROOT", None)
        os.environ.pop("PIPE_CALIB_ROOT", None)
        os.environ.pop("PIPE_OUTPUT_ROOT", None)
        self.outPath = tempfile.mkdtemp()

    def tearDown(self):
        try:
            shutil.rmtree(self.outPath)
        except Exception:
            print "WARNING: failed to remove temporary dir %r" % (self.outPath,)
        del self.outPath

    def testMultiple(self):
        """Test use of a CmdLineTask with multiple identifiers"""
        args = [DataPath, "--output", self.outPath,
                "--one", "visit=1", "filter=g",
                "--two", "visit=2", "filter=g",
                ]
        retVal = TestMultipleIdTask.parseAndRun(args=args)
        self.assertEqual(len(retVal.resultList), 1)

def suite():
    """Return a suite containing all the test cases in this module.
    """
    utilsTests.init()

    suites = []

    suites += unittest.makeSuite(CmdLineTaskTestCase)
    suites += unittest.makeSuite(MultipleIdTaskTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)

    return unittest.TestSuite(suites)


def run(shouldExit=False):
    """Run the tests"""
    if ObsTestDir is None:
        return
    utilsTests.run(suite(), shouldExit)

if __name__ == "__main__":
    run(True)
