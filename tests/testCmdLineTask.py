#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2008-2015 AURA/LSST.
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
# see <https://www.lsstcorp.org/LegalNotices/>.
#
import os
import shutil
import unittest
import tempfile

import lsst.utils
import lsst.utils.tests as utilsTests
import lsst.pex.logging as pexLog
import lsst.pipe.base as pipeBase
from lsst.obs.test import TestConfig

ObsTestDir = lsst.utils.getPackageDir("obs_test")
DataPath = os.path.join(ObsTestDir, "data", "input")

class ExampleTask(pipeBase.CmdLineTask):
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

class CannotConstructTask(ExampleTask):
    """A task that cannot be constructed; used to test error handling
    """
    def __init__(self, *args, **kwargs):
        raise RuntimeError("This task cannot be constructed")

class NoMultiprocessTask(ExampleTask):
    """Version of ExampleTask that does not support multiprocessing"""
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
        retVal = ExampleTask.parseAndRun(args=[DataPath, "--output", self.outPath, "--id", "visit=1"])
        self.assertEqual(retVal.resultList, [None])
        task = ExampleTask(config=retVal.parsedCmd.config)
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
        config = ExampleTask.ConfigClass()
        config.floatField = -99.9
        defLog = pexLog.getDefaultLog()
        log = pexLog.Log(defLog, "cmdLineTask")
        retVal = ExampleTask.parseAndRun(
            args=[DataPath, "--output", self.outPath, "--id", "visit=2"],
            config = config,
            log = log
        )
        self.assertEquals(retVal.parsedCmd.config.floatField, -99.9)
        self.assertTrue(retVal.parsedCmd.log is log)

    def testDoReturnResults(self):
        """Test the doReturnResults flag
        """
        retVal = ExampleTask.parseAndRun(args=[DataPath, "--output", self.outPath,
                                            "--id", "visit=3", "filter=r"], doReturnResults=True)
        self.assertEqual(len(retVal.resultList), 1)
        result = retVal.resultList[0]
        self.assertEqual(result.metadata.get("numProcessed"), 1)
        self.assertEqual(result.result.numProcessed, 1)

    def testDoReturnResultsOnFailure(self):
        retVal = ExampleTask.parseAndRun(args=[DataPath, "--output", self.outPath,
                                            "--id", "visit=3", "filter=r", "--config", "doFail=True",
                                            "--clobber-config"], doReturnResults=True)
        self.assertEqual(len(retVal.resultList), 1)
        result = retVal.resultList[0]
        self.assertEqual(result.metadata.get("numProcessed"), 0)
        self.assertEqual(retVal.resultList[0].result, None)

    def testBackupConfig(self):
        """Test backup config file creation
        """
        ExampleTask.parseAndRun(args=[DataPath, "--output", self.outPath, "--id", "visit=3", "filter=r"])
        # Rerun with --clobber-config to ensure backup config file is created
        ExampleTask.parseAndRun(args=[DataPath, "--output", self.outPath, "--id", "visit=3", "filter=r",
                                   "--config", "floatField=-99.9", "--clobber-config"])
        # Ensure backup config file was created
        self.assertTrue(os.path.exists(os.path.join(self.outPath, "config", ExampleTask._DefaultName + ".py~1")))

    def testNoBackupConfig(self):
        """Test no backup config file creation
        """
        ExampleTask.parseAndRun(args=[DataPath, "--output", self.outPath, "--id", "visit=3", "filter=r"])
        # Rerun with --clobber-config and --no-backup-config to ensure backup config file is NOT created
        ExampleTask.parseAndRun(args=[DataPath, "--output", self.outPath, "--id", "visit=3", "filter=r",
                                   "--config", "floatField=-99.9", "--clobber-config", "--no-backup-config"])
        # Ensure backup config file was NOT created
        self.assertFalse(
            os.path.exists(os.path.join(self.outPath, "config",ExampleTask._DefaultName + ".py~1")))

    def testMultiprocess(self):
        """Test multiprocessing at a very minimal level
        """
        for TaskClass in (ExampleTask, NoMultiprocessTask):
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


class EaxmpleMultipleIdTaskRunner(pipeBase.TaskRunner):
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

class ExampleMultipleIdTask(pipeBase.CmdLineTask):
    _DefaultName = "test"
    ConfigClass = TestConfig
    RunnerClass = EaxmpleMultipleIdTaskRunner

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
        retVal = ExampleMultipleIdTask.parseAndRun(args=args)
        self.assertEqual(len(retVal.resultList), 1)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
