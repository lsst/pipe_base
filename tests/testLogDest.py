import unittest
import lsst.utils.tests as utilsTests

import os
import socket
import sys

import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

class LoggingConfig(pexConfig.Config):
    pass

class LoggingTask(pipeBase.CmdLineTask):
    _DefaultName = "HelloWorld"
    ConfigClass = LoggingConfig
    @pipeBase.timeMethod
    def run(self, dataRef):
        self.log.info("Hello world")

class LogTestCase(unittest.TestCase):
    """A test case for Task logging"""

    def verifyContents(self, contents):
        self.assertTrue(contents[0].startswith(": input"), contents[0])
        self.assertTrue(contents[0].endswith("tests/data/input\n"))
        self.assertEqual(contents[1], ": calib=None\n")
        self.assertEqual(contents[2], ": output=None\n")
        self.assertEqual(contents[3], "HelloWorld WARNING: "
            "Could not persist config for dataId={'sensor': '1,0', "
            "'visit': 85470982, 'snap': 1, 'raft': '0,2', 'skyTile': 1, "
            "'channel': '1,1'}: "
            "'TestMapper' object has no attribute 'map_HelloWorld_config'\n")
        self.assertEqual(contents[4], "HelloWorld: Hello world\n")
        self.assertEqual(contents[5], "HelloWorld WARNING: "
            "Could not persist metadata for dataId={'sensor': '1,0', "
            "'visit': 85470982, 'snap': 1, 'raft': '0,2', 'skyTile': 1, "
            "'channel': '1,1'}: "
            "'TestMapper' object has no attribute 'map_HelloWorld_metadata'\n") 

    def checkOutput(self, fname, nLines):
        self.assertTrue(os.path.exists(fname))

        with file(fname) as f:
            contents = f.readlines()
        self.assertEqual(len(contents), nLines,
                "%d lines instead of %d in:\n%s" %
                (len(contents), nLines, contents))
        if nLines == 8:
            self.assertTrue(contents[0].startswith(
                ": Config override file does not exist:"))
            self.assertTrue(contents[0].endswith("/config/HelloWorld.py'\n"))
            self.assertTrue(contents[1].startswith(
                ": Config override file does not exist:"))
            self.assertTrue(
                    contents[1].endswith("config/test/HelloWorld.py'\n"))
            contents = contents[2:]
        self.verifyContents(contents)
        os.unlink(fname)

    @unittest.skipUnless("OBS_TEST_DIR" in os.environ, "obs_test is not setup")
    def testDefault(self):
        fd = os.open("test1.out", os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        tmpFd = os.dup(2)
        os.close(2)
        os.dup2(fd, 2)
        os.close(fd)

        LoggingTask.parseAndRun(args=(
            os.path.join(os.environ["OBS_TEST_DIR"], "tests", "data", "input"),
            "--id", "visit=85470982",
            "snap=1", "raft=0,2", "sensor=1,0", "channel=1,1"
            ))

        os.close(2)
        os.dup2(tmpFd, 2)
        os.close(tmpFd)

        self.checkOutput("test1.out", 8)

    @unittest.skipUnless("OBS_TEST_DIR" in os.environ, "obs_test is not setup")
    def testCmdSimple(self):
        try:
            os.unlink("test2.out")
        except:
            pass
        LoggingTask.parseAndRun(args=(
            os.path.join(os.environ["OBS_TEST_DIR"], "tests", "data", "input"),
            "--id", "visit=85470982",
            "snap=1", "raft=0,2", "sensor=1,0", "channel=1,1",
            "--logdest", "test2.out"
            ))
        self.checkOutput("test2.out", 6)

    @unittest.skipUnless("OBS_TEST_DIR" in os.environ, "obs_test is not setup")
    def testEnvSimple(self):
        try:
            os.unlink("test3.out")
        except:
            pass
        os.environ["LSST_LOGDEST"] = "test3.out"
        LoggingTask.parseAndRun(args=(
            os.path.join(os.environ["OBS_TEST_DIR"], "tests", "data", "input"),
            "--id", "visit=85470982",
            "snap=1", "raft=0,2", "sensor=1,0", "channel=1,1"
            ))
        self.checkOutput("test3.out", 6)

    @unittest.skipUnless("OBS_TEST_DIR" in os.environ, "obs_test is not setup")
    def testCmdUrl(self):
        try:
            os.unlink("test4.out")
        except:
            pass
        LoggingTask.parseAndRun(args=(
            os.path.join(os.environ["OBS_TEST_DIR"], "tests", "data", "input"),
            "--id", "visit=85470982",
            "snap=1", "raft=0,2", "sensor=1,0", "channel=1,1",
            "--logdest", "file:test4.out"
            ))
        self.checkOutput("test4.out", 6)

    @unittest.skipUnless("OBS_TEST_DIR" in os.environ, "obs_test is not setup")
    def testEnvUrl(self):
        try:
            os.unlink("test5.out")
        except:
            pass
        os.environ["LSST_LOGDEST"] = "file:test5.out"
        LoggingTask.parseAndRun(args=(
            os.path.join(os.environ["OBS_TEST_DIR"], "tests", "data", "input"),
            "--id", "visit=85470982",
            "snap=1", "raft=0,2", "sensor=1,0", "channel=1,1"
            ))
        self.checkOutput("test5.out", 6)

    @unittest.skipUnless("OBS_TEST_DIR" in os.environ, "obs_test is not setup")
    @unittest.skipUnless("CTRL_EVENTS_DIR" in os.environ, "ctrl_events is not setup")
    def testZZZEnvEvent(self):
        # Note: must come after other tests, since this affects the default
        # logger.  Also note that this can only be run once since it affects
        # the default EventSystem.
        os.environ["LSST_LOGDEST"] = "pexlog://lsst8.ncsa.illinois.edu/" + \
                "?runid=logging_test_x&workerid=%d" % (os.getpid(),)
        import lsst.ctrl.events as events
        eventSystem = events.EventSystem()
        eventSystem.createReceiver("lsst8.ncsa.illinois.edu",
                events.EventLog.LOGGING_TOPIC, "RUNID = 'logging_test_x'")
        LoggingTask.parseAndRun(args=(
            os.path.join(os.environ["OBS_TEST_DIR"], "tests", "data", "input"),
            "--id", "visit=85470982",
            "snap=1", "raft=0,2", "sensor=1,0", "channel=1,1"
            ))
        contents = []
        for i in range(6):
            val = eventSystem.receiveEvent(events.EventLog.LOGGING_TOPIC, 100)
            ps = val.getPropertySet()
            level = ""
            if ps.get("LEVEL") == 10:
                level = " WARNING"
            contents.append(ps.get("LOG") + level + ": " + ps.get("COMMENT") + "\n")
        self.verifyContents(contents)


def suite():
    """Return a suite containing all the test cases in this module."""
    utilsTests.init()
    suites = []
    suites += unittest.makeSuite(LogTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit=False):
    """Run the tests"""
    utilsTests.run(suite(), shouldExit)
    
if __name__ == "__main__":
    run(True)
