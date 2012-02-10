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
import unittest
import warnings

import eups
import lsst.utils.tests as utilsTests
import lsst.pex.config as pexConfig
import lsst.pex.logging as pexLog
import lsst.pipe.base as pipeBase

try:
    from lsst.obs.lsstSim import LsstSimMapper
    DataPath = os.path.join(eups.productDir("obs_lsstSim"), "tests/data")
except ImportError:
    DataPath = None
    warnings.warn("skipping all tests because obs_lsstSim is not setup")

LocalDataPath = os.path.join(eups.productDir("pipe_base"), "tests/data")

class SampleConfig(pexConfig.Config):
    floatItem = pexConfig.Field(doc="sample float field", dtype=float, default=3.1)
    strItem = pexConfig.Field(doc="sample str field", dtype=str, default="strDefault")
    
class ArgumentParserTestCase(unittest.TestCase):
    """A test case for ArgumentParser."""
    def setUp(self):
        self.ap = pipeBase.ArgumentParser()
        self.config = SampleConfig()
    
    def tearDown(self):
        del self.ap
        del self.config
    
    def testBasicId(self):
        """Test --id basics"""
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath, "--id", "raft=0,3", "sensor=0,1", "visit=85470982"],
        )
        self.assertEqual(len(namespace.dataIdList), 1)
        self.assertEqual(len(namespace.dataRefList), 1)
        
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath, "--id", "raft=0,3", "sensor=0,1", "visit=1"],
        )
        self.assertEqual(len(namespace.dataIdList), 1)
        self.assertEqual(len(namespace.dataRefList), 0) # no data for this ID
        
    def testIdCross(self):
        """Test --id cross product"""
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath, "--id", "raft=0,10^0,20^0,3", "sensor=10,0^1,1", "visit=85470982"],
        )
        self.assertEqual(len(namespace.dataIdList), 6)
        self.assertEqual(len(namespace.dataRefList), 1) # just one of the IDs has data
    
    def testConfig(self):
        """Test --config"""
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath, "--config", "floatItem=-67.1", "strItem=overridden value"],
        )
        self.assertEqual(namespace.config.floatItem, -67.1)
        self.assertEqual(namespace.config.strItem, "overridden value")

        # verify that order of setting values is left to right
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath,
                "--config", "floatItem=-67.1", "strItem=overridden value",
                "--config", "strItem=final value"],
        )
        self.assertEqual(namespace.config.floatItem, -67.1)
        self.assertEqual(namespace.config.strItem, "final value")
        
        # verify that incorrect names are caught
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            argv = ["lsstSim", DataPath, "--config", "missingItem=-67.1"],
        )
    
    def testConfigFile(self):
        """Test --configfile"""
        configFilePath = os.path.join(LocalDataPath, "argumentParserConfig.py")
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath, "--configfile", configFilePath],
        )
        self.assertEqual(namespace.config.floatItem, -9e9)
        self.assertEqual(namespace.config.strItem, "set in override file")
       
        # verify that order of setting values is left to right
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath,
                "--config", "floatItem=5.5",
                "--configfile", configFilePath,
                "--config", "strItem=value from cmd line"],
        )
        self.assertEqual(namespace.config.floatItem, -9e9)
        self.assertEqual(namespace.config.strItem, "value from cmd line")

        # verify that missing files are caught
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            argv = ["lsstSim", DataPath, "--configfile", "missingFile"],
        )
    
    def testAtFile(self):
        """Test @file"""
        argPath = os.path.join(LocalDataPath, "args.txt")
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath, "@%s" % (argPath,)],
        )
        self.assertEqual(len(namespace.dataIdList), 1)
        self.assertEqual(namespace.config.floatItem, 4.7)
        self.assertEqual(namespace.config.strItem, "new value")
    
    def testLog(self):
        """Test --log
        """
        logFile = "tests_argumentParser_testLog_temp.txt"
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath, "--log", logFile],
        )
        self.assertTrue(os.path.isfile(logFile))
        os.remove(logFile)
    
    def testTrace(self):
        """Test --trace
        
        I'm not sure how to verify that tracing is enabled; just see if it runs
        """
        namespace = self.ap.parse_args(
            config = self.config,
            argv = ["lsstSim", DataPath,
                "--trace", "afw.math=2", "meas.algorithms=3",
                "--trace", "something=5",
            ],
        )
        pexLog.Trace("something", 4, "You should see this message")
        pexLog.Trace("something", 6, "You should not see this message")
    
    def testLogLevel(self):
        """Test --log-level"""
#         for logLevel in ("debug", "Info", "WARN", "fatal"):
#             intLevel = getattr(pexLog.Log, logLevel.upper())
#             namespace = self.ap.parse_args(
#                 config = self.config,
#                 argv = ["lsstSim", DataPath, "--log-level", logLevel],
#             )
#             self.assertEqual(namespace.log.getThreshold(), intLevel)
# 
#             namespace = self.ap.parse_args(
#                 config = self.config,
#                 argv = ["lsstSim", DataPath, "--log-level", str(intLevel)],
#             )
#             self.assertEqual(namespace.log.getThreshold(), intLevel)
        
        self.assertRaises(SystemExit, self.ap.parse_args,
            config = self.config,
            argv = ["lsstSim", DataPath,
                "--log-level", "INVALID_LEVEL",
            ],
        )


def suite():
    utilsTests.init()

    suites = []
    suites += unittest.makeSuite(ArgumentParserTestCase)
    suites += unittest.makeSuite(utilsTests.MemoryTestCase)
    return unittest.TestSuite(suites)

def run(shouldExit = False):
    if DataPath == None:
        return
    utilsTests.run(suite(), shouldExit)

if __name__ == '__main__':
    run(True)
