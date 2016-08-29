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
from __future__ import print_function
import itertools
import os
import unittest

from future import standard_library
standard_library.install_aliases()
from builtins import str

import lsst.utils
import lsst.utils.tests
import lsst.log as lsstLog
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

ObsTestDir = lsst.utils.getPackageDir("obs_test")
DataPath = os.path.realpath(os.path.join(ObsTestDir, "data", "input"))
LocalDataPath = os.path.join(os.path.dirname(__file__), "data")
#
# Context manager to intercept stdout/err
#  http://stackoverflow.com/questions/5136611/capture-stdout-from-a-script-in-python
#
# Use as:
#   with capture() as out:
#      print 'hi'
#
import contextlib


@contextlib.contextmanager
def capture():
    import sys
    from io import StringIO
    oldout, olderr = sys.stdout, sys.stderr
    try:
        out = [StringIO(), StringIO()]
        sys.stdout, sys.stderr = out
        yield out
    finally:
        sys.stdout, sys.stderr = oldout, olderr
        out[0] = out[0].getvalue()
        out[1] = out[1].getvalue()


class SubConfig(pexConfig.Config):
    intItem = pexConfig.Field(doc="sample int field", dtype=int, default=8)


class SampleConfig(pexConfig.Config):
    boolItem = pexConfig.Field(doc="sample bool field", dtype=bool, default=True)
    floatItem = pexConfig.Field(doc="sample float field", dtype=float, default=3.1)
    strItem = pexConfig.Field(doc="sample str field", dtype=str, default="strDefault")
    subItem = pexConfig.ConfigField(doc="sample subfield", dtype=SubConfig)
    multiDocItem = pexConfig.Field(doc="1. sample... \n#2...multiline \n##3...#\n###4...docstring",
                                   dtype=str, default="multiLineDoc")
    dsType = pexConfig.Field(doc="dataset type for --id argument", dtype=str, default="calexp")
    dsTypeNoDefault = pexConfig.Field(doc="dataset type for --id argument; no default", dtype=str,
                                      optional=True)


class ArgumentParserTestCase(unittest.TestCase):
    """A test case for ArgumentParser."""

    def setUp(self):
        self.ap = pipeBase.InputOnlyArgumentParser(name="argumentParser")
        self.ap.add_id_argument("--id", "raw", "help text")
        self.ap.add_id_argument("--otherId", "raw", "more help")
        self.config = SampleConfig()
        os.environ.pop("PIPE_INPUT_ROOT", None)
        os.environ.pop("PIPE_CALIB_ROOT", None)
        os.environ.pop("PIPE_OUTPUT_ROOT", None)

    def tearDown(self):
        del self.ap
        del self.config

    def testBasicId(self):
        """Test --id basics"""
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath, "--id", "visit=1", "filter=g"],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(len(namespace.id.refList), 1)

        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath, "--id", "visit=22", "filter=g"],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(len(namespace.id.refList), 0)  # no data for this ID

    def testOtherId(self):
        """Test --other"""
        # By itself
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath, "--other", "visit=99"],
        )
        self.assertEqual(len(namespace.otherId.idList), 1)
        self.assertEqual(len(namespace.otherId.refList), 0)

        # And together
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath, "--id", "visit=1",
                  "--other", "visit=99"],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(len(namespace.id.refList), 1)
        self.assertEqual(len(namespace.otherId.idList), 1)
        self.assertEqual(len(namespace.otherId.refList), 0)  # no data for this ID

    def testIdCross(self):
        """Test --id cross product, including order"""
        visitList = (1, 2, 3)
        filterList = ("g", "r")
        namespace = self.ap.parse_args(
            config=self.config,
            args=[
                DataPath,
                "--id",
                "filter=%s" % ("^".join(filterList),),
                "visit=%s" % ("^".join(str(visit) for visit in visitList),),
            ]
        )
        self.assertEqual(len(namespace.id.idList), 6)
        predValList = itertools.product(filterList, visitList)
        for id, predVal in zip(namespace.id.idList, predValList):
            idVal = tuple(id[key] for key in ("filter", "visit"))
            self.assertEqual(idVal, predVal)
        self.assertEqual(len(namespace.id.refList), 3)  # only have data for three of these

    def testIdDuplicate(self):
        """Verify that each ID name can only appear once in a given ID argument"""
        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--id", "visit=1", "visit=2"],
                               )

    def testConfigBasics(self):
        """Test --config"""
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath, "--config", "boolItem=False", "floatItem=-67.1",
                  "strItem=overridden value", "subItem.intItem=5", "multiDocItem=edited value"],
        )
        self.assertEqual(namespace.config.boolItem, False)
        self.assertEqual(namespace.config.floatItem, -67.1)
        self.assertEqual(namespace.config.strItem, "overridden value")
        self.assertEqual(namespace.config.subItem.intItem, 5)
        self.assertEqual(namespace.config.multiDocItem, "edited value")

    def testConfigLeftToRight(self):
        """Verify that order of overriding config values is left to right"""
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath,
                  "--config", "floatItem=-67.1", "strItem=overridden value",
                  "--config", "strItem=final value"],
        )
        self.assertEqual(namespace.config.floatItem, -67.1)
        self.assertEqual(namespace.config.strItem, "final value")

    def testConfigWrongNames(self):
        """Verify that incorrect names for config fields are caught"""
        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--config", "missingItem=-67.1"],
                               )

    def testShow(self):
        """Test --show"""
        with capture() as out:
            self.ap.parse_args(
                config=self.config,
                args=[DataPath, "--show", "config", "data", "tasks", "run"],
            )
        res = out[0]
        self.assertIn("config.floatItem", res)
        self.assertIn("config.subItem", res)
        self.assertIn("config.boolItem", res)
        self.assertIn("config.strItem", res)
        self.assertIn("config.multiDocItem", res)
        # Test show with exact config name and with one sided, embedded, and two sided globs
        for configStr in ("config.multiDocItem", "*ultiDocItem", "*ulti*Item", "*ultiDocI*"):
            with capture() as out:
                self.ap.parse_args(self.config, [DataPath, "--show", "config=" + configStr, "run"])
            res = out[0]
            self.assertIn("config.multiDocItem", res)

        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--show", "config"],
                               )
        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--show", "config=X"],
                               )

        # Test show with glob for single and multi-line doc strings
        for configStr, assertStr in ("*strItem*", "strDefault"), ("*multiDocItem", "multiLineDoc"):
            with capture() as out:
                self.ap.parse_args(self.config, [DataPath, "--show", "config=" + configStr, "run"])
            stdout = out[0]
            stdoutList = stdout.rstrip().split("\n")
            self.assertGreater(len(stdoutList), 2)  # at least 2 docstring lines (1st line is always a \n)
            # and 1 config parameter
            answer = [ans for ans in stdoutList if not ans.startswith("#")]  # get rid of comment lines
            answer = [ans for ans in answer if ":NOIGNORECASE to prevent this" not in ans]
            self.assertEqual(len(answer), 2)       # only 1 config item matches (+ 1 \n entry per doc string)
            self.assertIn(assertStr, answer[1])

        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--show", "badname", "run"],
                               )
        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--show"],  # no --show arguments
                               )

        # Test show with and without case sensitivity
        for configStr, shouldMatch in [("*multiDocItem*", True),
                                       ("*multidocitem*", True),
                                       ("*multidocitem*:NOIGNORECASE", False)]:
            with capture() as out:
                self.ap.parse_args(self.config, [DataPath, "--show", "config=" + configStr, "run"])
            res = out[0]

            if shouldMatch:
                self.assertIn("config.multiDocItem", res)
            else:
                self.assertNotIn("config.multiDocItem", res)

    def testConfigFileBasics(self):
        """Test --configfile"""
        configFilePath = os.path.join(LocalDataPath, "argumentParserConfig.py")
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath, "--configfile", configFilePath],
        )
        self.assertEqual(namespace.config.floatItem, -9e9)
        self.assertEqual(namespace.config.strItem, "set in override file")

    def testConfigFileLeftRight(self):
        """Verify that order of setting values is with a mix of config file and config is left to right"""
        configFilePath = os.path.join(LocalDataPath, "argumentParserConfig.py")
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath,
                  "--config", "floatItem=5.5",
                  "--configfile", configFilePath,
                  "--config", "strItem=value from cmd line"],
        )
        self.assertEqual(namespace.config.floatItem, -9e9)
        self.assertEqual(namespace.config.strItem, "value from cmd line")

    def testConfigFileMissingFiles(self):
        """Verify that missing config override files are caught"""
        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--configfile", "missingFile"],
                               )

    def testAtFile(self):
        """Test @file"""
        argPath = os.path.join(LocalDataPath, "args.txt")
        namespace = self.ap.parse_args(
            config=self.config,
            args=[DataPath, "@%s" % (argPath,)],
        )
        self.assertEqual(len(namespace.id.idList), 1)
        self.assertEqual(namespace.config.floatItem, 4.7)
        self.assertEqual(namespace.config.strItem, "new value")

    def testLogLevel(self):
        """Test --loglevel"""
        for logLevel in ("trace", "debug", "Info", "WARN", "eRRoR", "fatal"):
            intLevel = getattr(lsstLog.Log, logLevel.upper())
            print("testing logLevel=%r" % (logLevel,))
            namespace = self.ap.parse_args(
                config=self.config,
                args=[DataPath, "--loglevel", logLevel],
            )
            self.assertEqual(namespace.log.getLevel(), intLevel)
            self.assertFalse(hasattr(namespace, "loglevel"))

            bazLevel = "TRACE"
            namespace = self.ap.parse_args(
                config=self.config,
                args=[DataPath, "--loglevel", logLevel,
                      "foo.bar=%s" % (logLevel,),
                      "baz=INFO",
                      "baz=%s" % bazLevel,  # test that later values override earlier values
                      ],
            )
            self.assertEqual(namespace.log.getLevel(), intLevel)
            self.assertEqual(lsstLog.Log.getLogger("foo.bar").getLevel(), intLevel)
            self.assertEqual(lsstLog.Log.getLogger("baz").getLevel(), getattr(lsstLog.Log, bazLevel))

        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--loglevel", "1234"],
                               )

        with self.assertRaises(SystemExit):
            self.ap.parse_args(config=self.config,
                               args=[DataPath, "--loglevel", "INVALID_LEVEL"],
                               )

    def testPipeVars(self):
        """Test handling of $PIPE_x_ROOT environment variables, where x is INPUT, CALIB or OUTPUT
        """
        os.environ["PIPE_INPUT_ROOT"] = DataPath
        namespace = self.ap.parse_args(
            config=self.config,
            args=["."],
        )
        self.assertEqual(namespace.input, os.path.abspath(DataPath))
        self.assertEqual(namespace.calib, None)

        os.environ["PIPE_CALIB_ROOT"] = DataPath
        namespace = self.ap.parse_args(
            config=self.config,
            args=["."],
        )
        self.assertEqual(namespace.input, os.path.abspath(DataPath))
        self.assertEqual(namespace.calib, os.path.abspath(DataPath))

        os.environ.pop("PIPE_CALIB_ROOT", None)
        os.environ["PIPE_OUTPUT_ROOT"] = DataPath
        namespace = self.ap.parse_args(
            config=self.config,
            args=["."],
        )
        self.assertEqual(namespace.input, os.path.abspath(DataPath))
        self.assertEqual(namespace.calib, None)
        self.assertEqual(namespace.output, None)

    def testBareHelp(self):
        """Make sure bare help does not print an error message (ticket #3090)
        """
        for helpArg in ("-h", "--help"):
            try:
                self.ap.parse_args(
                    config=self.config,
                    args=[helpArg],
                )
                self.fail("should have raised SystemExit")
            except SystemExit as e:
                self.assertEqual(e.code, 0)

    def testDatasetArgumentBasics(self):
        """Test DatasetArgument basics"""
        dsTypeHelp = "help text for dataset argument"
        for name in (None, "--foo"):
            for default in (None, "raw"):
                argName = name if name is not None else "--id_dstype"
                ap = pipeBase.InputOnlyArgumentParser(name="argumentParser")
                dsType = pipeBase.DatasetArgument(name=name, help=dsTypeHelp, default=default)
                self.assertEqual(dsType.help, dsTypeHelp)

                ap.add_id_argument("--id", dsType, "help text")
                namespace = ap.parse_args(
                    config=self.config,
                    args=[DataPath,
                          argName, "calexp",
                          "--id", "visit=2",
                          ],
                )
                self.assertEqual(namespace.id.datasetType, "calexp")
                self.assertEqual(len(namespace.id.idList), 1)

                del namespace

                if default is None:
                    # make sure dataset type argument is required
                    with self.assertRaises(SystemExit):
                        ap.parse_args(
                            config=self.config,
                            args=[DataPath,
                                  "--id", "visit=2",
                                  ],
                        )
                else:
                    namespace = ap.parse_args(
                        config=self.config,
                        args=[DataPath,
                              "--id", "visit=2",
                              ],
                    )
                    self.assertEqual(namespace.id.datasetType, default)
                    self.assertEqual(len(namespace.id.idList), 1)

    def testDatasetArgumentPositional(self):
        """Test DatasetArgument with a positional argument"""
        name = "foo"
        defaultDsTypeHelp = "dataset type to process from input data repository"
        ap = pipeBase.InputOnlyArgumentParser(name="argumentParser")
        dsType = pipeBase.DatasetArgument(name=name)
        self.assertEqual(dsType.help, defaultDsTypeHelp)

        ap.add_id_argument("--id", dsType, "help text")
        namespace = ap.parse_args(
            config=self.config,
            args=[DataPath,
                  "calexp",
                  "--id", "visit=2",
                  ],
        )
        self.assertEqual(namespace.id.datasetType, "calexp")
        self.assertEqual(len(namespace.id.idList), 1)

        # make sure dataset type argument is required
        with self.assertRaises(SystemExit):
            ap.parse_args(
                config=self.config,
                args=[DataPath,
                      "--id", "visit=2",
                      ],
            )

    def testConfigDatasetTypeFieldDefault(self):
        """Test ConfigDatasetType with a config field that has a default value"""
        # default value for config field "dsType" is "calexp";
        # use a different value as the default for the ConfigDatasetType
        # so the test can tell the difference
        name = "dsType"
        ap = pipeBase.InputOnlyArgumentParser(name="argumentParser")
        dsType = pipeBase.ConfigDatasetType(name=name)

        ap.add_id_argument("--id", dsType, "help text")
        namespace = ap.parse_args(
            config=self.config,
            args=[DataPath,
                  "--id", "visit=2",
                  ],
        )
        self.assertEqual(namespace.id.datasetType, "calexp")  # default of config field dsType
        self.assertEqual(len(namespace.id.idList), 1)

    def testConfigDatasetTypeNoFieldDefault(self):
        """Test ConfigDatasetType with a config field that has no default value"""
        name = "dsTypeNoDefault"
        ap = pipeBase.InputOnlyArgumentParser(name="argumentParser")
        dsType = pipeBase.ConfigDatasetType(name=name)

        ap.add_id_argument("--id", dsType, "help text")
        # neither the argument nor the config field has a default,
        # so the user must specify the argument (or specify doMakeDataRefList=False
        # and post-process the ID list)
        with self.assertRaises(RuntimeError):
            ap.parse_args(
                config=self.config,
                args=[DataPath,
                      "--id", "visit=2",
                      ],
            )

    def testOutputs(self):
        """Test output directories, specified in different ways"""
        parser = pipeBase.ArgumentParser(name="argumentParser")
        self.assertTrue(parser.requireOutput)

        path = os.path.join(DataPath, "testOutputs")

        # Specified by "--output"
        args = parser.parse_args(config=self.config, args=[DataPath, "--output", path])
        self.assertEqual(args.input, DataPath)
        self.assertEqual(args.output, path)
        self.assertIsNone(args.rerun)

        # Specified by rerun
        args = parser.parse_args(config=self.config, args=[DataPath, "--rerun", "foo"])
        self.assertEqual(args.input, DataPath)
        self.assertEqual(args.output, os.path.join(DataPath, "rerun", "foo"))
        self.assertEqual(args.rerun, ["foo"])

        # Specified by multiple reruns
        args = parser.parse_args(config=self.config, args=[DataPath, "--rerun", "foo:bar"])
        self.assertEqual(args.input, os.path.join(DataPath, "rerun", "foo"))
        self.assertEqual(args.output, os.path.join(DataPath, "rerun", "bar"))
        self.assertEqual(args.rerun, ["foo", "bar"])

        # Unspecified
        with self.assertRaises(SystemExit):
            parser.parse_args(config=self.config, args=[DataPath, ])


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()

if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
