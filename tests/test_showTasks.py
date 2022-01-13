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
import io
import sys
import textwrap
import unittest

import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

try:
    import lsst.pipe.base.argumentParser as argumentParser
except ImportError:
    argumentParser = None
import lsst.utils.tests


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


@unittest.skipIf(argumentParser is None, "Gen2 argument parser is not available")
class ShowTasksTestCase(unittest.TestCase):
    """A test case for the code that implements ArgumentParser's --show tasks
    option.
    """

    def testBasicShowTaskHierarchy(self):
        """Test basic usage of show"""
        config = MainTaskConfig()
        expectedData = """
        Subtasks:
        st1: {0}.TaskWithSubtasks
        st1.sst1: {0}.SimpleTask
        st1.sst2: {0}.SimpleTask
        st2: {0}.TaskWithSubtasks
        st2.sst1: {0}.SimpleTask
        st2.sst2: {0}.SimpleTask
        """.format(
            __name__
        )
        tempStdOut = io.StringIO()
        savedStdOut, sys.stdout = sys.stdout, tempStdOut
        try:
            argumentParser.showTaskHierarchy(config)
        finally:
            sys.stdout = savedStdOut
        formatRead = tempStdOut.getvalue().strip()
        formatExpected = textwrap.dedent(expectedData).strip()
        self.assertEqual(formatRead, formatExpected)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
