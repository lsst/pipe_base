# This file is part of task_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import sys
import io
import unittest
import textwrap

# import lsst.utils.tests
import lsst.pex.config as pexConfig
import lsst.pipe.base as taskBase


class SimpleTaskConfig(pexConfig.Config):
    ff3 = pexConfig.Field(doc="float 1", dtype=float, default=3.1)
    sf3 = pexConfig.Field(doc="str 1", dtype=str, default="default for sf1")


class SimpleTask(taskBase.Task):
    ConfigClass = SimpleTaskConfig


class TaskWithSubtasksConfig(pexConfig.Config):
    sst1 = SimpleTask.makeField(doc="sub-subtask 1")
    sst2 = SimpleTask.makeField(doc="sub-subtask 2")
    ff2 = pexConfig.Field(doc="float 1", dtype=float, default=3.1)
    sf2 = pexConfig.Field(doc="str 1", dtype=str, default="default for sf1")


class TaskWithSubtasks(taskBase.Task):
    ConfigClass = TaskWithSubtasksConfig


class MainTaskConfig(pexConfig.Config):
    st1 = TaskWithSubtasks.makeField(doc="subtask 1")
    st2 = TaskWithSubtasks.makeField(doc="subtask 2")
    ff1 = pexConfig.Field(doc="float 2", dtype=float, default=3.1)
    sf1 = pexConfig.Field(doc="str 2", dtype=str, default="default for strField")


class MainTask(taskBase.Task):
    ConfigClass = MainTaskConfig


c = MainTaskConfig()


class ShowTasksTestCase(unittest.TestCase):
    """A test case for task_utils.showTaskHierarchy,
    the code that implements ArgumentParser's --show tasks option.
    """

    def testBasicShowTaskHierarchy(self):
        """Test basic usage of show
        """
        config = MainTaskConfig()
        expectedData = """
        Subtasks:
        st1: {0}.TaskWithSubtasks
        st1.sst1: {0}.SimpleTask
        st1.sst2: {0}.SimpleTask
        st2: {0}.TaskWithSubtasks
        st2.sst1: {0}.SimpleTask
        st2.sst2: {0}.SimpleTask
        """.format(__name__)
        tempStdOut = io.StringIO()
        savedStdOut, sys.stdout = sys.stdout, tempStdOut
        try:
            taskBase.task_utils.showTaskHierarchy(config)
        finally:
            sys.stdout = savedStdOut
        formatRead = tempStdOut.getvalue().strip()
        formatExpected = textwrap.dedent(expectedData).strip()
        self.assertEqual(formatRead, formatExpected)
#
#
# class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
#     pass
#
#
# def setup_module(module):
#     lsst.utils.tests.init()


if __name__ == "__main__":
    # lsst.utils.tests.init()
    unittest.main()
