# This file is part of ctrl_mpexec.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

import unittest

from lsst.ctrl.mpexec import ExecutionStatus, QuantumReport, Report
from lsst.pipe.base import QgraphSummary


class ReportsTestCase(unittest.TestCase):
    """A test case for reports module."""

    def test_quantumReport(self):
        """Test for QuantumReport class."""
        dataId = {"instrument": "LSST"}
        taskLabel = "task"

        qr = QuantumReport(dataId=dataId, taskLabel=taskLabel)
        self.assertEqual(qr.status, ExecutionStatus.SUCCESS)
        self.assertEqual(qr.dataId, dataId)
        self.assertEqual(qr.taskLabel, taskLabel)
        self.assertIsNone(qr.exitCode)
        self.assertIsNone(qr.exceptionInfo)

        qr = QuantumReport(status=ExecutionStatus.TIMEOUT, dataId=dataId, taskLabel=taskLabel)
        self.assertEqual(qr.status, ExecutionStatus.TIMEOUT)

        qr = QuantumReport.from_exception(
            exception=RuntimeError("runtime error"), dataId=dataId, taskLabel=taskLabel
        )
        self.assertEqual(qr.status, ExecutionStatus.FAILURE)
        self.assertEqual(qr.dataId, dataId)
        self.assertEqual(qr.taskLabel, taskLabel)
        self.assertIsNone(qr.exitCode)
        self.assertEqual(qr.exceptionInfo.className, "RuntimeError")
        self.assertEqual(qr.exceptionInfo.message, "runtime error")

        qr = QuantumReport.from_exit_code(exitCode=0, dataId=dataId, taskLabel=taskLabel)
        self.assertEqual(qr.status, ExecutionStatus.SUCCESS)
        self.assertEqual(qr.dataId, dataId)
        self.assertEqual(qr.taskLabel, taskLabel)
        self.assertEqual(qr.exitCode, 0)
        self.assertIsNone(qr.exceptionInfo)

        qr = QuantumReport.from_exit_code(exitCode=1, dataId=dataId, taskLabel=taskLabel)
        self.assertEqual(qr.status, ExecutionStatus.FAILURE)
        self.assertEqual(qr.dataId, dataId)
        self.assertEqual(qr.taskLabel, taskLabel)
        self.assertEqual(qr.exitCode, 1)
        self.assertIsNone(qr.exceptionInfo)

    def test_report(self):
        """Test for Report class."""
        report = Report(qgraphSummary=QgraphSummary(graphID="uuid"))
        self.assertEqual(report.status, ExecutionStatus.SUCCESS)
        self.assertIsNotNone(report.cmdLine)
        self.assertIsNone(report.exitCode)
        self.assertIsNone(report.exceptionInfo)

        dataId = {"instrument": "LSST"}
        taskLabel = "task"

        qr = QuantumReport.from_exception(
            exception=RuntimeError("runtime error"), dataId=dataId, taskLabel=taskLabel
        )
        report = Report(
            status=ExecutionStatus.FAILURE, exitCode=-1, qgraphSummary=QgraphSummary(graphID="uuid")
        )
        report.set_exception(RuntimeError("runtime error"))
        report.quantaReports.append(qr)
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(report.exitCode, -1)
        self.assertEqual(report.exceptionInfo.className, "RuntimeError")
        self.assertEqual(report.exceptionInfo.message, "runtime error")
        self.assertEqual(len(report.quantaReports), 1)

    def test_json(self):
        """Test for conversion to/from JSON."""
        dataId = {"instrument": "LSST"}
        taskLabel = "task"

        qr = QuantumReport.from_exception(
            exception=RuntimeError("runtime error"), dataId=dataId, taskLabel=taskLabel
        )
        report = Report(
            status=ExecutionStatus.FAILURE, exitCode=-1, qgraphSummary=QgraphSummary(graphID="uuid")
        )
        report.set_exception(RuntimeError("runtime error"))
        report.quantaReports.append(qr)
        json = report.model_dump_json(exclude_none=True, indent=2)
        self.assertIsInstance(json, str)

        report = Report.model_validate_json(json)
        self.assertEqual(report.status, ExecutionStatus.FAILURE)
        self.assertEqual(report.exitCode, -1)
        self.assertEqual(report.exceptionInfo.className, "RuntimeError")
        self.assertEqual(report.exceptionInfo.message, "runtime error")
        self.assertEqual(len(report.quantaReports), 1)
        qr = report.quantaReports[0]
        self.assertEqual(qr.status, ExecutionStatus.FAILURE)
        self.assertEqual(qr.dataId, dataId)
        self.assertEqual(qr.taskLabel, taskLabel)
        self.assertIsNone(qr.exitCode)
        self.assertEqual(qr.exceptionInfo.className, "RuntimeError")
        self.assertEqual(qr.exceptionInfo.message, "runtime error")


if __name__ == "__main__":
    unittest.main()
