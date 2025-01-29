# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Simple unit test for execution_reports."""

import unittest

from lsst.pipe.base.execution_reports import QuantumGraphExecutionReport
from lsst.pipe.base.tests import simpleQGraph
from lsst.utils.tests import temporaryDirectory


class ExecutionReportsTestCase(unittest.TestCase):
    """Test execution reports.

    More tests are in lsst/ci_middleware/tests/test_prod_outputs.py
    """

    def test_make_reports(self) -> None:
        """Test make_reports to verify that output exists."""
        with temporaryDirectory() as root:
            # make a simple qgraph to make an execution report on
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root)
            report = QuantumGraphExecutionReport.make_reports(butler, qgraph)
            # make a summary dictionary with a certain amount of
            # expected failures and check that they are there
            exp_failures = report.to_summary_dict(butler, do_store_logs=False)
            self.assertIsNotNone(exp_failures["task0"]["failed_quanta"])
            self.assertEqual(exp_failures["task1"]["outputs"]["add_dataset2"]["blocked"], 1)
            self.assertDictEqual(exp_failures["task2"]["failed_quanta"], {})
            self.assertEqual(exp_failures["task3"]["outputs"]["add_dataset4"]["produced"], 0)
            self.assertEqual(exp_failures["task4"]["n_quanta_blocked"], 1)
            self.assertIsNotNone(exp_failures["task0"]["n_expected"])
            # now we'll make a human-readable summary dict and
            # repeat the tests:
            hr_exp_failures = report.to_summary_dict(butler, do_store_logs=False, human_readable=True)
            self.assertIsNotNone(hr_exp_failures["task0"]["failed_quanta"])
            self.assertEqual(hr_exp_failures["task1"]["outputs"]["add_dataset2"]["blocked"], 1)
            self.assertListEqual(hr_exp_failures["task2"]["failed_quanta"], [])
            self.assertEqual(hr_exp_failures["task3"]["outputs"]["add_dataset4"]["produced"], 0)
            self.assertEqual(hr_exp_failures["task4"]["n_quanta_blocked"], 1)
            self.assertIsNotNone(hr_exp_failures["task0"]["n_expected"])
