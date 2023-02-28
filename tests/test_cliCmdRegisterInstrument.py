# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit tests for daf_butler CLI register-instrument command.
"""

import unittest

from lsst.daf.butler.tests import CliCmdTestBase
from lsst.pipe.base.cli.cmd import register_instrument


class RegisterInstrumentTest(CliCmdTestBase, unittest.TestCase):
    mockFuncName = "lsst.pipe.base.cli.cmd.commands.script.register_instrument"

    @staticmethod
    def defaultExpected():
        return dict()

    @staticmethod
    def command():
        return register_instrument

    def test_repoBasic(self):
        """Test the most basic required arguments."""
        self.run_test(
            ["register-instrument", "here", "a.b.c"],
            self.makeExpected(repo="here", instrument=("a.b.c",), update=False),
        )

    def test_missing(self):
        """test a missing argument"""
        self.run_missing(["register-instrument"], "Missing argument ['\"]REPO['\"]")
        self.run_missing(["register-instrument", "here"], "Missing argument ['\"]INSTRUMENT ...['\"]")


if __name__ == "__main__":
    unittest.main()
