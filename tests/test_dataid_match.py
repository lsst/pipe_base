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

from lsst.pipe.base.tests.mocks import DataIdMatch


class DataIdMatchTestCase(unittest.TestCase):
    """A test case for DataidMatch class."""

    dataIds = (
        {"instrument": "INSTR", "detector": 1, "number": 4},
        {"instrument": "INSTR", "detector": 2, "number": 3},
        {"instrument": "LSST", "detector": 3, "number": 2},
        {"instrument": "LSST", "detector": 4, "number": 1},
    )

    def test_strings(self):
        """Tests for string comparisons method."""
        tests = (
            ("instrument = 'INSTR'", [True, True, False, False]),
            ("instrument = 'LSST'", [False, False, True, True]),
            ("instrument < 'LSST'", [True, True, False, False]),
            ("instrument IN ('LSST', 'INSTR')", [True, True, True, True]),
        )

        for expr, result in tests:
            dataIdMatch = DataIdMatch(expr)
            self.assertEqual([dataIdMatch.match(dataId) for dataId in self.dataIds], result)

    def test_comparisons(self):
        """Test all supported comparison operators."""
        tests = (
            ("detector = 1", [True, False, False, False]),
            ("detector != 1", [False, True, True, True]),
            ("detector > 2", [False, False, True, True]),
            ("2 <= detector", [False, True, True, True]),
            ("2 > detector", [True, False, False, False]),
            ("2 >= detector", [True, True, False, False]),
        )

        for expr, result in tests:
            dataIdMatch = DataIdMatch(expr)
            self.assertEqual([dataIdMatch.match(dataId) for dataId in self.dataIds], result)

    def test_arith(self):
        """Test all supported arithmetical operators."""
        tests = (
            ("detector + number = 5", [True, True, True, True]),
            ("detector - number = 1", [False, False, True, False]),
            ("detector * number = 6", [False, True, True, False]),
            ("detector / number = 1.5", [False, False, True, False]),
            ("detector % number = 1", [True, False, True, False]),
            ("+detector = 1", [True, False, False, False]),
            ("-detector = -4", [False, False, False, True]),
        )

        for expr, result in tests:
            dataIdMatch = DataIdMatch(expr)
            self.assertEqual([dataIdMatch.match(dataId) for dataId in self.dataIds], result)

    def test_logical(self):
        """Test all supported logical operators."""
        tests = (
            ("detector = 1 OR instrument = 'LSST'", [True, False, True, True]),
            ("detector = 1 AND instrument = 'INSTR'", [True, False, False, False]),
            ("NOT detector = 1", [False, True, True, True]),
        )

        for expr, result in tests:
            dataIdMatch = DataIdMatch(expr)
            self.assertEqual([dataIdMatch.match(dataId) for dataId in self.dataIds], result)

    def test_parens(self):
        """Test parentheses."""
        tests = (("(detector = 1 OR number = 1) AND instrument = 'LSST'", [False, False, False, True]),)

        for expr, result in tests:
            dataIdMatch = DataIdMatch(expr)
            self.assertEqual([dataIdMatch.match(dataId) for dataId in self.dataIds], result)

    def test_in(self):
        """Test IN expression."""
        tests = (
            ("detector in (1, 3, 2)", [True, True, True, False]),
            ("detector not in (1, 3, 2)", [False, False, False, True]),
            ("detector in (1..4:2)", [True, False, True, False]),
            ("detector in (1..4:2, 4)", [True, False, True, True]),
        )

        for expr, result in tests:
            dataIdMatch = DataIdMatch(expr)
            self.assertEqual([dataIdMatch.match(dataId) for dataId in self.dataIds], result)

    def test_errors(self):
        """Test for errors in expressions."""
        dataId = {"instrument": "INSTR", "detector": 1}

        # Unknown identifier
        expr = "INSTRUMENT = 'INSTR'"
        dataIdMatch = DataIdMatch(expr)
        with self.assertRaisesRegex(KeyError, "INSTRUMENT"):
            dataIdMatch.match(dataId)

        # non-boolean expression
        expr = "instrument"
        dataIdMatch = DataIdMatch(expr)
        with self.assertRaisesRegex(TypeError, "Expression 'instrument' returned non-boolean object"):
            dataIdMatch.match(dataId)

        # operations on unsupported combination of types
        expr = "instrument - detector = 0"
        dataIdMatch = DataIdMatch(expr)
        with self.assertRaisesRegex(TypeError, "unsupported operand type"):
            dataIdMatch.match(dataId)

        # function calls are not implemented
        expr = "POINT(2, 1) != POINT(1, 2)"
        dataIdMatch = DataIdMatch(expr)
        with self.assertRaises(NotImplementedError):
            dataIdMatch.match(dataId)


if __name__ == "__main__":
    unittest.main()
