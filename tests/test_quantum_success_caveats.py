# This file is part of pipe_base.
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

from lsst.pipe.base import QuantumSuccessCaveats


class QuantumSuccessCaveatsTestCase(unittest.TestCase):
    """Tests for the QuantumSuccessCaveats flag enum."""

    def test_from_adjust_quantum_no_work(self):
        """Test caveats for the case where adjustQuantum raises NoWorkFound."""
        caveats = QuantumSuccessCaveats.from_adjust_quantum_no_work()
        self.assertEqual(caveats.concise(), "*A")
        self.assertLessEqual(set(caveats.concise()), caveats.legend().keys())

    def test_concise(self):
        """Test the concise representation of caveats."""
        caveats = QuantumSuccessCaveats.ANY_OUTPUTS_MISSING | QuantumSuccessCaveats.UPSTREAM_FAILURE_NO_WORK
        self.assertEqual(caveats.concise(), "+U")
        self.assertLessEqual(set(caveats.concise()), caveats.legend().keys())
        caveats = (
            QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
            | QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
            | QuantumSuccessCaveats.UNPROCESSABLE_DATA
        )
        self.assertEqual(caveats.concise(), "*D")
        self.assertLessEqual(set(caveats.concise()), caveats.legend().keys())
        caveats = QuantumSuccessCaveats.ANY_OUTPUTS_MISSING | QuantumSuccessCaveats.PARTIAL_OUTPUTS_ERROR
        self.assertEqual(caveats.concise(), "+P")
        self.assertLessEqual(set(caveats.concise()), caveats.legend().keys())
        caveats = QuantumSuccessCaveats.NO_WORK
        self.assertEqual(caveats.concise(), "N")
        self.assertLessEqual(set(caveats.concise()), caveats.legend().keys())


if __name__ == "__main__":
    unittest.main()
