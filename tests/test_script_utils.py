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

"""Tests for scripts/utils.py functions."""

import logging
import unittest

from lsst.pipe.base.script.utils import filter_by_existence


class FilterByExistenceTestCase(unittest.TestCase):
    """Test filter_by_existence function."""

    def setUp(self):
        # Currently function itself does not need real DatasetRefs.
        # Only the butler calls requires DatasetRefs, but butler
        # is being mocked in these tests. So using strings in place
        # of DatasetRefs.
        self.orig_refs = [f"{i:05}" for i in range(1, 11)]
        self.mock_exists = {}
        for i in range(1, 11):
            self.mock_exists[f"{i:05}"] = True if i % 3 == 0 else False
        self.filtered_refs = [f"{i:05}" for i in range(1, 11) if i % 3 != 0]

    def test_success(self):
        butler_mock = unittest.mock.Mock()
        butler_mock._datastore.knows_these.return_value = self.mock_exists
        with self.assertLogs(logging.getLogger("lsst.pipe.base"), level="VERBOSE") as cm:
            filtered = filter_by_existence(butler_mock, self.orig_refs)
            self.assertCountEqual(self.filtered_refs, filtered)
        log_messages = " ".join(cm.output)
        self.assertIn("Filtering out datasets already known to the target butler", log_messages)
        self.assertIn("number of datasets to transfer", log_messages)


if __name__ == "__main__":
    unittest.main()
