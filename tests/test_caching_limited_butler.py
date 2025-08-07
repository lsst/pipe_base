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

"""Tests for execution butler."""

import os
import unittest

import lsst.utils.tests
from lsst.pipe.base.caching_limited_butler import CachingLimitedButler
from lsst.pipe.base.tests.mocks import InMemoryRepo

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class CachingLimitedButlerTestCase(unittest.TestCase):
    """A very minimal test of CachingLimitedButler"""

    # TODO: expand these tests in DM-43353

    def test_init(self):
        helper = InMemoryRepo("base.yaml")
        helper.add_task()
        refs = helper.insert_datasets("dataset_auto0")
        in_memory_butler = helper.make_limited_butler()
        caching_butler = CachingLimitedButler(in_memory_butler)
        mock_dataset = caching_butler.get(refs[0])
        self.assertIsNotNone(mock_dataset)
        with caching_butler.record_metrics() as metrics:
            caching_butler.get(refs[0])
            self.assertEqual(metrics.n_get, 1)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
