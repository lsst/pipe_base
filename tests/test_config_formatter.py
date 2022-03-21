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

"""Tests for PexConfigFormatter.
"""

import os
import unittest

import lsst.pex.config
from lsst.daf.butler import Butler, DatasetType
from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SimpleConfig(lsst.pex.config.Config):
    """Config to use in tests for butler put/get"""

    i = lsst.pex.config.Field("integer test", int)
    c = lsst.pex.config.Field("string", str)


class PexConfigFormatterTestCase(unittest.TestCase):
    """Tests for PexConfigFormatter, using local file datastore."""

    def setUp(self):
        """Create a new butler root for each test."""
        self.root = makeTestTempDir(TESTDIR)
        Butler.makeRepo(self.root)
        self.butler = Butler(self.root, run="test_run")
        # No dimensions in dataset type so we don't have to worry about
        # inserting dimension data or defining data IDs.
        self.datasetType = DatasetType(
            "config", dimensions=(), storageClass="Config", universe=self.butler.registry.dimensions
        )
        self.butler.registry.registerDatasetType(self.datasetType)

    def tearDown(self):
        removeTestTempDir(self.root)

    def testPexConfig(self) -> None:
        """Test that we can put and get pex_config Configs"""
        c = SimpleConfig(i=10, c="hello")
        self.assertEqual(c.i, 10)
        ref = self.butler.put(c, "config")
        butler_c = self.butler.get(ref)
        self.assertEqual(c, butler_c)
        self.assertIsInstance(butler_c, SimpleConfig)


if __name__ == "__main__":
    unittest.main()
