# This file is part of pipe_base.
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

"""Tests of the DataIdContainer class."""

import unittest
import unittest.mock

try:
    import lsst.daf.persistence as dafPersistence
except ImportError:
    dafPersistence = None
try:
    from lsst.pipe.base import DataIdContainer
except ImportError:
    DataIdContainer = None
import lsst.utils.tests


@unittest.skipIf(DataIdContainer is None or dafPersistence is None, "Gen2 infrastructure is not available")
class DataIdContainerTestCase(lsst.utils.tests.TestCase):
    def setUp(self):
        self.container = DataIdContainer()
        self.butler = unittest.mock.MagicMock(spec=dafPersistence.Butler)

    def test_castDataIdsNoneDatasetType(self):
        """Raise RuntimeError if we haven't set container.datasetType."""
        with self.assertRaises(RuntimeError):
            self.container.castDataIds(self.butler)

    def test_makeDataRefListNoneDatasetType(self):
        """Raise RuntimeError if we haven't set container.datasetType."""
        with self.assertRaises(RuntimeError):
            self.container.makeDataRefList(self.butler)

    def test_castDataIdsRaiseKeyError(self):
        """Test that castDataIds re-raises a butler.getKeys() exception."""
        self.container.setDatasetType("nonsense!")
        self.butler.getKeys.side_effect = KeyError
        with self.assertRaises(KeyError) as cm:
            self.container.castDataIds(self.butler)
        self.assertIsInstance(cm.exception.__cause__, KeyError)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
