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

import unittest
from collections.abc import Sequence

import lsst.pipe.base as taskBase
import yaml


class TaskTestCase(unittest.TestCase):
    """A test case for TaskMetadata
    """

    def setUp(self):
        self.meta = taskBase.task_metadata.TaskMetadata({})
        self.fullmeta = taskBase.task_metadata.TaskMetadata({'t1': self.meta})

    def tearDown(self):
        self.meta = None
        self.fullmeta = None

    def testTaskMetadata(self):
        self.meta['i1'] = 'item_1'
        self.fullmeta['t2.i2'] = 2

        self.assertTrue('i1' in self.meta)
        self.assertEqual(self.meta['i1'], 'item_1')
        self.assertTrue('t1.i1' in self.fullmeta)
        self.assertEqual(self.fullmeta['t1.i1'], 'item_1')
        self.assertTrue('t2.i2' in self.fullmeta)
        self.assertEqual(self.fullmeta['t2.i2'], 2)
        self.assertTrue(isinstance(self.fullmeta['t2'], taskBase.task_metadata.TaskMetadata))
        # test deprecated method
        self.assertTrue(self.fullmeta.names(False).issuperset({'t1.i1', 't2.i2'}))

        # test deprecated method
        self.meta.add('i1', 'item_11')
        self.assertTrue(isinstance(self.meta['i1'], Sequence))
        self.assertTrue(isinstance(self.fullmeta['t1.i1'], Sequence))
        # test deprecated method
        self.fullmeta.add('t2.i3', 3)
        self.assertEqual(self.fullmeta['t2.i3'], 3)

        # test serialization and deserialization
        ser = yaml.dump(self.fullmeta)
        deser = yaml.safe_load(ser)
        self.assertEqual(deser['t2.i3'], 3)

        deleted = self.fullmeta.pop('t2.i3')
        self.assertEqual(deleted, 3)
        self.assertFalse('t2.i3' in self.fullmeta)
