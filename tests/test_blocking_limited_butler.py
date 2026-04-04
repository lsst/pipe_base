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

import logging
import os
import unittest

import lsst.utils.tests
from lsst.daf.butler import DataCoordinate, DatasetRef
from lsst.pipe.base.blocking_limited_butler import _LOG, BlockingLimitedButler
from lsst.pipe.base.tests.mocks import InMemoryRepo

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class BlockingLimitedButlerTestCase(unittest.TestCase):
    """Unit tests for BlockingLimitedButler"""

    def test_no_block_nonexistent(self):
        """Test checking/getting with no dataset and blocking disabled."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task()
        helper.pipeline_graph.resolve(helper.butler.registry)
        ref = DatasetRef(
            helper.pipeline_graph.dataset_types["dataset_auto0"].dataset_type,
            DataCoordinate.make_empty(helper.butler.dimensions),
            run="input_run",
        )
        helper.pipeline_graph.register_dataset_types(helper.butler)
        in_memory_butler = helper.make_limited_butler()
        blocking_butler = BlockingLimitedButler(in_memory_butler, timeouts={})
        with self.assertNoLogs(_LOG, level=logging.INFO):
            self.assertFalse(blocking_butler.stored_many([ref])[ref])
            with self.assertRaises(FileNotFoundError):
                blocking_butler.get(ref)

    def test_timeout_nonexistent(self):
        """Test checking/getting with no dataset, leading to a timeout."""
        helper = InMemoryRepo("base.yaml")
        helper.add_task()
        helper.pipeline_graph.resolve(helper.butler.registry)
        ref = DatasetRef(
            helper.pipeline_graph.dataset_types["dataset_auto0"].dataset_type,
            DataCoordinate.make_empty(helper.butler.dimensions),
            run="input_run",
        )
        helper.pipeline_graph.register_dataset_types(helper.butler)
        in_memory_butler = helper.make_limited_butler()
        blocking_butler = BlockingLimitedButler(in_memory_butler, timeouts={"dataset_auto0": 0.1})
        with self.assertLogs(_LOG, level=logging.INFO) as cm:
            self.assertFalse(blocking_butler.stored_many([ref])[ref])
        self.assertIn("not immediately available", cm.output[0])
        with self.assertLogs(_LOG, level=logging.INFO) as cm:
            with self.assertRaises(FileNotFoundError):
                blocking_butler.get(ref)
        self.assertIn("not immediately available", cm.output[0])

    def test_no_waiting_if_exists(self):
        """Test checking/getting with dataset present immediately, so no
        waiting should be necessary.
        """
        helper = InMemoryRepo("base.yaml")
        helper.add_task()
        (ref,) = helper.insert_datasets("dataset_auto0")
        helper.pipeline_graph.register_dataset_types(helper.butler)
        in_memory_butler = helper.make_limited_butler()
        blocking_butler = BlockingLimitedButler(in_memory_butler, timeouts={})
        with self.assertNoLogs(_LOG, level=logging.INFO):
            self.assertTrue(blocking_butler.stored_many([ref])[ref])
            self.assertIsNotNone(blocking_butler.get(ref))


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
