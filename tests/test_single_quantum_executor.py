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

from __future__ import annotations

import os
import time
import unittest

import lsst.pipe.base.automatic_connection_constants as acc
from lsst.pipe.base.resource_usage import QuantumResourceUsage
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.pipe.base.tests.mocks import InMemoryRepo

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SingleQuantumExecutorTestCase(unittest.TestCase):
    """Tests for SingleQuantumExecutor implementation."""

    def test_simple_execute(self) -> None:
        """Run execute() method in simplest setup."""
        helper = InMemoryRepo("base.yaml")
        self.enterContext(helper)
        helper.add_task()
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        executor, butler = helper.make_single_quantum_executor()
        nQuanta = 1
        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]
        t1 = time.time()
        executor.execute(node.task_node, node.quantum)
        t2 = time.time()
        # There must be one dataset of task's output connection
        self.assertEqual(len(butler.get_datasets("dataset_auto1")), 1)
        # Test that we can construct resource usage information from the
        # metadata.
        (md,) = butler.get_datasets(acc.METADATA_OUTPUT_TEMPLATE.format(label="task_auto1")).values()
        ru = QuantumResourceUsage.from_task_metadata(md)
        self.assertIsNotNone(ru)
        self.assertGreater(ru.memory, 0)
        self.assertGreater(ru.prep_time, 0)
        self.assertGreater(ru.init_time, 0)
        self.assertGreater(ru.run_time, 0)
        self.assertGreater(ru.run_time_cpu, 0)
        self.assertGreater(ru.total_time, 0)
        self.assertLess(ru.total_time, t2 - t1)

    def test_skip_existing_execute(self) -> None:
        """Run execute() method twice, with skip_existing_in."""
        helper = InMemoryRepo("base.yaml")
        self.enterContext(helper)
        helper.add_task()
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        executor, butler = helper.make_single_quantum_executor()
        nQuanta = 1
        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]
        executor.execute(node.task_node, node.quantum)

        outputs1 = butler.get_datasets("dataset_auto1")
        self.assertEqual(len(outputs1), 1)
        ref1, obj1 = outputs1.popitem()

        # Re-run it with skip_existing, it should not run.  Note that if it did
        # run (and called 'butler.put') that would raise an exception.
        executor = SingleQuantumExecutor(limited_butler_factory=butler.factory, skip_existing=True)
        executor.execute(node.task_node, node.quantum)

        outputs2 = butler.get_datasets("dataset_auto1")
        self.assertEqual(len(outputs2), 1)
        ref2, obj2 = outputs2.popitem()
        self.assertEqual(ref1, ref2)
        # Objects should be the same (but not identities, because the butler
        # will copy them).
        self.assertEqual(obj1, obj2)

    def test_clobber_outputs_execute(self) -> None:
        """Run execute() method twice, with clobber_outputs."""
        helper = InMemoryRepo("base.yaml")
        self.enterContext(helper)
        helper.add_task()
        qgraph = helper.make_quantum_graph_builder().build(attach_datastore_records=False)
        executor, butler = helper.make_single_quantum_executor()
        nQuanta = 1
        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]
        executor.execute(node.task_node, node.quantum)

        outputs1 = butler.get_datasets("dataset_auto1")
        self.assertEqual(len(outputs1), 1)
        ref1, obj1 = outputs1.popitem()

        # Remove the dataset ourself, and replace it with something
        # different so we can check later whether it got replaced.
        butler.pruneDatasets([ref1], disassociate=False, unstore=True, purge=False)
        obj1.quantum = None
        butler.put(obj1, ref1)

        # Re-run it with clobber_outputs and skip_existing, it should not
        # clobber but should skip instead.
        executor = SingleQuantumExecutor(
            limited_butler_factory=butler.factory, skip_existing=True, clobber_outputs=True
        )
        executor.execute(node.task_node, node.quantum)
        outputs2 = butler.get_datasets("dataset_auto1")
        self.assertEqual(len(outputs2), 1)
        ref2, obj2 = outputs2.popitem()
        self.assertEqual(ref1, ref2)
        self.assertEqual(obj1, obj2)

        # Re-run it with clobber_outputs but without skip_existing_in, it
        # should clobber.
        executor = SingleQuantumExecutor(limited_butler_factory=butler.factory, clobber_outputs=True)
        executor.execute(node.task_node, node.quantum)
        outputs3 = butler.get_datasets("dataset_auto1")
        self.assertEqual(len(outputs3), 1)
        ref3, obj3 = outputs3.popitem()
        self.assertEqual(ref1, ref3)
        self.assertNotEqual(obj1, obj3)


if __name__ == "__main__":
    unittest.main()
