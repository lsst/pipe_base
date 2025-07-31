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
import unittest

from lsst.daf.butler.tests.utils import makeTestTempDir, removeTestTempDir
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.pipe.base.tests.simpleQGraph import AddTaskFactoryMock, makeSimpleQGraph

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class SingleQuantumExecutorTestCase(unittest.TestCase):
    """Tests for SingleQuantumExecutor implementation."""

    instrument = "lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"

    def setUp(self) -> None:
        self.root = makeTestTempDir(TESTDIR)

    def tearDown(self) -> None:
        removeTestTempDir(self.root)

    def test_simple_execute(self) -> None:
        """Run execute() method in simplest setup."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        task_factory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler=butler, task_factory=task_factory)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(task_factory.countExec, 1)

        # There must be one dataset of task's output connection
        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)

    def test_skip_existing_execute(self) -> None:
        """Run execute() method twice, with skip_existing_in."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        task_factory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler=butler, task_factory=task_factory)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(task_factory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_1 = refs[0].id

        # Re-run it with skip_existing_in, it should not run.
        assert butler.run is not None
        executor = SingleQuantumExecutor(
            butler=butler, task_factory=task_factory, skip_existing_in=[butler.run]
        )
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(task_factory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_2 = refs[0].id
        self.assertEqual(dataset_id_1, dataset_id_2)

    def test_clobber_outputs_execute(self) -> None:
        """Run execute() method twice, with clobber_outputs."""
        nQuanta = 1
        butler, qgraph = makeSimpleQGraph(nQuanta, root=self.root, instrument=self.instrument)

        nodes = list(qgraph)
        self.assertEqual(len(nodes), nQuanta)
        node = nodes[0]

        task_factory = AddTaskFactoryMock()
        executor = SingleQuantumExecutor(butler=butler, task_factory=task_factory)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(task_factory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_1 = refs[0].id

        original_dataset = butler.get(refs[0])

        # Remove the dataset ourself, and replace it with something
        # different so we can check later whether it got replaced.
        butler.pruneDatasets([refs[0]], disassociate=False, unstore=True, purge=False)
        replacement = original_dataset + 10
        butler.put(replacement, refs[0])

        # Re-run it with clobber_outputs and skip_existing_in, it should not
        # clobber but should skip instead.
        assert butler.run is not None
        executor = SingleQuantumExecutor(
            butler=butler, task_factory=task_factory, skip_existing_in=[butler.run], clobber_outputs=True
        )
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(task_factory.countExec, 1)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_2 = refs[0].id
        self.assertEqual(dataset_id_1, dataset_id_2)

        second_dataset = butler.get(refs[0])
        self.assertEqual(list(second_dataset), list(replacement))

        # Re-run it with clobber_outputs but without skip_existing_in, it
        # should clobber.
        assert butler.run is not None
        executor = SingleQuantumExecutor(butler=butler, task_factory=task_factory, clobber_outputs=True)
        executor.execute(node.task_node, node.quantum)
        self.assertEqual(task_factory.countExec, 2)

        refs = list(butler.registry.queryDatasets("add_dataset1", collections=butler.run))
        self.assertEqual(len(refs), 1)
        dataset_id_3 = refs[0].id

        third_dataset = butler.get(refs[0])
        self.assertEqual(list(third_dataset), list(original_dataset))

        # No change in UUID even after replacement
        self.assertEqual(dataset_id_1, dataset_id_3)


if __name__ == "__main__":
    unittest.main()
