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

"""Tests of things related to the GraphBuilder class."""

import io
import logging
import unittest

import lsst.utils.tests
from lsst.daf.butler.registry import UserExpressionError
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import DatasetQueryConstraintVariant
from lsst.pipe.base.tests import simpleQGraph
from lsst.utils.tests import temporaryDirectory

_LOG = logging.getLogger(__name__)


class GraphBuilderTestCase(unittest.TestCase):
    """Test graph building."""

    def _assertGraph(self, graph: QuantumGraph) -> None:
        """Check basic structure of the graph."""
        for taskDef in graph.iterTaskGraph():
            refs = graph.initOutputRefs(taskDef)
            # task has one initOutput, second ref is for config dataset
            self.assertEqual(len(refs), 2)

        self.assertEqual(len(list(graph.inputQuanta)), 1)

        # This includes only "packages" dataset for now.
        refs = graph.globalInitOutputRefs()
        self.assertEqual(len(refs), 1)

    def testDefault(self):
        """Simple test to verify makeSimpleQGraph can be used to make a Quantum
        Graph.
        """
        with temporaryDirectory() as root:
            # makeSimpleQGraph calls GraphBuilder.
            butler, qgraph = simpleQGraph.makeSimpleQGraph(root=root)
            # by default makeSimpleQGraph makes a graph with 5 nodes
            self.assertEqual(len(qgraph), 5)
            self._assertGraph(qgraph)
            constraint = DatasetQueryConstraintVariant.OFF
            _, qgraph2 = simpleQGraph.makeSimpleQGraph(
                butler=butler, datasetQueryConstraint=constraint, callPopulateButler=False
            )
            # When all outputs are random resolved refs, direct comparison
            # of graphs does not work because IDs are different. Can only
            # verify the number of quanta in the graph without doing something
            # terribly complicated.
            self.assertEqual(len(qgraph2), 5)
            constraint = DatasetQueryConstraintVariant.fromExpression("add_dataset0")
            _, qgraph3 = simpleQGraph.makeSimpleQGraph(
                butler=butler, datasetQueryConstraint=constraint, callPopulateButler=False
            )
            self.assertEqual(len(qgraph3), 5)

    # Inconsistent governor dimensions are no longer an error, so this test
    # fails with the new query system. We should probably check instead that
    # logging includes an explanation for the empty QG, but it might not
    # because explain_no_results isn't good enough yet.
    @unittest.expectedFailure
    def testAddInstrumentMismatch(self):
        """Verify that a RuntimeError is raised if the instrument in the user
        query does not match the instrument in the pipeline.
        """
        with temporaryDirectory() as root:
            pipeline = simpleQGraph.makeSimplePipeline(
                nQuanta=5, instrument="lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"
            )
            with self.assertRaises(UserExpressionError):
                simpleQGraph.makeSimpleQGraph(root=root, pipeline=pipeline, userQuery="instrument = 'foo'")

    def testUserQueryBind(self):
        """Verify that bind values work for user query."""
        pipeline = simpleQGraph.makeSimplePipeline(
            nQuanta=5, instrument="lsst.pipe.base.tests.simpleQGraph.SimpleInstrument"
        )
        instr = simpleQGraph.SimpleInstrument.getName()
        # With a literal in the user query
        with temporaryDirectory() as root:
            simpleQGraph.makeSimpleQGraph(root=root, pipeline=pipeline, userQuery=f"instrument = '{instr}'")
        # With a bind for the user query
        with temporaryDirectory() as root:
            simpleQGraph.makeSimpleQGraph(
                root=root, pipeline=pipeline, userQuery="instrument = instr", bind={"instr": instr}
            )

    def test_datastore_records(self):
        """Test for generating datastore records."""
        with temporaryDirectory() as root:
            # need FileDatastore for this tests
            butler, qgraph1 = simpleQGraph.makeSimpleQGraph(
                root=root, inMemory=False, makeDatastoreRecords=True
            )

            # save and reload
            buffer = io.BytesIO()
            qgraph1.save(buffer)
            buffer.seek(0)
            qgraph2 = QuantumGraph.load(buffer, universe=butler.dimensions)
            del buffer

            for qgraph in (qgraph1, qgraph2):
                self.assertEqual(len(qgraph), 5)
                for i, qnode in enumerate(qgraph):
                    quantum = qnode.quantum
                    self.assertIsNotNone(quantum.datastore_records)
                    # only the first quantum has a pre-existing input
                    if i == 0:
                        datastore_name = "FileDatastore@<butlerRoot>"
                        self.assertEqual(set(quantum.datastore_records.keys()), {datastore_name})
                        records_data = quantum.datastore_records[datastore_name]
                        records = dict(records_data.records)
                        self.assertEqual(len(records), 1)
                        _, records = records.popitem()
                        records = records["file_datastore_records"]
                        self.assertEqual(
                            [record.path for record in records],
                            ["test/add_dataset0/add_dataset0_INSTR_det0_test.pickle"],
                        )
                    else:
                        self.assertEqual(quantum.datastore_records, {})


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
