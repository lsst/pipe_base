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

import logging
import unittest

import lsst.utils.tests
from lsst.daf.butler import Butler
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry
from lsst.pipe.base import PipelineGraph, QuantumGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
)
from lsst.resources import ResourcePath

_LOG = logging.getLogger(__name__)


class AllDimensionsQuantumGraphBuilderTestCase(unittest.TestCase):
    """Tests for AllDimensionsQuantumGraphBuilder with various interesting
    combinations of dimensions.
    """

    @staticmethod
    def make_butler() -> Butler:
        DATA_ROOT = ResourcePath("resource://lsst.daf.butler/tests/registry_data", forceDirectory=True)
        return create_populated_sqlite_registry(
            *[DATA_ROOT.join(filename) for filename in ("base.yaml", "spatial.yaml")]
        )

    def setUp(self):
        self.butler = self.make_butler()

    def tearDown(self):
        del self.butler
        return super().tearDown()

    def test_one_to_one(self) -> None:
        """Test building a QG with a single task whose inputs and outputs and
        quanta all have the same dimensions.
        """
        config = DynamicTestPipelineTaskConfig()
        config.dimensions = ["visit", "detector"]
        config.inputs["i1"] = DynamicConnectionConfig(
            dataset_type_name="d1", dimensions=["visit", "detector"]
        )
        config.outputs["o2"] = DynamicConnectionConfig(
            dataset_type_name="d2", dimensions=["visit", "detector"]
        )
        pipeline_graph = PipelineGraph(universe=self.butler.dimensions)
        pipeline_graph.add_task("t1", DynamicTestPipelineTask, config=config)
        pipeline_graph.resolve(self.butler.registry)
        self._insert_overall_inputs(pipeline_graph)
        builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            self.butler,
            input_collections=["c1"],
            output_run="c2",
        )
        qg: QuantumGraph = builder.build(attach_datastore_records=False)
        quanta = qg.get_task_quanta("t1")
        self.assertEqual(len(quanta), 8)  # 2 visits x 4 detectors

    def _insert_overall_inputs(self, pipeline_graph: PipelineGraph) -> None:
        """Insert overall-input datasets for a pipeline graph."""
        self.butler.collections.register("c1")
        for _, node in pipeline_graph.iter_overall_inputs():
            self.butler.registry.registerDatasetType(node.dataset_type)
            data_ids = self.butler.query_data_ids(node.dimensions, explain=False)
            self.butler.registry.insertDatasets(node.dataset_type, data_ids, run="c1")


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
