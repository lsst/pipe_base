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

import astropy.table

import lsst.utils.tests
from lsst.daf.butler import Butler, DataCoordinate
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry
from lsst.pipe.base import PipelineGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import (
    AllDimensionsQuantumGraphBuilder,
    DatasetQueryConstraintVariant,
)
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
)
from lsst.resources import ResourcePath
from lsst.sphgeom import RangeSet

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
        self.enterContext(self.butler)

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
        qg = builder.finish(attach_datastore_records=False).assemble()
        quanta = qg.quanta_by_task["t1"]
        self.assertEqual(len(quanta), 8)  # 2 visits x 4 detectors

    def test_patch_to_hpx_to_global(self) -> None:
        """Test building a QG with patch inputs and a hierarchy of healpix
        outputs and a final global step.
        """
        pipeline_graph = self._make_hpx_pipeline_graph(
            patch_to_hpx8_and_hpx11=True,
            hpx8_to_hpx5=True,
            hpx5_to_global=True,
        )
        self._insert_overall_inputs(pipeline_graph)
        builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            self.butler,
            input_collections=["c1"],
            output_run="c2",
        )
        qg = builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(len(qg.quanta_by_task["patch_to_hpx8_and_hpx11"]), 22)
        self.assertEqual(len(qg.quanta_by_task["hpx8_to_hpx5"]), 2)
        self.assertEqual(len(qg.quanta_by_task["hpx5_to_global"]), 1)

    def test_patch_to_hpx_to_instrument(self) -> None:
        """Test building a QG with patch inputs and a hierarchy of healpix
        outputs and a final per-instrument step.
        """
        pipeline_graph = self._make_hpx_pipeline_graph(
            patch_to_hpx8_and_hpx11=True,
            hpx8_to_hpx5=True,
            hpx5_to_instrument=True,
        )
        self._insert_overall_inputs(pipeline_graph)
        builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            self.butler,
            input_collections=["c1"],
            output_run="c2",
        )
        qg = builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(len(qg.quanta_by_task["patch_to_hpx8_and_hpx11"]), 22)
        self.assertEqual(len(qg.quanta_by_task["hpx8_to_hpx5"]), 2)
        self.assertEqual(len(qg.quanta_by_task["hpx5_to_instrument"]), 1)

    def test_hpx_to_global_dataset_constraint(self) -> None:
        """Test building a QG with healpix inputs and a hierarchy of healpix
        outputs and a final global step, with the healpix input used as a
        dataset constraint (i.e. the default behavior).
        """
        pipeline_graph = self._make_hpx_pipeline_graph(
            hpx8_to_hpx5=True,
            hpx5_to_global=True,
        )
        self._insert_overall_inputs(pipeline_graph)
        builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            self.butler,
            input_collections=["c1"],
            output_run="c2",
        )
        qg = builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(len(qg.quanta_by_task["hpx8_to_hpx5"]), 2)
        self.assertEqual(len(qg.quanta_by_task["hpx5_to_global"]), 1)

    @unittest.expectedFailure
    def test_hpx_to_global_where_constraint(self) -> None:
        """Test building a QG with healpix inputs and a hierarchy of healpix
        outputs and a final global step, with a 'where' constraint instead of
        a dataset constraint.

        It would be nice for this to work, but we do not currently expect it to
        given the limitations of the butler query system.
        """
        pipeline_graph = self._make_hpx_pipeline_graph(
            hpx8_to_hpx5=True,
            hpx5_to_global=True,
        )
        self._insert_overall_inputs(pipeline_graph)
        builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            self.butler,
            input_collections=["c1"],
            output_run="c2",
            dataset_query_constraint=DatasetQueryConstraintVariant.OFF,
            where="healpix5 IN (4864, 4522)",
        )
        qg = builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(len(qg.quanta_by_task["hpx8_to_hpx5"]), 2)
        self.assertEqual(len(qg.quanta_by_task["hpx5_to_global"]), 1)

    def test_hpx_to_global_data_id_table(self) -> None:
        """Test building a QG with healpix inputs and a hierarchy of healpix
        outputs and a final global step, with healpix IDs provided via a data
        ID table.
        """
        pipeline_graph = self._make_hpx_pipeline_graph(
            hpx8_to_hpx5=True,
            hpx5_to_global=True,
        )
        self._insert_overall_inputs(pipeline_graph)
        tbl = astropy.table.Table(rows=[(4864,), (4522,)], names=["healpix5"])
        builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            self.butler,
            input_collections=["c1"],
            output_run="c2",
            dataset_query_constraint=DatasetQueryConstraintVariant.OFF,
            data_id_tables=[tbl],
        )
        qg = builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(len(qg.quanta_by_task["hpx8_to_hpx5"]), 2)
        self.assertEqual(len(qg.quanta_by_task["hpx5_to_global"]), 1)

    def _make_hpx_pipeline_graph(
        self,
        *,
        patch_to_hpx8_and_hpx11: bool = False,
        hpx8_to_hpx5: bool = False,
        hpx5_to_global=False,
        hpx5_to_instrument: bool = False,
    ) -> PipelineGraph:
        """Generate a pipeline graph with tasks that use healpix dimensions
        in various ways.
        """
        pipeline_graph = PipelineGraph(universe=self.butler.dimensions)
        if patch_to_hpx8_and_hpx11:
            config = DynamicTestPipelineTaskConfig()
            config.dimensions = {"healpix8"}
            config.inputs["i1"] = DynamicConnectionConfig(
                dataset_type_name="patch_dataset",
                dimensions={"patch"},
                multiple=True,
            )
            config.outputs["o1"] = DynamicConnectionConfig(
                dataset_type_name="hpx11_dataset",
                dimensions={"healpix11"},
                multiple=True,
            )
            config.outputs["o2"] = DynamicConnectionConfig(
                dataset_type_name="hpx8_dataset",
                dimensions={"healpix8"},
            )
            pipeline_graph.add_task("patch_to_hpx8_and_hpx11", DynamicTestPipelineTask, config=config)
        if hpx8_to_hpx5:
            config = DynamicTestPipelineTaskConfig()
            config.dimensions = {"healpix5"}
            config.inputs["i1"] = DynamicConnectionConfig(
                dataset_type_name="hpx8_dataset",
                dimensions={"healpix8"},
                multiple=True,
            )
            config.outputs["o1"] = DynamicConnectionConfig(
                dataset_type_name="hpx5_dataset",
                dimensions={"healpix5"},
            )
            pipeline_graph.add_task("hpx8_to_hpx5", DynamicTestPipelineTask, config=config)
        if hpx5_to_global:
            config = DynamicTestPipelineTaskConfig()
            config.dimensions = {}
            config.inputs["i1"] = DynamicConnectionConfig(
                dataset_type_name="hpx5_dataset",
                dimensions={"healpix5"},
                multiple=True,
            )
            config.outputs["o1"] = DynamicConnectionConfig(
                dataset_type_name="global_dataset",
                dimensions={},
            )
            pipeline_graph.add_task("hpx5_to_global", DynamicTestPipelineTask, config=config)
        if hpx5_to_instrument:
            config = DynamicTestPipelineTaskConfig()
            config.dimensions = {"instrument"}
            config.inputs["i1"] = DynamicConnectionConfig(
                dataset_type_name="hpx5_dataset",
                dimensions={"healpix5"},
                multiple=True,
            )
            config.outputs["o1"] = DynamicConnectionConfig(
                dataset_type_name="instrument_dataset",
                dimensions={"instrument"},
            )
            pipeline_graph.add_task("hpx5_to_instrument", DynamicTestPipelineTask, config=config)
        pipeline_graph.resolve(self.butler.registry)
        return pipeline_graph

    def _insert_overall_inputs(self, pipeline_graph: PipelineGraph) -> None:
        """Insert overall-input datasets for a pipeline graph."""
        self.butler.collections.register("c1")
        for _, node in pipeline_graph.iter_overall_inputs():
            self.butler.registry.registerDatasetType(node.dataset_type)
            if node.dimensions.skypix:
                if len(node.dimensions) == 1:
                    (skypix_name,) = node.dimensions.skypix
                    pixelization = node.dimensions.universe.skypix_dimensions[skypix_name].pixelization
                    ranges = RangeSet()
                    for patch_record in self.butler.query_dimension_records("patch"):
                        ranges |= pixelization.envelope(patch_record.region)
                    data_ids = []
                    for begin, end in ranges:
                        for index in range(begin, end):
                            data_ids.append(DataCoordinate.from_required_values(node.dimensions, (index,)))
                else:
                    raise NotImplementedError(
                        "Can only generate data IDs for queryable dimensions and isolated skypix."
                    )
            else:
                data_ids = self.butler.query_data_ids(node.dimensions, explain=False)
            self.butler.registry.insertDatasets(node.dataset_type, data_ids, run="c1")


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
