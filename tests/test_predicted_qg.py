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

import unittest

import lsst.utils.tests
from lsst.daf.butler import Butler, DataCoordinate
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry
from lsst.pipe.base import PipelineGraph
from lsst.pipe.base.all_dimensions_quantum_graph_builder import (
    AllDimensionsQuantumGraphBuilder,
)
from lsst.pipe.base.quantum_graph import PredictedQuantumGraphComponents
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
)
from lsst.resources import ResourcePath
from lsst.sphgeom import RangeSet


class PredictedQuantumGraphTestCase(unittest.TestCase):
    """Unit tests for the predicted quantum graph classes."""

    @staticmethod
    def make_butler() -> Butler:
        DATA_ROOT = ResourcePath("resource://lsst.daf.butler/tests/registry_data", forceDirectory=True)
        return create_populated_sqlite_registry(
            *[DATA_ROOT.join(filename) for filename in ("base.yaml", "spatial.yaml")]
        )

    def test_writer_from_old_quantum_graph(self) -> None:
        """Test creating a new predicted quantum graph writer from an old
        `QuantumGraph` instance.
        """
        butler = self.make_butler()
        pipeline_graph = self.make_pipeline_graph(butler)
        self.insert_overall_inputs(pipeline_graph, butler, "input_collection")
        builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            butler,
            input_collections=["input_collection"],
            output_run="output_chain/run",
        )
        old_qg = builder.build(
            metadata={
                "input": ["input_collection"],
                "output": "output_chain",
                "output_run": "output_chain/run",
                "user": "me",
                "stuff": "whatever",
            },
            attach_datastore_records=False,
        )
        pw = PredictedQuantumGraphComponents.from_old_quantum_graph(old_qg)
        self.assertEqual(pw.header.inputs, ["input_collection"])
        self.assertEqual(pw.header.output, "output_chain")
        self.assertEqual(pw.header.output_run, "output_chain/run")
        self.assertEqual(pw.header.user, "me")
        self.assertEqual(pw.header.metadata["stuff"], "whatever")
        self.assertEqual(pw.header.n_task_quanta["calibrate"], 8)
        self.assertEqual(pw.header.n_task_quanta["consolidate"], 2)
        self.assertEqual(pw.header.n_task_quanta["resample"], 10)
        self.assertEqual(pw.header.n_task_quanta["coadd"], 10)
        self.assertEqual(pw.header.n_task_quanta["match"], 2)
        self.assertEqual(pw.header.n_quanta, 32 + len(pw.init_quanta.root))
        self.assertNotIn("htm6", pw.dimension_data.records)
        self.assertEqual(
            [q.task_label for q in pw.init_quanta.root],
            ["", "calibrate", "consolidate", "resample", "coadd", "match"],
        )

    @staticmethod
    def make_pipeline_graph(butler: Butler) -> PipelineGraph:
        """Generate a pipeline graph with some interestingly complex tasks,
        sort of analogous to a real pipeline (just to make names and flow
        easier to remember).
        """
        pipeline_graph = PipelineGraph(universe=butler.dimensions)
        # 'calibrate': a simple 1-1 {visit, detector} processing task.
        config = DynamicTestPipelineTaskConfig()
        config.dimensions = {"visit", "detector"}
        config.inputs["input_image"] = DynamicConnectionConfig(
            dataset_type_name="raw",
            dimensions={"visit", "detector"},
        )
        config.prerequisite_inputs["refcat"] = DynamicConnectionConfig(
            dataset_type_name="references",
            dimensions={"htm7"},
            multiple=True,
        )
        config.init_outputs["output_schema"] = DynamicConnectionConfig(
            dataset_type_name="source_schema",
        )
        config.outputs["output_image"] = DynamicConnectionConfig(
            dataset_type_name="image",
            dimensions={"visit", "detector"},
        )
        config.outputs["output_table"] = DynamicConnectionConfig(
            dataset_type_name="source_detector",
            dimensions={"visit", "detector"},
        )
        pipeline_graph.add_task("calibrate", DynamicTestPipelineTask, config=config)
        # 'consolidate': a visit-level gather table aggregation task.
        config = DynamicTestPipelineTaskConfig()
        config.dimensions = {"visit"}
        config.init_inputs["input_schema"] = DynamicConnectionConfig(
            dataset_type_name="source_schema",
        )
        config.inputs["input_table"] = DynamicConnectionConfig(
            dataset_type_name="source_detector",
            dimensions={"visit", "detector"},
            multiple=True,
        )
        config.outputs["output_table"] = DynamicConnectionConfig(
            dataset_type_name="source",
            dimensions={"visit"},
        )
        pipeline_graph.add_task("consolidate", DynamicTestPipelineTask, config=config)
        # 'resample': a {patch, visit} spatial-join task.  This can be run
        # in parallel with 'consolidate'.
        config = DynamicTestPipelineTaskConfig()
        config.dimensions = {"patch", "visit"}
        config.inputs["input_image"] = DynamicConnectionConfig(
            dataset_type_name="image",
            dimensions={"visit", "detector"},
            multiple=True,
        )
        config.outputs["output_image"] = DynamicConnectionConfig(
            dataset_type_name="warp",
            dimensions={"patch", "visit"},
        )
        pipeline_graph.add_task("resample", DynamicTestPipelineTask, config=config)
        # 'coadd': a {patch, band} gather task.  This can be run
        # in parallel with 'consolidate'.
        config = DynamicTestPipelineTaskConfig()
        config.dimensions = {"patch", "band"}
        config.inputs["input_image"] = DynamicConnectionConfig(
            dataset_type_name="warp",
            dimensions={"patch", "visit"},
            multiple=True,
        )
        config.outputs["output_image"] = DynamicConnectionConfig(
            dataset_type_name="coadd",
            dimensions={"patch", "band"},
        )
        config.outputs["output_table"] = DynamicConnectionConfig(
            dataset_type_name="object",
            dimensions={"patch", "band"},
        )
        pipeline_graph.add_task("coadd", DynamicTestPipelineTask, config=config)
        # 'match': a {htm6} gather task that depends on (indirect) outputs
        # of all other tasks.
        config = DynamicTestPipelineTaskConfig()
        config.dimensions = {"htm6"}
        config.inputs["input_object"] = DynamicConnectionConfig(
            dataset_type_name="object",
            dimensions={"patch", "band"},
            multiple=True,
        )
        config.inputs["input_source"] = DynamicConnectionConfig(
            dataset_type_name="source",
            dimensions={"visit"},
            multiple=True,
        )
        config.outputs["output_table"] = DynamicConnectionConfig(
            dataset_type_name="matches",
            dimensions={"htm6"},
        )
        pipeline_graph.add_task("match", DynamicTestPipelineTask, config=config)
        pipeline_graph.resolve(butler.registry)
        return pipeline_graph

    def insert_overall_inputs(self, pipeline_graph: PipelineGraph, butler: Butler, collection: str) -> None:
        """Insert overall-input datasets for a pipeline graph."""
        butler.collections.register(collection)
        for _, node in pipeline_graph.iter_overall_inputs():
            butler.registry.registerDatasetType(node.dataset_type)
            if node.dimensions.skypix:
                if len(node.dimensions) == 1:
                    (skypix_name,) = node.dimensions.skypix
                    pixelization = node.dimensions.universe.skypix_dimensions[skypix_name].pixelization
                    ranges = RangeSet()
                    for patch_record in butler.query_dimension_records("patch"):
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
                data_ids = butler.query_data_ids(node.dimensions, explain=False)
            butler.registry.insertDatasets(node.dataset_type, data_ids, run=collection)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
