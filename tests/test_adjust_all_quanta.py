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

__all__ = ()

import operator
import unittest
from collections import defaultdict

import lsst.pipe.base.connectionTypes as cT
from lsst.daf.butler import Butler, DataCoordinate
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry
from lsst.pipe.base import (
    PipelineGraph,
    PipelineTask,
    PipelineTaskConfig,
    PipelineTaskConnections,
    QuantaAdjuster,
)
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.resources import ResourcePath


class GroupTestConnections(PipelineTaskConnections, dimensions=("detector",)):
    """Connections for a task whose quanta read in all of the biases for all
    detectors with the same purpose, use flat-field exposures as prerequisites,
    and (theoretically) writes both a single summary output for all inputs and
    another output for each input. The data IDs of the quanta and the summary
    outputs using the detector with the lowest ID in that group.
    """

    input_group = cT.Input(
        "bias",
        "Exposure",
        multiple=True,
        dimensions=("detector",),
        isCalibration=True,
    )
    prereq_input_group = cT.PrerequisiteInput(
        "flat",
        "Exposure",
        multiple=True,
        dimensions=("detector", "physical_filter", "band"),
        isCalibration=True,
    )
    output_group = cT.Output(
        "bias_stuff",
        "StructuredDataDict",
        multiple=True,
        dimensions=("detector",),
    )
    single_output = cT.Output(
        "bias_summary",
        "StructuredDataDict",
        multiple=False,
        dimensions=("detector",),
    )

    def adjust_all_quanta(self, adjuster: QuantaAdjuster) -> None:
        # Group the quanta by their detector's purpose.
        quanta_by_detector_purpose: defaultdict[str, list[DataCoordinate]] = defaultdict(list)
        for quantum_data_id in adjuster.iter_data_ids():
            quantum_data_id = adjuster.expand_quantum_data_id(quantum_data_id)
            quanta_by_detector_purpose[quantum_data_id.records["detector"].purpose].append(quantum_data_id)
        # Within each group, keep only the one with the lowest detector ID,
        # while transferring the inputs and outputs of the others to that
        # quantum.
        for data_id_group in quanta_by_detector_purpose.values():
            data_id_group.sort(key=operator.itemgetter("detector"))
            keep, *drop = data_id_group
            for drop_data_id in drop:
                for input_data_id in adjuster.get_inputs(drop_data_id)["input_group"]:
                    adjuster.add_input(keep, "input_group", input_data_id)
                for input_uuid in adjuster.get_prerequisite_inputs(drop_data_id)["prereq_input_group"]:
                    adjuster.add_prerequisite_input(keep, "prereq_input_group", input_uuid)
                for output_data_id in adjuster.get_outputs(drop_data_id)["output_group"]:
                    adjuster.move_output(keep, "output_group", output_data_id)
                adjuster.remove_quantum(drop_data_id)


class GroupTestConfig(PipelineTaskConfig, pipelineConnections=GroupTestConnections):
    pass


class GroupTestTask(PipelineTask):
    ConfigClass = GroupTestConfig


class AdjustAllQuantaTestCase(unittest.TestCase):
    """Tests for the `PipelineTaskConnections.adjust_all_quanta` hook in
    quantum-graph generation.
    """

    @staticmethod
    def make_butler() -> Butler:
        DATA_ROOT = ResourcePath("resource://lsst.daf.butler/tests/registry_data", forceDirectory=True)
        return create_populated_sqlite_registry(
            *[DATA_ROOT.join(filename) for filename in ("base.yaml", "datasets.yaml")]
        )

    def test_adjust_all_quanta(self) -> None:
        """Build a quantum graph for a task that implements the
        adjust_all_quanta hook, and check that it works as expected.
        """
        butler = self.make_butler()
        self.enterContext(butler)
        pipeline_graph = PipelineGraph(universe=butler.dimensions)
        pipeline_graph.add_task("grouper", GroupTestTask)
        collections = ["imported_g", "imported_r"]
        qgb = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            butler,
            input_collections=collections,
            output_run="irrelevant",
        )
        qg = qgb.finish(attach_datastore_records=False).assemble()
        quanta = {
            quantum.dataId["detector"]: quantum
            for quantum in qg.build_execution_quanta(task_label="grouper").values()
        }
        # This test camera (defined in daf_butler test data) has 4 detectors;
        # 1-3 have purpose=SCIENCE, and 4 has purpose=WAVEFRONT.
        self.assertEqual(quanta.keys(), {1, 4})
        self.assertEqual(len(quanta[1].inputs["bias"]), 3)
        self.assertEqual(len(quanta[1].inputs["flat"]), 5)
        self.assertEqual(len(quanta[1].outputs["bias_stuff"]), 3)
        self.assertCountEqual(
            quanta[1].inputs["bias"],
            butler.query_datasets("bias", collections=collections, where="detector.purpose = 'SCIENCE'"),
        )
        self.assertCountEqual(
            quanta[1].inputs["flat"],
            butler.query_datasets("flat", collections=collections, where="detector.purpose = 'SCIENCE'"),
        )
        self.assertEqual(len(quanta[1].outputs["bias_summary"]), 1)
        self.assertEqual(quanta[1].outputs["bias_summary"][0].dataId["detector"], 1)
        self.assertEqual(len(quanta[4].inputs["bias"]), 1)
        self.assertEqual(len(quanta[4].inputs["flat"]), 2)
        self.assertEqual(len(quanta[4].outputs["bias_stuff"]), 1)
        self.assertCountEqual(
            quanta[4].inputs["bias"],
            butler.query_datasets("bias", collections=collections, where="detector.purpose = 'WAVEFRONT'"),
        )
        self.assertCountEqual(
            quanta[4].inputs["flat"],
            butler.query_datasets("flat", collections=collections, where="detector.purpose = 'WAVEFRONT'"),
        )
        self.assertEqual(len(quanta[4].outputs["bias_summary"]), 1)
        self.assertEqual(quanta[4].outputs["bias_summary"][0].dataId["detector"], 4)
