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

import unittest

from lsst.daf.butler import DatasetIdGenEnum, DatasetRef
from lsst.pipe.base.tests.mocks import DynamicConnectionConfig, InMemoryRepo
from lsst.pipe.base.trivial_quantum_graph_builder import TrivialQuantumGraphBuilder


class TrivialQuantumGraphBuilderTestCase(unittest.TestCase):
    """Tests for the TrivialQuantumGraphBuilder class."""

    def test_trivial_qg_builder(self) -> None:
        # Make a test helper with a mock task appropriate for the QG builder:
        # - the QG will have no branching
        # - while the task have different dimensions, they can be 1-1 related
        #   (for the purposes of this test, at least).
        helper = InMemoryRepo("base.yaml")
        helper.add_task(
            "a",
            dimensions=["band", "detector"],
            prerequisite_inputs={
                "prereq_connection": DynamicConnectionConfig(
                    dataset_type_name="dataset_prereq0", dimensions=["detector"]
                )
            },
        )
        helper.add_task(
            "b",
            dimensions=["physical_filter", "detector"],
            inputs={
                "input_connection": DynamicConnectionConfig(
                    dataset_type_name="dataset_auto1", dimensions=["band", "detector"]
                ),
                "extra_input_connection": DynamicConnectionConfig(
                    dataset_type_name="dataset_extra1", dimensions=["physical_filter", "detector"]
                ),
            },
        )
        # Use the helper to make a quantum graph using the general-purpose
        # builder.  This will cover all data IDs in the test dataset, which
        # includes 4 detectors, 3 physical_filters, and 2 bands.
        # This also has useful side-effects: it inserts the input datasets
        # and registers all dataset types.
        general_qg = helper.make_quantum_graph()
        # Make the trivial QG builder we want to test giving it only one
        # detector and one band (is the one that corresponds to only one
        # physical_filter).
        (a_data_id,) = [
            data_id
            for data_id in general_qg.quanta_by_task["a"]
            if data_id["detector"] == 1 and data_id["band"] == "g"
        ]
        (b_data_id,) = [
            data_id
            for data_id in general_qg.quanta_by_task["b"]
            if data_id["detector"] == 1 and data_id["band"] == "g"
        ]
        prereq_data_id = a_data_id.subset(["detector"])
        dataset_auto0_ref = helper.butler.get_dataset(general_qg.datasets_by_type["dataset_auto0"][a_data_id])
        assert dataset_auto0_ref is not None, "Input dataset should have been inserted above."
        dataset_prereq0_ref = helper.butler.get_dataset(
            general_qg.datasets_by_type["dataset_prereq0"][prereq_data_id]
        )
        assert dataset_prereq0_ref is not None, "Input dataset should have been inserted above."
        trivial_builder = TrivialQuantumGraphBuilder(
            helper.pipeline_graph,
            helper.butler,
            data_ids={a_data_id.dimensions: a_data_id, b_data_id.dimensions: b_data_id},
            input_refs={
                "a": {"input_connection": [dataset_auto0_ref], "prereq_connection": [dataset_prereq0_ref]}
            },
            dataset_id_modes={"dataset_auto2": DatasetIdGenEnum.DATAID_TYPE_RUN},
            output_run="trivial_output_run",
            input_collections=general_qg.header.inputs,
        )
        trivial_qg = trivial_builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(len(trivial_qg.quanta_by_task), 2)
        self.assertEqual(trivial_qg.quanta_by_task["a"].keys(), {a_data_id})
        self.assertEqual(trivial_qg.quanta_by_task["b"].keys(), {b_data_id})
        self.assertEqual(trivial_qg.datasets_by_type["dataset_prereq0"].keys(), {prereq_data_id})
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_prereq0"][prereq_data_id],
            general_qg.datasets_by_type["dataset_prereq0"][prereq_data_id],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_auto0"].keys(), {a_data_id})
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_auto0"][a_data_id],
            general_qg.datasets_by_type["dataset_auto0"][a_data_id],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_extra1"].keys(), {b_data_id})
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_extra1"][b_data_id],
            general_qg.datasets_by_type["dataset_extra1"][b_data_id],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_auto1"].keys(), {a_data_id})
        self.assertNotEqual(
            trivial_qg.datasets_by_type["dataset_auto1"][a_data_id],
            general_qg.datasets_by_type["dataset_auto1"][a_data_id],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_auto2"].keys(), {b_data_id})
        self.assertNotEqual(
            trivial_qg.datasets_by_type["dataset_auto2"][b_data_id],
            general_qg.datasets_by_type["dataset_auto2"][b_data_id],
        )
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_auto2"][b_data_id],
            DatasetRef(
                helper.pipeline_graph.dataset_types["dataset_auto2"].dataset_type,
                b_data_id,
                run="trivial_output_run",
                id_generation_mode=DatasetIdGenEnum.DATAID_TYPE_RUN,
            ).id,
        )
        qo_xg = trivial_qg.quantum_only_xgraph
        self.assertEqual(len(qo_xg.nodes), 2)
        self.assertEqual(len(qo_xg.edges), 1)
        bp_xg = trivial_qg.bipartite_xgraph
        self.assertEqual(
            set(bp_xg.predecessors(trivial_qg.quanta_by_task["a"][a_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto0"].values())
            | set(trivial_qg.datasets_by_type["dataset_prereq0"].values()),
        )
        self.assertEqual(
            set(bp_xg.successors(trivial_qg.quanta_by_task["a"][a_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto1"].values())
            | set(trivial_qg.datasets_by_type["a_metadata"].values())
            | set(trivial_qg.datasets_by_type["a_log"].values()),
        )
        self.assertEqual(
            set(bp_xg.predecessors(trivial_qg.quanta_by_task["b"][b_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto1"].values())
            | set(trivial_qg.datasets_by_type["dataset_extra1"].values()),
        )
        self.assertEqual(
            set(bp_xg.successors(trivial_qg.quanta_by_task["b"][b_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto2"].values())
            | set(trivial_qg.datasets_by_type["b_metadata"].values())
            | set(trivial_qg.datasets_by_type["b_log"].values()),
        )


if __name__ == "__main__":
    unittest.main()
