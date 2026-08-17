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

from astropy.time import Time

from lsst.daf.butler import CollectionType, DatasetIdGenEnum, DatasetRef, Timespan
from lsst.pipe.base.tests.mocks import DynamicConnectionConfig, InMemoryRepo
from lsst.pipe.base.trivial_quantum_graph_builder import TrivialQuantumGraphBuilder


class TrivialQuantumGraphBuilderTestCase(unittest.TestCase):
    """Tests for the TrivialQuantumGraphBuilder class.

    This uses two data IDs with related dimensions ({detector, band} vs
    {detector, physical_filter}), with a prerequisite input provided directly
    instead of found via the fallback base QG builder.
    """

    @classmethod
    def setUpClass(cls) -> None:
        # Make a test helper with a mock task appropriate for the QG builder:
        # - the QG will have no branching
        # - while the task have different dimensions, they can be 1-1 related
        #   (for the purposes of this test, at least).
        cls.helper = InMemoryRepo("base.yaml")
        cls.helper.add_task(
            "a",
            dimensions=["band", "detector"],
            prerequisite_inputs={
                "prereq_connection": DynamicConnectionConfig(
                    dataset_type_name="dataset_prereq0", dimensions=["detector"], minimum=0
                )
            },
        )
        cls.helper.add_task(
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
        cls.general_qg = cls.helper.make_quantum_graph()
        # Make the trivial QG builder we want to test giving it only one
        # detector and one band (is the one that corresponds to only one
        # physical_filter).
        (cls.a_data_id,) = [
            data_id
            for data_id in cls.general_qg.quanta_by_task["a"]
            if data_id["detector"] == 1 and data_id["band"] == "g"
        ]
        (cls.b_data_id,) = [
            data_id
            for data_id in cls.general_qg.quanta_by_task["b"]
            if data_id["detector"] == 1 and data_id["band"] == "g"
        ]
        cls.dataset_auto0_ref = cls.helper.butler.get_dataset(
            cls.general_qg.datasets_by_type["dataset_auto0"][cls.a_data_id]
        )
        assert cls.dataset_auto0_ref is not None, "Input dataset should have been inserted above."
        prereq_data_id = cls.a_data_id.subset(["detector"])
        cls.dataset_prereq0_ref = cls.helper.butler.get_dataset(
            cls.general_qg.datasets_by_type["dataset_prereq0"][prereq_data_id]
        )
        assert cls.dataset_prereq0_ref is not None, "Input dataset should have been inserted above."

    def test_trivial_qg_builder(self) -> None:
        """Test the usual/baseline behavior of the trivial QG builder."""
        trivial_builder = TrivialQuantumGraphBuilder(
            self.helper.pipeline_graph,
            self.helper.butler,
            data_ids={self.a_data_id.dimensions: self.a_data_id, self.b_data_id.dimensions: self.b_data_id},
            input_refs={
                "a": {
                    "input_connection": [self.dataset_auto0_ref],
                    "prereq_connection": [self.dataset_prereq0_ref],
                }
            },
            dataset_id_modes={"dataset_auto2": DatasetIdGenEnum.DATAID_TYPE_RUN},
            output_run="trivial_output_run",
            input_collections=self.general_qg.header.inputs,
        )
        trivial_qg = trivial_builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(len(trivial_qg.quanta_by_task), 2)
        self.assertEqual(trivial_qg.quanta_by_task["a"].keys(), {self.a_data_id})
        self.assertEqual(trivial_qg.quanta_by_task["b"].keys(), {self.b_data_id})
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_prereq0"].keys(), {self.dataset_prereq0_ref.dataId}
        )
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_prereq0"][self.dataset_prereq0_ref.dataId],
            self.general_qg.datasets_by_type["dataset_prereq0"][self.dataset_prereq0_ref.dataId],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_auto0"].keys(), {self.a_data_id})
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_auto0"][self.a_data_id],
            self.general_qg.datasets_by_type["dataset_auto0"][self.a_data_id],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_extra1"].keys(), {self.b_data_id})
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_extra1"][self.b_data_id],
            self.general_qg.datasets_by_type["dataset_extra1"][self.b_data_id],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_auto1"].keys(), {self.a_data_id})
        self.assertNotEqual(
            trivial_qg.datasets_by_type["dataset_auto1"][self.a_data_id],
            self.general_qg.datasets_by_type["dataset_auto1"][self.a_data_id],
        )
        self.assertEqual(trivial_qg.datasets_by_type["dataset_auto2"].keys(), {self.b_data_id})
        self.assertNotEqual(
            trivial_qg.datasets_by_type["dataset_auto2"][self.b_data_id],
            self.general_qg.datasets_by_type["dataset_auto2"][self.b_data_id],
        )
        self.assertEqual(
            trivial_qg.datasets_by_type["dataset_auto2"][self.b_data_id],
            DatasetRef(
                self.helper.pipeline_graph.dataset_types["dataset_auto2"].dataset_type,
                self.b_data_id,
                run="trivial_output_run",
                id_generation_mode=DatasetIdGenEnum.DATAID_TYPE_RUN,
            ).id,
        )
        qo_xg = trivial_qg.quantum_only_xgraph
        self.assertEqual(len(qo_xg.nodes), 2)
        self.assertEqual(len(qo_xg.edges), 1)
        bp_xg = trivial_qg.bipartite_xgraph
        self.assertEqual(
            set(bp_xg.predecessors(trivial_qg.quanta_by_task["a"][self.a_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto0"].values())
            | set(trivial_qg.datasets_by_type["dataset_prereq0"].values()),
        )
        self.assertEqual(
            set(bp_xg.successors(trivial_qg.quanta_by_task["a"][self.a_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto1"].values())
            | set(trivial_qg.datasets_by_type["a_metadata"].values())
            | set(trivial_qg.datasets_by_type["a_log"].values()),
        )
        self.assertEqual(
            set(bp_xg.predecessors(trivial_qg.quanta_by_task["b"][self.b_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto1"].values())
            | set(trivial_qg.datasets_by_type["dataset_extra1"].values()),
        )
        self.assertEqual(
            set(bp_xg.successors(trivial_qg.quanta_by_task["b"][self.b_data_id])),
            set(trivial_qg.datasets_by_type["dataset_auto2"].values())
            | set(trivial_qg.datasets_by_type["b_metadata"].values())
            | set(trivial_qg.datasets_by_type["b_log"].values()),
        )

    def test_input_ref_prerequsites_win(self) -> None:
        """Test that a prerequisite connection's datasets provided via
        ``input_refs`` are respected (even if that set is empty).
        """
        trivial_builder = TrivialQuantumGraphBuilder(
            self.helper.pipeline_graph,
            self.helper.butler,
            data_ids={self.a_data_id.dimensions: self.a_data_id, self.b_data_id.dimensions: self.b_data_id},
            input_refs={
                "a": {
                    "input_connection": [self.dataset_auto0_ref],
                    "prereq_connection": [],
                }
            },
            dataset_id_modes={"dataset_auto2": DatasetIdGenEnum.DATAID_TYPE_RUN},
            output_run="trivial_output_run",
            input_collections=self.general_qg.header.inputs,
        )
        trivial_qg = trivial_builder.finish(attach_datastore_records=False).assemble()
        self.assertEqual(trivial_qg.datasets_by_type["dataset_prereq0"], {})


class TestTrivialQuantumGraphBuilderPrerequisiteTests(unittest.TestCase):
    """Test the trivial QG builder with various fallback prerequisite lookup
    cases: calibration lookup, skypix lookup, and the trailing vanilla
    ``query_datasets`` branch.
    """

    @classmethod
    def setUpClass(cls) -> None:
        cls.helper = InMemoryRepo(
            "base.yaml", "datasets.yaml", "spatial.yaml", use_import_collections_as_input=False
        )
        cls.helper.add_task(
            "a",
            dimensions=["visit", "detector"],
            prerequisite_inputs={
                "refcat": DynamicConnectionConfig(
                    dataset_type_name="refcat", dimensions=["htm7"], multiple=True, minimum=0
                ),
                "bias": DynamicConnectionConfig(
                    dataset_type_name="bias",
                    dimensions=["detector"],
                    multiple=True,
                    minimum=0,
                    is_calibration=True,
                    storage_class="Exposure",
                    mock_storage_class=False,
                ),
                "vanilla": DynamicConnectionConfig(
                    dataset_type_name="vanilla",
                    dimensions=["detector", "band"],
                    multiple=True,
                    minimum=0,
                ),
            },
        )
        # Set up a CALIBRATION collection with certified biases so the
        # calibration-lookup branch of the prerequisite finder is actually
        # exercised.  The collection must be part of the input chain (which is
        # what the trivial builder searches) before the general QG is built.
        cls.helper.butler.collections.register("calib", CollectionType.CALIBRATION)
        cls.helper.butler.collections.redefine_chain(cls.helper.input_chain, [cls.helper.input_run, "calib"])
        # The `bias` datasets provided by `datasets.yaml` include exactly two
        # detector-2 biases (one in `imported_g`, one in `imported_r`), which
        # we certify below with distinct validity ranges so that exactly one of
        # them overlaps the visit-1 timespan.
        cls.visit1_start = Time("2021-09-09T03:00:00", scale="tai")
        cls.bias_matching_ref = cls.helper.butler.registry.findDataset(
            "bias", instrument="Cam1", detector=2, collections="imported_g"
        )
        cls.bias_before_ref = cls.helper.butler.registry.findDataset(
            "bias", instrument="Cam1", detector=2, collections="imported_r"
        )
        # The "before" bias validity ends exactly at the visit-1 start, so it
        # is excluded by half-open [begin, end) semantics for the visit-1
        # timespan.
        cls.helper.butler.registry.certify(
            "calib",
            [cls.bias_before_ref],
            Timespan(Time("2021-09-09T02:59:00", scale="tai"), cls.visit1_start),
        )
        # The "matching" bias validity fully contains the visit-1 timespan, so
        # it is the only certified detector-2 bias selected for visit 1.
        cls.helper.butler.registry.certify(
            "calib",
            [cls.bias_matching_ref],
            Timespan(cls.visit1_start, Time("2021-09-09T03:02:00", scale="tai")),
        )
        # Use the helper to make a quantum graph using the general-purpose
        # builder.  This will cover all data IDs in the test dataset, which
        # includes 4 detectors, 3 physical_filters, 2 bands, and two visits.
        # This also has useful side-effects: it inserts the input datasets
        # and registers all dataset types.
        cls.general_qg = cls.helper.make_quantum_graph()
        cls.data_id = cls.helper.butler.registry.expandDataId(instrument="Cam1", visit=1, detector=2)
        trivial_builder = TrivialQuantumGraphBuilder(
            cls.helper.pipeline_graph,
            cls.helper.butler,
            data_ids={cls.data_id.dimensions: cls.data_id},
            input_refs={},
            output_run="trivial_output_run",
            input_collections=cls.general_qg.header.inputs,
        )
        cls.trivial_qg = trivial_builder.finish(attach_datastore_records=False).assemble()

    def test_skypix_lookup(self) -> None:
        """Test a skypix-dimensioned prerequisite lookup using the quantum's
        natural region.
        """
        self.assertEqual(
            {data_id["htm7"] for data_id in self.trivial_qg.datasets_by_type["refcat"].keys()},
            {253965},
        )

    def test_calibration_lookup(self) -> None:
        """Test a calibration prerequisite lookup driven by the quantum's
        timespan.

        Only the "matching" bias (whose certified validity fully contains the
        visit-1 timespan) should be selected; the "before" bias (whose validity
        ends exactly at the visit-1 start) should be excluded by half-open
        [begin, end) semantics.
        """
        self.assertEqual(
            list(self.trivial_qg.datasets_by_type["bias"].values()),
            [self.bias_matching_ref.id],
        )

    def test_vanilla_query(self) -> None:
        """Test a simple prerequisite lookup that is neither temporal nor
        spatial.
        """
        self.assertEqual(
            {
                (data_id["detector"], data_id["band"])
                for data_id in self.trivial_qg.datasets_by_type["vanilla"].keys()
            },
            {(2, "g")},
        )


if __name__ == "__main__":
    unittest.main()
