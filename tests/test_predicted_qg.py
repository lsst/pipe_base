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

import itertools
import logging
import os
import shutil
import subprocess
import tempfile
import unittest
import uuid
from io import StringIO

import lsst.utils.tests
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.dot_tools import graph2dot
from lsst.pipe.base.mermaid_tools import graph2mermaid
from lsst.pipe.base.quantum_graph import (
    FORMAT_VERSION,
    AddressReader,
    PredictedDatasetInfo,
    PredictedQuantumGraph,
    PredictedQuantumInfo,
)
from lsst.pipe.base.tests.mocks import DynamicConnectionConfig, InMemoryRepo

logging.getLogger("timer").setLevel(logging.INFO)
logging.getLogger("lsst").setLevel(logging.INFO)
logging.getLogger("lsst.pipe.base.quantum_graph").setLevel(logging.DEBUG)


class PredictedQuantumGraphTestCase(unittest.TestCase):
    """Unit tests for the predicted quantum graph classes.

    These tests focus on the interface of the predicted quantum graph
    especially I/O and conversion to and from the old quantum graph class.

    Additional test coverage is provided by the tests for quantum graph
    builders and executors, which all use this class as well.
    """

    def setUp(self):
        self.helper = InMemoryRepo("base.yaml", "spatial.yaml")
        self.enterContext(self.helper)
        self.helper.add_task(
            "calibrate",
            dimensions=["visit", "detector"],
            inputs={
                "input_image": DynamicConnectionConfig(
                    dataset_type_name="raw",
                    dimensions=["visit", "detector"],
                )
            },
            prerequisite_inputs={
                "refcat": DynamicConnectionConfig(
                    dataset_type_name="references",
                    dimensions=["htm7"],
                    multiple=True,
                )
            },
            init_outputs={
                "output_schema": DynamicConnectionConfig(
                    dataset_type_name="source_schema",
                )
            },
            outputs={
                "output_image": DynamicConnectionConfig(
                    dataset_type_name="image",
                    dimensions=["visit", "detector"],
                ),
                "output_table": DynamicConnectionConfig(
                    dataset_type_name="source_detector",
                    dimensions=["visit", "detector"],
                ),
            },
        )
        self.helper.add_task(
            "consolidate",
            dimensions=["visit"],
            init_inputs={
                "input_schema": DynamicConnectionConfig(
                    dataset_type_name="source_schema",
                )
            },
            inputs={
                "input_table": DynamicConnectionConfig(
                    dataset_type_name="source_detector",
                    dimensions=["visit", "detector"],
                    multiple=True,
                )
            },
            outputs={
                "output_table": DynamicConnectionConfig(
                    dataset_type_name="source",
                    dimensions=["visit"],
                )
            },
        )
        self.helper.add_task(
            "resample",
            dimensions=["patch", "visit"],
            inputs={
                "input_image": DynamicConnectionConfig(
                    dataset_type_name="image",
                    dimensions=["visit", "detector"],
                    multiple=True,
                )
            },
            outputs={
                "output_image": DynamicConnectionConfig(
                    dataset_type_name="warp",
                    dimensions=["patch", "visit"],
                )
            },
        )
        self.helper.add_task(
            "coadd",
            dimensions=["patch", "band"],
            inputs={
                "input_image": DynamicConnectionConfig(
                    dataset_type_name="warp",
                    dimensions=["patch", "visit"],
                    multiple=True,
                )
            },
            outputs={
                "output_image": DynamicConnectionConfig(
                    dataset_type_name="coadd",
                    dimensions=["patch", "band"],
                ),
                "output_table": DynamicConnectionConfig(
                    dataset_type_name="object",
                    dimensions=["patch", "band"],
                    # Like all other (defaulted) storage classes here,
                    # 'ArrowAstropy' below will be mocked; we pick it so we can
                    # try a component input, and because we know its pytype is
                    # safe to import here.
                    storage_class="ArrowAstropy",
                ),
            },
        )
        self.helper.add_task(
            "match",
            dimensions=["htm6"],
            inputs={
                "input_object": DynamicConnectionConfig(
                    dataset_type_name="object",
                    dimensions=["patch", "band"],
                    multiple=True,
                    storage_class="ArrowAstropy",
                ),
                "input_source": DynamicConnectionConfig(
                    dataset_type_name="source",
                    dimensions=["visit"],
                    multiple=True,
                ),
                "input_object_schema": DynamicConnectionConfig(
                    dataset_type_name="object.schema",
                    dimensions=["visit"],
                    multiple=True,
                    storage_class="ArrowAstropySchema",
                ),
            },
            outputs={
                "output_table": DynamicConnectionConfig(
                    dataset_type_name="matches",
                    dimensions=["htm6"],
                )
            },
        )
        self.builder = self.helper.make_quantum_graph_builder()

    def check_quantum_graph(
        self,
        qg: PredictedQuantumGraph,
        *,
        all_tasks: bool = True,
        thin_graph: bool = True,
        all_quantum_datasets: bool = True,
        init_quanta: bool = True,
        dimension_data_loaded: bool = True,
        dimension_data_deserialized: bool = True,
        converting_partial: bool = False,
    ) -> None:
        """Test the attributes and methods of a quantum graph produced by this
        test case's builder.

        Parameters
        ----------
        qg : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            Graph to test.
        all_tasks : `bool`, optional
            Whether all tasks in the pipeline were loaded.
        thin_graph : `bool`, optional
            Whether the ``thin_graph`` component was loaded.
        all_quantum_datasets : `bool`, optional
            Whether all quantum datasets were loaded.
        init_quanta : `bool`, optional
            Whether the ``init_quanta`` component was loaded.
        dimension_data_loaded : `bool`, optional
            Whether the ``dimension_data`` component was loaded at all.
        dimension_data_deserialized : `bool`, optional
            Whether all dimension records are expected to have been
            deserialized.  Ignored if ``dimension_data_loaded`` is `False`.
        converting_partial : `bool`, optional
            Whether this was a partial load that only included some quanta.
        """
        self.assertEqual(qg.header.inputs, ["input_chain"])
        self.assertEqual(qg.header.output, "output_chain")
        self.assertEqual(qg.header.output_run, "output_run")
        self.assertEqual(qg.header.metadata["stuff"], "whatever")
        self.assertEqual(qg.header.version, FORMAT_VERSION)
        if not converting_partial:
            # When partial-reading an old QG, counts reflect what was loaded.
            # In all other cases they reflect the full original graph.
            self.assertEqual(qg.header.n_task_quanta["calibrate"], 8)
            self.assertEqual(qg.header.n_task_quanta["consolidate"], 2)
            self.assertEqual(qg.header.n_task_quanta["resample"], 10)
            self.assertEqual(qg.header.n_task_quanta["coadd"], 10)
            self.assertEqual(qg.header.n_task_quanta["match"], 2)
            self.assertEqual(qg.header.n_quanta, 32)
            self.assertEqual(
                qg.header.n_datasets,
                63  # non-automatic datasets (see breakdown below)
                + 32 * 2  # one metadata and one log for each quantam
                + 5  # one config for each task
                + 1,  # packages
            )
        if all_tasks:
            self.assertFalse(self.helper.pipeline_graph.diff_tasks(qg.pipeline_graph))
        if thin_graph or all_quantum_datasets:
            self.assertEqual(len(qg), qg.header.n_quanta)
            self.assertEqual(len(qg.quantum_only_xgraph), qg.header.n_quanta)
            self.assertEqual(len(qg.quanta_by_task["calibrate"]), 8)
            self.assertEqual(len(qg.quanta_by_task["consolidate"]), 2)
            self.assertEqual(len(qg.quanta_by_task["resample"]), 10)
            self.assertEqual(len(qg.quanta_by_task["coadd"]), 10)
            self.assertEqual(len(qg.quanta_by_task["match"]), 2)
        if init_quanta:
            if all_tasks:
                self.assertEqual(len(qg.datasets_by_type["calibrate_config"]), 1)
                self.assertEqual(len(qg.datasets_by_type["consolidate_config"]), 1)
                self.assertEqual(len(qg.datasets_by_type["resample_config"]), 1)
                self.assertEqual(len(qg.datasets_by_type["coadd_config"]), 1)
                self.assertEqual(len(qg.datasets_by_type["match_config"]), 1)
                self.assertEqual(len(qg.datasets_by_type["source_schema"]), 1)
            self.assertEqual(len(qg.datasets_by_type["packages"]), 1)
            n_init_datasets = 7
        else:
            n_init_datasets = 0
        if all_quantum_datasets:
            assert all_tasks
            self.assertEqual(
                len(qg.bipartite_xgraph),
                qg.header.n_quanta + qg.header.n_datasets - n_init_datasets,
            )
            self.assertEqual(len(qg.datasets_by_type["raw"]), 8)
            self.assertEqual(len(qg.datasets_by_type["references"]), 4)
            self.assertEqual(len(qg.datasets_by_type["image"]), 8)
            self.assertEqual(len(qg.datasets_by_type["source_detector"]), 8)
            self.assertEqual(len(qg.datasets_by_type["source"]), 2)
            self.assertEqual(len(qg.datasets_by_type["warp"]), 10)
            self.assertEqual(len(qg.datasets_by_type["coadd"]), 10)
            self.assertEqual(len(qg.datasets_by_type["object"]), 10)
            self.assertEqual(len(qg.datasets_by_type["matches"]), 2)
            # Spot-check some edges and graph structure.
            for data_id, dataset_id in qg.datasets_by_type["source_detector"].items():
                for producer_id, _, edge_data in qg.bipartite_xgraph.in_edges(dataset_id, data=True):
                    self.assertIn(producer_id, qg.quanta_by_task["calibrate"].values())
                    self.assertFalse(edge_data["is_read"])
                    self.assertEqual(
                        edge_data["pipeline_edges"],
                        [qg.pipeline_graph.tasks["calibrate"].get_output_edge("output_table")],
                    )
                for _, consumer_id, edge_data in qg.bipartite_xgraph.out_edges(dataset_id, data=True):
                    self.assertIn(consumer_id, qg.quanta_by_task["consolidate"].values())
                    self.assertTrue(edge_data["is_read"])
                    self.assertEqual(
                        edge_data["pipeline_edges"],
                        [qg.pipeline_graph.tasks["consolidate"].get_input_edge("input_table")],
                    )
        # We use 'is' checks instead of just equality because we don't want
        # duplicates of these objects floating around wasting memory.
        for task_label, quanta_for_task in qg.quanta_by_task.items():
            for data_id, quantum_id in quanta_for_task.items():
                d1: PredictedQuantumInfo = qg.quantum_only_xgraph.nodes[quantum_id]
                self.assertEqual(d1["task_label"], task_label)
                self.assertIs(d1["data_id"], data_id)
                self.assertIs(d1["pipeline_node"], qg.pipeline_graph.tasks[task_label])
                d2: PredictedQuantumInfo = qg.bipartite_xgraph.nodes[quantum_id]
                self.assertEqual(d2["task_label"], task_label)
                self.assertIs(d2["data_id"], data_id)
                self.assertIs(d2["pipeline_node"], qg.pipeline_graph.tasks[task_label])
        for dataset_type_name, datasets_for_type in qg.datasets_by_type.items():
            if (
                dataset_type_name == "source_schema"
                or dataset_type_name == "packages"
                or dataset_type_name.endswith("_config")
            ):
                continue
            for data_id, dataset_id in datasets_for_type.items():
                d3: PredictedDatasetInfo = qg.bipartite_xgraph.nodes[dataset_id]
                self.assertIs(d3["data_id"], data_id)
                self.assertIs(d3["pipeline_node"], qg.pipeline_graph.dataset_types[dataset_type_name])
        if dimension_data_loaded:
            self.assertIsNotNone(qg.dimension_data)
            if dimension_data_deserialized and not converting_partial:
                self.assertEqual(len(qg.dimension_data.records["instrument"]), 1)
                self.assertEqual(len(qg.dimension_data.records["visit"]), 2)
                self.assertEqual(len(qg.dimension_data.records["detector"]), 4)
                self.assertEqual(len(qg.dimension_data.records["visit_detector_region"]), 8)
                self.assertEqual(len(qg.dimension_data.records["tract"]), 2)
                self.assertEqual(len(qg.dimension_data.records["patch"]), 9)
                self.assertEqual(len(qg.dimension_data.records["band"]), 2)
                self.assertEqual(len(qg.dimension_data.records["physical_filter"]), 2)

    def define_partial_read(self, qg: PredictedQuantumGraph) -> list[uuid.UUID]:
        """Return a list of quantum IDs to test partial loads.

        Parameters
        ----------
        qg : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            Full graph to obtain quantum IDs from.  Note that quantum IDs are
            random and change every test run, and hence test behavior should
            never depend on explicit values or their sort order.

        Returns
        -------
        quanta : `list` [ `uuid.UUID` ]
            Quanta to use for partial-read tests.  These always have the same
            task labels and data IDs: all ``calibrate`` and ``consolidate``
            quanta for ``visit==1``.
        """
        return [
            quantum_id
            for data_id, quantum_id in qg.quanta_by_task["calibrate"].items()
            if data_id["visit"] == 1
        ] + [
            quantum_id
            for data_id, quantum_id in qg.quanta_by_task["consolidate"].items()
            if data_id["visit"] == 1
        ]

    def check_partial_read(
        self, qg: PredictedQuantumGraph, quanta: list[uuid.UUID], converting: bool = False
    ) -> None:
        """Test the attributes and methods of a partially-loaded quantum graph.

        Parameters
        ----------
        qg : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            Graph for which only ``quanta`` were loaded.
        quanta : `list` [ `uuid.UUID` ]
            IDs of the quanta that were loaded.  Must be the result of a call
            to `define_partial_read`.
        converting : `bool`, optional
            Whether this graph was read via the old
            `lsst.pipe.base.QuantumGraph` class and then converted.
        """
        self.check_quantum_graph(
            qg,
            all_tasks=False,
            thin_graph=False,
            all_quantum_datasets=False,
            converting_partial=converting,
            dimension_data_deserialized=converting,
        )
        self.assertEqual(len(qg), 5)
        self.assertEqual(len(qg.quantum_only_xgraph), 5)
        execution_quanta = qg.build_execution_quanta()
        self.assertEqual(execution_quanta.keys(), set(quanta))
        for quantum_id, quantum in execution_quanta.items():
            self.assertIs(qg.bipartite_xgraph.nodes[quantum_id]["quantum"], quantum)
            self.assertIs(qg.quantum_only_xgraph.nodes[quantum_id]["quantum"], quantum)

    def test_build(self) -> None:
        """Test building a `PredictedQuantumGraph` by
        inspecting the result.
        """
        qg = self.builder.finish(
            output="output_chain",
            metadata={"stuff": "whatever"},
            attach_datastore_records=False,
        ).assemble()
        self.check_quantum_graph(qg)

    def test_from_old_quantum_graph(self) -> None:
        """Test building a old `QuantumGraph` and then
        converting it to a `PredictedQuantumGraph`.
        """
        old_qg = self.builder.build(
            metadata={
                "input": ["input_chain"],
                "output": "output_chain",
                "output_run": "output_run",
                "stuff": "whatever",
            },
            attach_datastore_records=False,
        )
        new_qg = PredictedQuantumGraph.from_old_quantum_graph(old_qg)
        self.check_quantum_graph(new_qg)

    def test_read_execution_quanta_old_file(self) -> None:
        """Test building a old `QuantumGraph`, saving it, and then reading it
        via `PredictedQuantumGraph.read_execution_quanta`.
        """
        old_qg = self.builder.build(
            metadata={
                "input": ["input_chain"],
                "output": "output_chain",
                "output_run": "output_run",
                "stuff": "whatever",
            },
            attach_datastore_records=False,
        )
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            tmpfile = os.path.join(tmpdir, "old.qgraph")
            old_qg.saveUri(tmpfile)
            # Read everything.
            qg1 = PredictedQuantumGraph.read_execution_quanta(tmpfile)
            self.check_quantum_graph(qg1, thin_graph=False)
            # Read just a few quanta.
            quanta = self.define_partial_read(qg1)
            qg2 = PredictedQuantumGraph.read_execution_quanta(tmpfile, quantum_ids=quanta)
            self.check_partial_read(qg2, quanta, converting=True)

    def test_roundtrip_old_quantum_graph(self) -> None:
        """Test building a `PredictedQuantumGraph` and round-tripping it
        through the old `QuantumGraph` class.
        """
        qg1 = self.builder.finish(
            output="output_chain",
            metadata={"stuff": "whatever"},
            attach_datastore_records=False,
        ).assemble()
        old_qg = qg1.to_old_quantum_graph()
        qg2 = PredictedQuantumGraph.from_old_quantum_graph(old_qg)
        self.check_quantum_graph(qg2)

    def test_write_new_as_old(self) -> None:
        """Test building a `PredictedQuantumGraphComponents`, saving it with
        the old extension, and reading it back in both the old class and the
        new class.
        """
        components = self.builder.finish(
            output="output_chain",
            metadata={"stuff": "whatever"},
            attach_datastore_records=False,
        )
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            tmpfile = os.path.join(tmpdir, "old.qgraph")
            components.write(tmpfile)
            qg1 = PredictedQuantumGraph.read_execution_quanta(tmpfile)
            self.check_quantum_graph(qg1)
            old_qg = QuantumGraph.loadUri(tmpfile)
            qg2 = PredictedQuantumGraph.from_old_quantum_graph(old_qg)
            self.check_quantum_graph(qg2)

    def test_read_new_as_old(self) -> None:
        """Test building a `PredictedQuantumGraphComponents`, saving it with
        the new extension, and reading it back in via the old class.
        """
        components = self.builder.finish(
            output="output_chain",
            metadata={"stuff": "whatever"},
            attach_datastore_records=False,
        )
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            tmpfile = os.path.join(tmpdir, "new.qg")
            components.write(tmpfile)
            old_qg = QuantumGraph.loadUri(tmpfile)
            new_qg = PredictedQuantumGraph.from_old_quantum_graph(old_qg)
            self.check_quantum_graph(new_qg)

    def test_io(self) -> None:
        """Test saving a `PredictedQuantumGraphComponents` and reading it back
        in as a `PredictedQuantumGraph`, both fully and partially, and as a
        `QuantumGraph`, both fully and partially.
        """
        components = self.builder.finish(
            output="output_chain",
            metadata={"stuff": "whatever"},
            attach_datastore_records=False,
        )
        # We use a small page size since this is a tiny graph and hence
        # otherwise all addresses would fit in a single page.
        four_row_page_size = AddressReader.compute_row_size(int_size=8, n_addresses=1) * 4
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            tmpfile = os.path.join(tmpdir, "new.qg")
            components.write(tmpfile, zstd_dict_n_inputs=24)  # enable dict compression code path
            # Test a full read with the new class.
            with PredictedQuantumGraph.open(tmpfile, page_size=four_row_page_size) as reader:
                reader.read_all()
                full_qg = reader.finish()
                self.check_quantum_graph(full_qg, dimension_data_deserialized=False)
            # Test a full read with the old class (uses new class and then
            # converts to old, and we convert back to new for the test).
            self.check_quantum_graph(
                PredictedQuantumGraph.from_old_quantum_graph(QuantumGraph.loadUri(tmpfile))
            )
            # Test a partial read with the old class.
            quanta = self.define_partial_read(full_qg)
            self.check_partial_read(
                PredictedQuantumGraph.from_old_quantum_graph(QuantumGraph.loadUri(tmpfile, nodes=quanta)),
                quanta,
                converting=True,
            )
            # Test a thin but shallow read with the new class.
            with PredictedQuantumGraph.open(tmpfile, page_size=four_row_page_size) as reader:
                reader.read_thin_graph()
                thin_qg = reader.finish()
                self.check_quantum_graph(
                    thin_qg,
                    dimension_data_deserialized=False,
                    all_quantum_datasets=False,
                    dimension_data_loaded=False,
                    init_quanta=False,
                )
            # Test a deep read of just a few quanta with the new class.
            narrow_qg = PredictedQuantumGraph.read_execution_quanta(
                tmpfile,
                quantum_ids=quanta,
                page_size=four_row_page_size,
            )
            self.check_partial_read(narrow_qg, quanta)

    def test_no_compression_dict(self) -> None:
        """Test saving a `PredictedQuantumGraphComponents` and reading it back
        in as a `PredictedQuantumGraph` with the compression dictionary
        disabled.
        """
        components = self.builder.finish(
            output="output_chain",
            metadata={"stuff": "whatever"},
            attach_datastore_records=False,
        )
        # We use a small page size since this is a tiny graph and hence
        # otherwise all addresses would fit in a single page.
        three_row_page_size = AddressReader.compute_row_size(int_size=8, n_addresses=1) * 3
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            tmpfile = os.path.join(tmpdir, "new.qg")
            components.write(tmpfile, zstd_dict_size=0)
            with PredictedQuantumGraph.open(tmpfile, page_size=three_row_page_size) as reader:
                reader.read_all()
                full_qg = reader.finish()
                self.check_quantum_graph(full_qg, dimension_data_deserialized=False)

    def test_dot(self) -> None:
        """Test visualization via GraphViz dot."""
        qg = self.builder.finish(attach_datastore_records=False).assemble()
        stream = StringIO()
        graph2dot(qg, stream)
        if (dot_cmd := shutil.which("dot")) is None:
            raise unittest.SkipTest("Aborting test early; 'dot' command is not available.")
        # Just check that the dot command can parse what we've given it.
        result = subprocess.run(
            [dot_cmd, "-Txdot"], input=stream.getvalue().encode(), stdout=subprocess.DEVNULL
        )
        result.check_returncode()

    def test_mermaid(self) -> None:
        """Test visualization via Mermaid."""
        qg = self.builder.finish(attach_datastore_records=False).assemble()
        stream = StringIO()
        # Just check that it runs without error.
        graph2mermaid(qg, stream)

    def test_update_output_run(self) -> None:
        """Test the PredictedQuantumGraphComponents.output_run method."""
        components = self.builder.finish(
            output="output_chain",
            metadata={"stuff": "whatever"},
            attach_datastore_records=False,
        )
        components.update_output_run("new_output_run")
        qg = components.assemble()
        for ref in qg.get_init_outputs("calibrate").values():
            self.assertEqual(ref.run, "new_output_run")
        for quantum in qg.build_execution_quanta().values():
            for ref in itertools.chain.from_iterable(quantum.outputs.values()):
                self.assertEqual(ref.run, "new_output_run", msg=str(ref))
            for ref in itertools.chain.from_iterable(quantum.inputs.values()):
                if ref.datasetType.name not in ("raw", "references"):
                    self.assertEqual(ref.run, "new_output_run", msg=str(ref))


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
