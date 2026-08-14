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

import unittest

import lsst.utils.tests
from lsst.daf.butler import MissingDatasetTypeError
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph
from lsst.pipe.base.quantum_graph_skeleton import DatasetKey
from lsst.pipe.base.tests.mocks import DynamicConnectionConfig, InMemoryRepo

PREREQ_DIMENSIONS = ["visit", "detector"]


class GeneralPathQuantumGraphBuilder(AllDimensionsQuantumGraphBuilder):
    """A test-only `AllDimensionsQuantumGraphBuilder` subclass that reproduces
    the general per-quantum prerequisite path.

    The real `AllDimensionsQuantumGraphBuilder._find_followup_datasets`
    handles "simple" prerequisite finders with a single bulk query and then
    deletes those finders from `prerequisite_info`, so the base class never
    runs `PrerequisiteFinder.find()` for them.  There is no dedicated hook in
    the current production code to disable that optimization, so this subclass
    re-implements `_find_followup_datasets` to find regular (non-prerequisite)
    datasets exactly as the base class does while *leaving every
    `PrerequisiteFinder` in place*.  The base class per-quantum `find()` path
    then handles all prerequisites.
    """

    def _find_followup_datasets(self, tree, skeleton):
        # Docstring inherited from AllDimensionsQuantumGraphBuilder, except
        # that the per-branch bulk query for prerequisites (and the
        # corresponding removal of PrerequisiteFinder objects) is deliberately
        # omitted so that the base class QuantumGraphBuilder per-quantum find()
        # path handles all prerequisites.
        dataset_key: DatasetKey
        for dimensions, branch in tree.branches_by_dimensions.items():
            if not dimensions:
                for dataset_type_name in branch.dataset_types.keys():
                    dataset_key = DatasetKey(dataset_type_name, self.empty_data_id.required_values)
                    if ref := self.empty_dimensions_datasets.inputs.get(dataset_key):
                        skeleton.set_dataset_ref(ref, dataset_key)
                    if ref := self.empty_dimensions_datasets.outputs_for_skip.get(dataset_key):
                        skeleton.set_output_for_skip(ref)
                    if ref := self.empty_dimensions_datasets.outputs_in_the_way.get(dataset_key):
                        skeleton.set_output_in_the_way(ref)
                continue
            if not branch.dataset_types and not branch.tasks:
                continue
            if not branch.data_ids:
                continue
            with self.butler.query() as butler_query:
                butler_query = butler_query.join_data_coordinates(branch.data_ids)
                for dataset_type_node in branch.dataset_types.values():
                    if tree.subgraph.producer_of(dataset_type_node.name) is None:
                        # Dataset type is an overall input; we always need to
                        # try to find these.
                        count = 0
                        try:
                            for ref in butler_query.datasets(dataset_type_node.name, self.input_collections):
                                skeleton.set_dataset_ref(ref)
                                count += 1
                        except MissingDatasetTypeError:
                            pass
                        self.log.verbose(
                            "Found %d overall-input dataset(s) of type %r.",
                            count,
                            dataset_type_node.name,
                        )
                        continue
                    if self.skip_existing_in:
                        # Dataset type is an intermediate or output; need to
                        # find these if only they're from previously executed
                        # quanta that we might skip...
                        count = 0
                        try:
                            for ref in butler_query.datasets(dataset_type_node.name, self.skip_existing_in):
                                skeleton.set_output_for_skip(ref)
                                count += 1
                                if ref.run == self.output_run:
                                    skeleton.set_output_in_the_way(ref)
                        except MissingDatasetTypeError:
                            pass
                        self.log.verbose(
                            "Found %d output dataset(s) of type %r in %s.",
                            count,
                            dataset_type_node.name,
                            self.skip_existing_in,
                        )
                    if self.output_run_exists and not self.skip_existing_starts_with_output_run:
                        # ...or if they're in the way and would need to be
                        # clobbered (and we haven't already found them in the
                        # previous block).
                        count = 0
                        try:
                            for ref in butler_query.datasets(dataset_type_node.name, [self.output_run]):
                                skeleton.set_output_in_the_way(ref)
                                count += 1
                        except MissingDatasetTypeError:
                            pass
                        self.log.verbose(
                            "Found %d output dataset(s) of type %r in %s.",
                            count,
                            dataset_type_node.name,
                            self.output_run,
                        )
            # Deliberately no bulk prerequisite finding here: all
            # PrerequisiteFinder objects remain in self.prerequisite_info so
            # that the base class per-quantum find() path handles them.
            del branch.data_ids


def _hashable_data_id(data_id) -> tuple:
    """Return a hashable, order-independent representation of a data ID."""
    return tuple(sorted(data_id.required.items()))


def _prerequisite_edges(qg: PredictedQuantumGraph) -> set[tuple]:
    """Extract the set of prerequisite edges from a predicted quantum graph.

    Each element is a tuple of:

    - the prerequisite dataset type name;
    - the (full) data ID of the prerequisite dataset;
    - the label of the consuming task;
    - the (full) data ID of the consuming quantum.

    Prerequisite datasets are identified by their pipeline node
    (`DatasetTypeNode.is_prerequisite`), and each read edge out of such a node
    to a quantum is a (dataset, quantum) prerequisite edge.
    """
    result: set[tuple] = set()
    for dataset_id, dataset_data in qg.bipartite_xgraph.nodes(data=True):
        # Quantum nodes have a "task_label" instead of a "dataset_type_name";
        # only dataset nodes carry the latter, and only those might be
        # prerequisite datasets.
        if "dataset_type_name" not in dataset_data:
            continue
        if not dataset_data["pipeline_node"].is_prerequisite:
            continue
        dataset_data_id = dataset_data["data_id"]
        for _, consumer_id, edge_data in qg.bipartite_xgraph.out_edges(dataset_id, data=True):
            if not edge_data.get("is_read", False):
                continue
            consumer_data = qg.bipartite_xgraph.nodes[consumer_id]
            result.add(
                (
                    dataset_data["dataset_type_name"],
                    _hashable_data_id(dataset_data_id),
                    consumer_data["task_label"],
                    _hashable_data_id(consumer_data["data_id"]),
                )
            )
    return result


class PrerequisitePathEquivalenceTestCase(unittest.TestCase):
    """Verify that the bulk-optimized prerequisite path and the general
    per-quantum `PrerequisiteFinder.find()` path produce identical
    prerequisite edges for the same quanta.
    """

    def setUp(self) -> None:
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
                "flat": DynamicConnectionConfig(
                    dataset_type_name="flat_calib",
                    dimensions=PREREQ_DIMENSIONS,
                    multiple=True,
                    is_calibration=True,
                ),
                "bias": DynamicConnectionConfig(
                    dataset_type_name="bias_calib",
                    dimensions=PREREQ_DIMENSIONS,
                    is_calibration=True,
                ),
                "missing": DynamicConnectionConfig(
                    dataset_type_name="missing_calib",
                    dimensions=PREREQ_DIMENSIONS,
                    is_calibration=True,
                    # Must be optional so that its (intentional) absence
                    # doesn't make adjustQuantum raise before the graph is
                    # built.
                    minimum=0,
                ),
            },
            outputs={
                "output_image": DynamicConnectionConfig(
                    dataset_type_name="image",
                    dimensions=["visit", "detector"],
                )
            },
        )
        # Insert the overall-input datasets that actually exist: the raw
        # input and the present calibration prerequisites.  "missing_calib"
        # is intentionally left unpopulated so that neither path produces a
        # prerequisite edge for it (see test_missing_dataset).  Note that we
        # deliberately do NOT rely on the mock's insert_mocked_inputs
        # auto-population, because that would also insert "missing_calib".
        self.helper.insert_datasets("raw")
        self.helper.insert_datasets("flat_calib")
        self.helper.insert_datasets("bias_calib")

    def _make_general_builder(self) -> GeneralPathQuantumGraphBuilder:
        """Construct a `GeneralPathQuantumGraphBuilder` for the shared
        pipeline, mirroring `MockRepo.make_quantum_graph_builder` with
        ``insert_mocked_inputs=False``.
        """
        builder = GeneralPathQuantumGraphBuilder(
            self.helper.pipeline_graph,
            self.helper.butler,
            input_collections=[self.helper.input_chain],
            output_run="output_run",
        )
        self.helper.pipeline_graph.register_dataset_types(self.helper.butler)
        return builder

    def _build(self, builder):
        return builder.finish(attach_datastore_records=False).assemble()

    def test_bulk_vs_general_equivalence(self) -> None:
        """The bulk-optimized and general paths produce identical prerequisite
        edges for the same quanta of the same pipeline.
        """
        bulk_qg = self._build(self.helper.make_quantum_graph_builder(insert_mocked_inputs=False))
        general_qg = self._build(self._make_general_builder())

        # Sanity checks that this is a non-trivial test: at least one bulk
        # "simple" finder was handled and produced edges, and both graphs agree
        # on which quanta exist.
        self.assertTrue(bulk_qg.quanta_by_task["calibrate"])
        self.assertEqual(
            bulk_qg.quanta_by_task["calibrate"].keys(),
            general_qg.quanta_by_task["calibrate"].keys(),
        )
        self.assertTrue(_prerequisite_edges(bulk_qg), "Expected some prerequisite edges to compare.")

        self.assertEqual(_prerequisite_edges(bulk_qg), _prerequisite_edges(general_qg))

    def test_missing_dataset(self) -> None:
        """A prerequisite dataset type with no datasets in the input
        collections yields no prerequisite edge, regardless of path.
        """
        bulk_qg = self._build(self.helper.make_quantum_graph_builder(insert_mocked_inputs=False))
        general_qg = self._build(self._make_general_builder())

        for qg in (bulk_qg, general_qg):
            edges = _prerequisite_edges(qg)
            # No edge for the missing prerequisite dataset type.
            self.assertNotIn("missing_calib", {edge[0] for edge in edges})
            # Sanity check that the test is not vacuous: the present
            # prerequisite types still produce edges.
            self.assertIn("flat_calib", {edge[0] for edge in edges})
            self.assertIn("bias_calib", {edge[0] for edge in edges})


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
