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
from typing import ClassVar

import pytest

import lsst.utils.tests
from lsst.daf.butler import CollectionType, DataCoordinate, MissingCollectionError
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.quantum_graph_builder import InitInputMissingError, QuantumGraphBuilderError
from lsst.pipe.base.tests.mocks import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
    InMemoryRepo,
)
from lsst.pipe.base.tests.mocks._pipeline_task import DynamicTestPipelineTaskConnections


class _TrimmingConnectionsBase(DynamicTestPipelineTaskConnections):
    """Base connections class for tasks whose ``adjustQuantum`` trims a
    all datasets in connection away.

    Subclasses set ``input_to_drop`` and/or ``output_to_drop`` to the
    names of the connection(s) whose refs should be removed by
    ``adjustQuantum``.
    """

    input_to_drop: ClassVar[str] | None = None
    output_to_drop: ClassVar[str] | None = None

    def adjustQuantum(self, inputs, outputs, label, data_id):
        # Only the returned adjusted dicts are consumed by
        # AdjustQuantumHelper.adjust_in_place; the ``inputs``/``outputs``
        # mappings passed in are read-only here, so we must not mutate them in
        # place.
        adjusted_inputs = {}
        if self.input_to_drop is not None and self.input_to_drop in inputs:
            input_connection, _ = inputs[self.input_to_drop]
            adjusted_inputs[self.input_to_drop] = (input_connection, [])
        adjusted_outputs = {}
        if self.output_to_drop is not None and self.output_to_drop in outputs:
            output_connection, _ = outputs[self.output_to_drop]
            adjusted_outputs[self.output_to_drop] = (output_connection, [])
        super().adjustQuantum(inputs, outputs, label, data_id)
        return adjusted_inputs, adjusted_outputs


class _DropOutputConnections(_TrimmingConnectionsBase):
    """Variant that drops the output connection named ``dropped``."""

    output_to_drop = "dropped"


class _DropOutputConfig(DynamicTestPipelineTaskConfig, pipelineConnections=_DropOutputConnections):
    pass


class _DropOutputTask(DynamicTestPipelineTask):
    ConfigClass = _DropOutputConfig


class _DropInputConnections(_TrimmingConnectionsBase):
    """Variant that drops the input connection named ``dropped``."""

    input_to_drop = "dropped"


class _DropInputConfig(DynamicTestPipelineTaskConfig, pipelineConnections=_DropInputConnections):
    pass


class _DropInputTask(DynamicTestPipelineTask):
    ConfigClass = _DropInputConfig


class AdjustQuantumTrimmingConnectionsTestCase(unittest.TestCase):
    """Tests for the ``adjustQuantum`` output- and input-trimming paths of
    `QuantumGraphBuilder` (``outputs_adjusted``/``inputs_adjusted``,
    `_find_removed`, and ``remove_input_edges``).
    """

    def setUp(self):
        self.helper = InMemoryRepo("base.yaml", "spatial.yaml")
        self.enterContext(self.helper)

    def add_trimmer(self, task, *, inputs, outputs) -> None:
        """Add a single trimming task consuming ``inputs`` and producing
        ``outputs`` (mappings of connection name to `DynamicConnectionConfig`).
        """
        self.helper.add_task(
            "trimmer",
            task_class=task,
            config=task.ConfigClass(),
            dimensions=["visit"],
            inputs=inputs,
            outputs=outputs,
        )

    def test_output_trimming_removes_output_nodes(self) -> None:
        """Test that trimming an output connection removes the corresponding
        dataset nodes from the graph.
        """
        self.add_trimmer(
            _DropOutputTask,
            inputs={"i": DynamicConnectionConfig(dataset_type_name="input_runtime", dimensions=["visit"])},
            outputs={
                "kept": DynamicConnectionConfig(dataset_type_name="kept_out", dimensions=["visit"]),
                "dropped": DynamicConnectionConfig(dataset_type_name="dropped_out", dimensions=["visit"]),
            },
        )
        qg = self.helper.make_quantum_graph()
        self.assertEqual(len(qg), 2)
        # The trimmed dataset type has no dataset nodes left in the graph.
        self.assertEqual(qg.datasets_by_type["dropped_out"], {})
        # ...while the retained output is still present, one per visit.
        self.assertEqual(len(qg.datasets_by_type["kept_out"]), 2)
        # ...and the per-quantum output list is empty for the trimmed
        # connection but populated for the retained one.
        for quantum in qg.build_execution_quanta().values():
            outputs = {data_type.name: refs for data_type, refs in quantum.outputs.items()}
            self.assertEqual(len(outputs["dropped_out"]), 0)
            self.assertEqual(len(outputs["kept_out"]), 1)

    def test_input_trimming_removes_input_edges(self) -> None:
        """Test that trimming some (but not all) input refs removes only the
        affected input edges from the graph, leaving the other inputs.
        """
        self.add_trimmer(
            _DropInputTask,
            inputs={
                "keep": DynamicConnectionConfig(dataset_type_name="input_keep", dimensions=["visit"]),
                "dropped": DynamicConnectionConfig(
                    dataset_type_name="input_drop",
                    dimensions=["visit"],
                    minimum=0,
                ),
            },
            outputs={"o": DynamicConnectionConfig(dataset_type_name="out_keep", dimensions=["visit"])},
        )
        qg = self.helper.make_quantum_graph()
        self.assertEqual(len(qg), 2)
        for quantum in qg.build_execution_quanta().values():
            inputs = {data_type.name: refs for data_type, refs in quantum.inputs.items()}
            # The trimmed input connection has no refs left on the quantum ...
            self.assertEqual(len(inputs["input_drop"]), 0)
            # ... while the retained input connection is untouched.
            self.assertEqual(len(inputs["input_keep"]), 1)

    def test_input_trimming_all_inputs_error(self) -> None:
        """Test that adjusting away every input while retaining outputs raises
        `QuantumGraphBuilderError`.
        """
        self.add_trimmer(
            _DropInputTask,
            inputs={
                "dropped": DynamicConnectionConfig(
                    dataset_type_name="input_only",
                    dimensions=["visit"],
                    minimum=0,
                )
            },
            outputs={"o": DynamicConnectionConfig(dataset_type_name="out_keep", dimensions=["visit"])},
        )
        with pytest.raises(QuantumGraphBuilderError):
            self.helper.make_quantum_graph()


class InitInputMissingTestCase(unittest.TestCase):
    """Tests for the `InitInputMissingError` behavior of
    `QuantumGraphBuilder`.
    """

    def test_overall_init_input_missing(self) -> None:
        """Test that an overall init-input that cannot be found in the input
        collections raises `InitInputMissingError`.
        """
        helper = InMemoryRepo()
        self.enterContext(helper)
        helper.add_task(
            "t",
            inputs={"i": DynamicConnectionConfig(dataset_type_name="input_runtime")},
            init_inputs={"ii": DynamicConnectionConfig(dataset_type_name="input_init")},
            outputs={"o": DynamicConnectionConfig(dataset_type_name="output_runtime")},
        )
        # Insert the regular (per-quantum) overall input but leave the init
        # input absent, so a quantum still exists for the init-input check to
        # run against.
        helper.insert_datasets("input_runtime")
        with pytest.raises(InitInputMissingError):
            helper.make_quantum_graph(insert_mocked_inputs=False)

    def test_skipped_task_init_output_missing(self) -> None:
        """Test that a skipped task whose init-output is missing from
        ``skip_existing_in`` raises `InitInputMissingError`.
        """
        helper = InMemoryRepo()
        self.enterContext(helper)
        helper.add_task(
            "t",
            inputs={"i": DynamicConnectionConfig(dataset_type_name="input_runtime")},
            init_outputs={"io": DynamicConnectionConfig(dataset_type_name="init_output")},
            outputs={"o": DynamicConnectionConfig(dataset_type_name="output_runtime")},
        )
        helper.butler.collections.register("prior_run")
        # Resolve the graph and insert the task's metadata so its single
        # quantum is skipped, but leave its init-outputs absent from
        # skip_existing_in.
        helper.make_quantum_graph_builder(output_run="output_run", skip_existing_in=["prior_run"])
        task_node = helper.pipeline_graph.tasks["t"]
        metadata_name = task_node.metadata_output.parent_dataset_type_name
        metadata_dt = helper.pipeline_graph.dataset_types[metadata_name].dataset_type
        empty_data_id = DataCoordinate.make_empty(helper.butler.dimensions)
        helper.butler.registry.insertDatasets(metadata_dt, [empty_data_id], run="prior_run")
        with pytest.raises(InitInputMissingError):
            helper.make_quantum_graph(skip_existing_in=["prior_run"])


class ConstructorFallbackTestCase(unittest.TestCase):
    """Tests for the `QuantumGraphBuilder` constructor fallbacks."""

    def setUp(self):
        self.helper = InMemoryRepo()
        self.enterContext(self.helper)
        self.helper.add_task()
        self.pipeline_graph = self.helper.pipeline_graph
        self.butler = self.helper.butler

    def test_no_input_collections_raises(self) -> None:
        """An empty input-collections sequence raises `ValueError`."""
        with pytest.raises(ValueError):
            AllDimensionsQuantumGraphBuilder(
                self.pipeline_graph, self.butler, input_collections=[], output_run="output_run"
            )

    def test_no_output_run_raises(self) -> None:
        """An absent output RUN collection (via ``butler.run``) raises
        `ValueError`.
        """
        with pytest.raises(ValueError):
            AllDimensionsQuantumGraphBuilder(
                self.pipeline_graph, self.butler, input_collections=[self.helper.input_chain]
            )

    def test_non_run_output_collection_raises(self) -> None:
        """An output collection that exists but is not a RUN collection raises
        `RuntimeError`.
        """
        self.butler.collections.register("out_chain", CollectionType.CHAINED)
        with pytest.raises(RuntimeError):
            AllDimensionsQuantumGraphBuilder(
                self.pipeline_graph,
                self.butler,
                input_collections=[self.helper.input_chain],
                output_run="out_chain",
            )

    def test_skip_existing_in_missing_collection_raises(self) -> None:
        """Test that a nonexistent ``skip_existing_in`` collection raises
        `~lsst.daf.butler.MissingCollectionError` rather than silently
        disabling skips.
        """
        with pytest.raises(MissingCollectionError):
            self.helper.make_quantum_graph_builder(
                output_run="output_run", skip_existing_in=["definitely_missing"]
            )


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
