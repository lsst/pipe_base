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

"""Tests for `QuantumGraphBuilder` / `AllDimensionsQuantumGraphBuilder`.

The first set of tests covers ``adjustQuantum``-driven trimming of input and
output connections, reporting of missing init inputs, and constructor
fallbacks.

The second set covers the per-connection ``PrerequisiteQuery`` hook (the
``query=`` attribute on prerequisite inputs).  These tests are written ahead of
the production wiring that invokes ``PrerequisiteQuery`` during quantum-graph
building: the builder currently uses the default prerequisite path and never
calls ``query``, so the prerequisites found do not match the query-driven
expectations below.  Every concrete test in that set is therefore marked
``@pytest.mark.xfail(strict=True)`` so the suite stays green and the intended
failure is documented.
"""

from __future__ import annotations

__all__ = ()

import unittest
from typing import ClassVar

import pytest
from astropy.time import Time

import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.daf.butler import (
    CollectionType,
    DataCoordinate,
    DatasetType,
    MissingCollectionError,
    Timespan,
)
from lsst.pex.config import Field, ListField
from lsst.pipe.base import (
    PipelineTask,
    PipelineTaskConfig,
    PipelineTaskConnections,
    PrerequisiteQuery,
)
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


# ---------------------------------------------------------------------------
# Per-connection PrerequisiteQuery hook.
#
# These tests exercise the ``query=`` attribute on prerequisite inputs.  The
# PrerequisiteQuery wiring is not (yet) implemented in the builders, so every
# concrete test class below is marked ``@pytest.mark.xfail(strict=True)`` and is
# expected to fail until that wiring lands.
# ---------------------------------------------------------------------------


# htm7 pixels used to seed the reference-catalog datasets inserted below.  The
# tests derive their expected values from the overlap of these pixels with the
# visit/tract regions of the standard spatial test data.
REFCAT_PIXELS = (253952, 253953, 253954, 253955, 253965, 253966)
# Two decoy htm7 pixels (subset of the above) that overlap neither the Cam1
# visit regions nor the SkyMap1 tract/skymap regions of the standard spatial
# test data (verified: 253953 and 253966 are outside both families).  A
# query-driven lookup constrained to either the visit or the tract/skymap
# family must never return them.
DECOY_PIXELS = frozenset({253953, 253966})


def _region_pixels(pixelization, region) -> set[int]:
    """Return the set of skypix pixel IDs covered by ``region``."""
    result = set()
    for begin, end in pixelization.envelope(region):
        result.update(range(begin, end))
    return result


def _skypix_pixels(pixelization, regions) -> set[int]:
    """Return the skypix pixel IDs covered by the union of ``regions``, using
    each region's pixel envelope (a superset of the exact overlap).
    """
    result = set()
    for region in regions:
        result |= _region_pixels(pixelization, region)
    return result


class _PrerequisiteQueryBuilderTestCase(unittest.TestCase):
    """Shared repository fixture for the ``PrerequisiteQuery`` hook tests.

    Sets up an `InMemoryRepo` from the standard spatial test data.  Concrete
    subclasses are marked ``@pytest.mark.xfail(strict=True)``; this base class
    itself declares no tests so it carries no marker.
    """

    def setUp(self) -> None:
        self.helper = InMemoryRepo("base.yaml", "spatial.yaml", use_import_collections_as_input=False)
        self.enterContext(self.helper)
        self.butler = self.helper.butler

    def add_real_task(self, label, task_class, config=None) -> None:
        """Add a task with real `lsst.pipe.base.PipelineTaskConnections` to the
        pipeline graph (the mock ``DynamicConnectionConfig`` has no ``query``
        field, so these tests must declare connections as real task classes).
        """
        if config is None:
            self.helper.pipeline_graph.add_task(label, task_class)
        else:
            self.helper.pipeline_graph.add_task(label, task_class, config=config)

    def insert_skypix_datasets(self, name, skypix_name, pixels, *, run=None) -> None:
        """Register ``name`` over the single skypix dimension ``skypix_name``
        and insert one dataset per pixel in ``pixels``.

        Parameters
        ----------
        name : `str`
            Dataset type name.
        skypix_name : `str`
            Name of the skypix dimension (e.g. ``"htm7"``).
        pixels : iterable of `int`
            Skypix pixel ids to insert datasets for.
        run : `str`, optional
            Run collection to insert into; defaults to the helper's input run.
        """
        self.butler.registry.registerDatasetType(
            DatasetType(name, self.butler.dimensions.conform([skypix_name]), "SimpleCatalog")
        )
        run = self.helper.input_run if run is None else run
        for pixel in pixels:
            self.butler.registry.insertDatasets(name, [{skypix_name: pixel}], run=run)


class _Htm7RefcatSpatialTestCase(_PrerequisiteQueryBuilderTestCase):
    """Shared fixture for spatial tests that seed an htm7 ``refcat`` reference
    catalog and compute expected cells from the htm7 pixelization.

    ``setUp`` installs the htm7 pixelization on ``self.pixelization`` and
    inserts one ``refcat`` dataset per ``REFCAT_PIXELS`` cell.
    """

    def setUp(self) -> None:
        super().setUp()
        self.pixelization = self.butler.dimensions["htm7"].pixelization
        self.insert_skypix_datasets("refcat", "htm7", REFCAT_PIXELS)


class _PatchVisitTaskConnections(PipelineTaskConnections, dimensions=("patch",)):
    """Task whose quantum dimension is ``patch`` and which gathers all input
    visits overlapping each patch; the ``refcat`` prerequisite is searched per
    visit rather than per patch.
    """

    visits = cT.Input("visits", "Exposure", dimensions=("visit",), multiple=True)
    output = cT.Output("spatial_patch_out", "Exposure", dimensions=("patch",))
    refcat = cT.PrerequisiteInput(
        "refcat",
        "SimpleCatalog",
        multiple=True,
        dimensions=("htm7",),
        minimum=0,
        query=PrerequisiteQuery(constraint_dimensions=["visit"]),
    )


class _PatchVisitConfig(PipelineTaskConfig, pipelineConnections=_PatchVisitTaskConnections):
    pass


class _PatchVisitTask(PipelineTask):
    ConfigClass = _PatchVisitConfig


@pytest.mark.xfail(strict=True)
class PatchVisitSpatialTestCase(_Htm7RefcatSpatialTestCase):
    """Spatial constraint dimensions spanning ``patch`` quanta and ``visit`` /
    ``htm7`` datasets: a ``patch`` task constrains its ``refcat`` lookup by the
    input visits it consumes rather than by the patch region alone.
    """

    def setUp(self) -> None:
        super().setUp()
        self.visit_pixels = {v: self._visit_pixels(self.butler, self.pixelization, v) for v in (1, 2)}
        self.helper.pipeline_graph.add_task("spatial_patch", _PatchVisitTask)
        self.qg = self.helper.make_quantum_graph(
            insert_mocked_inputs=True,
            where="skymap='SkyMap1' and tract=0 and patch in (0,4)",
        )
        self.quanta = {q.dataId["patch"]: q for q in self.qg.build_execution_quanta().values()}

    def test_patch_visit_constraint_dimensions(self) -> None:
        """The ``refcat`` cells attached to a patch quantum are the union of
        the htm7 pixels of every input visit overlapping that patch, not just
        the pixels overlapping the patch region itself.
        """
        self.assertEqual(set(self.quanta), {0, 4})
        for patch, quantum in self.quanta.items():
            expected = set()
            for ref in quantum.inputs["visits"]:
                expected |= self.visit_pixels[ref.dataId["visit"]]
            expected &= set(REFCAT_PIXELS)
            observed = {ref.dataId["htm7"] for ref in quantum.inputs["refcat"]}
            self.assertEqual(observed, expected, msg=f"patch {patch}")
        # The decoy pixels overlap no input-visit region and must never be
        # returned for any quantum.
        for quantum in self.quanta.values():
            for ref in quantum.inputs["refcat"]:
                self.assertNotIn(ref.dataId["htm7"], DECOY_PIXELS)

    @staticmethod
    def _visit_pixels(butler, pixelization, visit: int) -> set[int]:
        """Return the htm7 pixels covered by the full region of ``visit``."""
        result = set()
        for record in butler.registry.queryDimensionRecords(
            "visit", where=f"instrument='Cam1' and visit={visit}"
        ):
            result |= _region_pixels(pixelization, record.region)
        return result


class _ExposureDetectorTaskConnections(PipelineTaskConnections, dimensions=("visit",)):
    """Task with per-exposure per-detector inputs whose prerequisites (the
    ``refcat`` reference catalog and the ``bias`` calibration) are constrained
    to the selected ``{visit, detector}`` region.
    """

    expdet = cT.Input("expdet", "Exposure", dimensions=("exposure", "detector"), multiple=True)
    output = cT.Output("spatial_expdet_out", "Exposure", dimensions=("visit",))
    refcat = cT.PrerequisiteInput(
        "refcat",
        "SimpleCatalog",
        multiple=True,
        dimensions=("htm7",),
        minimum=0,
        query=PrerequisiteQuery(constraint_dimensions=["visit", "detector"]),
    )
    bias = cT.PrerequisiteInput(
        "bias",
        "Exposure",
        multiple=True,
        dimensions=("detector",),
        minimum=0,
        isCalibration=True,
        query=PrerequisiteQuery(constraint_dimensions=["exposure", "detector"]),
    )


class _ExposureDetectorConfig(PipelineTaskConfig, pipelineConnections=_ExposureDetectorTaskConnections):
    pass


class _ExposureDetectorTask(PipelineTask):
    ConfigClass = _ExposureDetectorConfig


@pytest.mark.xfail(strict=True)
class ExposureDetectorWhereTestCase(_Htm7RefcatSpatialTestCase):
    """Exposure/detector inputs with ``refcat`` and ``bias`` prerequisites
    restricted to a ``detector``-based ``where`` selection.
    """

    SELECTED_DETECTORS = (1, 2)

    def setUp(self) -> None:
        super().setUp()
        self.butler.registry.insertDimensionData("group", {"instrument": "Cam1", "name": "grp1"})
        exposures = {
            1: {
                "instrument": "Cam1",
                "physical_filter": "Cam1-G",
                "id": 1,
                "obs_id": "exp1",
                "group": "grp1",
                "day_obs": 20210909,
                "exposure_time": 60.0,
                "observation_type": "science",
                "observation_reason": "science",
                "target_name": "test_target",
                "science_program": "test_survey",
                "zenith_angle": 5.0,
            },
            2: {
                "instrument": "Cam1",
                "physical_filter": "Cam1-R1",
                "id": 2,
                "obs_id": "exp2",
                "group": "grp1",
                "day_obs": 20210909,
                "exposure_time": 45.0,
                "observation_type": "science",
                "observation_reason": "science",
                "target_name": "test_target",
                "science_program": "test_survey",
                "zenith_angle": 10.0,
            },
        }
        for exposure in exposures.values():
            self.butler.registry.insertDimensionData("exposure", exposure)
        for exposure_id, visit_id in ((1, 1), (2, 2)):
            self.butler.registry.insertDimensionData(
                "visit_definition", {"instrument": "Cam1", "exposure": exposure_id, "visit": visit_id}
            )

        self.helper.insert_datasets(
            DatasetType("expdet", self.butler.dimensions.conform(["exposure", "detector"]), "Exposure")
        )
        for detector in (1, 2, 3, 4):
            self.butler.registry.insertDatasets(
                "bias", [{"instrument": "Cam1", "detector": detector}], run=self.helper.input_run
            )

        self.helper.pipeline_graph.add_task("spatial_expdet", _ExposureDetectorTask)
        self.qg = self.helper.make_quantum_graph(
            insert_mocked_inputs=False,
            where="instrument='Cam1' and detector in (1, 2) and exposure in (1, 2)",
        )
        self.quanta = {q.dataId["visit"]: q for q in self.qg.build_execution_quanta().values()}

    def test_exposure_detector_where(self) -> None:
        """``refcat`` and ``bias`` prerequisites are restricted to the selected
        ``{visit, detector}`` region rather than the whole visit.
        """
        self.assertEqual(set(self.quanta), {1, 2})
        deselected_only = self._deselected_only_pixels()
        for visit, quantum in self.quanta.items():
            # refcat: every returned cell overlaps at least one selected
            # {visit, detector} region, and nothing overlapping only a
            # deselected detector is returned.
            observed_refcat = {ref.dataId["htm7"] for ref in quantum.inputs["refcat"]}
            self.assertFalse(observed_refcat & DECOY_PIXELS, msg=f"visit {visit}")
            self.assertFalse(observed_refcat & deselected_only, msg=f"visit {visit}")
            # bias: only the selected detectors' calibration is used.
            observed_bias = {ref.dataId["detector"] for ref in quantum.inputs["bias"]}
            self.assertEqual(observed_bias, set(self.SELECTED_DETECTORS), msg=f"visit {visit}")

    def _deselected_only_pixels(self) -> set[int]:
        """htm7 pixels inserted as ``refcat`` that overlap only deselected
        detector regions (i.e. detectors other than ``SELECTED_DETECTORS``).
        """
        deselected = set()
        result = set()
        for visit in (1, 2):
            for detector in (3, 4):
                for record in self.butler.registry.queryDimensionRecords(
                    "visit_detector_region",
                    where=f"instrument='Cam1' and visit={visit} and detector={detector}",
                ):
                    deselected |= _region_pixels(self.pixelization, record.region)
        for pixel in REFCAT_PIXELS:
            # A pixel is "deselected-only" if it overlaps a deselected detector
            # region but no selected detector region.
            overlapping_selected = False
            for visit in (1, 2):
                for detector in self.SELECTED_DETECTORS:
                    for record in self.butler.registry.queryDimensionRecords(
                        "visit_detector_region",
                        where=f"instrument='Cam1' and visit={visit} and detector={detector}",
                    ):
                        if pixel in _region_pixels(self.pixelization, record.region):
                            overlapping_selected = True
            if pixel in deselected and not overlapping_selected:
                result.add(pixel)
        return result


class _DropQuantumDimTaskConnections(PipelineTaskConnections, dimensions=("visit", "detector")):
    """Task whose quantum dimensions are ``(visit, detector)`` but whose
    ``src`` prerequisite is searched per visit (the ``detector`` quantum
    dimension is dropped from the constraint, as in a crosstalk-style lookup).
    """

    visdet = cT.Input("visdet", "Exposure", dimensions=("visit", "detector"))
    output = cT.Output("spatial_dropdim_out", "Exposure", dimensions=("visit", "detector"))
    src = cT.PrerequisiteInput(
        "src",
        "SimpleCatalog",
        multiple=True,
        dimensions=("visit", "detector"),
        minimum=0,
        query=PrerequisiteQuery(constraint_dimensions=["visit"]),
    )


class _DropQuantumDimConfig(PipelineTaskConfig, pipelineConnections=_DropQuantumDimTaskConnections):
    pass


class _DropQuantumDimTask(PipelineTask):
    ConfigClass = _DropQuantumDimConfig


@pytest.mark.xfail(strict=True)
class DropQuantumDimTestCase(_PrerequisiteQueryBuilderTestCase):
    """Dropping a quantum dimension widens the prerequisite search: each
    ``(visit, detector)`` quantum receives the full per-visit ``src`` set (its
    own detector and the other detectors' ``src``), and nothing from the other
    visit.
    """

    DETECTORS = (1, 2, 3, 4)

    def setUp(self) -> None:
        super().setUp()
        self.helper.insert_datasets(
            DatasetType("visdet", self.butler.dimensions.conform(["visit", "detector"]), "Exposure")
        )
        self.butler.registry.registerDatasetType(
            DatasetType("src", self.butler.dimensions.conform(["visit", "detector"]), "SimpleCatalog")
        )
        for visit in (1, 2):
            for detector in self.DETECTORS:
                self.butler.registry.insertDatasets(
                    "src",
                    [{"instrument": "Cam1", "visit": visit, "detector": detector}],
                    run=self.helper.input_run,
                )

        self.helper.pipeline_graph.add_task("spatial_dropdim", _DropQuantumDimTask)
        self.qg = self.helper.make_quantum_graph(insert_mocked_inputs=False)
        self.quanta = {
            (q.dataId["visit"], q.dataId["detector"]): q for q in self.qg.build_execution_quanta().values()
        }

    def test_drop_quantum_dimension(self) -> None:
        """Every ``(visit, detector)`` quantum receives the full per-visit
        ``src`` set and no ``src`` from the other visit.
        """
        self.assertEqual(set(self.quanta), {(v, d) for v in (1, 2) for d in self.DETECTORS})
        for (visit, detector), quantum in self.quanta.items():
            observed = {(ref.dataId["visit"], ref.dataId["detector"]) for ref in quantum.inputs["src"]}
            expected = {(visit, d) for d in self.DETECTORS}
            self.assertEqual(observed, expected, msg=f"quantum ({visit}, {detector})")


class _CalibBaseConnections(PipelineTaskConnections, dimensions=("instrument", "detector")):
    """Task quanta are ``(instrument, detector)`` with a regular input so the
    builder can enumerate detector quanta.
    """

    postisr = cT.Input("postisr", "Exposure", dimensions=("instrument", "detector"))
    output = cT.Output("calexp", "Exposure", dimensions=("instrument", "detector"))


class _PtcLookupQuery(PrerequisiteQuery):
    """A query that gathers all matching ``ptc`` datasets for a quantum's data
    IDs and raises if more than one matches (duplicate ambiguity).
    """

    def run(self, butler, dataset_type, data_ids, task_node):
        result = {}
        for data_id in data_ids:
            matches = list(
                butler.registry.queryDatasets(
                    dataset_type,
                    collections=None,
                    dataId=data_id.subset(task_node.dimensions),
                )
            )
            if len(matches) >= 2:
                raise RuntimeError(f"Multiple ptc datasets match quantum {data_id}: {len(matches)}")
            result[data_id.subset(task_node.dimensions)] = matches or []
        return result


class _PtcConnections(_CalibBaseConnections):
    """``ptc`` is a detector-level calibration whose lookup raises on duplicate
    ambiguity.
    """

    ptc = cT.PrerequisiteInput(
        "ptc",
        "Exposure",
        multiple=True,
        isCalibration=True,
        minimum=0,
        dimensions=("instrument", "detector"),
        query=_PtcLookupQuery(),
    )


class _PtcConfig(PipelineTaskConfig, pipelineConnections=_PtcConnections):
    pass


class _PtcTask(PipelineTask):
    ConfigClass = _PtcConfig


class _FlatMetadataQuery(PrerequisiteQuery):
    """A config-driven lookup that enumerates the configured physical filters
    and performs an epoch-pinned (timespan) calibration lookup for each.
    """

    def run(self, butler, dataset_type, data_ids, task_node):
        epoch = Time(task_node.config.epoch_time, format="jd", scale="tai")
        result: dict = {}
        for data_id in data_ids:
            refs = []
            for physical_filter in task_node.config.physical_filters:
                ref = butler.registry.findDataset(
                    dataset_type,
                    collections=butler.collections,
                    instrument=data_id["instrument"],
                    detector=data_id["detector"],
                    physical_filter=physical_filter,
                    timespan=Timespan(epoch, epoch),
                )
                if ref is not None:
                    refs.append(ref)
            result[data_id.subset(task_node.dimensions)] = refs
        return result


class _FlatMetadataConnections(_CalibBaseConnections):
    """The ``flat`` calibration lookup is driven by a config-aware query."""

    flat = cT.PrerequisiteInput(
        "flat",
        "Exposure",
        multiple=True,
        isCalibration=True,
        minimum=0,
        dimensions=("instrument", "detector", "physical_filter", "band"),
        query=_FlatMetadataQuery(),
    )


class _FlatMetadataTaskConfig(DynamicTestPipelineTaskConfig, pipelineConnections=_FlatMetadataConnections):
    physical_filters = ListField[str](
        doc="Physical filters whose flat should be looked up.", dtype=str, default=[]
    )
    epoch_time = Field[float](
        doc="Epoch (TAI Julian date) at which to evaluate calibration validity.",
        dtype=float,
        default=0.0,
    )


class _FlatMetadataTask(PipelineTask):
    ConfigClass = _FlatMetadataTaskConfig


class _CalibrationBaseTestCase(_PrerequisiteQueryBuilderTestCase):
    """Shared fixture for the calibration-prerequisite query tests: an
    ``(instrument, detector)`` repository plus helpers for inserting detector
    calibration datasets.
    """

    def setUp(self) -> None:
        super().setUp()
        self.flat_dims = self.butler.dimensions.conform(["instrument", "detector", "physical_filter", "band"])
        self.ptc_dims = self.butler.dimensions.conform(["instrument", "detector"])

    def _add_postisr(self) -> None:
        self.helper.insert_datasets(
            DatasetType("postisr", self.butler.dimensions.conform(["instrument", "detector"]), "Exposure")
        )

    def _insert_flat(self, detector, physical_filter, band, run=None):
        return self.butler.registry.insertDatasets(
            "flat",
            [
                {
                    "instrument": "Cam1",
                    "detector": detector,
                    "physical_filter": physical_filter,
                    "band": band,
                }
            ],
            run=run if run is not None else self.helper.input_run,
        )[0]


class CalibAddDimsTestCase(_CalibrationBaseTestCase):
    """A ``{instrument, detector}`` non-temporal calibration task is not
    distinguishable in this codebase, so this case has no test.

    The default prerequisite path already returns all ``physical_filter``
    ``flat`` combos for a non-temporal task (no ``NotImplementedError`` is
    raised; the trailing vanilla ``query_datasets`` returns them all), so a
    ``lookupStaticCalibrations``-style lookup is redundant here and there is
    nothing for a `PrerequisiteQuery` replacement to restore.  This case is
    intentionally untested.
    """


@pytest.mark.xfail(strict=True)
class PtcAmbiguityTestCase(_CalibrationBaseTestCase):
    """Single-match / raise-on-duplicate calibration lookup: the ``ptc`` query
    must raise only when an ambiguous (multi-match) quantum is present in the
    graph.
    """

    def setUp(self) -> None:
        super().setUp()
        self._add_postisr()
        self.butler.registry.registerDatasetType(
            DatasetType("ptc", self.ptc_dims, "Exposure", isCalibration=True)
        )
        for det in (1, 2, 4):
            self._insert_ptc(det, self.helper.input_run)
        # Ambiguous detector 3: one ptc in input_run and an identical-data-id
        # one in a second run, both in the input chain.
        self.butler.collections.register("run2", CollectionType.RUN)
        self._insert_ptc(3, self.helper.input_run)
        self._insert_ptc(3, "run2")
        self.butler.collections.redefine_chain(self.helper.input_chain, [self.helper.input_run, "run2"])

    def test_ptc_ambiguity_only_fails_when_ambiguous_quantum_present(self) -> None:
        self.add_real_task("ptc", _PtcTask)

        ok_qg = self.helper.make_quantum_graph(where="detector in (1,2,4)", insert_mocked_inputs=False)
        ok_by_detector = {}
        for quantum in ok_qg.build_execution_quanta(task_label="ptc").values():
            ok_by_detector[quantum.dataId["detector"]] = list(quantum.inputs.get("ptc", ()))
        self.assertEqual({d: len(v) for d, v in ok_by_detector.items()}, {1: 1, 2: 1, 4: 1})

        # Ambiguous detector 3 included: the query must raise RuntimeError.
        with pytest.raises(RuntimeError):
            self.helper.make_quantum_graph(where="detector in (1,3)", insert_mocked_inputs=False)

    def _insert_ptc(self, detector, run):
        return self.butler.registry.insertDatasets(
            "ptc", [{"instrument": "Cam1", "detector": detector}], run=run
        )[0]


@pytest.mark.xfail(strict=True)
class FlatMetadataConfigTestCase(_CalibrationBaseTestCase):
    """Config-driven calibration lookup via a custom ``run`` that enumerates the
    configured physical filters at the configured epoch and returns each valid
    calibration.
    """

    def test_flat_metadata_config_driven_timespan_lookup(self) -> None:
        self._add_postisr()
        self.butler.registry.registerDatasetType(
            DatasetType("flat", self.flat_dims, "Exposure", isCalibration=True)
        )
        config = _FlatMetadataTaskConfig()
        config.physical_filters = ["Cam1-G", "Cam1-R1"]
        config.epoch_time = 2460000.0
        self.add_real_task("flat", _FlatMetadataTask, config=config)

        cam1g = self._insert_flat(2, "Cam1-G", "g")
        cam1r1 = self._insert_flat(2, "Cam1-R1", "r")
        cam1r2 = self._insert_flat(2, "Cam1-R2", "r")

        self.butler.collections.register("calib", CollectionType.CALIBRATION)
        self.butler.collections.redefine_chain(self.helper.input_chain, [self.helper.input_run, "calib"])
        self.butler.registry.certify(
            "calib",
            [cam1g],
            Timespan(Time(2459999.0, format="jd", scale="tai"), Time(2460001.0, format="jd", scale="tai")),
        )
        self.butler.registry.certify(
            "calib",
            [cam1r1],
            Timespan(Time(2460002.0, format="jd", scale="tai"), Time(2460003.0, format="jd", scale="tai")),
        )
        self.butler.registry.certify(
            "calib",
            [cam1r2],
            Timespan(Time(2459999.0, format="jd", scale="tai"), Time(2460001.0, format="jd", scale="tai")),
        )

        qg = self.helper.make_quantum_graph(insert_mocked_inputs=False)
        by_detector = {}
        for quantum in qg.build_execution_quanta(task_label="flat").values():
            refs = quantum.inputs.get("flat", ())
            by_detector[quantum.dataId["detector"]] = [
                (ref.dataId["physical_filter"], ref.dataId["band"]) for ref in refs
            ]
        self.assertEqual(by_detector[2], [("Cam1-G", "g")])


class _RefCatSpatialUnionQuery(PrerequisiteQuery):
    """A query that projects the builder-provided data IDs onto each spatial
    family and delegates to the base ``run`` once per family, then unions the
    resulting per-quantum dictionaries (a single ``constraint_dimensions``
    cannot bound a skypix search by the union of two disjoint spatial families).
    """

    def __init__(self):
        super().__init__(constraint_dimensions=["visit", "tract", "skymap"])

    def run(self, butler, dataset_type, data_ids, task_node):
        visit_ids = {data_id.subset(["instrument", "visit"]) for data_id in data_ids}
        tract_ids = {data_id.subset(["instrument", "tract", "skymap"]) for data_id in data_ids}
        result = dict(super().run(butler, dataset_type, visit_ids, task_node))
        for quantum_id, refs in super().run(butler, dataset_type, tract_ids, task_node).items():
            result.setdefault(quantum_id, []).extend(refs)
        return result


class _MultiFamilyBaseConnections(PipelineTaskConnections, dimensions=("instrument",)):
    """Shared connections for a task whose quanta are per-instrument and whose
    ``ref_cat`` prerequisite is found by unioning the visit and tract/skymap
    spatial families.
    """

    isolated_star_cats = cT.Input(
        "isolated_star_cats",
        "Catalog",
        multiple=True,
        dimensions=("instrument", "tract", "skymap"),
    )
    visit_summaries = cT.Input(
        "visit_summaries",
        "Catalog",
        multiple=True,
        dimensions=("instrument", "visit"),
    )
    output = cT.Output("multi_family_output", "Catalog", dimensions=("instrument",))
    ref_cat = cT.PrerequisiteInput(
        "ref_cat",
        "SimpleCatalog",
        multiple=True,
        dimensions=("htm7",),
        query=_RefCatSpatialUnionQuery(),
    )


class _MultiFamilyHealpixConnections(_MultiFamilyBaseConnections):
    ref_cat = cT.PrerequisiteInput(
        "ref_cat",
        "SimpleCatalog",
        multiple=True,
        dimensions=("healpix7",),
        query=_RefCatSpatialUnionQuery(),
    )


def _make_task(connections):
    class _Config(PipelineTaskConfig, pipelineConnections=connections):
        pass

    class _Task(PipelineTask):
        ConfigClass = _Config
        _DefaultName = "spatial_union_query"

    return _Task


_MultiFamilyTask = _make_task(_MultiFamilyBaseConnections)
_MultiFamilyHealpixTask = _make_task(_MultiFamilyHealpixConnections)


class _MultiFamilyQueryTestCase(_PrerequisiteQueryBuilderTestCase):
    """Shared fixture for the multi-family spatial-union prerequisite tests."""

    def setUp(self) -> None:
        super().setUp()
        self._insert_regular_inputs()

    def _insert_regular_inputs(self) -> None:
        for name, dims in (
            ("isolated_star_cats", ["instrument", "tract", "skymap"]),
            ("visit_summaries", ["instrument", "visit"]),
        ):
            self.butler.registry.registerDatasetType(
                DatasetType(name, self.butler.dimensions.conform(dims), "Catalog")
            )
            self.helper.insert_datasets(
                DatasetType(name, self.butler.dimensions.conform(dims), "Catalog"), register=False
            )

    def _visit_and_tract_region_sets(self):
        visit_regions = [
            self.butler.registry.expandDataId(instrument="Cam1", visit=visit).region for visit in (1, 2)
        ]
        tract_regions = [
            self.butler.registry.expandDataId(skymap="SkyMap1", tract=tract, instrument="Cam1").region
            for tract in (0, 1)
        ]
        return visit_regions, tract_regions

    def _single_quantum_ref_cat_pixels(self, label: str, skypix_name: str) -> set[int]:
        qg = self.helper.make_quantum_graph(insert_mocked_inputs=False)
        quanta = list(qg.build_execution_quanta().values())
        self.assertEqual(len(quanta), 1)
        return {ref.dataId[skypix_name] for ref in quanta[0].inputs["ref_cat"]}


@pytest.mark.xfail(strict=True)
class MultiFamilySpatialUnionTestCase(_MultiFamilyQueryTestCase):
    """Multi-family spatial union with ``ref_cat`` on the common ``htm7``
    skypix: the query unions the visit and tract/skymap families.

    The default (unimplemented-query) path returns the full sky, so it also
    picks up the "neither" decoy cells that the query-driven expectation
    excludes -- this divergence is what makes the test fail now.
    """

    def test_multi_family_spatial_union_htm7(self) -> None:
        self.helper.pipeline_graph.add_task("multi_family", _MultiFamilyTask)
        pixelization = self.butler.dimensions["htm7"].pixelization
        visit_regions, tract_regions = self._visit_and_tract_region_sets()
        visit_cells = _skypix_pixels(pixelization, visit_regions)
        tract_cells = _skypix_pixels(pixelization, tract_regions)
        expected_cells = visit_cells | tract_cells
        # Some of the htm7 cells covered by the tract/skymap region are shared
        # with the visit regions ("both"), and the "neither" decoy cells (in
        # DECOY_PIXELS) lie outside the union; the visit regions add no cells
        # of their own beyond the tract region here.
        self.assertTrue(tract_cells - visit_cells)
        self.assertTrue(visit_cells & tract_cells)
        self.assertTrue(expected_cells.isdisjoint(DECOY_PIXELS))
        self.insert_skypix_datasets("ref_cat", "htm7", sorted(expected_cells | set(DECOY_PIXELS)))
        actual_cells = self._single_quantum_ref_cat_pixels("multi_family", "htm7")
        self.assertEqual(actual_cells, expected_cells)


@pytest.mark.xfail(strict=True)
class MultiFamilyHealpixSpatialUnionTestCase(_MultiFamilyQueryTestCase):
    """Same multi-family spatial union but with ``ref_cat`` on the non-common
    ``healpix7`` skypix.

    Healpix7 is not the common skypix dimension, so the base ``run`` region ->
    pixel matching requires dimension records.  Healpix level 7 maps the
    visit/tract regions onto a modest, non-trivial cell set with non-empty
    visit-only / tract-only / both classes.
    """

    # A healpix7 pixel overlapping neither the Cam1 visit regions nor the
    # SkyMap1 tract/skymap regions, inserted so the (unimplemented) default
    # path picks it up while the query-driven expectation excludes it.
    NEITHER_PIXEL = 72359

    def test_multi_family_spatial_union_healpix7(self) -> None:
        self.helper.pipeline_graph.add_task("multi_family_hp", _MultiFamilyHealpixTask)
        pixelization = self.butler.dimensions["healpix7"].pixelization
        visit_regions, tract_regions = self._visit_and_tract_region_sets()
        visit_cells = _skypix_pixels(pixelization, visit_regions)
        tract_cells = _skypix_pixels(pixelization, tract_regions)
        expected_cells = visit_cells | tract_cells
        self.assertTrue(visit_cells - tract_cells)
        self.assertTrue(tract_cells - visit_cells)
        self.assertTrue(visit_cells & tract_cells)
        self.assertNotIn(self.NEITHER_PIXEL, expected_cells)
        self.insert_skypix_datasets("ref_cat", "healpix7", sorted(expected_cells | {self.NEITHER_PIXEL}))
        actual_cells = self._single_quantum_ref_cat_pixels("multi_family_hp", "healpix7")
        self.assertEqual(actual_cells, expected_cells)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
