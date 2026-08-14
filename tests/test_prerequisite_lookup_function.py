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

"""Tests for the ``lookupFunction`` prerequisite hook."""

from __future__ import annotations

import unittest

import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.daf.butler import DatasetType
from lsst.pipe.base import PipelineTask, PipelineTaskConfig, PipelineTaskConnections
from lsst.pipe.base.tests.mocks import InMemoryRepo

# Refcat pixels inserted into the test repository during setUp.
REFERENCE_PIXELS = (253952, 253953, 253954, 253955, 253965, 253966)


def _lookup_even_pixels(dataset_type, registry, data_id, input_collections):
    """Return only the reference-catalog datasets whose ``htm7`` pixel ID is
    even, entirely independent of the quantum's dimensions and spatial bounds.
    """
    return [
        ref
        for ref in registry.queryDatasets(dataset_type, collections=input_collections)
        if ref.dataId["htm7"] % 2 == 0
    ]


def _lookup_with_none(dataset_type, registry, data_id, input_collections):
    """Return all reference-catalog refs but insert `None` entries, which must
    be filtered out.
    """
    refs = list(registry.queryDatasets(dataset_type, collections=input_collections))
    return [refs[0], None, *refs[1:], None]


class _LookupFunctionBaseTaskConnections(PipelineTaskConnections, dimensions=("visit", "detector")):
    """Connections shared by all ``lookupFunction`` test tasks."""

    input_image = cT.Input("calexp", "Exposure", dimensions=("visit", "detector"))
    output = cT.Output("calexp_proc", "Exposure", dimensions=("visit", "detector"))
    refcat = cT.PrerequisiteInput("refcat", "SimpleCatalog", multiple=True, dimensions=("htm7",))


class _LookupEvenPixelsTaskConnections(_LookupFunctionBaseTaskConnections):
    refcat = cT.PrerequisiteInput(
        "refcat",
        "SimpleCatalog",
        multiple=True,
        dimensions=("htm7",),
        lookupFunction=_lookup_even_pixels,
    )


class _LookupWithNoneTaskConnections(_LookupFunctionBaseTaskConnections):
    refcat = cT.PrerequisiteInput(
        "refcat",
        "SimpleCatalog",
        multiple=True,
        dimensions=("htm7",),
        lookupFunction=_lookup_with_none,
    )


class _LookupTaskConfig(PipelineTaskConfig, pipelineConnections=_LookupEvenPixelsTaskConnections):
    pass


class _LookupTask(PipelineTask):
    ConfigClass = _LookupTaskConfig


class _LookupNoneTaskConfig(PipelineTaskConfig, pipelineConnections=_LookupWithNoneTaskConnections):
    pass


class _LookupNoneTask(PipelineTask):
    ConfigClass = _LookupNoneTaskConfig


class LookupFunctionTestCase(unittest.TestCase):
    """Tests for the ``lookupFunction`` prerequisite hook."""

    def setUp(self) -> None:
        self.helper = InMemoryRepo("base.yaml", "spatial.yaml", use_import_collections_as_input=False)
        self.butler = self.helper.butler
        self.helper.insert_datasets(
            DatasetType("calexp", self.butler.dimensions.conform(["visit", "detector"]), "Exposure")
        )
        self.butler.registry.registerDatasetType(
            DatasetType("refcat", self.butler.dimensions.conform(["htm7"]), "SimpleCatalog")
        )
        for pixel in REFERENCE_PIXELS:
            self.butler.registry.insertDatasets("refcat", [{"htm7": pixel}], run=self.helper.input_run)

    def _check_refcat_pixels(self, qg, expected) -> None:
        quanta = {(q.dataId["visit"], q.dataId["detector"]): q for q in qg.build_execution_quanta().values()}
        self.assertEqual(set(quanta), expected.keys())
        for (visit, detector), expected_pixels in expected.items():
            quantum = quanta[(visit, detector)]
            self.assertEqual(
                [ref.dataId["htm7"] for ref in quantum.inputs["refcat"]],
                sorted(expected_pixels),
                msg=f"quantum ({visit}, {detector})",
            )

    def test_lookup_function_is_used(self) -> None:
        """Test that a ``lookupFunction`` takes over prerequisite finding,
        returning exactly its own refs in the predicted quantum graph even when
        those refs lie on pixels the default spatial lookup would have pruned.
        """
        self.helper.pipeline_graph.add_task("lookup", _LookupTask)
        qg = self.helper.make_quantum_graph(insert_mocked_inputs=False)
        expected_even_pixels = sorted(p for p in REFERENCE_PIXELS if p % 2 == 0)
        # Every (visit, detector) pair gets exactly the even pixels, regardless
        # of its region; notably this includes 253966, which is not covered by
        # any visit-1 or visit-2 detector region.
        expected = {(visit, detector): expected_even_pixels for visit in (1, 2) for detector in range(1, 5)}
        self._check_refcat_pixels(qg, expected)

    def test_lookup_function_filters_none(self) -> None:
        """Test that `None` entries returned by a ``lookupFunction`` are
        ignored.
        """
        self.helper.pipeline_graph.add_task("lookup", _LookupNoneTask)
        qg = self.helper.make_quantum_graph(insert_mocked_inputs=False)
        expected = {
            (visit, detector): sorted(REFERENCE_PIXELS) for visit in (1, 2) for detector in range(1, 5)
        }
        self._check_refcat_pixels(qg, expected)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
