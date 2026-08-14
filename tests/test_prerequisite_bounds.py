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

import lsst.pipe.base.connectionTypes as cT
import lsst.utils.tests
from lsst.pipe.base import PipelineTask, PipelineTaskConfig, PipelineTaskConnections
from lsst.pipe.base.tests.mocks import InMemoryRepo


class _BoundsHooksTaskConnections(PipelineTaskConnections, dimensions=("visit", "detector")):
    """Connections for a task whose prerequisite search is driven by the
    spatial bounds of the ``input1`` and ``input2`` connections' data IDs.
    """

    input1 = cT.Input("input1", "Exposure", dimensions=("visit", "detector"))
    input2 = cT.Input("input2", "Exposure", dimensions=("visit",))
    output = cT.Output("output", "Exposure", dimensions=("visit", "detector"))
    refcat = cT.PrerequisiteInput("refcat", "SimpleCatalog", multiple=True, dimensions=("htm7",))

    def getSpatialBoundsConnections(self) -> frozenset[str]:
        return frozenset(["input1", "input2"])


class _BoundsHooksTaskConfig(PipelineTaskConfig, pipelineConnections=_BoundsHooksTaskConnections):
    pass


class _BoundsHooksTask(PipelineTask):
    ConfigClass = _BoundsHooksTaskConfig


class BoundsHooksTestCase(unittest.TestCase):
    """Tests for the spatial-bounds prerequisite-finding hook."""

    def setUp(self) -> None:
        self.helper = InMemoryRepo("base.yaml", "spatial.yaml", use_import_collections_as_input=False)
        self.butler = self.helper.butler
        self.helper.pipeline_graph.add_task("bounds", _BoundsHooksTask)
        # make_quantum_graph adds all of the inputs we need as a side effect.
        self.qg = self.helper.make_quantum_graph()
        self.quanta = {
            (q.dataId["visit"], q.dataId["detector"]): q for q in self.qg.build_execution_quanta().values()
        }

    def test_spatial_bounds(self) -> None:
        """Test that the skypix spatial bounds for a quantum are computed over
        the full set of bound-contributing connections, so a visit-level
        connection enlarges the region beyond what the ``(visit, detector)``
        quantum alone would cover.
        """
        htm7_pixelization = self.butler.dimensions["htm7"].pixelization
        selected_anywhere = set()
        for (visit, detector), quantum in self.quanta.items():
            regions = []
            for connection in ("input1", "input2"):
                for ref in quantum.inputs[connection]:
                    regions.append(self.butler.registry.expandDataId(ref.dataId).region)
            # The expected refcat set is exactly the union of the htm7 pixels
            # covered by the two bound-connection regions (the visit-level
            # ``input2`` region enlarges the ``(visit, detector)`` quantum's).
            expected_pixels = set()
            for region in regions:
                for begin, end in htm7_pixelization.envelope(region):
                    expected_pixels.update(range(begin, end))
            selected = {r.dataId["htm7"] for r in quantum.inputs["refcat"]}
            self.assertEqual(selected, expected_pixels, msg=f"quantum ({visit}, {detector})")
            selected_anywhere |= selected
        # The repository holds more refcat pixels than are ever selected;
        # assert some are correctly excluded, proving the builder selects only
        # the overlapping pixels ("and no more").
        available = {
            r.dataId["htm7"]
            for r in self.butler.registry.queryDatasets("refcat", collections=[self.helper.input_run])
        }
        self.assertTrue(available)
        self.assertLess(selected_anywhere, available)


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
