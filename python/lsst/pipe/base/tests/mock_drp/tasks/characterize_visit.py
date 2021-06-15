# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("CharacterizeVisitTask",)

from lsst.pex.config import Field

from .... import PipelineTask, PipelineTaskConnections, PipelineTaskConfig
from .... import connectionTypes as cT
from .._run_quantum_helper import RunQuantumHelper


class CharacterizeVisitConnections(PipelineTaskConnections, dimensions=("instrument", "visit")):
    input_images = cT.Input(
        "bootstrapped_detector_image",
        dimensions=("instrument", "detector", "visit"),
        storageClass="StructuredData",
        multiple=True
    )
    input_catalog = cT.Input(
        "bootstrapped_visit_sources",
        dimensions=("instrument", "visit"),
        storageClass="StructuredData",
    )
    input_matches_global = cT.Input(
        "global_matches",
        dimensions=("instrument",),
        storageClass="StructuredData",
    )
    input_matches_tract = cT.Input(
        "tract_matches",
        dimensions=("instrument", "skymap", "tract"),
        storageClass="StructuredData",
    )
    input_calibrations_global = cT.Input(
        "global_calibrations",
        dimensions=("instrument", "visit"),
        storageClass="StructuredData",
    )
    input_calibrations_tract = cT.Input(
        "tract_calibrations",
        dimensions=("instrument", "visit", "skymap", "tract"),
        storageClass="StructuredData",
    )
    output_characterizations = cT.Output(
        "characterizations",
        dimensions=("instrument", "visit"),
        storageClass="StructuredData",
    )


class CharacterizeVisitConfig(PipelineTaskConfig, pipelineConnections=CharacterizeVisitConnections):
    use_tract = Field("Use per-tract matches and calibration?", dtype=bool, default=False)
    use_global = Field("Use global matches and calibration?", dtype=bool, default=False)


class CharacterizeVisitTask(PipelineTask):
    ConfigClass = CharacterizeVisitConfig
    _DefaultName = "characterize_visit"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        helper = RunQuantumHelper(self.name, butlerQC, inputRefs, outputRefs)
        helper.read_many("input_images")
        helper.read_one("input_catalog")
        if self.config.use_tract:
            helper.read_one("input_matches_tract")
            helper.read_one("input_calibrations_tract")
        if self.config.use_global:
            helper.read_one("input_matches_global")
            helper.read_one("input_calibrations_global")
        helper.run()
        helper.write_one("output_characterizations")
