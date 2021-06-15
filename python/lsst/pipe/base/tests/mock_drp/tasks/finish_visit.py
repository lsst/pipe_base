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

__all__ = ("FinishVisitTask",)

from lsst.pex.config import Field

from .... import PipelineTask, PipelineTaskConnections, PipelineTaskConfig
from .... import connectionTypes as cT
from .._run_quantum_helper import RunQuantumHelper


class FinishVisitConnections(PipelineTaskConnections, dimensions=("instrument", "visit", "detector")):
    input_image = cT.Input(
        "bootstrapped_detector_image",
        dimensions=("instrument", "detector", "visit"),
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
    input_characterizations = cT.Input(
        "characterizations",
        dimensions=("instrument", "visit"),
        storageClass="StructuredData",
    )
    output_image = cT.Output(
        "final_detector_image",
        dimensions=("instrument", "detector", "visit"),
        storageClass="StructuredData",
    )
    output_catalog = cT.Output(
        "final_detector_sources",
        dimensions=("instrument", "visit", "detector"),
        storageClass="StructuredData",
    )


class FinishVisitConfig(PipelineTaskConfig, pipelineConnections=FinishVisitConnections):
    use_tract = Field("Use per-tract calibration?", dtype=bool, default=False)
    use_global = Field("Use global calibration?", dtype=bool, default=False)


class FinishVisitTask(PipelineTask):
    ConfigClass = FinishVisitConfig
    _DefaultName = "characterize_visit"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        helper = RunQuantumHelper(self.name, butlerQC, inputRefs, outputRefs)
        helper.read_one("input_image")
        helper.read_one("input_catalog")
        if self.config.use_tract:
            helper.read_one("input_calibrations_tract")
        if self.config.use_global:
            helper.read_one("input_calibrations_global")
        helper.run()
        helper.write_one("output_image")
        helper.write_one("output_catalog")
