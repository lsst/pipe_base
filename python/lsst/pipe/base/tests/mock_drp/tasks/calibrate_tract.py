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

__all__ = ("CalibrateTractTask",)

from .... import PipelineTask, PipelineTaskConnections, PipelineTaskConfig
from .... import connectionTypes as cT
from .._run_quantum_helper import RunQuantumHelper


class CalibrateTractConnections(
    PipelineTaskConnections,
    dimensions=("instrument", "skymap", "tract"),
):
    input_catalogs = cT.PrerequisiteInput(
        "bootstrapped_visit_sources",
        dimensions=("instrument", "visit"),
        storageClass="StructuredData",
        multiple=True,
    )
    reference_catalogs = cT.PrerequisiteInput(
        "refcat",
        dimensions=("skypix",),
        storageClass="StructuredData",
        multiple=True,
    )
    camera = cT.PrerequisiteInput(
        "camera",
        dimensions=("instrument",),
        storageClass="StructuredData",
        isCalibration=True,
        # TODO: lookupFunction
    )
    output_matches = cT.Output(
        "tract_matches",
        dimensions=("instrument", "skymap", "tract"),
        storageClass="StructuredData",
    )
    output_calibrations = cT.Output(
        "tract_calibrations",
        dimensions=("instrument", "visit", "skymap", "tract"),
        storageClass="StructuredData",
        multiple=True
    )


class CalibrateTractConfig(PipelineTaskConfig, pipelineConnections=CalibrateTractConnections):
    pass


class CalibrateTractTask(PipelineTask):
    ConfigClass = CalibrateTractConfig
    _DefaultName = "calibrate_tract"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        helper = RunQuantumHelper(self.name, butlerQC, inputRefs, outputRefs)
        helper.read_many("input_catalogs")
        helper.read_many("reference_catalogs")
        helper.read_one("camera")
        helper.run()
        helper.write_one("output_matches")
        helper.write_many("output_calibrations")
