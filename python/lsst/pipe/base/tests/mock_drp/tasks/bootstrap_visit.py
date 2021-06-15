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

__all__ = ("BootstrapVisitTask",)

from .... import PipelineTask, PipelineTaskConnections, PipelineTaskConfig
from .... import connectionTypes as cT
from .._run_quantum_helper import RunQuantumHelper


class BootstrapVisitConnections(PipelineTaskConnections, dimensions=("instrument", "detector", "visit")):
    input_images = cT.Input(
        "detrended_image",
        dimensions=("instrument", "detector", "exposure"),
        storageClass="StructuredData",
        multiple=True,
    )
    reference_catalogs = cT.PrerequisiteInput(
        "refcat",
        dimensions=("skypix",),
        storageClass="StructuredData",
        multiple=True,
    )
    output_image = cT.Input(
        "bootstrapped_detector_image",
        dimensions=("instrument", "detector", "visit"),
        storageClass="StructuredData",
    )
    output_catalog = cT.Output(
        "bootstrapped_detector_sources",
        dimensions=("instrument", "detector", "visit"),
        storageClass="StructuredData",
    )


class BootstrapVisitConfig(PipelineTaskConfig, pipelineConnections=BootstrapVisitConnections):
    pass


class BootstrapVisitTask(PipelineTask):
    ConfigClass = BootstrapVisitConfig
    _DefaultName = "bootstrap_visit"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        helper = RunQuantumHelper(self.name, butlerQC, inputRefs, outputRefs)
        helper.read_many("input_images", maximum=2)
        helper.read_many("reference_catalogs")
        helper.run()
        helper.write_one("output_image")
        helper.write_one("output_catalog")
