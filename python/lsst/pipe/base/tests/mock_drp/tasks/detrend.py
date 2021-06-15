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

__all__ = ("DetrendTask",)

from lsst.pex.config import Field, ListField

from .... import PipelineTask, PipelineTaskConnections, PipelineTaskConfig
from .... import connectionTypes as cT
from .._run_quantum_helper import RunQuantumHelper


class DetrendConnections(PipelineTaskConnections, dimensions=("instrument", "detector", "exposure")):
    input_image = cT.Input(
        "raw",
        dimensions=("instrument", "detector", "exposure"),
        storageClass="StructuredData",
    )
    output_image = cT.Input(
        "detrended_image",
        dimensions=("instrument", "detector", "exposure"),
        storageClass="StructuredData",
    )
    bias_image = cT.PrerequisiteInput(
        "master_bias",
        dimensions=(
            "instrument",
            "detector",
        ),
        storageClass="StructuredData",
        isCalibration=True,
    )
    fringe_image = cT.PrerequisiteInput(
        "master_fringe",
        dimensions=(
            "instrument",
            "detector",
            "physical_filter",
        ),
        storageClass="StructuredData",
        isCalibration=True,
        minimum=0,
    )

    def __init__(self, config: DetrendConfig):
        super().__init__(config=config)
        if not config.do_bias:
            self.connections.discard("bias_image")
        if not config.do_fringe:
            self.connections.discard("fringe_image")

    def adjustQuantum(self, inputs, outputs, label, data_id):
        # Check for fringes if they are required for this band, and trim them
        # from the inputs if they are present but not needed for this band.
        connection_instance, fringe_refs = outputs.get("fringe_image", (None, ()))
        adjusted_outputs = {}
        if data_id["band"] in self.config.fringe_bands:
            if not fringe_refs:
                raise FileNotFoundError(
                    f"Fringes needed for band={data_id['band']} for task {label}@{data_id}."
                )
        else:
            if fringe_refs:
                adjusted_outputs["fringe_image"] = (connection_instance, [])
        outputs.update(adjusted_outputs)
        super().adjustQuantum(inputs, outputs, label, data_id)
        return {}, adjusted_outputs


class DetrendConfig(PipelineTaskConfig, pipelineConnections=DetrendConnections):
    do_bias = Field("Apply biases?", dtype=bool, default=True)
    do_fringe = Field("Apply fringes?", dtype=bool, default=True)
    fringe_bands = ListField("Bands to which fringes should be applied.", dtype=str, default=["y"])


class DetrendTask(PipelineTask):
    ConfigClass = DetrendConfig
    _DefaultName = "detrend"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        helper = RunQuantumHelper(self.name, butlerQC, inputRefs, outputRefs)
        helper.read_one("input_image")
        if self.config.do_bias:
            helper.read_one("bias_image")
        if self.config.do_fringe and butlerQC.quantum.dataId["band"] in self.config.fringe_bands:
            helper.read_one("fringe_image")
        helper.run()
        helper.write_one("output_image")
