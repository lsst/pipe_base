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

__all__ = ("ConsolidateVisitCatalogTask",)

from .... import PipelineTask, PipelineTaskConnections, PipelineTaskConfig
from .... import connectionTypes as cT
from .._run_quantum_helper import RunQuantumHelper


class ConsolidateVisitCatalogConnections(PipelineTaskConnections, dimensions=("instrument", "visit")):
    input_catalogs = cT.Input(
        "bootstrapped_detector_sources",
        dimensions=("instrument", "detector", "visit"),
        storageClass="StructuredData",
        multiple=True,
    )
    output_catalog = cT.Output(
        "bootstrapped_visit_sources",
        dimensions=("instrument", "visit"),
        storageClass="StructuredData",
        multiple=True,
    )


class ConsolidateVisitCatalogConfig(
    PipelineTaskConfig, pipelineConnections=ConsolidateVisitCatalogConnections
):
    pass


class ConsolidateVisitCatalogTask(PipelineTask):
    ConfigClass = ConsolidateVisitCatalogConfig
    _DefaultName = "consolidate_visit_catalog"

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        helper = RunQuantumHelper(self.name, butlerQC, inputRefs, outputRefs)
        helper.read_many("input_catallogs")
        helper.run()
        helper.write_one("output_catalog")
