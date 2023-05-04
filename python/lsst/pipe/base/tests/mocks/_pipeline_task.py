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

__all__ = ("MockPipelineTask", "MockPipelineTaskConfig")

import logging
from typing import TYPE_CHECKING, Any

from lsst.pex.config import Field
from lsst.utils.doImport import doImportType

from ...pipelineTask import PipelineTask
from ...config import PipelineTaskConfig
from ...connections import PipelineTaskConnections, InputQuantizedConnection, OutputQuantizedConnection
from ._data_id_match import DataIdMatch

_LOG = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ...butlerQuantumContext import ButlerQuantumContext


class MockPipelineTaskConfig(PipelineTaskConfig, pipelineConnections=PipelineTaskConnections):
    fail_condition: Field[str] = Field(
        dtype=str,
        default="",
        doc=(
            "Condition on DataId to raise an exception. String expression which includes attributes of "
            "quantum DataId using a syntax of daf_butler user expressions (e.g. 'visit = 123')."
        ),
    )

    fail_exception: Field[str] = Field(
        dtype=str,
        default="builtins.ValueError",
        doc=(
            "Class name of the exception to raise when fail condition is triggered. Can be "
            "'lsst.pipe.base.NoWorkFound' to specify non-failure exception."
        ),
    )

    def data_id_match(self) -> DataIdMatch | None:
        if not self.fail_condition:
            return None
        return DataIdMatch(self.fail_condition)


class MockPipelineTask(PipelineTask):
    """Implementation of PipelineTask used for running a mock pipeline.

    Notes
    -----
    This class overrides `runQuantum` to read all input datasetRefs and to
    store simple dictionary as output data. Output dictionary contains some
    provenance data about inputs, the task that produced it, and corresponding
    quantum. This class depends on `MockButlerQuantumContext` which knows how
    to store the output dictionary data with special dataset types.
    """

    ConfigClass = MockPipelineTaskConfig

    def __init__(self, *, config: MockPipelineTaskConfig | None = None, **kwargs: Any):
        super().__init__(config=config, **kwargs)
        self.fail_exception: type | None = None
        self.data_id_match: DataIdMatch | None = None
        if config is not None:
            self.data_id_match = config.data_id_match()
            if self.data_id_match:
                self.fail_exception = doImportType(config.fail_exception)

    def runQuantum(
        self,
        butlerQC: ButlerQuantumContext,
        inputRefs: InputQuantizedConnection,
        outputRefs: OutputQuantizedConnection,
    ) -> None:
        # docstring is inherited from the base class
        quantum = butlerQC.quantum

        _LOG.info("Mocking execution of task '%s' on quantum %s", self.getName(), quantum.dataId)

        assert quantum.dataId is not None, "Quantum DataId cannot be None"

        # Possibly raise an exception.
        if self.data_id_match is not None and self.data_id_match.match(quantum.dataId):
            _LOG.info("Simulating failure of task '%s' on quantum %s", self.getName(), quantum.dataId)
            message = f"Simulated failure: task={self.getName()} dataId={quantum.dataId}"
            assert self.fail_exception is not None, "Exception type must be defined"
            raise self.fail_exception(message)

        # read all inputs
        inputs = butlerQC.get(inputRefs)

        _LOG.info("Read input data for task '%s' on quantum %s", self.getName(), quantum.dataId)

        # To avoid very deep provenance we trim inputs to a single level
        for name, data in inputs.items():
            if isinstance(data, dict):
                data = [data]
            if isinstance(data, list):
                for item in data:
                    qdata = item.get("quantum", {})
                    qdata.pop("inputs", None)

        # store mock outputs
        for name, refs in outputRefs:
            if not isinstance(refs, list):
                refs = [refs]
            for ref in refs:
                data = {
                    "ref": {
                        "dataId": {key.name: ref.dataId[key] for key in ref.dataId.keys()},
                        "datasetType": ref.datasetType.name,
                    },
                    "quantum": {
                        "task": self.getName(),
                        "dataId": {key.name: quantum.dataId[key] for key in quantum.dataId.keys()},
                        "inputs": inputs,
                    },
                    "outputName": name,
                }
                butlerQC.put(data, ref)

        _LOG.info("Finished mocking task '%s' on quantum %s", self.getName(), quantum.dataId)
