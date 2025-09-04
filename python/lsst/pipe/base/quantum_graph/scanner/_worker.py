# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ()

import dataclasses
import itertools
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import ClassVar

from lsst.daf.butler import (
    ButlerConfig,
    ButlerLogRecords,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    QuantumBackedButler,
)

from ... import automatic_connection_constants as acc
from ..._status import QuantumSuccessCaveats
from ..._task_metadata import TaskMetadata
from ...pipeline_graph import PipelineGraph, TaskImportMode
from ...quantum_provenance_graph import ExceptionInfo
from .._multiblock import Compressor
from .._predicted import (
    PredictedDatasetModel,
    PredictedQuantumGraph,
    PredictedQuantumGraphReader,
)
from .._provenance import ProvenanceDatasetModel
from ._config import ScannerConfig
from ._results import DatasetScanResult, QuantumScanResult, QuantumScanStatus


@dataclasses.dataclass
class ScannerWorker:
    reader: PredictedQuantumGraphReader
    qbb: QuantumBackedButler
    compressor: Compressor
    config: ScannerConfig

    @classmethod
    @contextmanager
    def open(cls, config: ScannerConfig) -> Iterator[ScannerWorker]:
        with PredictedQuantumGraph.open(
            config.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            reader.address_reader.read_all()
            reader.read_dimension_data()
            butler_config = ButlerConfig(config.butler_path)
            qbb = QuantumBackedButler.from_predicted(
                butler_config,
                predicted_inputs=[],
                predicted_outputs=[],
                dimensions=reader.components.pipeline_graph.universe,
                # We don't need the datastore records in the QG because we're
                # only going to read metadata and logs, and those are never
                # overall inputs.
                datastore_records={},
                dataset_types={
                    node.name: node.dataset_type
                    for node in reader.components.pipeline_graph.dataset_types.values()
                },
            )
            compressor, _ = reader.make_compressor(config.zstd_level)
            yield cls(reader=reader, qbb=qbb, compressor=compressor, config=config)

    @property
    def pipeline_graph(self) -> PipelineGraph:
        return self.reader.components.pipeline_graph

    def scan_dataset(
        self, predicted: PredictedDatasetModel, *, producer: uuid.UUID, exists: bool | None = None
    ) -> DatasetScanResult:
        provenance_dataset = ProvenanceDatasetModel.from_predicted(predicted, producer)
        if exists is None:
            ref = self.make_ref(predicted)
            exists = self.qbb.stored(ref)
        provenance_dataset.exists = exists
        return DatasetScanResult(
            provenance_dataset.dataset_id,
            producer,
            exists,
            self.compressor.compress(provenance_dataset.model_dump_json().encode()),
        )

    def scan_quantum(
        self, quantum_id: uuid.UUID, result: QuantumScanResult | None = None
    ) -> QuantumScanResult:
        if result is None:
            self.reader.read_quantum_datasets([quantum_id])
            result = QuantumScanResult.from_predicted(self.reader.components.quantum_datasets[quantum_id])
        times_for_task = self.config.get_times_for_task(result.predicted.task_label)
        result.update_wait_interval(times_for_task)
        self.read_and_compress_log(result)
        if not result.log and not self.config.assume_complete:
            return result
        output_dataset_ids: set[uuid.UUID] | None = None
        self.read_and_compress_metadata(result)
        if result.metadata:
            result.status = QuantumScanStatus.SUCCESSFUL
            output_dataset_ids = result.metadata.ids_put
        else:
            # We found the log dataset, but no metadata; this means the quantum
            # failed, but a retry might still happen that could turn it into a
            # success if we can't yet assume the run is complete.
            if not self.config.assume_complete:
                if result.first_failure_time is None:
                    result.first_failure_time = time.time()
                else:
                    if time.time() - result.first_failure_time > times_for_task["retry_timeout"]:
                        # Give up on scanning this quantum and all that follow
                        # it. A later invocation of the scanner with
                        # assume_complete=True will recover it in the unlikely
                        # event it's still retrying, but (according to timeout
                        # configuration) that should have already happened if
                        # it was going to.
                        result.status = QuantumScanStatus.ABANDONED
                return result
            result.status = QuantumScanStatus.FAILED
        result.outputs = [
            self.scan_dataset(
                predicted_output,
                producer=result.quantum_id,
                exists=(
                    (predicted_output.dataset_id in output_dataset_ids)
                    if output_dataset_ids is not None
                    else None
                ),
            )
            for predicted_output in itertools.chain.from_iterable(result.predicted.outputs.values())
        ]
        result.set_provenance(self.compressor)
        return result

    def process_blocked_quantum(self, quantum_id: uuid.UUID) -> QuantumScanResult:
        self.reader.read_quantum_datasets([quantum_id])
        result = QuantumScanResult.from_predicted(self.reader.components.quantum_datasets[quantum_id])
        result.status = QuantumScanStatus.BLOCKED
        result.outputs = [
            self.scan_dataset(predicted_output, producer=result.quantum_id, exists=False)
            for predicted_output in itertools.chain.from_iterable(result.predicted.outputs.values())
        ]
        result.set_provenance(self.compressor, blocked=True)
        return result

    def delete_dataset(self, ref_json: bytes) -> uuid.UUID:
        ref: DatasetRef = DatasetRef.from_json(ref_json)
        self.qbb.pruneDatasets([ref], purge=True, unstore=True)
        return ref.id

    @staticmethod
    def scan_quantum_in_pool(quantum_id: uuid.UUID, result: QuantumScanResult | None) -> QuantumScanResult:
        return ScannerWorker.instance.scan_quantum(quantum_id, result)

    @staticmethod
    def process_blocked_quantum_in_pool(quantum_id: uuid.UUID) -> QuantumScanResult:
        return ScannerWorker.instance.process_blocked_quantum(quantum_id)

    @staticmethod
    def delete_dataset_in_pool(ref_json: bytes) -> uuid.UUID:
        return ScannerWorker.instance.delete_dataset(ref_json)

    def make_ref(self, predicted: PredictedDatasetModel) -> DatasetRef:
        try:
            dataset_type = self.pipeline_graph.dataset_types[predicted.dataset_type_name].dataset_type
        except KeyError:
            if predicted.dataset_type_name == acc.PACKAGES_INIT_OUTPUT_NAME:
                dataset_type = DatasetType(
                    acc.PACKAGES_INIT_OUTPUT_NAME,
                    self.pipeline_graph.universe.empty,
                    storageClass=acc.PACKAGES_INIT_OUTPUT_STORAGE_CLASS,
                )
            else:
                raise
        if self.reader.components.dimension_data is None:
            self.reader.read_dimension_data()
            assert self.reader.components.dimension_data is not None
        (data_id,) = self.reader.components.dimension_data.attach(
            dataset_type.dimensions,
            [DataCoordinate.from_full_values(dataset_type.dimensions, tuple(predicted.data_coordinate))],
        )
        return DatasetRef(
            dataset_type,
            data_id,
            run=predicted.run,
            id=predicted.dataset_id,
        )

    def read_and_compress_metadata(self, result: QuantumScanResult) -> None:
        if result.metadata:
            return
        (predicted,) = result.predicted.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME]
        ref = self.make_ref(predicted)
        try:
            # TODO: check whether QBB metadata writes are atomic; if not, we
            # need to look our for races here.
            content: TaskMetadata = self.qbb.get(ref, storageClass="TaskMetadata")
        except FileNotFoundError:
            if not self.config.assume_complete:
                return
        else:
            try:
                # Int conversion guards against spurious conversion to
                # float that can apparently sometimes happen in
                # TaskMetadata.
                result.metadata.caveats = QuantumSuccessCaveats(int(content["quantum"]["caveats"]))
            except LookupError:
                pass
            try:
                result.metadata.exception = ExceptionInfo._from_metadata(
                    content[result.predicted.task_label]["failure"]
                )
            except LookupError:
                pass
            try:
                result.metadata.ids_put = {uuid.UUID(id_str) for id_str in content["quantum"]["outputs"]}
            except LookupError:
                pass
            result.metadata.exists = True
            result.metadata.content = self.compressor.compress(content.model_dump_json().encode())
        return

    def read_and_compress_log(self, result: QuantumScanResult) -> None:
        if result.log:
            return
        (predicted,) = result.predicted.outputs[acc.LOG_OUTPUT_CONNECTION_NAME]
        ref = self.make_ref(predicted)
        try:
            # TODO: check whether QBB metadata writes are atomic; if not, we
            # need to look our for races here.
            content: ButlerLogRecords = self.qbb.get(ref)
        except FileNotFoundError:
            if not self.config.assume_complete:
                return
        else:
            result.log.exists = True
            result.log.content = self.compressor.compress(content.model_dump_json().encode())
        return

    instance: ClassVar[ScannerWorker]

    @staticmethod
    def _initialize_for_pool(config: ScannerConfig) -> None:
        worker_context = ScannerWorker.open(config)
        # As of Python 3.12, we don't have a way to invoke __exit__, which we'd
        # want to happen when the worker process shuts down (*not* at the end
        # of any particular task performed by that process; the whole point is
        # to reuse a ScannerWorker instance for efficiency).  Apparently that
        # could be done with `sys.atexit` in Python 3.13 (see
        # https://github.com/python/cpython/pull/114279).  In the meantime, if
        # we poke through the encapsulation a bit, all the __exit__ is going to
        # do here is close the file descriptors used by the ZipFile instance,
        # and the process closing should do that anyway.
        ScannerWorker.instance = worker_context.__enter__()
