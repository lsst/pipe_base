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

import concurrent.futures
import dataclasses
import itertools
import logging
import multiprocessing
import os
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import IO, ClassVar, Generic, TypedDict, TypeVar, cast

import tqdm
import zstandard

from lsst.daf.butler import (
    ButlerConfig,
    ButlerLogRecords,
    DataCoordinate,
    DatasetRef,
    DimensionDataAttacher,
    DimensionRecordSetDeserializer,
    DimensionUniverse,
    QuantumBackedButler,
    SerializableDimensionData,
)
from lsst.resources import ResourcePathExpression

from .. import automatic_connection_constants as acc
from .._status import QuantumSuccessCaveats
from .._task_metadata import TaskMetadata
from ..pipeline_graph import PipelineGraph
from ..pipeline_graph.io import SerializedPipelineGraph, TaskImportMode
from ..quantum_provenance_graph import ExceptionInfo, QuantumRunStatus
from .address import AddressReader, AddressWriter
from .models import (
    PredictedDatasetModel,
    PredictedFullQuantumModel,
    PredictedGraph,
    ProvenanceDatasetModel,
    ProvenanceQuantumModel,
    QuantumIndex,
)
from .walker import GraphWalker

_LOG = logging.getLogger(__name__)


_U = TypeVar("_U")


class Files(TypedDict, Generic[_U]):
    quanta: _U
    datasets: _U
    metadata: _U
    logs: _U


@dataclasses.dataclass
class Scanner:
    predicted: PredictedGraph
    worker: ScannerWorker
    files: Files[IO[bytes]]
    quantum_address_writer: AddressWriter
    dataset_address_writer: AddressWriter
    quantum_progress: tqdm.tqdm
    dataset_progress: tqdm.tqdm
    quanta: dict[uuid.UUID, ProvenanceQuantumModel] = dataclasses.field(default_factory=dict)
    datasets: dict[uuid.UUID, bool] = dataclasses.field(default_factory=dict)

    @classmethod
    def scan(
        cls,
        predicted_graph_uri: ResourcePathExpression,
        butler_uri: ResourcePathExpression,
        work_dir: str,
        n_processes: int = 1,
    ) -> None:
        predicted_graph = PredictedGraph.read_zip(
            predicted_graph_uri, read_thin_quanta=False, read_bipartite_edges=False
        )
        butler_config = ButlerConfig(butler_uri)
        with cls.from_predicted(predicted_graph, butler_config, work_dir) as scanner:
            scanner.read_progress()
            scanner.scan_init_outputs()
            executor: concurrent.futures.Executor
            if n_processes > 1:
                executor = concurrent.futures.ProcessPoolExecutor(
                    max_workers=n_processes - 1,
                    mp_context=multiprocessing.get_context("spawn"),
                    initializer=ScannerWorker.initialize,
                    initargs=(
                        butler_uri,
                        SerializedPipelineGraph.serialize(predicted_graph.pipeline_graph),
                        predicted_graph.dimension_data,
                    ),
                )
            else:
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                ScannerWorker.instance = scanner.worker
            scanner.scan_graph(executor)

    @property
    def int_size(self) -> int:
        return self.predicted.header.int_size

    @staticmethod
    def make_filenames(root: str) -> Files[str]:
        return cast(
            Files[str], {name: os.path.join(root, f"{name}.dat.zst") for name in Files.__required_keys__}
        )

    @staticmethod
    def files_exist(filenames: Files[str]) -> Files[bool]:
        return cast(Files[bool], {k: os.path.exists(cast(str, v)) for k, v in filenames.items()})

    @classmethod
    def open(cls, filenames: Files[str], exit_stack: ExitStack, *, mode: str) -> Files[IO[bytes]]:
        return cast(
            Files[IO[bytes]],
            {k: exit_stack.enter_context(open(cast(str, v), mode)) for k, v in filenames.items()},
        )

    @contextmanager
    @classmethod
    def from_predicted(
        cls, predicted: PredictedGraph, butler_config: ButlerConfig, work_dir: str
    ) -> Iterator[Scanner]:
        filenames = cls.make_filenames(work_dir)
        exists = cls.files_exist(filenames)
        universe = cast(DimensionUniverse, predicted.pipeline_graph.universe)
        dimension_data_attacher = DimensionDataAttacher(deserializers=predicted.deserialize_records())
        os.makedirs(work_dir, exist_ok=True)
        with ExitStack() as exit_stack:
            if all(exists.values()):
                _LOG.info("Restoring scanner state from file.")
                mode = "r+b"
            elif any(exists.values()):
                raise RuntimeError("Some scanner files are present, but not all.")
            else:
                mode = "w+b"
            files = cls.open(filenames, exit_stack, mode=mode)
            qbb = QuantumBackedButler.from_predicted(
                butler_config,
                predicted_inputs=[],
                predicted_outputs=[],
                dimensions=universe,
                # We don't need the datastore records because we're never going
                # to look for overall inputs.
                datastore_records={},
                dataset_types={
                    node.name: node.dataset_type for node in predicted.pipeline_graph.dataset_types.values()
                },
            )
            scanner = cls(
                predicted,
                worker=ScannerWorker(qbb, predicted.pipeline_graph, dimension_data_attacher),
                files=files,
                quantum_address_writer=AddressWriter(predicted.header.int_size, {}, [0, 0, 0]),
                dataset_address_writer=AddressWriter(predicted.header.int_size, {}, [0]),
                quantum_progress=tqdm.tqdm(desc="Quanta", total=len(predicted.quantum_indices)),
                dataset_progress=tqdm.tqdm(desc="Datasets", total=len(predicted.quantum_indices)),
            )
            for quantum_id in scanner.predicted.quantum_indices.keys():
                scanner.quantum_address_writer.add_empty(quantum_id)
            for dataset_id in scanner.predicted.dataset_indices.keys():
                scanner.dataset_address_writer.add_empty(dataset_id)
            yield scanner
        scanner.quantum_progress.close()
        scanner.dataset_progress.close()

    def read_progress(self) -> None:
        decompressor = zstandard.ZstdDecompressor()
        total_metadata_size: int = 0
        total_log_size: int = 0
        for quantum_offset, quantum_size, quantum_data in tqdm.tqdm(
            AddressReader.read_all_subfiles(self.files["quanta"], int_size=self.int_size),
            "Reading provenance quanta.",
            leave=False,
        ):
            quantum = ProvenanceQuantumModel.model_validate_json(decompressor.decompress(quantum_data))
            address = self.quantum_address_writer.addresses[quantum.quantum_id]
            address.offsets[0] = quantum_offset
            address.sizes[0] = quantum_size
            address.offsets[1] = quantum.metadata_offset
            address.sizes[1] = quantum.metadata_size
            total_metadata_size += quantum.metadata_size
            address.offsets[2] = quantum.log_offset
            address.sizes[2] = quantum.log_size
            total_log_size += quantum.log_size
            self.quanta[quantum.quantum_id] = quantum
            self.quantum_progress.update(1)
        for dataset_offset, dataset_size, dataset_data in tqdm.tqdm(
            AddressReader.read_all_subfiles(self.files["datasets"], int_size=self.int_size),
            "Reading provenance datasets.",
            leave=False,
        ):
            dataset = ProvenanceDatasetModel.model_validate_json(decompressor.decompress(dataset_data))
            address = self.dataset_address_writer.addresses[dataset.dataset_id]
            address.offsets[0] = dataset_offset
            address.sizes[0] = dataset_size
            self.datasets[dataset.dataset_id] = dataset.exists
            self.dataset_progress.update(1)
        self.files["metadata"].seek(0, os.SEEK_END)
        assert self.files["metadata"].tell() == total_metadata_size
        self.files["logs"].seek(0, os.SEEK_END)
        assert self.files["logs"].tell() == total_log_size
        _LOG.info("Restored progress from %s quanta and %s datasets.", len(self.quanta), len(self.datasets))

    def finalize_dataset(self, dataset_id: uuid.UUID, exists: bool, dataset_bytes: bytes) -> None:
        self.dataset_address_writer.write_subfile(self.files["datasets"], dataset_id, dataset_bytes)
        self.datasets[dataset_id] = exists
        self.dataset_progress.update(1)

    def process_input_dataset(self, predicted: PredictedDatasetModel) -> bool:
        if predicted.dataset_id in self.datasets:
            return True
        provenance = ProvenanceDatasetModel.from_predicted(predicted)
        dataset_bytes = self.worker.compressor.compress(predicted.model_dump_json().encode())
        self.finalize_dataset(provenance.dataset_id, provenance.exists, dataset_bytes)
        return False

    def finalize_quantum(self, quantum: ProvenanceQuantumModel) -> None:
        quantum_bytes = self.worker.compressor.compress(quantum.model_dump_json().encode())
        self.quantum_address_writer.write_subfile(self.files["quanta"], quantum.quantum_id, quantum_bytes)
        self.quanta[quantum.quantum_id] = quantum
        self.quantum_progress.update(1)

    def process_blocked_quantum(self, predicted: PredictedFullQuantumModel) -> bool:
        if predicted.quantum_id in self.quanta:
            return False
        provenance = ProvenanceQuantumModel.from_predicted(predicted)
        provenance.status = QuantumRunStatus.BLOCKED
        self.finalize_quantum(provenance)
        return True

    def process_init_quantum(self, predicted: PredictedFullQuantumModel) -> bool:
        if predicted.quantum_id in self.quanta:
            return False
        provenance = ProvenanceQuantumModel.from_predicted(predicted)
        for predicted_dataset in itertools.chain.from_iterable(predicted.outputs.values()):
            if not self.datasets[predicted_dataset.dataset_id]:
                break
        else:
            provenance.status = QuantumRunStatus.SUCCESSFUL
        self.finalize_quantum(provenance)
        return True

    def scan_init_outputs(self) -> None:
        for predicted_quantum in self.predicted.init_quanta.root:
            quantum_index = self.predicted.quantum_indices[predicted_quantum.quantum_id]
            if quantum_index in self.quanta:
                continue
            for predicted_dataset in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                self.process_input_dataset(predicted_dataset)
            for predicted_dataset in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                if predicted_dataset.dataset_id not in self.datasets:
                    self.worker.scan_dataset(predicted_dataset, predicted_quantum.quantum_id).finalize(self)
        for predicted_quantum in self.predicted.init_quanta.root:
            self.process_init_quantum(predicted_quantum)

    def scan_graph(self, executor: concurrent.futures.Executor) -> None:
        walker = GraphWalker[QuantumIndex](self.predicted.quantum_xgraph.copy())
        scanning: set[concurrent.futures.Future[QuantumScanResult | DatasetScanResult]] = set()
        for ready in walker:
            for quantum_index in ready:
                predicted_quantum = self.predicted.full_quanta[quantum_index]
                task_node = self.predicted.pipeline_graph.tasks[predicted_quantum.task_label]
                if (provenance_quantum := self.quanta.get(predicted_quantum.quantum_id)) is not None:
                    if provenance_quantum.status.blocks_downstream:
                        for blocked_quantum_index in walker.fail(quantum_index):
                            self.predicted.full_quanta[blocked_quantum_index]
                    else:
                        walker.finish(quantum_index)
                else:
                    scanning.add(executor.submit(ScanQuantumRunner(predicted_quantum)))
                for predicted_dataset in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                    self.process_input_dataset(predicted_dataset)
                for write_edge in task_node.outputs.values():
                    for predicted_dataset in predicted_quantum.outputs.get(write_edge.connection_name, []):
                        if predicted_dataset.dataset_id not in self.datasets:
                            scanning.add(
                                executor.submit(
                                    ScanDatasetRunner(predicted_dataset, predicted_quantum.quantum_id)
                                )
                            )
            futures_done, scanning = concurrent.futures.wait(scanning, return_when="FIRST_COMPLETED")
            for future in futures_done:
                future.result().finalize(self, walker)


@dataclasses.dataclass
class QuantumScanResult:
    quantum: ProvenanceQuantumModel
    metadata_bytes: bytes
    log_bytes: bytes

    def finalize(self, scanner: Scanner, walker: GraphWalker) -> None:
        if self.metadata_bytes:
            metadata_address = scanner.quantum_address_writer.write_subfile(
                scanner.files["metadata"],
                self.quantum.quantum_id,
                self.metadata_bytes,
                column=1,
            )
            self.quantum.metadata_offset = metadata_address.offsets[1]
            self.quantum.metadata_size = metadata_address.sizes[1]
        if self.log_bytes:
            log_address = scanner.quantum_address_writer.write_subfile(
                scanner.files["logs"],
                self.quantum.quantum_id,
                self.log_bytes,
                column=2,
            )
            self.quantum.log_offset = log_address.offsets[2]
            self.quantum.log_size = log_address.sizes[2]
        scanner.finalize_quantum(self.quantum)
        quantum_index = scanner.predicted.quantum_indices[self.quantum.quantum_id]
        if self.quantum.status.blocks_downstream:
            for blocked_quantum_index in walker.fail(quantum_index):
                scanner.process_blocked_quantum(scanner.predicted.full_quanta[blocked_quantum_index])
        else:
            walker.finish(quantum_index)


@dataclasses.dataclass
class DatasetScanResult:
    dataset_id: uuid.UUID
    exists: bool
    dataset_bytes: bytes

    def finalize(self, scanner: Scanner, walker: GraphWalker | None = None) -> None:
        scanner.finalize_dataset(self.dataset_id, self.exists, self.dataset_bytes)


@dataclasses.dataclass
class ScannerWorker:
    qbb: QuantumBackedButler
    pipeline_graph: PipelineGraph
    dimension_data_attacher: DimensionDataAttacher
    compressor: zstandard.ZstdCompressor = dataclasses.field(default_factory=zstandard.ZstdCompressor)

    def scan_dataset(self, predicted: PredictedDatasetModel, producer: uuid.UUID) -> DatasetScanResult:
        ref = self.make_ref(predicted)
        provenance_dataset = ProvenanceDatasetModel.from_predicted(predicted, producer)
        provenance_dataset.exists = self.qbb.stored(ref)
        return DatasetScanResult(
            provenance_dataset.dataset_id,
            provenance_dataset.exists,
            self.compressor.compress(provenance_dataset.model_dump_json().encode()),
        )

    def scan_quantum(self, predicted: PredictedFullQuantumModel) -> QuantumScanResult:
        provenance = ProvenanceQuantumModel.from_predicted(predicted)
        metadata_bytes = self.read_and_compress_metadata(predicted, provenance)
        log_bytes = self.read_and_compress_logs(predicted)
        if metadata_bytes:
            if log_bytes:
                provenance.status = QuantumRunStatus.SUCCESSFUL
            else:
                provenance.status = QuantumRunStatus.LOGS_MISSING
        else:
            if log_bytes:
                provenance.status = QuantumRunStatus.FAILED
            else:
                provenance.status = QuantumRunStatus.METADATA_MISSING
        return QuantumScanResult(provenance, metadata_bytes=metadata_bytes, log_bytes=log_bytes)

    def make_ref(self, predicted: PredictedDatasetModel) -> DatasetRef:
        dimensions = self.pipeline_graph.dataset_types[predicted.dataset_type_name].dimensions
        (data_id,) = self.dimension_data_attacher.attach(
            dimensions, [DataCoordinate.from_full_values(dimensions, tuple(predicted.data_id))]
        )
        return DatasetRef(
            self.pipeline_graph.dataset_types[predicted.dataset_type_name].dataset_type,
            data_id,
            run=predicted.run,
            id=predicted.dataset_id,
        )

    def read_and_compress_logs(self, predicted: PredictedFullQuantumModel) -> bytes:
        ref = self.make_ref(predicted.outputs[acc.LOG_OUTPUT_CONNECTION_NAME][0])
        try:
            logs: ButlerLogRecords = self.qbb.get(ref)
        except FileNotFoundError:
            return b""
        return self.compressor.compress(logs.model_dump_json().encode())

    def read_and_compress_metadata(
        self, predicted: PredictedFullQuantumModel, provenance: ProvenanceQuantumModel
    ) -> bytes:
        ref = self.make_ref(predicted.outputs[acc.METADATA_OUTPUT_CONNECTION_NAME][0])
        try:
            metadata: TaskMetadata = self.qbb.get(ref)
        except FileNotFoundError:
            return b""
        try:
            # Int conversion guards against spurious conversion to
            # float that can apparently sometimes happen in
            # TaskMetadata.
            provenance.caveats = QuantumSuccessCaveats(int(metadata["quantum"]["caveats"]))
        except LookupError:
            pass
        try:
            provenance.exception = ExceptionInfo._from_metadata(metadata[provenance.task_label]["failure"])
        except LookupError:
            pass
        # TODO: add resource usage information
        return self.compressor.compress(metadata.model_dump_json().encode())

    instance: ClassVar[ScannerWorker]

    @staticmethod
    def initialize(
        butler_uri: ResourcePathExpression,
        serialized_pipeline_graph: SerializedPipelineGraph,
        dimension_data: SerializableDimensionData,
    ) -> None:
        pipeline_graph = serialized_pipeline_graph.deserialize(TaskImportMode.DO_NOT_IMPORT)
        butler_config = ButlerConfig(butler_uri)
        qbb = QuantumBackedButler.from_predicted(
            butler_config,
            predicted_inputs=[],
            predicted_outputs=[],
            dimensions=cast(DimensionUniverse, pipeline_graph.universe),
            # We don't need the datastore records in the QG because we're
            # only going to read metadata and logs, and those are never
            # overall inputs.
            datastore_records={},
            dataset_types={node.name: node.dataset_type for node in pipeline_graph.dataset_types.values()},
        )
        universe = cast(DimensionUniverse, pipeline_graph.universe)
        deserializers = [
            DimensionRecordSetDeserializer.from_raw(universe[element_name], raw_records)
            for element_name, raw_records in dimension_data.root.items()
        ]
        ScannerWorker.instance = ScannerWorker(
            qbb, pipeline_graph, DimensionDataAttacher(deserializers=deserializers)
        )


@dataclasses.dataclass
class ScanDatasetRunner:
    predicted: PredictedDatasetModel
    producer: uuid.UUID

    def __call__(self) -> DatasetScanResult:
        return ScannerWorker.instance.scan_dataset(self.predicted, self.producer)


class ScanQuantumRunner:
    def __init__(self, predicted: PredictedFullQuantumModel) -> None:
        self.predicted_json = predicted.model_dump_json()

    def __call__(self) -> QuantumScanResult:
        predicted = PredictedFullQuantumModel.model_validate_json(self.predicted_json)
        return ScannerWorker.instance.scan_quantum(predicted)
