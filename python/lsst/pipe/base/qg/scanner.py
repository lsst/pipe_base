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
import logging
import os
import uuid
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from typing import IO, Generic, TypedDict, TypeVar, cast

import tqdm
import zstandard

from lsst.daf.butler import ButlerConfig, DataCoordinate, DatasetRef, QuantumBackedButler

from ..quantum_provenance_graph import QuantumRunStatus
from .address import AddressReader, AddressWriter
from .models import (
    PredictedFullQuantumModel,
    PredictedGraph,
    ProvenanceDatasetModel,
    ProvenanceQuantumModel,
)

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
    files: Files[IO[bytes]]
    qbb: QuantumBackedButler
    quantum_address_writer: AddressWriter
    dataset_address_writer: AddressWriter
    compressor: zstandard.ZstdCompressor = dataclasses.field(default_factory=zstandard.ZstdCompressor)
    quanta: dict[uuid.UUID, ProvenanceQuantumModel] = dataclasses.field(default_factory=dict)
    datasets: dict[uuid.UUID, ProvenanceDatasetModel] = dataclasses.field(default_factory=dict)

    @property
    def int_size(self) -> int:
        return self.quantum_address_writer.int_size

    @staticmethod
    def make_filenames(root: str) -> Files[str]:
        return {name: os.path.join(root, f"{name}.dat.zst") for name in Files.__required_keys__}

    @staticmethod
    def files_exist(filenames: Files[str]) -> Files[bool]:
        return {k: os.path.exists(v) for k, v in filenames.items()}

    @classmethod
    def open(
        cls, filenames: Files[str], exit_stack: ExitStack, *, int_size: int, mode: str
    ) -> Files[IO[bytes]]:
        return {k: exit_stack.enter_context(open(cast(str, v), "r+b")) for k, v in filenames.items()}

    @contextmanager
    @classmethod
    def from_predicted(
        cls, predicted: PredictedGraph, butler_config: ButlerConfig, workdir: str, int_size: int
    ) -> Iterator[Scanner]:
        filenames = cls.make_filenames(workdir)
        exists = cls.files_exist(filenames)
        with ExitStack() as exit_stack:
            if all(exists.values()):
                _LOG.info("Restoring scanner state from file.")
                mode = "r+b"
            elif any(exists.values()):
                raise RuntimeError("Some scanner files are present, but not all.")
            else:
                mode = "w+b"
            files = cls.open(filenames, exit_stack, int_size=int_size, mode=mode)
            qbb = QuantumBackedButler.from_predicted(
                butler_config,
                predicted_inputs=predicted.dataset_indices.keys(),
                predicted_outputs=[],
                dimensions=predicted.pipeline_graph.universe,
                # We don't need the datastore records because we're never going
                # to look for overall inputs.
                datastore_records={},
                dataset_types={
                    node.name: node.dataset_type for node in predicted.pipeline_graph.dataset_types.values()
                },
            )
            scanner = cls(
                predicted,
                files=files,
                qbb=qbb,
                quantum_address_writer=AddressWriter(int_size, {}, [0, 0, 0]),
                dataset_address_writer=AddressWriter(int_size, {}, [0]),
            )
            for quantum_id in scanner.predicted.quantum_indices.keys():
                scanner.quantum_address_writer.add_empty(quantum_id)
            for dataset_id in scanner.predicted.dataset_indices.keys():
                scanner.dataset_address_writer.add_empty(dataset_id)
            yield scanner

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
            self.quanta[address.index] = quantum
        for dataset_offset, dataset_size, dataset_data in tqdm.tqdm(
            AddressReader.read_all_subfiles(self.files["datasets"], int_size=self.int_size),
            "Reading provenance datasets.",
            leave=False,
        ):
            dataset = ProvenanceDatasetModel.model_validate_json(decompressor.decompress(dataset_data))
            address = self.dataset_address_writer.addresses[dataset.dataset_id]
            address.offsets[0] = dataset_offset
            address.sizes[0] = dataset_size
            self.datasets[address.index] = dataset
        self.files["metadata"].seek(0, os.SEEK_END)
        assert self.files["metadata"].tell() == total_metadata_size
        self.files["log"].seek(0, os.SEEK_END)
        assert self.files["log"].tell() == total_log_size

    def finalize_dataset(self, dataset: ProvenanceDatasetModel) -> None:
        self.dataset_address_writer.write_subfile(
            self.files["datasets"],
            dataset.dataset_id,
            self.compressor.compress(dataset.model_dump_json()),
        )
        self.datasets[dataset.dataset_id] = dataset

    def finalize_quantum(self, quantum: ProvenanceQuantumModel) -> None:
        self.quantum_address_writer.write_subfile(
            self.files["quanta"],
            quantum.quantum_id,
            self.compressor.compress(quantum.model_dump_json()),
        )
        self.quanta[quantum.quantum_id] = quantum

    def all_outputs_exist(self, quantum: PredictedFullQuantumModel) -> bool:
        for predicted_dataset in itertools.chain.from_iterable(quantum.outputs.values()):
            provenance_dataset = self.datasets[predicted_dataset.dataset_id]
            if not provenance_dataset.exists:
                return False
        return True

    def scan_datasets(self, datasets: dict[uuid.UUID, ProvenanceDatasetModel]) -> None:
        refs = []
        for dataset in datasets.values():
            dataset_type = self.predicted.pipeline_graph.dataset_types[dataset.dataset_type_name].dataset_type
            refs.append(
                # TODO: need to expand data IDs when this moves beyond inits.
                DatasetRef(
                    dataset_type,
                    DataCoordinate.from_full_values(dataset_type.dimensions, dataset.data_id),
                    run=dataset.run,
                    id=dataset.dataset_id,
                )
            )
        for ref, exists in self.qbb.stored_many(refs).items():
            dataset = datasets[ref.id]
            dataset.exists = exists
            self.finalize_dataset(dataset)

    def scan_init_outputs(self) -> bool:
        new_outputs: dict[uuid.UUID, ProvenanceDatasetModel] = {}
        for predicted_quantum in self.predicted.init_quanta.root:
            quantum_index = self.predicted.quantum_indices[predicted_quantum.quantum_id]
            if quantum_index in self.quanta:
                continue
            for predicted_dataset in itertools.chain.from_iterable(predicted_quantum.inputs.values()):
                if (
                    predicted_dataset.dataset_id not in self.datasets
                    and predicted_dataset.dataset_id not in new_outputs
                ):
                    # This is an overall input, or we'd have seen it as an
                    # output already.
                    self.finalize_dataset(ProvenanceDatasetModel.from_predicted(predicted_dataset))
            for predicted_dataset in itertools.chain.from_iterable(predicted_quantum.outputs.values()):
                if predicted_dataset.dataset_id not in self.datasets:
                    provenance_dataset = ProvenanceDatasetModel.from_predicted(
                        predicted_dataset, producer=predicted_quantum.quantum_id
                    )
                    new_outputs[provenance_dataset.dataset_id] = provenance_dataset
        self.scan_datasets(new_outputs)
        all_successful = True
        for predicted_quantum in self.predicted.init_quanta.root:
            if (provenance_quantum := self.quanta.get(predicted_quantum.quantum_id)) is None:
                provenance_quantum = ProvenanceQuantumModel.from_predicted(predicted_quantum)
                if self.all_outputs_exist(predicted_quantum):
                    provenance_quantum.status = QuantumRunStatus.SUCCESSFUL
                self.finalize_quantum(provenance_quantum)
            if provenance_quantum.status != QuantumRunStatus.SUCCESSFUL:
                all_successful = False
        return all_successful
