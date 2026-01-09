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

__all__ = ("Writer",)

import dataclasses

import zstandard

from ...log_on_close import LogOnClose
from ...pipeline_graph import TaskImportMode
from .._predicted import PredictedQuantumGraphComponents, PredictedQuantumGraphReader
from .._provenance import ProvenanceQuantumGraphWriter, ProvenanceQuantumScanData
from ._communicators import WriterCommunicator


@dataclasses.dataclass
class Writer:
    """A helper class for the provenance aggregator actually writes the
    provenance quantum graph file.
    """

    predicted_path: str
    """Path to the predicted quantum graph."""

    comms: WriterCommunicator
    """Communicator object for this worker."""

    predicted: PredictedQuantumGraphComponents = dataclasses.field(init=False)
    """Components of the predicted quantum graph."""

    pending_compression_training: list[ProvenanceQuantumScanData] = dataclasses.field(default_factory=list)
    """Unprocessed quantum scans that are being accumulated in order to
    build a compression dictionary.
    """

    def __post_init__(self) -> None:
        assert self.comms.config.output_path is not None, "Writer should not be used if writing is disabled."
        self.comms.log.info("Reading predicted quantum graph.")
        with PredictedQuantumGraphReader.open(
            self.predicted_path, import_mode=TaskImportMode.DO_NOT_IMPORT
        ) as reader:
            self.comms.check_for_cancel()
            reader.read_init_quanta()
            self.comms.check_for_cancel()
            reader.read_quantum_datasets()
            self.predicted = reader.components

    @staticmethod
    def run(predicted_path: str, comms: WriterCommunicator) -> None:
        """Run the writer.

        Parameters
        ----------
        predicted_path : `str`
            Path to the predicted quantum graph.
        comms : `WriterCommunicator`
            Communicator for the writer.

        Notes
        -----
        This method is designed to run as the ``target`` in
        `WorkerContext.make_worker`.
        """
        with comms:
            writer = Writer(predicted_path, comms)
            writer.loop()

    def loop(self) -> None:
        """Run the main loop for the writer."""
        qg_writer: ProvenanceQuantumGraphWriter | None = None
        if not self.comms.config.zstd_dict_size:
            qg_writer = self.make_qg_writer()
        self.comms.log.info("Polling for write requests from scanners.")
        for request in self.comms.poll():
            if qg_writer is None:
                self.pending_compression_training.append(request)
                if len(self.pending_compression_training) >= self.comms.config.zstd_dict_n_inputs:
                    qg_writer = self.make_qg_writer()
            else:
                qg_writer.write_scan_data(request)
                self.comms.report_write()
        if qg_writer is None:
            qg_writer = self.make_qg_writer()
        self.comms.log.info("Writing init outputs.")
        qg_writer.write_init_outputs(assume_existence=False)

    def make_qg_writer(self) -> ProvenanceQuantumGraphWriter:
        """Make a compression dictionary, open the low-level writers, and
        write any accumulated scans that were needed to make the compression
        dictionary.

        Returns
        -------
        qg_writer : `ProvenanceQuantumGraphWriter`
            Low-level writers struct.
        """
        cdict = self.make_compression_dictionary()
        self.comms.send_compression_dict(cdict.as_bytes())
        assert self.comms.config.output_path is not None
        self.comms.log.info("Opening output files and processing predicted graph.")
        qg_writer = ProvenanceQuantumGraphWriter(
            self.comms.config.output_path,
            exit_stack=self.comms.exit_stack,
            log_on_close=LogOnClose(self.comms.log_progress),
            predicted=self.predicted,
            zstd_level=self.comms.config.zstd_level,
            cdict_data=cdict.as_bytes(),
            loop_wrapper=self.comms.periodically_check_for_cancel,
            log=self.comms.log,
        )
        self.comms.check_for_cancel()
        self.comms.log.info("Compressing and writing queued scan requests.")
        for request in self.pending_compression_training:
            qg_writer.write_scan_data(request)
            self.comms.report_write()
        del self.pending_compression_training
        self.comms.check_for_cancel()
        self.comms.log.info("Writing overall inputs.")
        qg_writer.write_overall_inputs(self.comms.periodically_check_for_cancel)
        qg_writer.write_packages()
        self.comms.log.info("Returning to write request loop.")
        return qg_writer

    def make_compression_dictionary(self) -> zstandard.ZstdCompressionDict:
        """Make the compression dictionary.

        Returns
        -------
        cdict : `zstandard.ZstdCompressionDict`
            The compression dictionary.
        """
        if (
            not self.comms.config.zstd_dict_size
            or len(self.pending_compression_training) < self.comms.config.zstd_dict_n_inputs
        ):
            self.comms.log.info("Making compressor with no dictionary.")
            return zstandard.ZstdCompressionDict(b"")
        self.comms.log.info("Training compression dictionary.")
        training_inputs: list[bytes] = []
        # We start the dictionary training with *predicted* quantum dataset
        # models, since those have almost all of the same attributes as the
        # provenance quantum and dataset models, and we can get a nice random
        # sample from just the first N, since they're ordered by UUID.  We
        # chop out the datastore records since those don't appear in the
        # provenance graph.
        for predicted_quantum in self.predicted.quantum_datasets.values():
            if len(training_inputs) == self.comms.config.zstd_dict_n_inputs:
                break
            predicted_quantum.datastore_records.clear()
            training_inputs.append(predicted_quantum.model_dump_json().encode())
        # Add the provenance quanta, metadata, and logs we've accumulated.
        for write_request in self.pending_compression_training:
            assert not write_request.is_compressed, "We can't compress without the compression dictionary."
            training_inputs.append(write_request.quantum)
            training_inputs.append(write_request.metadata)
            training_inputs.append(write_request.logs)
        return zstandard.train_dictionary(self.comms.config.zstd_dict_size, training_inputs)
