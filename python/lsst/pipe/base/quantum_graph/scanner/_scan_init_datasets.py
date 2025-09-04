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

import itertools
import uuid

from lsst.daf.butler import Butler

from . import db
from ._config import ScannerConfig
from ._worker import ScannerWorker
from ._writer import ScannerWriter


def scan_init_datasets(config: ScannerConfig) -> None:
    with ScannerWorker.open(config) as worker:
        if config.db_path.exists():
            raise FileExistsError(f"Scanner database {config.db_path} already exists.")
        worker.reader.read_init_quanta()
        writer = ScannerWriter()
        for predicted_quantum in worker.reader.components.init_quanta.root:
            writer.add_dataset_scans(
                predicted_quantum,
                [
                    worker.scan_dataset(predicted_dataset, producer=predicted_quantum.quantum_id)
                    for predicted_dataset in itertools.chain.from_iterable(predicted_quantum.outputs.values())
                ],
                worker,
            )
            writer.quanta.append(
                db.InitQuantum(quantum_id=uuid.UUID, task_label=predicted_quantum.task_label)
            )
        butler = Butler.from_config(config.butler_path, writeable=True)
        engine = config.create_db_engine()
        db.DatabaseModel.metadata.create_all(engine)
        writer.write_if_ready(config, engine, butler, worker, force_all=True)
