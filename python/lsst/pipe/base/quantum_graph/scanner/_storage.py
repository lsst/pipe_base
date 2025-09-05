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
import uuid
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, contextmanager

import sqlalchemy

from ._config import ScannerConfig
from ._results import QuantumScanResult, QuantumScanStatus


@dataclasses.dataclass
class Tables:
    schema: sqlalchemy.MetaData = dataclasses.field(default_factory=sqlalchemy.MetaData)
    dataset: sqlalchemy.Table = dataclasses.field(init=False)
    quantum: sqlalchemy.Table = dataclasses.field(init=False)
    to_ingest: sqlalchemy.Table = dataclasses.field(init=False)
    to_delete: sqlalchemy.Table = dataclasses.field(init=False)
    deleting_now: sqlalchemy.Table = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self.dataset = sqlalchemy.Table(
            "dataset",
            self.schema,
            sqlalchemy.Column("dataset_id", sqlalchemy.Uuid, primary_key=True),
            # TODO: we might not need "exists" and "producer".
            sqlalchemy.Column("exists", sqlalchemy.Boolean, nullable=False),
            sqlalchemy.Column("producer", sqlalchemy.Uuid, nullable=True),
            sqlalchemy.Column("provenance", sqlalchemy.LargeBinary, nullable=False),
        )
        self.quantum = sqlalchemy.Table(
            "quantum",
            self.schema,
            sqlalchemy.Column("quantum_id", sqlalchemy.Uuid, primary_key=True),
            sqlalchemy.Column("successful", sqlalchemy.Boolean, nullable=False),
            sqlalchemy.Column("provenance", sqlalchemy.LargeBinary, nullable=False),
            sqlalchemy.Column(
                "log_id", sqlalchemy.Uuid, sqlalchemy.ForeignKey("dataset.dataset_id"), nullable=True
            ),
            sqlalchemy.Column("log_content", sqlalchemy.LargeBinary, nullable=False),
            sqlalchemy.Column(
                "metadata_id", sqlalchemy.Uuid, sqlalchemy.ForeignKey("dataset.dataset_id"), nullable=True
            ),
            sqlalchemy.Column("metadata_content", sqlalchemy.LargeBinary, nullable=False),
        )
        self.to_ingest = sqlalchemy.Table(
            "to_ingest",
            self.schema,
            sqlalchemy.Column("ref_json", sqlalchemy.LargeBinary, nullable=False),
        )
        self.to_delete = sqlalchemy.Table(
            "to_delete",
            self.schema,
            sqlalchemy.Column(
                "dataset_id", sqlalchemy.Uuid, sqlalchemy.ForeignKey("dataset.dataset_id"), primary_key=True
            ),
            sqlalchemy.Column("ref_json", sqlalchemy.LargeBinary, nullable=False),
        )
        self.deleting_now = sqlalchemy.Table(
            "deleting_now",
            self.schema,
            sqlalchemy.Column(
                "dataset_id", sqlalchemy.Uuid, sqlalchemy.ForeignKey("to_delete.dataset_id"), primary_key=True
            ),
        )


class ScannerStorage(AbstractContextManager):
    def __init__(self, config: ScannerConfig):
        self._config = config
        is_new = False
        if self._config.checkpoint_path is not None and self._config.checkpoint_path.exists():
            self._config.db_path.transfer_from(self._config.checkpoint_path, "copy")
        elif not self._config.db_path.exists():
            is_new = True
        self._engine = sqlalchemy.create_engine(
            f"sqlite:///{self._config.db_path.ospath}",
            connect_args={"autocommit": False},
            poolclass=sqlalchemy.NullPool,
        )
        # We don't use the context manager interface for the connection because
        # we want to be able to close it, checkpoint by copying the SQLite
        # file, and then open them again.  But we do use __enter__ and __exit__
        # on the class to provide an external context manager interface.
        self._connection: sqlalchemy.Connection = self._connect()
        self.tables = Tables()
        if is_new:
            # In write-ahead log mode with synchronous=1 (which we have to set
            # on each connection), transaction overhead is lower and hard
            # failures (e.g. power loss) can lead to lost commits but not
            # corruption. That plays nicely with the copy-to-persistent-storage
            # logic we need to have anyway for environments with no persistent
            # POSIX storage for the DB; we can force a WAL checkpoint (even
            # when we do have persistent POSIX storage) and then run any
            # post-checkpoint operations (like deleting dataset artifacts) that
            # require durable incremental storage.
            self._connection.execute(sqlalchemy.text("PRAGMA journal_mode='WAL'"))
            self.tables.schema.create_all(self._connection)

    def __enter__(self) -> ScannerStorage:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def _connect(self) -> sqlalchemy.Connection:
        connection = self._engine.connect()
        connection.execute(sqlalchemy.text("PRAGMA synchronous=1"))
        return connection

    def close(self) -> None:
        if not self._connection.closed:
            self._connection.close()

    def checkpoint(self) -> None:
        self._connection.execute(sqlalchemy.text("PRAGMA wal_checkpoint(TRUNCATE)"))
        if self._config.checkpoint_path is not None:
            self._connection.close()
            self._config.checkpoint_path.transfer_from(self._config.db_path, "copy")
            self._connection = self._connect()

    @contextmanager
    def transaction(self) -> Iterator[sqlalchemy.Connection]:
        with self._connection.begin():
            yield self._connection

    def load_progress(self) -> dict[uuid.UUID, bool]:
        return {
            row.quantum_id: row.succeeded
            for row in self._connection.execute(
                sqlalchemy.select(
                    self.tables.quantum.c.quantum_id,
                    self.tables.quantum.c.successful,
                )
            )
        }

    @contextmanager
    def flush_ingest_queue(self) -> Iterator[list[bytes]]:
        with self.transaction() as connection:
            yield list(connection.execute(self.tables.to_ingest.select()).scalars())
            connection.execute(self.tables.to_ingest.delete())

    def start_deletions(self, reset: bool = False) -> list[bytes]:
        with self.transaction() as connection:
            if reset:
                connection.execute(self.tables.deleting_now.delete())
            new_deletions = {
                row.dataset_id: row.json_ref
                for row in connection.execute(
                    self.tables.to_delete.select().where(
                        self.tables.to_delete.c.dataset_id.not_in(self.tables.deleting_now.select())
                    )
                )
            }
            if new_deletions:
                connection.execute(
                    self.tables.deleting_now.insert().values(
                        [{"dataset_id": dataset_id} for dataset_id in new_deletions.keys()]
                    )
                )
            return list(new_deletions.values())

    def finish_deletions(self, dataset_ids: Iterable[uuid.UUID]) -> None:
        with self.transaction() as connection:
            connection.execute(
                self.tables.to_delete.delete().where(
                    self.tables.to_delete.c.dataset_id.in_(list(dataset_ids))
                )
            )
            connection.execute(
                self.tables.to_delete.delete().where(
                    self.tables.deleting_now.c.dataset_id.in_(list(dataset_ids))
                )
            )

    def save_quantum(
        self,
        quantum: QuantumScanResult,
        json_refs_to_ingest: Iterable[bytes],
        json_refs_to_delete: Iterable[tuple[uuid.UUID, bytes]],
    ) -> None:
        dataset_rows = [
            {
                "dataset_id": d.dataset_id,
                "exists": d.exists,
                "producer": d.producer,
                "provenance": d.provenance,
            }
            for d in quantum.iter_all_outputs()
        ]
        to_ingest_rows = [{"json_ref": json_ref} for json_ref in json_refs_to_ingest]
        to_delete_rows = [
            {"dataset_id": dataset_id, "json_ref": json_ref} for dataset_id, json_ref in json_refs_to_delete
        ]
        if quantum.status is QuantumScanStatus.SUCCESSFUL:
            successful = True
        else:
            assert quantum.status is QuantumScanStatus.FAILED, (
                f"Cannot save incomplete scan for {quantum.quantum_id} with status {quantum.status}."
            )
            successful = False
        quantum_row = {
            "quantum_id": quantum.quantum_id,
            "successful": successful,
            "provenance": quantum.provenance,
            "log_id": quantum.log.dataset_id,
            "log_content": quantum.log.content,
            "metadata_id": quantum.metadata.dataset_id,
            "metadata_content": quantum.metadata.content,
        }
        with self.transaction() as connection:
            connection.execute(self.tables.dataset.insert().values(dataset_rows))
            connection.execute(self.tables.to_ingest.insert().values(to_ingest_rows))
            connection.execute(self.tables.to_delete.insert().values(to_delete_rows))
            connection.execute(self.tables.quantum.insert().values(quantum_row))
