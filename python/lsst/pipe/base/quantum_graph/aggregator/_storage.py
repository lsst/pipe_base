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

__all__ = ("Storage", "Tables")

import dataclasses
import itertools
import os
import sqlite3
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator
from contextlib import AbstractContextManager, closing, contextmanager
from typing import ClassVar, Literal

import sqlalchemy
import sqlalchemy.event

from lsst.resources import ResourcePath

from ..._status import QuantumSuccessCaveats
from ...quantum_provenance_graph import ExceptionInfo
from ...resource_usage import QuantumResourceUsage
from ._config import AggregatorConfig
from ._structs import IngestRequest, ScanReport, ScanResult, ScanStatus


@dataclasses.dataclass
class Tables:
    """A struct containing the table definitions for the scanner dataabase."""

    schema: sqlalchemy.MetaData = dataclasses.field(default_factory=sqlalchemy.MetaData)
    """SQLAlchemy metadata object that represents the full schema."""

    dataset: sqlalchemy.Table = dataclasses.field(init=False)
    """Table holding compressed JSON dataset provenance models."""

    quantum: sqlalchemy.Table = dataclasses.field(init=False)
    """Table holding compressed JSON quantum provenance models, as well as
    compressed log and metadata content.
    """

    to_ingest: sqlalchemy.Table = dataclasses.field(init=False)
    """Table that acts a queue of datasets to ingest into the central butler
    repository.
    """

    def __post_init__(self) -> None:
        self.dataset = sqlalchemy.Table(
            "dataset",
            self.schema,
            sqlalchemy.Column("dataset_id", sqlalchemy.Uuid, primary_key=True),
            sqlalchemy.Column("producer_id", sqlalchemy.Uuid, nullable=False),
        )
        self.quantum = sqlalchemy.Table(
            "quantum",
            self.schema,
            sqlalchemy.Column("quantum_id", sqlalchemy.Uuid, primary_key=True),
            sqlalchemy.Column("status", sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column("caveats", sqlalchemy.Integer, nullable=True),
            sqlalchemy.Column("exception", sqlalchemy.LargeBinary, nullable=True),
            sqlalchemy.Column("resource_usage", sqlalchemy.LargeBinary, nullable=True),
            sqlalchemy.Column("log", sqlalchemy.LargeBinary, nullable=False),
            sqlalchemy.Column("metadata", sqlalchemy.LargeBinary, nullable=False),
            sqlalchemy.Column("is_compressed", sqlalchemy.Boolean, nullable=False),
        )
        self.to_ingest = sqlalchemy.Table(
            "to_ingest",
            self.schema,
            sqlalchemy.Column("producer_id", sqlalchemy.Uuid, primary_key=True),
            sqlalchemy.Column("data", sqlalchemy.LargeBinary, nullable=False),
        )


class Storage(AbstractContextManager):
    """A helper class for a provenance scanner that manages access to its
    internal database.

    Parameters
    ----------
    config: `ScannerConfig`
        Configuration for the scanner
    filename : `str`
        Name of the database file under the configured paths.
    progress : `BaseProgress`
        Progress-reporting helper.
    trust_local : `bool`
        If `True`, trust that a local DB file is up to date with its
        checkpoint.

    Notes
    -----
    This object is a context manager that manages the lifetime of SQLite
    database connections.
    """

    def __init__(
        self,
        config: AggregatorConfig,
        worker_id: int,
        trust_local: bool,
    ):
        self._config = config
        assert self._config.db_dir, "Storage should not be instantiated."
        os.makedirs(self._config.db_dir, exist_ok=True)
        filename = f"scanner-{worker_id:03d}"
        self._db_path = ResourcePath(self._config.db_dir, forceDirectory=True).join(filename)
        self._checkpoint_path = (
            ResourcePath(self._config.checkpoint_dir, forceDirectory=True).join(filename)
            if self._config.checkpoint_dir is not None
            else None
        )
        self._needs_checkpoint = False
        self._last_checkpoint = time.time()
        is_new = False
        if (
            self._checkpoint_path is not None
            and not (trust_local and self._db_path.exists())
            and self._checkpoint_path.exists()
        ):
            self._db_path.transfer_from(self._checkpoint_path, "copy")
        elif not self._db_path.exists():
            is_new = True
        self._engine = sqlalchemy.create_engine(
            f"sqlite:///{self._db_path.ospath}",
            # With autocommit=False, the Python sqlite3 module emits a BEGIN in
            # execute whenever a transaction is not already open, which is not
            # legal for most PRAGMAs.  It's not clear if there's a way to emit
            # PRAGMAs without using legacy transaction control.
            connect_args={"autocommit": sqlite3.LEGACY_TRANSACTION_CONTROL, "isolation_level": None},
            poolclass=sqlalchemy.NullPool,
        )
        # With isolation_level=None, we have to make SQLite start transactions
        # a bit more manually.
        sqlalchemy.event.listen(self._engine, "begin", self._on_begin)
        # We don't use the context manager interface for the connection because
        # we want to be able to close it, checkpoint by copying the SQLite
        # file, and then open them again.  But we do use __enter__ and __exit__
        # on the class to provide an external context manager interface.
        self._connect()
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
            self._execute_raw("PRAGMA journal_mode='WAL'")
            with self.transaction() as connection:
                self.tables.schema.create_all(connection)

    tables: ClassVar[Tables] = Tables()

    def __enter__(self) -> Storage:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> Literal[False]:
        self.close()
        return False

    def _connect(self) -> None:
        self._connection = self._engine.connect()
        self._execute_raw("PRAGMA synchronous=1")

    def _execute_raw(self, sql: str) -> None:
        with closing(self._connection.connection.cursor()) as cursor:
            cursor.execute(sql)

    def close(self) -> None:
        """Close the database connection."""
        if not self._connection.closed:
            self._connection.close()

    @property
    def needs_checkpoint(self) -> bool:
        """Whether there have been any writes to the scanner database since
        the last checkpoint.
        """
        return self._needs_checkpoint

    @property
    def should_checkpoint_now(self) -> bool:
        """Whether the database should be checkpointed now."""
        return (
            self._needs_checkpoint and time.time() - self._last_checkpoint > self._config.checkpoint_interval
        )

    def checkpoint(self) -> None:
        """Checkpoint the database, ensuring all transactions so far are
        durable.
        """
        if self._config.vacuum_on_checkpoint:
            self._execute_raw("VACUUM")
        self._execute_raw("PRAGMA wal_checkpoint(TRUNCATE)")
        if self._checkpoint_path is not None:
            self._connection.close()
            self._checkpoint_path.transfer_from(self._db_path, "copy")
            self._connect()
        self._needs_checkpoint = False
        self._last_checkpoint = time.time()

    @contextmanager
    def transaction(self) -> Iterator[sqlalchemy.Connection]:
        """Open a context manager for a transaction."""
        with self._connection.begin():
            yield self._connection

    def fetch_ingests(self) -> Iterator[IngestRequest]:
        """Fetch all pending ingests from the database.

        Yields
        ------
        ingest_request : `IngestRequest`
            A request to send to the ingester.
        """
        with self.transaction() as connection:
            for row in connection.execute(self.tables.to_ingest.select()):
                yield IngestRequest(row.producer_id, row.data)

    def save_quantum(self, scan_result: ScanResult, to_ingest: IngestRequest) -> None:
        """Save information from a quantum scan in the database.

        Parameters
        ----------
        scan_result : `ScanResult`
            Information about the scanned quantum.
        to_ingest : `IngestRequest`
            Information about the datasets to be ingested into the butler.
        """
        quantum_row = {
            "quantum_id": scan_result.quantum_id,
            "status": scan_result.status.value,
            "caveats": scan_result.caveats.value if scan_result.caveats is not None else None,
            "exception": (
                scan_result.exception.model_dump_json().encode() if scan_result.exception else None
            ),
            "resource_usage": (
                scan_result.resource_usage.model_dump_json().encode() if scan_result.resource_usage else None
            ),
            "log": scan_result.log,
            "metadata": scan_result.metadata,
            "is_compressed": scan_result.is_compressed,
        }
        dataset_rows = [
            {"dataset_id": dataset_id, "producer_id": scan_result.quantum_id}
            for dataset_id in scan_result.existing_outputs
        ]
        with self.transaction() as connection:
            if dataset_rows:
                for dataset_batch in itertools.batched(dataset_rows, self._config.query_batch_size):
                    connection.execute(self.tables.dataset.insert().values(dataset_batch))
            if to_ingest:
                connection.execute(
                    self.tables.to_ingest.insert().values(
                        {"producer_id": scan_result.quantum_id, "data": to_ingest.data}
                    )
                )
            connection.execute(self.tables.quantum.insert().values(quantum_row))
        self._needs_checkpoint = True

    def resume(self) -> Iterator[tuple[ScanReport, ScanResult | None]]:
        """Load saved scans to resume after an interruption.

        Yields
        ------
        scan_report : `ScanReport`
            Minimal information about the scan, sufficient for updating
            progress reports.
        scan_result : `ScanResult` or `None`
            Full information about the scan, necessary for writing provenance.
            `None` if writing provenance is disabled.
        """
        with self.transaction() as connection:
            datasets_by_producer: defaultdict[uuid.UUID, set[uuid.UUID]] = defaultdict(set)
            if self._config.output_path is None:
                sql = sqlalchemy.select(self.tables.quantum.c.quantum_id, self.tables.quantum.c.status)
            else:
                for row in connection.execute(self.tables.dataset.select()):
                    datasets_by_producer[row.producer_id].add(row.dataset_id)
                sql = self.tables.quantum.select()
            for row in connection.execute(sql):
                status = ScanStatus(row.status)
                scan_report = ScanReport(row.quantum_id, status)
                scan_result: ScanResult | None = None
                if self._config.output_path is not None:
                    scan_result = ScanResult(
                        row.quantum_id,
                        status=ScanStatus(row.status),
                        caveats=QuantumSuccessCaveats(row.caveats) if row.caveats is not None else None,
                        exception=(
                            ExceptionInfo.model_validate_json(row.exception)
                            if row.exception is not None
                            else None
                        ),
                        resource_usage=(
                            QuantumResourceUsage.model_validate_json(row.resource_usage)
                            if row.resource_usage is not None
                            else None
                        ),
                        existing_outputs=datasets_by_producer[row.quantum_id],
                        log=row.log,
                        metadata=row.metadata,
                        is_compressed=row.is_compressed,
                    )
                yield scan_report, scan_result

    @staticmethod
    def _on_begin(connection: sqlalchemy.engine.Connection) -> sqlalchemy.engine.Connection:
        assert connection.dialect.name == "sqlite"
        connection.execute(sqlalchemy.text("BEGIN"))
        return connection
