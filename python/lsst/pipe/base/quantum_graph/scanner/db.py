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

import uuid

import networkx
import sqlalchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from .._common import QuantumIndex


class DatabaseModel(DeclarativeBase):
    pass


class Dataset(DatabaseModel):
    __tablename__ = "dataset"
    dataset_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    exists: Mapped[bool] = mapped_column()
    producer: Mapped[QuantumIndex | None] = mapped_column()
    provenance: Mapped[bytes] = mapped_column()


class Quantum(DatabaseModel):
    __tablename__ = "quantum"
    quantum_index: Mapped[QuantumIndex] = mapped_column(primary_key=True)
    succeeded: Mapped[bool] = mapped_column()
    provenance: Mapped[bytes] = mapped_column()
    log_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"))
    log_data: Mapped[bytes] = mapped_column()
    metadata_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"))
    metadata_data: Mapped[bytes] = mapped_column()


class InitReadEdge(DatabaseModel):
    __tablename__ = "init_read_edge"
    dataset_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"), primary_key=True)
    quantum_index: Mapped[QuantumIndex] = mapped_column(
        sqlalchemy.ForeignKey("quantum.index"), primary_key=True
    )
    connection_name: Mapped[str] = mapped_column()


class ReadEdge(DatabaseModel):
    __tablename__ = "read_edge"
    dataset_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"), primary_key=True)
    quantum_index: Mapped[QuantumIndex] = mapped_column(
        sqlalchemy.ForeignKey("quantum.index"), primary_key=True
    )
    connection_name: Mapped[str] = mapped_column()
    ignored: Mapped[bool] = mapped_column()


class ToIngest(DatabaseModel):
    __tablename__ = "to_ingest"
    dataset_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"), primary_key=True)


class ToDelete(DatabaseModel):
    __tablename__ = "to_delete"
    dataset_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"), primary_key=True)
    path: Mapped[str] = mapped_column()


def read_progress(
    engine: sqlalchemy.Engine, init_quanta_done: set[QuantumIndex], quantum_only_xgraph: networkx.DiGraph
) -> set[uuid.UUID]:
    with Session(engine) as session:
        for row in session.execute(sqlalchemy.select(Quantum.quantum_index, Quantum.succeeded)):
            if row.quantum_index in quantum_only_xgraph:
                quantum_only_xgraph.nodes[row.quantum_index]["succeeded"] = row.succeeded
            else:
                init_quanta_done.add(row.quantum_index)
        datasets_done = {row.dataset_id for row in session.execute(sqlalchemy.select(Dataset.dataset_id))}
    return datasets_done
