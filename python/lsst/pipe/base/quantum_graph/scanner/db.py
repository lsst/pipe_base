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

import sqlalchemy
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class DatabaseModel(DeclarativeBase):
    pass


class Dataset(DatabaseModel):
    __tablename__ = "dataset"
    dataset_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    exists: Mapped[bool] = mapped_column()
    producer: Mapped[uuid.UUID | None] = mapped_column()
    provenance: Mapped[bytes] = mapped_column()


class InitQuantum(DatabaseModel):
    __tablename__ = "init_quantum"
    quantum_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    task_label: Mapped[str] = mapped_column()


class Quantum(DatabaseModel):
    __tablename__ = "quantum"
    quantum_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    successful: Mapped[bool] = mapped_column()
    provenance: Mapped[bytes] = mapped_column()
    log_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"))
    log_content: Mapped[bytes] = mapped_column()
    metadata_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"))
    metadata_content: Mapped[bytes] = mapped_column()


class ToIngest(DatabaseModel):
    __tablename__ = "to_ingest"
    dataset_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"), primary_key=True)
    ref_json: Mapped[bytes] = mapped_column()


class ToDelete(DatabaseModel):
    __tablename__ = "to_delete"
    dataset_id: Mapped[uuid.UUID] = mapped_column(sqlalchemy.ForeignKey("dataset.id"), primary_key=True)
    ref_json: Mapped[bytes] = mapped_column()
