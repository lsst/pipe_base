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
import uuid
from collections.abc import Iterator
from typing import IO, ClassVar

_LOG = logging.getLogger(__name__)


@dataclasses.dataclass
class Address:
    index: int
    offsets: list[int]
    sizes: list[int]

    @classmethod
    def read(cls, stream: IO[bytes], n: int, *, int_size: int) -> Address:
        index = int.from_bytes(stream.read(int_size))
        offsets: list[int] = []
        sizes: list[int] = []
        for i in range(n):
            offsets.append(int.from_bytes(stream.read(int_size)))
            sizes.append(int.from_bytes(stream.read(int_size)))
        return cls(index, offsets, sizes)

    def write(self, stream: IO[bytes], *, int_size: int) -> int:
        stream.write(self.index.to_bytes(int_size))
        total = int_size
        for offset, size in zip(self.offsets, self.sizes, strict=True):
            stream.write(offset.to_bytes(int_size))
            total += int_size
            stream.write(size.to_bytes(int_size))
            total += int_size
        return total


class AddressReader:
    MAX_UUID_INT: ClassVar[int] = 2**128

    def __init__(self, stream: IO[bytes], address_block_size: int = 1024, start_index: int = 0):
        self._stream = stream
        self.start_index = start_index
        self.int_size = int.from_bytes(self._stream.read(1))
        self.header_size = 1 + self.int_size * 2
        self.n_nodes = int.from_bytes(self._stream.read(self.int_size))
        self.n_offsets = int.from_bytes(self._stream.read(self.int_size))
        self.row_size = 16 + (1 + 2 * self.n_offsets) * self.int_size
        self._addresses: dict[uuid.UUID, Address] = {}
        self._block_size = address_block_size
        n_full_blocks, last_block_size = divmod(self.n_nodes, self._block_size)
        self._blocks_unread = dict.fromkeys(range(n_full_blocks), self._block_size)
        if last_block_size := self.n_nodes % self._block_size:
            self._blocks_unread[n_full_blocks] = last_block_size

    def read_all_addresses(self) -> dict[uuid.UUID, Address]:
        for _ in range(self.n_nodes):
            self._read_row()
        return self._addresses

    def find_address(self, id: uuid.UUID) -> Address:
        if (address := self._addresses.get(id)) is not None:
            return address
        guess_index_float = (id.int / self.MAX_UUID_INT) * self.n_nodes + self.start_index
        guess_block_float = (guess_index_float - self.start_index) / self._block_size
        guess_block = int(guess_block_float)
        _LOG.info(f"Looking for ID {id} at guessed index {guess_index_float} (block {guess_block_float}).")
        for block in self._address_block_search_path(guess_block):
            if block in self._blocks_unread:
                self._read_address_block(block)
                if (address := self._addresses.get(id)) is not None:
                    return address
            elif not self._blocks_unread:
                raise LookupError(f"Quantum with ID {id} not found.")
        raise AssertionError("Logic error in block tracking.")

    @staticmethod
    def read_subfile(stream: IO[bytes], address: Address, *, column: int = 0, int_size: int) -> bytes:
        if address.sizes[column] == 0:
            return b""
        stream.seek(address.offsets[column])
        embedded_size = int.from_bytes(stream.read(int_size))
        if embedded_size != address.sizes[column] - int_size:
            raise RuntimeError(
                f"Embedded size {embedded_size} does not match size from address "
                f"{address.sizes[0] - int_size}."
            )
        return stream.read(embedded_size)

    def _read_row(self) -> uuid.UUID:
        id = uuid.UUID(bytes=self._stream.read(16))
        address = Address.read(self._stream, self.n_offsets, int_size=self.int_size)
        self._addresses[id] = address
        return id

    def _address_block_search_path(self, mid: int) -> Iterator[int]:
        yield mid
        for abs_offset in itertools.count(1):
            yield mid + abs_offset
            yield mid - abs_offset

    def _read_address_block(self, block: int) -> None:
        size = self._blocks_unread.pop(block)
        self._stream.seek(block * self._block_size * self.row_size + self.header_size)
        a = self._read_row()
        for _ in range(1, size - 1):
            self._read_row()
        b = self._read_row()
        _LOG.info(
            "Read block %s (%s -> %s ... %s -> %s).",
            block,
            self.start_index + block * self._block_size,
            self.start_index + self.n_nodes * a.int / self.MAX_UUID_INT,
            self.start_index + (block + 1) * self._block_size,
            self.start_index + self.n_nodes * b.int / self.MAX_UUID_INT,
        )


@dataclasses.dataclass
class AddressWriter:
    int_size: int
    addresses: dict[uuid.UUID, Address]
    totals: list[int]

    @property
    def n_columns(self) -> int:
        return len(self.totals)

    @property
    def total(self) -> int:
        return sum(self.totals)

    def write_addresses(self, stream: IO[bytes]) -> int:
        stream.write(self.int_size.to_bytes(1))
        stream.write(len(self.addresses).to_bytes(self.int_size))
        stream.write(self.n_columns.to_bytes(self.int_size))
        address_total = 1 + 2 * self.int_size
        for key, address in self.addresses.items():
            stream.write(key.bytes)
            address_total += len(key.bytes)
            address_total += address.write(stream, int_size=self.int_size)
        return address_total

    def write_subfile(self, stream: IO[bytes], id: uuid.UUID, data: bytes, *, column: int = 0) -> int:
        stream.write(len(data).to_bytes(self.int_size))
        stream.write(data)
        if (address := self.addresses.get(id)) is None:
            address = Address(len(self.addresses), offsets=[0] * self.n_columns, sizes=[0] * self.n_columns)
            self.addresses[id] = address
        size = len(data) + self.int_size
        address.offsets[column] = self.totals[column]
        address.sizes[column] = size
        self.totals[column] += size
        return size
