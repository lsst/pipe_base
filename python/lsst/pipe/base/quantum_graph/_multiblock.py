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

__all__ = (
    "Address",
    "AddressReader",
    "AddressRow",
    "AddressWriter",
    "Compressor",
    "Decompressor",
    "InvalidQuantumGraphFileError",
    "MultiblockReader",
    "MultiblockWriter",
)

import dataclasses
import logging
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from io import BufferedReader, BytesIO
from operator import attrgetter
from typing import IO, TYPE_CHECKING, Protocol, TypeAlias, TypeVar

import pydantic

if TYPE_CHECKING:
    import zipfile


_LOG = logging.getLogger(__name__)


_T = TypeVar("_T", bound=pydantic.BaseModel)


UUID_int: TypeAlias = int

MAX_UUID_INT: UUID_int = 2**128


DEFAULT_PAGE_SIZE: int = 5_000_000
"""Default page size for reading chunks of quantum graph files.

This is intended to be large enough to avoid any possibility of individual
reads suffering from per-seek overheads, especially in network file access,
while still being small enough to only minimally slow down tiny reads of
individual quanta (especially for execution).
"""


class Compressor(Protocol):
    """A protocol for objects with a `compress` method that takes and returns
    `bytes`.
    """

    def compress(self, data: bytes) -> bytes:
        """Compress the given data.

        Parameters
        ----------
        data : `bytes`
            Uncompressed data.

        Returns
        -------
        compressed : `bytes`
            Compressed data.
        """
        ...


class Decompressor(Protocol):
    """A protocol for objects with a `decompress` method that takes and returns
    `bytes`.
    """

    def decompress(self, data: bytes) -> bytes:
        """Decompress the given data.

        Parameters
        ----------
        data : `bytes`
            Compressed data.

        Returns
        -------
        decompressed : `bytes`
            Uncompressed data.
        """
        ...


class InvalidQuantumGraphFileError(RuntimeError):
    """An exception raised when a quantum graph file has internal
    inconsistencies or does not actually appear to be a quantum graph file.
    """


@dataclasses.dataclass(slots=True)
class Address:
    """Struct that holds an address into a multi-block file."""

    offset: int = 0
    """Byte offset for the block."""

    size: int = 0
    """Size of the block.

    This always includes the size of the tiny header that records the block
    size.  That header does not include the size of the header, so these sizes
    differ by the ``int_size`` used to write the multi-block file.

    A size of zero is used (with, by convention, an offset of zero) to indicate
    an absent block.
    """

    def __str__(self) -> str:
        return f"{self.offset:06}[{self.size:06}]"


@dataclasses.dataclass(slots=True)
class AddressRow:
    """The in-memory representation of a single row in an address file."""

    key: uuid.UUID
    """Universally unique identifier for this row."""

    index: int
    """Monotonically increasing integer ID; unique within this file only."""

    addresses: list[Address] = dataclasses.field(default_factory=list)
    """Offsets and sizes into multi-block files."""

    def write(self, stream: IO[bytes], int_size: int) -> None:
        """Write this address row to a file-like object.

        Parameters
        ----------
        stream : `typing.IO` [ `bytes` ]
            Binary file-like object.
        int_size : `int`
            Number of bytes to use for all integers.
        """
        stream.write(self.key.bytes)
        stream.write(self.index.to_bytes(int_size))
        for address in self.addresses:
            stream.write(address.offset.to_bytes(int_size))
            stream.write(address.size.to_bytes(int_size))

    @classmethod
    def read(cls, stream: IO[bytes], n_addresses: int, int_size: int) -> AddressRow:
        """Read this address row from a file-like object.

        Parameters
        ----------
        stream : `typing.IO` [ `bytes` ]
            Binary file-like object.
        n_addresses : `int`
            Number of addresses included in each row.
        int_size : `int`
            Number of bytes to use for all integers.
        """
        key = uuid.UUID(int=int.from_bytes(stream.read(16)))
        index = int.from_bytes(stream.read(int_size))
        row = AddressRow(key, index)
        for _ in range(n_addresses):
            offset = int.from_bytes(stream.read(int_size))
            size = int.from_bytes(stream.read(int_size))
            row.addresses.append(Address(offset, size))
        return row

    def __str__(self) -> str:
        return f"{self.key} {self.index:06} {' '.join(str(a) for a in self.addresses)}"


@dataclasses.dataclass
class AddressWriter:
    """A helper object for writing address files for multi-block files."""

    indices: dict[uuid.UUID, int] = dataclasses.field(default_factory=dict)
    """Mapping from UUID to internal integer ID.

    The internal integer ID must always correspond to the index into the
    sorted list of all UUIDs, but this `dict` need not be sorted itself.
    """

    addresses: list[dict[uuid.UUID, Address]] = dataclasses.field(default_factory=list)
    """Addresses to store with each UUID.

    Every key in one of these dictionaries must have an entry in `indices`.
    The converse is not true.
    """

    def write(self, stream: IO[bytes], int_size: int) -> None:
        """Write all addresses to a file-like object.

        Parameters
        ----------
        stream : `typing.IO` [ `bytes` ]
            Binary file-like object.
        int_size : `int`
            Number of bytes to use for all integers.
        """
        for n, address_map in enumerate(self.addresses):
            if not self.indices.keys() >= address_map.keys():
                raise AssertionError(
                    f"Logic bug in quantum graph I/O: address map {n} of {len(self.addresses)} has IDs "
                    f"{address_map.keys() - self.indices.keys()} not in the index map."
                )
        stream.write(int_size.to_bytes(1))
        stream.write(len(self.indices).to_bytes(int_size))
        stream.write(len(self.addresses).to_bytes(int_size))
        empty_address = Address()
        for key in sorted(self.indices.keys(), key=attrgetter("int")):
            row = AddressRow(key, self.indices[key], [m.get(key, empty_address) for m in self.addresses])
            _LOG.debug("Wrote address %s.", row)
            row.write(stream, int_size)

    def write_to_zip(self, zf: zipfile.ZipFile, name: str, int_size: int) -> None:
        """Write all addresses to a file in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to add the file to.
        name : `str`
            Base name for the address file; an extension will be added.
        int_size : `int`
            Number of bytes to use for all integers.
        """
        with zf.open(f"{name}.addr", mode="w") as stream:
            self.write(stream, int_size=int_size)


@dataclasses.dataclass
class AddressPage:
    """A page of addresses in the `AddressReader`."""

    file_offset: int
    """Offset in bytes to this page from the beginning of the file."""

    begin: int
    """Index of the first row in this page."""

    n_rows: int
    """Number of rows in this page."""

    read: bool = False
    """Whether this page has already been read."""

    @property
    def end(self) -> int:
        """One past the last row index in this page."""
        return self.begin + self.n_rows


@dataclasses.dataclass
class PageBounds:
    """A page index and the UUID interval that page covers."""

    page_index: int
    """Index into the page array."""

    uuid_int_begin: UUID_int
    """Integer representation of the smallest UUID in this page."""

    uuid_int_end: UUID_int
    """One larger than the integer representation of the largest UUID in this
    page.
    """

    def __str__(self) -> str:
        return f"{self.page_index} [{self.uuid_int_begin:x}:{self.uuid_int_end:x}]"


@dataclasses.dataclass
class AddressReader:
    """A helper object for reading address files for multi-block files."""

    stream: IO[bytes]
    """Stream to read from."""

    int_size: int
    """Size of each integer in bytes."""

    n_rows: int
    """Number of rows in the file."""

    n_addresses: int
    """Number of addresses in each row."""

    rows_per_page: int
    """Number of addresses in each page."""

    rows: dict[uuid.UUID, AddressRow] = dataclasses.field(default_factory=dict)
    """Rows that have already been read."""

    pages: list[AddressPage] = dataclasses.field(default_factory=list)
    """Descriptions of the file offsets and integer row indexes of pages and
    flags for whether they have been read already.
    """

    page_bounds: dict[int, PageBounds] = dataclasses.field(default_factory=dict)
    """Mapping from page index to page boundary information."""

    @classmethod
    def from_stream(
        cls, stream: IO[bytes], *, page_size: int, n_addresses: int, int_size: int
    ) -> AddressReader:
        """Construct from a stream by reading the header.

        Parameters
        ----------
        stream : `typing.IO` [ `bytes` ]
            File-like object to read from.
        page_size : `int`
            Approximate number of bytes to read at a time when searching for an
            address.
        n_addresses : `int`
            Number of addresses to expect per row.  This is checked against
            the size embedded in the file.
        int_size : `int`
            Number of bytes to use for all integers.  This is checked against
            the size embedded in the file.
        """
        header_size = cls.compute_header_size(int_size)
        row_size = cls.compute_row_size(int_size, n_addresses)
        # Read the raw header page.
        header_page_data = stream.read(header_size)
        if len(header_page_data) < header_size:
            raise InvalidQuantumGraphFileError("Address file unexpectedly truncated.")
        # Interpret the raw header data and initialize the reader instance.
        header_page_stream = BytesIO(header_page_data)
        file_int_size = int.from_bytes(header_page_stream.read(1))
        if file_int_size != int_size:
            raise InvalidQuantumGraphFileError(
                f"int size in address file ({file_int_size}) does not match int size in header ({int_size})."
            )
        n_rows = int.from_bytes(header_page_stream.read(int_size))
        file_n_addresses = int.from_bytes(header_page_stream.read(int_size))
        if file_n_addresses != n_addresses:
            raise InvalidQuantumGraphFileError(
                f"Incorrect number of addresses per row: expected {n_addresses}, got {file_n_addresses}."
            )
        rows_per_page = max(page_size // row_size, 1)
        # Construct an instance.
        self = cls(stream, int_size, n_rows, n_addresses, rows_per_page=rows_per_page)
        # Calculate positions of each page of rows.
        row_index = 0
        file_offset = header_size
        while row_index < n_rows:
            self.pages.append(AddressPage(file_offset=file_offset, begin=row_index, n_rows=rows_per_page))
            row_index += rows_per_page
            file_offset += rows_per_page * row_size
        if row_index != n_rows:
            # Last page was too big.
            self.pages[-1].n_rows -= row_index - n_rows
            assert sum(p.n_rows for p in self.pages) == n_rows, "Bad logic setting page row counts."
        return self

    @classmethod
    @contextmanager
    def open_in_zip(
        cls,
        zf: zipfile.ZipFile,
        name: str,
        *,
        page_size: int,
        n_addresses: int,
        int_size: int,
    ) -> Iterator[AddressReader]:
        """Make a reader for an address file in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to read the file from.
        name : `str`
            Base name for the address file; an extension will be added.
        page_size : `int`
            Approximate number of bytes to read at a time when searching for an
            address.
        n_addresses : `int`
            Number of addresses to expect per row.  This is checked against
            the size embedded in the file.
        int_size : `int`
            Number of bytes to use for all integers.  This is checked against
            the size embedded in the file.

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ `AddressReader` ]
            Context manager that returns a reader when entered.
        """
        with zf.open(f"{name}.addr", mode="r") as stream:
            yield cls.from_stream(stream, page_size=page_size, n_addresses=n_addresses, int_size=int_size)

    @staticmethod
    def compute_header_size(int_size: int) -> int:
        """Return the size (in bytes) of the header of an address file.

        Parameters
        ----------
        int_size : `int`
            Size of each integer in bytes.

        Returns
        -------
        size : `int`
            Size of the header in bytes.
        """
        return (
            1  # int_size
            + int_size  # number of rows
            + int_size  # number of addresses in each row
        )

    @staticmethod
    def compute_row_size(int_size: int, n_addresses: int) -> int:
        """Return the size (in bytes) of each row of an address file.

        Parameters
        ----------
        int_size : `int`
            Size of each integer in bytes.
        n_addresses : `int`
            Number of addresses in each row.

        Returns
        -------
        size : `int`
            Size of each row in bytes.
        """
        return (
            16  # uuid
            + int_size
            * (
                1  # index
                + 2 * n_addresses
            )
        )

    @property
    def row_size(self) -> int:
        """The size (in bytes) of each row of this address file."""
        return self.compute_row_size(self.int_size, self.n_addresses)

    def read_all(self) -> dict[uuid.UUID, AddressRow]:
        """Read all addresses in the file.

        Returns
        -------
        rows : `dict` [ `uuid.UUID`, `AddressRow` ]
            Mapping of loaded address rows, keyed by UUID.
        """
        # Skip any pages from the beginning that have already been read; this
        # nicely handles both the case where we already read everything (or
        # there was nothing to read) while giving us a page with a file offset
        # to start from.
        for page in self.pages:
            if not page.read:
                break
        else:
            return self.rows
        # Read the entire rest of the file into memory.
        self.stream.seek(page.file_offset)
        data = self.stream.read()
        buffer = BytesIO(data)
        # Shortcut out if we've already read everything, but don't bother
        # optimizing previous partial reads.
        while len(self.rows) < self.n_rows:
            self._read_row(buffer)
        # Delete all pages; they don't matter anymore, and that's easier than
        # updating them to reflect the reads we've done.
        self.pages.clear()
        return self.rows

    def find(self, key: uuid.UUID) -> AddressRow:
        """Read the row for the given UUID or integer index.

        Parameters
        ----------
        key : `uuid.UUID`
            UUID to find.

        Returns
        -------
        row : `AddressRow`
            Addresses for the given UUID.
        """
        if (row := self.rows.get(key)) is not None:
            return row
        if self.n_rows == 0 or not self.pages:
            raise LookupError(f"Address for {key} not found.")

        # Use a binary search to find the page containing the target UUID.
        left = 0
        right = len(self.pages) - 1
        while left <= right:
            mid = left + ((right - left) // 2)
            self._read_page(mid)
            if (row := self.rows.get(key)) is not None:
                return row
            bounds = self.page_bounds[mid]
            if key.int < bounds.uuid_int_begin:
                right = mid - 1
            elif key.int > bounds.uuid_int_end:
                left = mid + 1
            else:
                # Should have been on this page, but it wasn't.
                raise LookupError(f"Address for {key} not found.")

        # Ran out of pages to search.
        raise LookupError(f"Address for {key} not found.")

    def _read_page(self, page_index: int, page_stream: BytesIO | None = None) -> bool:
        page = self.pages[page_index]
        if page.read:
            return False
        if page_stream is None:
            self.stream.seek(page.file_offset)
            page_stream = BytesIO(self.stream.read(page.n_rows * self.row_size))
        row = self._read_row(page_stream)
        uuid_int_begin = row.key.int
        for _ in range(1, page.n_rows):
            row = self._read_row(page_stream)
        uuid_int_end = row.key.int + 1  # Python's loop scoping rules are actually useful here!
        page.read = True
        bounds = PageBounds(page_index=page_index, uuid_int_begin=uuid_int_begin, uuid_int_end=uuid_int_end)
        self.page_bounds[page_index] = bounds
        _LOG.debug("Read page %s with rows [%s:%s].", bounds, page.begin, page.end)
        return True

    def _read_row(self, page_stream: BytesIO) -> AddressRow:
        row = AddressRow.read(page_stream, self.n_addresses, self.int_size)
        self.rows[row.key] = row
        _LOG.debug("Read address row %s.", row)
        return row


@dataclasses.dataclass
class MultiblockWriter:
    """A helper object for writing multi-block files."""

    stream: IO[bytes]
    """A binary file-like object to write to."""

    int_size: int
    """Number of bytes to use for all integers."""

    file_size: int = 0
    """Running size of the full file."""

    addresses: dict[uuid.UUID, Address] = dataclasses.field(default_factory=dict)
    """Running map of all addresses added to the file so far.

    When the multi-block file is fully written, this is appended to the
    `AddressWriter.addresses` to write the corresponding address file.
    """

    @classmethod
    @contextmanager
    def open_in_zip(
        cls, zf: zipfile.ZipFile, name: str, int_size: int, use_tempfile: bool = False
    ) -> Iterator[MultiblockWriter]:
        """Open a writer for a file in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to add the file to.
        name : `str`
            Base name for the multi-block file; an extension will be added.
        int_size : `int`
            Number of bytes to use for all integers.
        use_tempfile : `bool`, optional
            If `True`, send writes to a temporary file and only add the file to
            the zip archive when the context manager closes.  This involves
            more overall I/O, but it permits multiple multi-block files to be
            open for writing in the same zip archive at once.

        Returns
        -------
        writer : `contextlib.AbstractContextManager` [ `MultiblockWriter` ]
            Context manager that returns a writer when entered.
        """
        filename = f"{name}.mb"
        if use_tempfile:
            with tempfile.NamedTemporaryFile(suffix=filename) as tmp:
                yield MultiblockWriter(tmp, int_size)
                tmp.flush()
                zf.write(tmp.name, filename)
        else:
            with zf.open(f"{name}.mb", mode="w", force_zip64=True) as stream:
                yield MultiblockWriter(stream, int_size)

    def write_bytes(self, id: uuid.UUID, data: bytes) -> Address:
        """Write raw bytes to the multi-block file.

        Parameters
        ----------
        id : `uuid.UUID`
            Unique ID of the object described by this block.
        data : `bytes`
            Data to store directly.

        Returns
        -------
        address : `Address`
            Address of the bytes just written.
        """
        assert id not in self.addresses, "Duplicate write to multi-block file detected."
        self.stream.write(len(data).to_bytes(self.int_size))
        self.stream.write(data)
        block_size = len(data) + self.int_size
        address = Address(offset=self.file_size, size=block_size)
        self.file_size += block_size
        self.addresses[id] = address
        return address

    def write_model(self, id: uuid.UUID, model: pydantic.BaseModel, compressor: Compressor) -> Address:
        """Write raw bytes to the multi-block file.

        Parameters
        ----------
        id : `uuid.UUID`
            Unique ID of the object described by this block.
        model : `pydantic.BaseModel`
            Model to convert to JSON and compress.
        compressor : `Compressor`
            Object with a `compress` method that takes and returns `bytes`.

        Returns
        -------
        address : `Address`
            Address of the bytes just written.
        """
        json_data = model.model_dump_json().encode()
        compressed_data = compressor.compress(json_data)
        return self.write_bytes(id, compressed_data)


@dataclasses.dataclass
class MultiblockReader:
    """A helper object for reader multi-block files."""

    stream: IO[bytes]
    """A binary file-like object to read from."""

    int_size: int
    """Number of bytes to use for all integers."""

    @classmethod
    @contextmanager
    def open_in_zip(cls, zf: zipfile.ZipFile, name: str, *, int_size: int) -> Iterator[MultiblockReader]:
        """Open a reader for a file in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to read the file from.
        name : `str`
            Base name for the multi-block file; an extension will be added.
        int_size : `int`
            Number of bytes to use for all integers.

        Returns
        -------
        reader : `contextlib.AbstractContextManager` [ `MultiblockReader` ]
            Context manager that returns a reader when entered.
        """
        with zf.open(f"{name}.mb", mode="r") as stream:
            yield MultiblockReader(stream, int_size)

    @classmethod
    def read_all_bytes_in_zip(
        cls, zf: zipfile.ZipFile, name: str, *, int_size: int, page_size: int
    ) -> Iterator[bytes]:
        """Iterate over all of the byte blocks in a file in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to read the file from.
        name : `str`
            Base name for the multi-block file; an extension will be added.
        int_size : `int`
            Number of bytes to use for all integers.
        page_size : `int`
            Approximate number of bytes to read at a time.

        Returns
        -------
        byte_iter : `~collections.abc.Iterator` [ `bytes` ]
            Iterator over blocks.
        """
        with zf.open(f"{name}.mb", mode="r") as zf_stream:
            # The standard library typing of IO[bytes] tiers isn't consistent.
            buffered_stream = BufferedReader(zf_stream)  # type: ignore[type-var]
            size_data = buffered_stream.read(int_size)
            while size_data:
                internal_size = int.from_bytes(size_data)
                yield buffered_stream.read(internal_size)
                size_data = buffered_stream.read(int_size)

    @classmethod
    def read_all_models_in_zip(
        cls,
        zf: zipfile.ZipFile,
        name: str,
        model_type: type[_T],
        decompressor: Decompressor,
        *,
        int_size: int,
        page_size: int,
    ) -> Iterator[_T]:
        """Iterate over all of the models in a file in a zip archive.

        Parameters
        ----------
        zf : `zipfile.ZipFile`
            Zip archive to read the file from.
        name : `str`
            Base name for the multi-block file; an extension will be added.
        model_type : `type` [ `pydantic.BaseModel` ]
            Pydantic model to validate JSON with.
        decompressor : `Decompressor`
            Object with a `decompress` method that takes and returns `bytes`.
        int_size : `int`
            Number of bytes to use for all integers.
        page_size : `int`
            Approximate number of bytes to read at a time.

        Returns
        -------
        model_iter : `~collections.abc.Iterator` [ `pydantic.BaseModel` ]
            Iterator over model instances.
        """
        for compressed_data in cls.read_all_bytes_in_zip(zf, name, int_size=int_size, page_size=page_size):
            json_data = decompressor.decompress(compressed_data)
            yield model_type.model_validate_json(json_data)

    def read_bytes(self, address: Address) -> bytes | None:
        """Read raw bytes from the multi-block file.

        Parameters
        ----------
        address : `Address`
            Offset and size of the data to read.

        Returns
        -------
        data : `bytes` or `None`
            Data read directly, or `None` if the address has zero size.
        """
        if not address.size:
            return None
        self.stream.seek(address.offset)
        data = self.stream.read(address.size)
        internal_size = int.from_bytes(data[: self.int_size])
        data = data[self.int_size :]
        if len(data) != internal_size:
            raise InvalidQuantumGraphFileError(
                f"Internal size {internal_size} does not match loaded data size {len(data)}."
            )
        return data

    def read_model(self, address: Address, model_type: type[_T], decompressor: Decompressor) -> _T | None:
        """Read a single compressed JSON block.

        Parameters
        ----------
        address : `Address`
            Size and offset of the block.
        model_type : `type` [ `pydantic.BaseModel` ]
            Pydantic model to validate JSON with.
        decompressor : `Decompressor`
            Object with a `decompress` method that takes and returns `bytes`.

        Returns
        -------
        model : `pydantic.BaseModel`
            Validated model.
        """
        compressed_data = self.read_bytes(address)
        if compressed_data is None:
            return None
        json_data = decompressor.decompress(compressed_data)
        return model_type.model_validate_json(json_data)
