import tempfile
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from itertools import batched

import pyarrow as pa
import zstandard
from duckdb import DuckDBPyConnection, connect

from ._multiblock import MultiblockReader


def convert_multiblock_to_parquet(input_zip_path: str, output_zip_path: str) -> None:
    with zipfile.ZipFile(input_zip_path, "r") as zf:
        if (cdict_path := zipfile.Path(zf, "compression_dict")).exists():
            cdict = zstandard.ZstdCompressionDict(cdict_path.read_bytes())
        decompressor = zstandard.ZstdDecompressor(cdict)
        with _initialize_duckdb_connection() as db:
            for filename in ("quanta", "datasets", "metadata", "logs"):
                reader = _get_record_batch_reader(filename, zf, decompressor)
                db.execute(
                    """
                COPY (SELECT (data::JSON)::VARIANT as data FROM reader)
                TO $output_filename (FORMAT 'parquet', COMPRESSION 'zstd')
                """,
                    {"output_filename": f"{filename}.parquet"},
                )


@contextmanager
def _initialize_duckdb_connection(memory_limit: str = "8GB") -> Iterator[DuckDBPyConnection]:
    """Set up an in-memory DuckDB database, applying a memory limit and using
    the system tempdir for its working directory.
    """
    with (
        tempfile.TemporaryDirectory(suffix=".duckdb") as tmpdir,
        connect(
            config={
                # DuckDB will use up to 80% of system memory if you don't
                # explicitly set it.
                "memory_limit": memory_limit,
                # DuckDB will use the current working directory if you don't
                # set it, which is frequently a small quota-limited volume on
                # USDF.
                "temp_directory": tmpdir,
                # For paranoia -- some of the scratch volumes at USDF are
                # petabyte-sized.
                "max_temp_directory_size": "128GB",
            }
        ) as conn,
    ):
        yield conn


def _fetch_json_bytes(
    filename: str, zip: zipfile.ZipFile, decompressor: zstandard.ZstdDecompressor
) -> Iterator[bytes]:
    for data in MultiblockReader.read_all_bytes_in_zip(zip, filename, int_size=8, page_size=8 * 1024 * 1024):
        yield decompressor.decompress(data)


def _to_batches(schema: pa.Schema, it: Iterator[bytes]) -> Iterator[pa.RecordBatch]:
    for batch in batched(it, 1000):
        array = pa.array(batch)
        yield pa.RecordBatch.from_arrays([array], schema=schema)


def _get_record_batch_reader(
    filename: str, zip: zipfile.ZipFile, decompressor: zstandard.ZstdDecompressor
) -> pa.RecordBatchReader:
    schema = pa.schema([("data", pa.string())])
    return pa.RecordBatchReader.from_batches(
        schema, _to_batches(schema, _fetch_json_bytes(filename, zip, decompressor))
    )
