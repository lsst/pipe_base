"""Console output utilities for formatted table display and file export.

Provides functions to format astropy Tables for console output and export
them to CSV and Parquet file formats.  The per-quantum flat table cache
engine lives in ``lsst.pipe.base._runtime_analyzer.runtime_table``.
"""

from __future__ import annotations

__all__ = [
    "export_csv",
    "export_parquet",
    "format_table",
]

import csv
from pathlib import Path

import astropy.table
import numpy as np

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "Runtime-table caching requires 'pyarrow >= 20'. Install "
        "pipe_base with the [runtime] extra."
    ) from exc


def format_table(table: astropy.table.Table) -> str:
    """Format an astropy Table as a string for console output.

    Applies unit formatting and returns a complete formatted representation
    suitable for printing to stdout.

    Parameters
    ----------
    table : `astropy.table.Table`
        Table to format.

    Returns
    -------
    result : `str`
        Formatted table string with units.
    """
    result = '\n'.join(table.pformat())
    return result


def export_csv(table: astropy.table.Table, path: str | Path) -> None:
    """Export an astropy Table to a CSV file.

    Converts the table row by row and writes to the specified file path.
    Sequence cells (list/tuple/array) are stringified and ``bytes`` cells
    are hex-encoded so every value has a stable textual form.

    Parameters
    ----------
    table : `astropy.table.Table`
        Table to export.
    path : `str` or `~pathlib.Path`
        Destination file path (overwritten if it exists).
    """
    path = Path(path)
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(table.colnames)
        for row in table:
            csv_row = []
            for val in row:
                if isinstance(val, (list, tuple, np.ndarray)):
                    csv_row.append(str(list(val)))
                elif isinstance(val, bytes):
                    csv_row.append(val.hex())
                else:
                    csv_row.append(val)
            writer.writerow(csv_row)


def export_parquet(table: astropy.table.Table, path: str | Path) -> None:
    """Export an astropy Table to a Parquet file via pyarrow.

    Converts the astropy.Table column-by-column to pyarrow arrays and
    writes it using ``pyarrow.parquet``, preserving native dtypes:

    - Native numeric columns (int/float) are passed straight through and
      retain their numeric dtype on read-back (NaN stays NaN in float
      columns).
    - Fixed-width bytes columns (``'S...'``) become Parquet ``binary``
      columns (bytes round-trip as ``bytes``, not hex).
    - Object columns are normalized element-wise with
      :func:`_convert_numpy_types` (numpy scalars to Python natives,
      ``NaN`` to ``None``, ``bytes`` to hex strings) so pyarrow can infer
      a consistent type.
     - Masked columns write Parquet nulls (``None``) at masked positions;
       ``MaskedColumn.filled(None)`` is deliberately *not* used because it
       substitutes astropy's numeric fill sentinel (e.g. 999999) for
       nulls.

    Parameters
    ----------
    table : `astropy.table.Table`
        Table to export.
    path : `str` or `~pathlib.Path`
        Destination Parquet file path.
    """
    path = Path(path)

    # Convert astropy Table columns straight to pyarrow arrays.  Numeric
    # dtypes are preserved by handing the arrays straight to pyarrow; only
    # object and masked columns need element-wise care.
    names = []
    arrays = []
    for name in table.colnames:
        col = table[name]
        if isinstance(col, np.ma.MaskedArray):
            mask = np.ma.getmaskarray(col)
            raw = np.ma.getdata(col)
            if mask.shape == () or not mask.any():
                # No masked values: keep the native dtype.
                arrow_col = pa.array(np.asarray(raw))
            else:
                # Masked positions become None (Parquet nulls).
                values = np.asarray(raw).astype(object)
                values[mask] = None
                arrow_col = pa.array(values.tolist())
        else:
            arr = np.asarray(col)
            if arr.dtype == object:
                # Mixed/object data: normalize numpy scalars, NaN, and
                # bytes so pyarrow can infer a single consistent type.
                arrow_col = pa.array([_convert_numpy_types(v) for v in arr])
            else:
                arrow_col = pa.array(arr)
        names.append(name)
        arrays.append(arrow_col)

    if not names:
        table_pyarrow = pa.table({})
    else:
        table_pyarrow = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table_pyarrow, str(path))


def _convert_numpy_types(obj: object) -> object:
    """Convert numpy types to Python native types for serialization.

    Parameters
    ----------
    obj : `object`
        Value that may be a numpy type.

    Returns
    -------
    result : `object`
        Python native type representation.
    """
    if isinstance(obj, (
        np.integer,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
    )):
        return int(obj)
    elif isinstance(obj, (
        np.floating,
        np.float16,
        np.float32,
        np.float64,
    )):
        if obj != obj:  # NaN check
            return None
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, bytes):
        return obj.hex()
    elif isinstance(obj, (list, tuple)):
        return [_convert_numpy_types(v) for v in obj]
    else:
        return obj
