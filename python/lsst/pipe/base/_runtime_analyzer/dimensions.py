"""Parsing of data-ID (``DataCoordinate``) display strings.

Robust, dependency-free parsing of the ``str(DataCoordinate)``
representations produced by the extraction producers: the colon format
(``"{visit: 522, band: 'r'}"``) and the equals formats
(``"{visit=522, band='r'}"``, bare ``"visit=1,filter=g"``), including
bracketed sequence values and quoted commas.  This module imports
nothing from the rest of ``runtime_analyzer`` (not even
:mod:`lsst.pipe.base._runtime_analyzer.runtime_table`), so both the table layer
and the analysis layer can consume it without circular imports.
"""

from __future__ import annotations

__all__ = [
    "parse_dimension_string",
]


def _split_top_level(s: str) -> list[str]:
    """Split a string on commas that are not nested inside brackets/quotes.

    Commas inside ``[...]`` brackets *or* inside single/double quoted
    strings (e.g. ``"band='a,b'"``) are not split points.

    Parameters
    ----------
    s : `str`
        Input string, e.g., ``"visit=1, band='g'"``,
        ``"detector=[1,2,3], band='r'"``, or ``"band='a,b', visit=1"``.

    Returns
    -------
    parts : `list` of `str`
        The split segments (stripped of surrounding whitespace).
    """
    parts: list[str] = []
    depth = 0
    quote: str | None = None
    current: list[str] = []
    for ch in s:
        if quote is not None:
            # Inside a quoted string: everything is literal until the
            # matching close quote; commas are not split points.
            current.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
            current.append(ch)
            continue
        if ch == '[':
            depth += 1
        elif ch == ']':
            depth -= 1
        if ch == ',' and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    parts.append("".join(current).strip())
    return parts


def _strip_quotes(value: str) -> str:
    """Strip surrounding whitespace and single/double quotes."""
    return value.strip().strip("'\"").strip()


def _normalize_dimension_value(value: str) -> str:
    """Normalize a raw parsed dimension value string.

    Strips surrounding quotes/whitespace and canonicalizes bracketed
    sequence values (``"[1, 2]"`` → ``"1,2"``) so the string-fallback path
    produces the *same* group key as the captured ``DataCoordinate.mapping``
    path (which joins sequences with ``,`` via ``_format_dimension_value``).
    Without this, a dimension could split into two distinct group keys
    depending on whether the value was captured from an object or parsed from
    text.

    Parameters
    ----------
    value : `str`
        Raw value segment (may carry quotes, whitespace, or ``[ ... ]``).

    Returns
    -------
    normalized : `str`
        Canonical string value.
    """
    value = _strip_quotes(value)
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1]
        items = [_strip_quotes(p) for p in _split_top_level(inner) if p.strip()]
        return ",".join(items)
    return value


def parse_dimension_string(data_id_str: str) -> dict[str, str]:
    """Robustly parse a DataCoordinate string into a dimension dict.

    Handles both the colon-separated ``str(DataCoordinate)`` format
    (braces, quoted string values, e.g.
    ``"{instrument: 'INSTR', visit: 522}"``) and the equals form
    (``"{visit=522, band='r'}"``, bare ``"visit=1,filter=g"``).  The
    separator is auto-detected per segment (``:`` preferred, ``=``
    otherwise); dimension keys are identifiers and contain neither
    character, so splitting on the first occurrence is safe.

    Parameters
    ----------
    data_id_str : `str`
        String representation of a data ID.

    Returns
    -------
    dimensions : `dict` [ `str`, `str` ]
        Mapping from dimension key to (unquoted) string value.
    """
    s = data_id_str.strip()
    if s.startswith('{'):
        s = s[1:]
    if s.endswith('}'):
        s = s[:-1]

    dimensions: dict[str, str] = {}
    for part in _split_top_level(s):
        # Auto-detect separator: ":" preferred, "=" otherwise.
        sep = ":" if ":" in part else "="
        if sep not in part:
            continue
        key, _, value = part.partition(sep)
        key = _strip_quotes(key)
        if key:
            dimensions[key] = _normalize_dimension_value(value)
    return dimensions
