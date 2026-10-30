"""Shared fixtures for the resource usage analyzer test suite.

Thin wrappers delegating to the shared builders in the
sibling ``support`` module: ``make_analyzer``,
``make_task_analyzer`` and ``cache_table`` hand the builders out
unchanged as factory fixtures (see each builder's docstring in
``support``); ``write_cache`` binds the fixture's ``tmp_path`` as the
builder's first positional argument, so tests call
``write_cache(name, spec, dims=None, sources=None)``.
"""

from __future__ import annotations

from functools import partial

import pytest

# The support builders import the pyarrow-backed table module; gate the
# import on actual importability (importorskip here would raise Skipped
# during conftest import and abort collection).  Each test module that
# needs pyarrow skips itself via its own module-level importorskip.
try:
    import pyarrow  # noqa: F401
except ImportError:
    _HAS_PYARROW = False
else:
    _HAS_PYARROW = True

if _HAS_PYARROW:
    from . import support  # noqa: E402
else:
    support = None


def _support() -> object:
    """Return the shared builders, skipping if their deps are missing."""
    if support is None:
        pytest.skip("requires pyarrow (pipe_base [runtime] extra)")
    return support


make_analyzer = pytest.fixture(lambda: _support().make_analyzer)
make_task_analyzer = pytest.fixture(lambda: _support().make_task_analyzer)
cache_table = pytest.fixture(lambda: _support().cache_table)
write_cache = pytest.fixture(
    lambda tmp_path: partial(_support().write_cache, tmp_path),
)
