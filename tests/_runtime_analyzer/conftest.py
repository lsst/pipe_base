"""Shared fixtures for the runtime_analyzer test suite.

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

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from . import support  # noqa: E402

make_analyzer = pytest.fixture(lambda: support.make_analyzer)
make_task_analyzer = pytest.fixture(lambda: support.make_task_analyzer)
cache_table = pytest.fixture(lambda: support.cache_table)
write_cache = pytest.fixture(
    lambda tmp_path: partial(support.write_cache, tmp_path),
)
