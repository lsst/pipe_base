#
# LSST Data Management System
# Copyright 2008-2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
from __future__ import annotations

__all__: list[str] = []

import contextlib

from deprecated.sphinx import deprecated


@deprecated(
    reason="Replaced by lsst.utils.timer.profile().  Will be removed after v26.0",
    version="v25.0",
    category=FutureWarning,
)
@contextlib.contextmanager
def profile(filename, log=None):
    """Context manager for profiling with cProfile.


    Parameters
    ----------
    filename : `str`
        Filename to which to write profile (profiling disabled if `None` or
        empty).
    log : `logging.Logger`, optional
        Log object for logging the profile operations.

    If profiling is enabled, the context manager returns the cProfile.Profile
    object (otherwise it returns None), which allows additional control over
    profiling.  You can obtain this using the "as" clause, e.g.:

    .. code-block:: python

        with profile(filename) as prof:
            runYourCodeHere()

    The output cumulative profile can be printed with a command-line like:

    .. code-block:: bash

        python -c 'import pstats; \
            pstats.Stats("<filename>").sort_stats("cumtime").print_stats(30)'
    """
    if not filename:
        # Nothing to do
        yield
        return
    from cProfile import Profile

    profile = Profile()
    if log is not None:
        log.info("Enabling cProfile profiling")
    profile.enable()
    yield profile
    profile.disable()
    profile.dump_stats(filename)
    if log is not None:
        log.info("cProfile stats written to %s", filename)
