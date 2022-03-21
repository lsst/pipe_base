# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ["register_instrument"]

from typing import List

from lsst.daf.butler import Butler
from lsst.pipe.base import Instrument


def register_instrument(repo: str, instrument: List[str], update: bool = False) -> None:
    """Add an instrument to the data repository.

    Parameters
    ----------
    repo : `str`
        URI to the butler repository to register this instrument.
    instrument : `list` [`str`]
        The fully-qualified name of an `~lsst.pipe.base.Instrument` subclass.
    update : `bool`, optional
        If `True` (`False` is default), update the existing instrument and
        detector records if they differ from the new ones.

    Raises
    ------
    RuntimeError
        Raised if the instrument can not be imported, instantiated, or obtained
        from the registry.
    TypeError
        Raised iff the instrument is not a subclass of
        `lsst.pipe.base.Instrument`.
    """
    butler = Butler(repo, writeable=True)
    for string in instrument:
        instrument_instance = Instrument.from_string(string, butler.registry)
        instrument_instance.register(butler.registry, update=update)
