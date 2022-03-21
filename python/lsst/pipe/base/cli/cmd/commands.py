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

from typing import Any

import click
from lsst.daf.butler.cli.opt import repo_argument
from lsst.daf.butler.cli.utils import ButlerCommand

from ... import script
from ..opt import instrument_argument


@click.command(short_help="Add an instrument definition to the repository", cls=ButlerCommand)
@repo_argument(required=True)
@instrument_argument(required=True, nargs=-1, help="The fully-qualified name of an Instrument subclass.")
@click.option("--update", is_flag=True)
def register_instrument(*args: Any, **kwargs: Any) -> None:
    """Add an instrument to the data repository."""
    script.register_instrument(*args, **kwargs)
