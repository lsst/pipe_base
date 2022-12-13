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

__all__ = ("instrument_option", "update_output_chain_option")

from lsst.daf.butler.cli.utils import MWOptionDecorator, unwrap

instrument_option = MWOptionDecorator(
    "--instrument", help="The name or fully-qualified class name of an instrument."
)


update_output_chain_option = MWOptionDecorator(
    "--update-output-chain",
    help=unwrap(
        """If quantum graph metadata includes output run name and output collection
        which is a chain, update the chain definition to include run name as the
        first collection in the chain."""
    ),
    is_flag=True,
    default=False,
)
