# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = ("TRACE", "VERBOSE", "getTaskLogger")

from deprecated.sphinx import deprecated
from lsst.utils.logging import TRACE, VERBOSE, getLogger


@deprecated(
    reason="getTaskLogger has been replaced by lsst.utils.logging.getLogger. Will be removed after v24.",
    version="v24",
    category=FutureWarning,
)
def getTaskLogger(name=None, logger=None):
    """Get a logger compatible with LSST usage.

    Parameters
    ----------
    name : `str`, optional
        Name of the logger. Root logger if `None`.
    logger : `logging.Logger`
        If given the logger is converted to the relevant logger class.
        If ``name`` is given the logger is assumed to be a child of the
        supplied logger.

    Returns
    -------
    logger : `lsst.utils.logging.LsstLogAdapter`
        The relevant logger.

    Notes
    -----
    A `logging.LoggerAdapter` is used since it is easier to provide a more
    uniform interface than when using `logging.setLoggerClass`. An adapter
    can be wrapped around the root logger and the `~logging.setLoggerClass`
    will return the logger first given that name even if the name was
    used before the `Task` was created.
    """
    return getLogger(name=name, logger=logger)
