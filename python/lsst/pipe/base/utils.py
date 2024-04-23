# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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


from __future__ import annotations

__all__ = [
    "RegionTimeInfo",
]


import lsst.daf.butler
import pydantic


class RegionTimeInfo(pydantic.BaseModel):
    """A serializable container for a sky region and timespan.

    This container is intended to support translation of observation metadata
    between Butler dimension records, Butler datasets, and exposure headers.
    """

    region: lsst.daf.butler.pydantic_utils.SerializableRegion
    """The sky region stored in this object (`lsst.sphgeom.Region`)."""
    timespan: lsst.daf.butler.Timespan
    """The time interval stored in this object (`lsst.daf.butler.Timespan`)."""
