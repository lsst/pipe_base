#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
# see <http://www.lsstcorp.org/LegalNotices/>.
#


import unittest

import astropy
import pydantic

import lsst.daf.butler as dafButler
import lsst.pipe.base as pipeBase
import lsst.sphgeom as sphgeom
import lsst.utils.tests


class RegionTimeInfoTestCase(unittest.TestCase):
    """Unit tests for lsst.pipe.base.utils.RegionTimeInfo.

    Since RegionTimeInfo is a passive container that exists only for
    serialization convenience, the tests only cover serialization and assume
    e.g. input validation is handled by the component types.
    """

    def setUp(self):
        self.region = sphgeom.Circle(
            sphgeom.UnitVector3d(sphgeom.Angle.fromDegrees(34.5), sphgeom.Angle.fromDegrees(-42.0)),
            sphgeom.Angle.fromDegrees(1.0),
        )
        self.times = dafButler.Timespan(
            begin=astropy.time.Time("2013-06-17 13:34:45.775000", scale="tai", format="iso"),
            end=astropy.time.Time("2013-06-17 13:35:17.947000", scale="tai", format="iso"),
        )

    def test_init(self):
        # Both parameters are mandatory
        with self.assertRaises(ValueError):
            pipeBase.utils.RegionTimeInfo()
        with self.assertRaises(ValueError):
            pipeBase.utils.RegionTimeInfo(region=self.region)
        with self.assertRaises(ValueError):
            pipeBase.utils.RegionTimeInfo(timespan=self.times)

    def test_serialization(self):
        original = pipeBase.utils.RegionTimeInfo(region=self.region, timespan=self.times)
        adapter = pydantic.TypeAdapter(pipeBase.utils.RegionTimeInfo)

        roundtripped = adapter.validate_json(adapter.dump_json(original))
        self.assertEqual(roundtripped, original)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module):
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
