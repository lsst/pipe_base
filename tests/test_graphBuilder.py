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

"""Tests of things related to the GraphBuilder class."""

import unittest

from lsst.pipe.base import GraphBuilder

import lsst.utils.tests


class VerifyInstrumentRestrictionTestCase(unittest.TestCase):

    def testAddInstrument(self):
        """Verify the pipeline instrument is added to the query."""
        self.assertEqual(
            GraphBuilder._verifyInstrumentRestriction("HSC", "tract = 42"),
            "instrument = 'HSC' AND (tract = 42)")

    def testQueryContainsInstrument(self):
        """Verify the instrument is found and no further action is taken."""
        self.assertEqual(
            GraphBuilder._verifyInstrumentRestriction("HSC", "'HSC' = instrument AND tract = 42"),
            "'HSC' = instrument AND tract = 42")

    def testQueryContainsInstrumentAltOrder(self):
        """Verify instrument is found in a different order, with no further
        action."""
        self.assertEqual(
            GraphBuilder._verifyInstrumentRestriction("HSC", "tract=42 AND instrument='HSC'"),
            "tract=42 AND instrument='HSC'")

    def testQueryContainsSimilarKey(self):
        """Verify a key that contains "instrument" is not confused for the
        actual "instrument" key."""
        self.assertEqual(
            GraphBuilder._verifyInstrumentRestriction("HSC", "notinstrument=42 AND instrument='HSC'"),
            "notinstrument=42 AND instrument='HSC'")

    def testNoPipelineInstrument(self):
        """Verify that when no pipeline instrument is passed that the query is
        returned unchanged."""
        self.assertEqual(
            GraphBuilder._verifyInstrumentRestriction(None, "foo=bar"),
            "foo=bar")

    def testNoPipelineInstrumentTwoQueryInstruments(self):
        """Verify that when no pipeline instrument is passed that the query can
        contain two instruments."""
        self.assertEqual(
            GraphBuilder._verifyInstrumentRestriction(None, "instrument = 'HSC' OR instrument = 'LSSTCam'"),
            "instrument = 'HSC' OR instrument = 'LSSTCam'")

    def testTwoQueryInstruments(self):
        """Verify that when a pipeline instrument is passed and the query
        contains two instruments that a RuntimeError is raised."""
        with self.assertRaises(RuntimeError):
            GraphBuilder._verifyInstrumentRestriction("HSC", "instrument = 'HSC' OR instrument = 'LSSTCam'")

    def testNoQuery(self):
        """Test adding the instrument query to an empty query."""
        self.assertEqual(GraphBuilder._verifyInstrumentRestriction("HSC", ""), "instrument = 'HSC'")

    def testNoQueryNoInstruments(self):
        """Test the verify function when there is no instrument and no
        query."""
        self.assertEqual(GraphBuilder._verifyInstrumentRestriction("", ""), "")


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
