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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Tests of the Instrument class.
"""

import datetime
import unittest

from lsst.daf.butler import Registry, RegistryConfig
from lsst.daf.butler.formatters.json import JsonFormatter
from lsst.pipe.base import Instrument
from lsst.utils.introspection import get_full_type_name


class DummyInstrument(Instrument):
    @classmethod
    def getName(cls):
        return "DummyInstrument"

    def register(self, registry, update=False):
        detector_max = 2
        record = {
            "instrument": self.getName(),
            "class_name": get_full_type_name(DummyInstrument),
            "detector_max": detector_max,
        }
        with registry.transaction():
            registry.syncDimensionData("instrument", record, update=update)

    def getRawFormatter(self, dataId):
        return JsonFormatter


class BadInstrument(DummyInstrument):
    """Instrument with wrong class name."""

    @classmethod
    def getName(cls):
        return "BadInstrument"

    def register(self, registry, update=False):
        # Register a bad class name
        record = {
            "instrument": self.getName(),
            "class_name": "builtins.str",
            "detector_max": 1,
        }
        registry.syncDimensionData("instrument", record, update=update)


class UnimportableInstrument(DummyInstrument):
    """Instrument with class name that does not exist."""

    @classmethod
    def getName(cls):
        return "NoImportInstr"

    def register(self, registry, update=False):
        # Register a bad class name
        record = {
            "instrument": self.getName(),
            "class_name": "not.importable",
            "detector_max": 1,
        }
        registry.syncDimensionData("instrument", record, update=update)


class InstrumentTestCase(unittest.TestCase):
    """Test for Instrument."""

    def setUp(self):
        self.instrument = DummyInstrument()
        self.name = "DummyInstrument"

    def test_basics(self):
        self.assertEqual(self.instrument.getName(), self.name)
        self.assertEqual(self.instrument.getRawFormatter({}), JsonFormatter)

    def test_register(self):
        """Test that register() sets appropriate Dimensions."""
        registryConfig = RegistryConfig()
        registryConfig["db"] = "sqlite://"
        registry = Registry.createFromConfig(registryConfig)
        # Check that the registry starts out empty.
        self.instrument.importAll(registry)
        self.assertFalse(list(registry.queryDimensionRecords("instrument")))

        # Register and check again.
        self.instrument.register(registry)
        instruments = list(registry.queryDimensionRecords("instrument"))
        self.assertEqual(len(instruments), 1)
        self.assertEqual(instruments[0].name, self.name)
        self.assertEqual(instruments[0].detector_max, 2)
        self.assertIn("DummyInstrument", instruments[0].class_name)

        self.instrument.importAll(registry)
        from_registry = DummyInstrument.fromName("DummyInstrument", registry)
        self.assertIsInstance(from_registry, Instrument)
        with self.assertRaises(LookupError):
            Instrument.fromName("NotThrere", registry)

        # Register a bad instrument.
        BadInstrument().register(registry)
        with self.assertRaises(TypeError):
            Instrument.fromName("BadInstrument", registry)

        UnimportableInstrument().register(registry)
        with self.assertRaises(ImportError):
            Instrument.fromName("NoImportInstr", registry)

        # This should work even with the bad class name.
        self.instrument.importAll(registry)

    def test_defaults(self):
        self.assertEqual(self.instrument.makeDefaultRawIngestRunName(), "DummyInstrument/raw/all")
        self.assertEqual(
            self.instrument.makeUnboundedCalibrationRunName("a", "b"), "DummyInstrument/calib/a/b/unbounded"
        )
        self.assertEqual(
            self.instrument.makeCuratedCalibrationRunName("2018-05-04", "a"),
            "DummyInstrument/calib/a/curated/20180504T000000Z",
        )
        self.assertEqual(self.instrument.makeCalibrationCollectionName("c"), "DummyInstrument/calib/c")
        self.assertEqual(self.instrument.makeRefCatCollectionName(), "refcats")
        self.assertEqual(self.instrument.makeRefCatCollectionName("a"), "refcats/a")
        self.assertEqual(self.instrument.makeUmbrellaCollectionName(), "DummyInstrument/defaults")

        instrument = DummyInstrument(collection_prefix="Different")
        self.assertEqual(instrument.makeCollectionName("a"), "Different/a")
        self.assertEqual(self.instrument.makeCollectionName("a"), "DummyInstrument/a")

    def test_collection_timestamps(self):
        self.assertEqual(
            Instrument.formatCollectionTimestamp("2018-05-03"),
            "20180503T000000Z",
        )
        self.assertEqual(
            Instrument.formatCollectionTimestamp("2018-05-03T14:32:16"),
            "20180503T143216Z",
        )
        self.assertEqual(
            Instrument.formatCollectionTimestamp("20180503T143216Z"),
            "20180503T143216Z",
        )
        self.assertEqual(
            Instrument.formatCollectionTimestamp(datetime.datetime(2018, 5, 3, 14, 32, 16)),
            "20180503T143216Z",
        )
        formattedNow = Instrument.makeCollectionTimestamp()
        self.assertIsInstance(formattedNow, str)
        datetimeThen1 = datetime.datetime.strptime(formattedNow, "%Y%m%dT%H%M%S%z")
        self.assertEqual(datetimeThen1.tzinfo, datetime.timezone.utc)

        with self.assertRaises(TypeError):
            Instrument.formatCollectionTimestamp(0)


if __name__ == "__main__":
    unittest.main()
