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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Tests of the Instrument class."""

import datetime
import math
import unittest

from lsst.daf.butler import DataCoordinate, DimensionPacker, DimensionUniverse, RegistryConfig
from lsst.daf.butler.formatters.json import JsonFormatter
from lsst.daf.butler.registry.sql_registry import SqlRegistry
from lsst.pex.config import Config
from lsst.pipe.base import Instrument
from lsst.utils.introspection import get_full_type_name


class BaseDummyInstrument(Instrument):
    """Test instrument base class."""

    @classmethod
    def getName(cls):
        return "DummyInstrument"

    def register(self, registry, update=False):
        detector_max = 2
        visit_max = 10
        exposure_max = 8
        record = {
            "instrument": self.getName(),
            "class_name": get_full_type_name(DummyInstrument),
            "detector_max": detector_max,
            "visit_max": visit_max,
            "exposure_max": exposure_max,
        }
        with registry.transaction():
            registry.syncDimensionData("instrument", record, update=update)

    def getRawFormatter(self, dataId):
        return JsonFormatter


class DummyInstrument(BaseDummyInstrument):
    """Test instrument."""


class NotInstrument:
    """Not an instrument class at all."""

    def __init__(self, collection_prefix: str = ""):
        self.collection_prefix = collection_prefix


class BadInstrument(DummyInstrument):
    """Instrument with wrong class name."""

    raw_definition = ("raw2", ("instrument", "detector", "exposure"), "StructuredDataDict")

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


class DimensionPackerTestConfig(Config):
    """Configuration for the dimension packer."""

    packer = Instrument.make_dimension_packer_config_field()


class InstrumentTestCase(unittest.TestCase):
    """Test for Instrument."""

    def setUp(self):
        self.instrument = DummyInstrument()
        self.name = "DummyInstrument"

    def test_basics(self):
        self.assertEqual(self.instrument.getName(), self.name)
        self.assertEqual(self.instrument.getRawFormatter({}), JsonFormatter)
        self.assertIsNone(DummyInstrument.raw_definition)
        raw = BadInstrument.raw_definition
        self.assertEqual(raw[2], "StructuredDataDict")

    def test_register(self):
        """Test that register() sets appropriate Dimensions."""
        registryConfig = RegistryConfig()
        registryConfig["db"] = "sqlite://"
        registry = SqlRegistry.createFromConfig(registryConfig)
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

        # Check that from_string falls back to fromName.
        from_string = DummyInstrument.from_string("DummyInstrument", registry)
        self.assertIsInstance(from_string, type(from_registry))

    def test_from_string(self):
        """Test Instrument.from_string works."""
        with self.assertRaises(RuntimeError):
            # No registry.
            DummyInstrument.from_string("DummyInstrument")

        with self.assertRaises(TypeError):
            # This will raise because collection_prefix is not understood.
            DummyInstrument.from_string("lsst.pipe.base.Task")

        with self.assertRaises(TypeError):
            # Not an instrument but does have collection_prefix.
            DummyInstrument.from_string(get_full_type_name(NotInstrument))

        with self.assertRaises(TypeError):
            # This will raise because BaseDummyInstrument is not a subclass
            # of DummyInstrument.
            DummyInstrument.from_string(get_full_type_name(BaseDummyInstrument))

        instrument = DummyInstrument.from_string(
            get_full_type_name(DummyInstrument), collection_prefix="test"
        )
        self.assertEqual(instrument.collection_prefix, "test")

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
        self.assertEqual(datetimeThen1.tzinfo, datetime.UTC)

        with self.assertRaises(TypeError):
            Instrument.formatCollectionTimestamp(0)

    def test_dimension_packer_config_defaults(self):
        """Test the ObservationDimensionPacker class and the Instrument-based
        systems for constructing it, in the case where the Instrument-defined
        default is used.
        """
        registry_config = RegistryConfig()
        registry_config["db"] = "sqlite://"
        registry = SqlRegistry.createFromConfig(registry_config)
        self.instrument.register(registry)
        config = DimensionPackerTestConfig()
        instrument_data_id = registry.expandDataId(instrument=self.name)
        record = instrument_data_id.records["instrument"]
        self.check_dimension_packers(
            registry.dimensions,
            # Test just one packer-construction signature here as that saves us
            # from having to insert dimension records for visits, exposures,
            # and detectors for this test.
            # Note that we don't need to pass any more than the instrument in
            # the data ID yet, because we're just constructing packers, not
            # calling their pack method.
            visit_packers=[
                config.packer.apply(instrument_data_id, is_exposure=False),
                Instrument.make_default_dimension_packer(instrument_data_id, is_exposure=False),
            ],
            exposure_packers=[
                config.packer.apply(instrument_data_id, is_exposure=True),
                Instrument.make_default_dimension_packer(instrument_data_id, is_exposure=True),
            ],
            n_detectors=record.detector_max,
            n_visits=record.visit_max,
            n_exposures=record.exposure_max,
        )

    def test_dimension_packer_config_override(self):
        """Test the ObservationDimensionPacker class and the Instrument-based
        systems for constructing it, in the case where configuration overrides
        the Instrument's default.
        """
        registry_config = RegistryConfig()
        registry_config["db"] = "sqlite://"
        registry = SqlRegistry.createFromConfig(registry_config)
        # Intentionally do not register instrument or insert any other
        # dimension records to ensure we don't need them in this mode.
        config = DimensionPackerTestConfig()
        config.packer["observation"].n_observations = 16
        config.packer["observation"].n_detectors = 4
        config.packer.name = "observation"
        instrument_data_id = DataCoordinate.standardize(instrument=self.name, universe=registry.dimensions)
        full_data_id = DataCoordinate.standardize(instrument_data_id, detector=1, visit=3, exposure=7)
        visit_data_id = full_data_id.subset(full_data_id.universe.conform(["visit", "detector"]))
        exposure_data_id = full_data_id.subset(full_data_id.universe.conform(["exposure", "detector"]))
        self.check_dimension_packers(
            registry.dimensions,
            # Note that we don't need to pass any more than the instrument in
            # the data ID yet, because we're just constructing packers, not
            # calling their pack method.
            visit_packers=[
                config.packer.apply(visit_data_id),
                config.packer.apply(full_data_id),
                config.packer.apply(instrument_data_id, is_exposure=False),
            ],
            exposure_packers=[
                config.packer.apply(instrument_data_id, is_exposure=True),
                config.packer.apply(exposure_data_id),
            ],
            n_detectors=config.packer["observation"].n_detectors,
            n_visits=config.packer["observation"].n_observations,
            n_exposures=config.packer["observation"].n_observations,
        )

    def check_dimension_packers(
        self,
        universe: DimensionUniverse,
        visit_packers: list[DimensionPacker],
        exposure_packers: list[DimensionPacker],
        n_detectors: int,
        n_visits: int,
        n_exposures: int,
    ) -> None:
        """Test the behavior of one or more dimension packers constructed by
        an instrument.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Data model for butler data IDs.
        visit_packers : `list` [ `DimensionPacker` ]
            Packers with ``{visit, detector}`` dimensions to test.
        exposure_packers : `list` [ `DimensionPacker` ]
            Packers with ``{exposure, detector}`` dimensions to test.
        n_detectors : `int`
            Number of detectors all packers have been configured to reserve
            space for.
        n_visits : `int`
            Number of visits all packers in ``visit_packers`` have been
            configured to reserve space for.
        n_exposures : `int`
            Number of exposures all packers in ``exposure_packers`` have been
            configured to reserve space for.
        """
        full_data_id = DataCoordinate.standardize(
            instrument=self.name, detector=1, visit=3, exposure=7, universe=universe
        )
        visit_data_id = full_data_id.subset(full_data_id.universe.conform(["visit", "detector"]))
        exposure_data_id = full_data_id.subset(full_data_id.universe.conform(["exposure", "detector"]))
        for n, packer in enumerate(visit_packers):
            with self.subTest(n=n):
                packed_id, max_bits = packer.pack(full_data_id, returnMaxBits=True)
                self.assertEqual(packed_id, full_data_id["detector"] + full_data_id["visit"] * n_detectors)
                self.assertEqual(max_bits, math.ceil(math.log2(n_detectors * n_visits)))
                self.assertEqual(visit_data_id, packer.unpack(packed_id))
                with self.assertRaisesRegex(ValueError, f"Detector ID {n_detectors} is out of bounds"):
                    packer.pack(instrument=self.name, detector=n_detectors, visit=3)
                with self.assertRaisesRegex(ValueError, f"Visit ID {n_visits} is out of bounds"):
                    packer.pack(instrument=self.name, detector=1, visit=n_visits)
        for n, packer in enumerate(exposure_packers):
            with self.subTest(n=n):
                packed_id, max_bits = packer.pack(full_data_id, returnMaxBits=True)
                self.assertEqual(packed_id, full_data_id["detector"] + full_data_id["exposure"] * n_detectors)
                self.assertEqual(max_bits, math.ceil(math.log2(n_detectors * n_exposures)))
                self.assertEqual(exposure_data_id, packer.unpack(packed_id))
                with self.assertRaisesRegex(ValueError, f"Detector ID {n_detectors} is out of bounds"):
                    packer.pack(instrument=self.name, detector=n_detectors, exposure=3)
                with self.assertRaisesRegex(ValueError, f"Exposure ID {n_exposures} is out of bounds"):
                    packer.pack(instrument=self.name, detector=1, exposure=n_exposures)


if __name__ == "__main__":
    unittest.main()
