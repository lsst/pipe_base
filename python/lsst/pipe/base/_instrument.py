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

from __future__ import annotations

__all__ = ("Instrument",)

import datetime
import os.path
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Optional, Sequence, Type, Union

from lsst.daf.butler import DataId, Formatter
from lsst.daf.butler.registry import DataIdError
from lsst.utils import doImportType

if TYPE_CHECKING:
    from lsst.daf.butler import Registry
    from lsst.pex.config import Config


class Instrument(metaclass=ABCMeta):
    """Base class for instrument-specific logic for the Gen3 Butler.

    Parameters
    ----------
    collection_prefix : `str`, optional
        Prefix for collection names to use instead of the intrument's own name.
        This is primarily for use in simulated-data repositories, where the
        instrument name may not be necessary and/or sufficient to distinguish
        between collections.

    Notes
    -----
    Concrete instrument subclasses must have the same construction signature as
    the base class.
    """

    configPaths: Sequence[str] = ()
    """Paths to config files to read for specific Tasks.

    The paths in this list should contain files of the form `task.py`, for
    each of the Tasks that requires special configuration.
    """

    policyName: Optional[str] = None
    """Instrument specific name to use when locating a policy or configuration
    file in the file system."""

    def __init__(self, collection_prefix: Optional[str] = None):
        if collection_prefix is None:
            collection_prefix = self.getName()
        self.collection_prefix = collection_prefix

    @classmethod
    @abstractmethod
    def getName(cls) -> str:
        """Return the short (dimension) name for this instrument.

        This is not (in general) the same as the class name - it's what is used
        as the value of the "instrument" field in data IDs, and is usually an
        abbreviation of the full name.
        """
        raise NotImplementedError()

    @abstractmethod
    def register(self, registry: Registry, *, update: bool = False) -> None:
        """Insert instrument, and other relevant records into `Registry`.

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Registry client for the data repository to modify.
        update : `bool`, optional
            If `True` (`False` is default), update existing records if they
            differ from the new ones.

        Raises
        ------
        lsst.daf.butler.registry.ConflictingDefinitionError
            Raised if any existing record has the same key but a different
            definition as one being registered.

        Notes
        -----
        New records can always be added by calling this method multiple times,
        as long as no existing records have changed (if existing records have
        changed, ``update=True`` must be used).  Old records can never be
        removed by this method.

        Implementations should guarantee that registration is atomic (the
        registry should not be modified if any error occurs) and idempotent at
        the level of individual dimension entries; new detectors and filters
        should be added, but changes to any existing record should not be.
        This can generally be achieved via a block like

        .. code-block:: python

            with registry.transaction():
                registry.syncDimensionData("instrument", ...)
                registry.syncDimensionData("detector", ...)
                self.registerFilters(registry)

        """
        raise NotImplementedError()

    @staticmethod
    def fromName(name: str, registry: Registry, collection_prefix: Optional[str] = None) -> Instrument:
        """Given an instrument name and a butler registry, retrieve a
        corresponding instantiated instrument object.

        Parameters
        ----------
        name : `str`
            Name of the instrument (must match the return value of `getName`).
        registry : `lsst.daf.butler.Registry`
            Butler registry to query to find the information.
        collection_prefix : `str`, optional
            Prefix for collection names to use instead of the intrument's own
            name.  This is primarily for use in simulated-data repositories,
            where the instrument name may not be necessary and/or sufficient to
            distinguish between collections.

        Returns
        -------
        instrument : `Instrument`
            An instance of the relevant `Instrument`.

        Notes
        -----
        The instrument must be registered in the corresponding butler.

        Raises
        ------
        LookupError
            Raised if the instrument is not known to the supplied registry.
        ModuleNotFoundError
            Raised if the class could not be imported.  This could mean
            that the relevant obs package has not been setup.
        TypeError
            Raised if the class name retrieved is not a string or the imported
            symbol is not an `Instrument` subclass.
        """
        try:
            records = list(registry.queryDimensionRecords("instrument", instrument=name))
        except DataIdError:
            records = None
        if not records:
            raise LookupError(f"No registered instrument with name '{name}'.")
        cls_name = records[0].class_name
        if not isinstance(cls_name, str):
            raise TypeError(
                f"Unexpected class name retrieved from {name} instrument dimension (got {cls_name})"
            )
        instrument_cls: type = doImportType(cls_name)
        if not issubclass(instrument_cls, Instrument):
            raise TypeError(
                f"{instrument_cls!r}, obtained from importing {cls_name}, is not an Instrument subclass."
            )
        return instrument_cls(collection_prefix=collection_prefix)

    @staticmethod
    def from_string(
        name: str, registry: Optional[Registry] = None, collection_prefix: Optional[str] = None
    ) -> Instrument:
        """Return an instance from the short name or class name.

        If the instrument name is not qualified (does not contain a '.') and a
        butler registry is provided, this will attempt to load the instrument
        using `Instrument.fromName()`. Otherwise the instrument will be
        imported and instantiated.

        Parameters
        ----------
        name : `str`
            The name or fully-qualified class name of an instrument.
        registry : `lsst.daf.butler.Registry`, optional
            Butler registry to query to find information about the instrument,
            by default `None`.
        collection_prefix : `str`, optional
            Prefix for collection names to use instead of the intrument's own
            name. This is primarily for use in simulated-data repositories,
            where the instrument name may not be necessary and/or sufficient
            to distinguish between collections.

        Returns
        -------
        instrument : `Instrument`
            The instantiated instrument.

        Raises
        ------
        RuntimeError
            Raised if the instrument can not be imported, instantiated, or
            obtained from the registry.
        TypeError
            Raised if the instrument is not a subclass of
            `~lsst.pipe.base.Instrument`.

        See Also
        --------
        Instrument.fromName
        """
        if "." not in name and registry is not None:
            try:
                instr = Instrument.fromName(name, registry, collection_prefix=collection_prefix)
            except Exception as err:
                raise RuntimeError(
                    f"Could not get instrument from name: {name}. Failed with exception: {err}"
                ) from err
        else:
            try:
                instr_class = doImportType(name)
            except Exception as err:
                raise RuntimeError(
                    f"Could not import instrument: {name}. Failed with exception: {err}"
                ) from err
            instr = instr_class(collection_prefix=collection_prefix)
        if not isinstance(instr, Instrument):
            raise TypeError(f"{name} is not an Instrument subclass.")
        return instr

    @staticmethod
    def importAll(registry: Registry) -> None:
        """Import all the instruments known to this registry.

        This will ensure that all metadata translators have been registered.

        Parameters
        ----------
        registry : `lsst.daf.butler.Registry`
            Butler registry to query to find the information.

        Notes
        -----
        It is allowed for a particular instrument class to fail on import.
        This might simply indicate that a particular obs package has
        not been setup.
        """
        records = list(registry.queryDimensionRecords("instrument"))
        for record in records:
            cls = record.class_name
            try:
                doImportType(cls)
            except Exception:
                pass

    @abstractmethod
    def getRawFormatter(self, dataId: DataId) -> Type[Formatter]:
        """Return the Formatter class that should be used to read a particular
        raw file.

        Parameters
        ----------
        dataId : `DataId`
            Dimension-based ID for the raw file or files being ingested.

        Returns
        -------
        formatter : `lsst.daf.butler.Formatter` class
            Class to be used that reads the file into the correct
            Python object for the raw data.
        """
        raise NotImplementedError()

    def applyConfigOverrides(self, name: str, config: Config) -> None:
        """Apply instrument-specific overrides for a task config.

        Parameters
        ----------
        name : `str`
            Name of the object being configured; typically the _DefaultName
            of a Task.
        config : `lsst.pex.config.Config`
            Config instance to which overrides should be applied.
        """
        for root in self.configPaths:
            path = os.path.join(root, f"{name}.py")
            if os.path.exists(path):
                config.load(path)

    @staticmethod
    def formatCollectionTimestamp(timestamp: Union[str, datetime.datetime]) -> str:
        """Format a timestamp for use in a collection name.

        Parameters
        ----------
        timestamp : `str` or `datetime.datetime`
            Timestamp to format.  May be a date or datetime string in extended
            ISO format (assumed UTC), with or without a timezone specifier, a
            datetime string in basic ISO format with a timezone specifier, a
            naive `datetime.datetime` instance (assumed UTC) or a
            timezone-aware `datetime.datetime` instance (converted to UTC).
            This is intended to cover all forms that string ``CALIBDATE``
            metadata values have taken in the past, as well as the format this
            method itself writes out (to enable round-tripping).

        Returns
        -------
        formatted : `str`
            Standardized string form for the timestamp.
        """
        if isinstance(timestamp, str):
            if "-" in timestamp:
                # extended ISO format, with - and : delimiters
                timestamp = datetime.datetime.fromisoformat(timestamp)
            else:
                # basic ISO format, with no delimiters (what this method
                # returns)
                timestamp = datetime.datetime.strptime(timestamp, "%Y%m%dT%H%M%S%z")
        if not isinstance(timestamp, datetime.datetime):
            raise TypeError(f"Unexpected date/time object: {timestamp!r}.")
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(datetime.timezone.utc)
        return f"{timestamp:%Y%m%dT%H%M%S}Z"

    @staticmethod
    def makeCollectionTimestamp() -> str:
        """Create a timestamp string for use in a collection name from the
        current time.

        Returns
        -------
        formatted : `str`
            Standardized string form of the current time.
        """
        return Instrument.formatCollectionTimestamp(datetime.datetime.now(tz=datetime.timezone.utc))

    def makeDefaultRawIngestRunName(self) -> str:
        """Make the default instrument-specific run collection string for raw
        data ingest.

        Returns
        -------
        coll : `str`
            Run collection name to be used as the default for ingestion of
            raws.
        """
        return self.makeCollectionName("raw", "all")

    def makeUnboundedCalibrationRunName(self, *labels: str) -> str:
        """Make a RUN collection name appropriate for inserting calibration
        datasets whose validity ranges are unbounded.

        Parameters
        ----------
        *labels : `str`
            Extra strings to be included in the base name, using the default
            delimiter for collection names.  Usually this is the name of the
            ticket on which the calibration collection is being created.

        Returns
        -------
        name : `str`
            Run collection name.
        """
        return self.makeCollectionName("calib", *labels, "unbounded")

    def makeCuratedCalibrationRunName(self, calibDate: str, *labels: str) -> str:
        """Make a RUN collection name appropriate for inserting curated
        calibration datasets with the given ``CALIBDATE`` metadata value.

        Parameters
        ----------
        calibDate : `str`
            The ``CALIBDATE`` metadata value.
        *labels : `str`
            Strings to be included in the collection name (before
            ``calibDate``, but after all other terms), using the default
            delimiter for collection names.  Usually this is the name of the
            ticket on which the calibration collection is being created.

        Returns
        -------
        name : `str`
            Run collection name.
        """
        return self.makeCollectionName("calib", *labels, "curated", self.formatCollectionTimestamp(calibDate))

    def makeCalibrationCollectionName(self, *labels: str) -> str:
        """Make a CALIBRATION collection name appropriate for associating
        calibration datasets with validity ranges.

        Parameters
        ----------
        *labels : `str`
            Strings to be appended to the base name, using the default
            delimiter for collection names.  Usually this is the name of the
            ticket on which the calibration collection is being created.

        Returns
        -------
        name : `str`
            Calibration collection name.
        """
        return self.makeCollectionName("calib", *labels)

    @staticmethod
    def makeRefCatCollectionName(*labels: str) -> str:
        """Return a global (not instrument-specific) name for a collection that
        holds reference catalogs.

        With no arguments, this returns the name of the collection that holds
        all reference catalogs (usually a ``CHAINED`` collection, at least in
        long-lived repos that may contain more than one reference catalog).

        Parameters
        ----------
        *labels : `str`
            Strings to be added to the global collection name, in order to
            define a collection name for one or more reference catalogs being
            ingested at the same time.

        Returns
        -------
        name : `str`
            Collection name.

        Notes
        -----
        This is a ``staticmethod``, not a ``classmethod``, because it should
        be the same for all instruments.
        """
        return "/".join(("refcats",) + labels)

    def makeUmbrellaCollectionName(self) -> str:
        """Return the name of the umbrella ``CHAINED`` collection for this
        instrument that combines all standard recommended input collections.

        This method should almost never be overridden by derived classes.

        Returns
        -------
        name : `str`
            Name for the umbrella collection.
        """
        return self.makeCollectionName("defaults")

    def makeCollectionName(self, *labels: str) -> str:
        """Get the instrument-specific collection string to use as derived
        from the supplied labels.

        Parameters
        ----------
        *labels : `str`
            Strings to be combined with the instrument name to form a
            collection name.

        Returns
        -------
        name : `str`
            Collection name to use that includes the instrument's recommended
            prefix.
        """
        return "/".join((self.collection_prefix,) + labels)
