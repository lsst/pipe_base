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

from __future__ import annotations

__all__ = ("ObservationDimensionPacker", "ObservationDimensionPackerConfig", "observation_packer_registry")

from typing import Any, cast

from lsst.daf.butler import DataCoordinate, DimensionPacker
from lsst.pex.config import Config, Field, makeRegistry

observation_packer_registry = makeRegistry(
    "Configurables that can pack visit+detector or exposure+detector data IDs into integers."
)


class ObservationDimensionPackerConfig(Config):
    """Config associated with a `ObservationDimensionPacker`."""

    # Config fields are annotated as Any because support for better
    # annotations is broken on Fields with optional=True.
    n_detectors: Any = Field(
        "Number of detectors, or, more precisely, one greater than the "
        "maximum detector ID, for this instrument. "
        "Default (None) obtains this value from the instrument dimension record. "
        "This should rarely need to be overridden outside of tests.",
        dtype=int,
        default=None,
        optional=True,
    )
    n_observations: Any = Field(
        "Number of observations (visits or exposures, as per 'is_exposure`) "
        "expected, or, more precisely, one greater than the maximum "
        "visit/exposure ID. "
        "Default (None) obtains this value from the instrument dimension record. "
        "This should rarely need to be overridden outside of tests.",
        dtype=int,
        default=None,
        optional=True,
    )


class ObservationDimensionPacker(DimensionPacker):
    """A `DimensionPacker` for visit+detector or exposure+detector.

    Parameters
    ----------
    data_id : `lsst.daf.butler.DataCoordinate`
        Data ID that identifies at least the ``instrument`` dimension.  Must
        have dimension records attached unless ``config.n_detectors`` and
        ``config.n_visits`` are both not `None`.
    config : `ObservationDimensionPackerConfig`, optional
        Configuration for this dimension packer.
    is_exposure : `bool`, optional
        If `False`, construct a packer for visit+detector data IDs.  If `True`,
        construct a packer for exposure+detector data IDs.  If `None`,
        this is determined based on whether ``visit`` or ``exposure`` is
        present in ``data_id``, with ``visit`` checked first and hence used if
        both are present.

    Notes
    -----
    The standard pattern for constructing instances of the class is to use
    `Instrument.make_dimension_packer`; see that method for details.

    This packer assumes all visit/exposure and detector IDs are sequential or
    otherwise densely packed between zero and their upper bound, such that
    ``n_detectors`` * ``n_observations`` leaves plenty of bits remaining for
    any other IDs that need to be included in the same integer (such as a
    counter for Sources detected on an image with this data ID).  Instruments
    whose data ID values are not densely packed, should provide their own
    `~lsst.daf.butler.DimensionPacker` that takes advantage of the structure
    of its IDs to compress them into fewer bits.
    """

    ConfigClass = ObservationDimensionPackerConfig

    def __init__(
        self,
        data_id: DataCoordinate,
        config: ObservationDimensionPackerConfig | None = None,
        is_exposure: bool | None = None,
    ):
        if config is None:
            config = ObservationDimensionPackerConfig()
        fixed = data_id.subset(data_id.universe.conform(["instrument"]))
        if is_exposure is None:
            if "visit" in data_id.dimensions.names:
                is_exposure = False
            elif "exposure" in data_id.dimensions.names:
                is_exposure = True
            else:
                raise ValueError(
                    "'is_exposure' was not provided and 'data_id' has no visit or exposure value."
                )
        if is_exposure:
            dimensions = fixed.universe.conform(["instrument", "exposure", "detector"])
        else:
            dimensions = fixed.universe.conform(["instrument", "visit", "detector"])
        super().__init__(fixed, dimensions)
        self.is_exposure = is_exposure
        if config.n_detectors is not None:
            self._n_detectors = config.n_detectors
        else:
            # Records accessed here should never be None; that possibility is
            # only for non-dimension elements like join tables that are
            # are sometimes not present in an expanded data ID.
            self._n_detectors = fixed.records["instrument"].detector_max  # type: ignore[union-attr]
        if config.n_observations is not None:
            self._n_observations = config.n_observations
        elif self.is_exposure:
            self._n_observations = fixed.records["instrument"].exposure_max  # type: ignore[union-attr]
        else:
            self._n_observations = fixed.records["instrument"].visit_max  # type: ignore[union-attr]
        self._max_bits = (self._n_observations * self._n_detectors - 1).bit_length()

    @property
    def maxBits(self) -> int:
        # Docstring inherited from DimensionPacker.maxBits
        return self._max_bits

    def _pack(self, dataId: DataCoordinate) -> int:
        # Docstring inherited from DimensionPacker._pack
        detector_id = cast(int, dataId["detector"])
        if detector_id >= self._n_detectors:
            raise ValueError(f"Detector ID {detector_id} is out of bounds; expected <{self._n_detectors}.")
        observation_id = cast(int, dataId["exposure" if self.is_exposure else "visit"])
        if observation_id >= self._n_observations:
            raise ValueError(
                f"{'Exposure' if self.is_exposure else 'Visit'} ID {observation_id} is out of bounds; "
                f"expected <{self._n_observations}."
            )
        return detector_id + self._n_detectors * observation_id

    def unpack(self, packedId: int) -> DataCoordinate:
        # Docstring inherited from DimensionPacker.unpack
        observation, detector = divmod(packedId, self._n_detectors)
        return DataCoordinate.standardize(
            {
                "instrument": self.fixed["instrument"],
                "detector": detector,
                ("exposure" if self.is_exposure else "visit"): observation,
            },
            dimensions=self._dimensions,
        )


observation_packer_registry = makeRegistry(
    "Configurables that can pack visit+detector or exposure+detector data IDs into integers. "
    "Members of this registry should be callable with the same signature as "
    "`lsst.pipe.base.ObservationDimensionPacker` construction."
)
observation_packer_registry.register("observation", ObservationDimensionPacker)
