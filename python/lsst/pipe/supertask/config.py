#
# LSST Data Management System
# Copyright 2018 AURA/LSST.
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
"""
Module defining config classes for SuperTask.
"""

from __future__ import absolute_import, division, print_function

__all__ = ["QuantumConfig", "InputDatasetConfig", "OutputDatasetConfig",
           "InitInputDatasetConfig", "InitOutputDatasetConfig",
           "SuperTaskConfig"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# -----------------------------
#  Imports for other modules --
# -----------------------------
import lsst.pex.config as pexConfig

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------


class QuantumConfig(pexConfig.Config):
    """Configuration class which defines SuperTask quanta units.

    In addition to a list of dataUnit names this also includes optional list of
    SQL statements to be executed against Registry database. Exact meaning and
    format of SQL will be determined at later point.
    """
    units = pexConfig.ListField(dtype=str,
                                doc="list of DataUnits which define quantum")
    sql = pexConfig.ListField(dtype=str,
                              doc="sequence of SQL statements",
                              optional=True)


class _BaseDatasetTypeConfig(pexConfig.Config):
    """Intermediate base class for dataset type configuration in SuperTask.
    """
    name = pexConfig.Field(dtype=str,
                           doc="name of the DatasetType")
    storageClass = pexConfig.Field(dtype=str,
                                   doc="name of the StorageClass")


class _DatasetTypeConfig(_BaseDatasetTypeConfig):
    """Configuration class which defines dataset type used by SuperTask.

    Consists of DatasetType name, list of DataUnit names and StorageCass name.
    SuperTasks typically define one or more input and output datasets. This
    class should not be used directly, instead one of `InputDatasetConfig` or
    `OutputDatasetConfig` should be used in SuperTask config.
    """
    units = pexConfig.ListField(dtype=str,
                                doc="list of DataUnits for this DatasetType")


class InputDatasetConfig(_DatasetTypeConfig):
    pass


class OutputDatasetConfig(_DatasetTypeConfig):
    pass


class _GlobalDatasetTypeConfig(_BaseDatasetTypeConfig):
    """Configuration class which defines dataset types used in SuperTask
    initialization.

    Consists of DatasetType name and StorageCass name, with a read-only
    ``units`` property that returns an empty tuple, enforcing the constraint
    that datasets used in initialization are not associated with any
    DataUnits. This class should not be used directly, instead one of
    `InitInputDatasetConfig` or `InitOutputDatasetConfig` should be used in
    SuperTask config.
    """
    @property
    def units(self):
        """DataUnits associated with this DatasetType (always empty)."""
        return ()


class InitInputDatasetConfig(_GlobalDatasetTypeConfig):
    pass


class InitOutputDatasetConfig(_GlobalDatasetTypeConfig):
    pass


class SuperTaskConfig(pexConfig.Config):
    """Base class for all SuperTask configurations.

    This class defines fields that must be defined for every SuperTask.
    It will be used as a base class for all SuperTask configurations instead
    of `pex.config.Config`.
    """
    quantum = pexConfig.ConfigField(dtype=QuantumConfig,
                                    doc="configuration for SuperTask quantum")
