# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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

"""Constants used to define the connections automatically added for each
PipelineTask by the execution system.
"""

from __future__ import annotations

__all__ = (
    "CONFIG_INIT_OUTPUT_CONNECTION_NAME",
    "CONFIG_INIT_OUTPUT_STORAGE_CLASS",
    "CONFIG_INIT_OUTPUT_TEMPLATE",
    "LOG_OUTPUT_CONNECTION_NAME",
    "LOG_OUTPUT_STORAGE_CLASS",
    "LOG_OUTPUT_TEMPLATE",
    "METADATA_OUTPUT_CONNECTION_NAME",
    "METADATA_OUTPUT_STORAGE_CLASS",
    "METADATA_OUTPUT_TEMPLATE",
    "PACKAGES_INIT_OUTPUT_NAME",
    "PACKAGES_INIT_OUTPUT_STORAGE_CLASS",
)


CONFIG_INIT_OUTPUT_CONNECTION_NAME: str = "_config"
"""Internal task-side name for the config dataset connection.
"""

CONFIG_INIT_OUTPUT_TEMPLATE: str = "{label}" + CONFIG_INIT_OUTPUT_CONNECTION_NAME
"""String template used to form the name for config init-output dataset
type names.
"""

CONFIG_INIT_OUTPUT_STORAGE_CLASS: str = "Config"
"""Name of the storage class for config init-output datasets.
"""

PACKAGES_INIT_OUTPUT_NAME: str = "packages"
"""Name of the global init-output dataset type that holds software versions.
"""

PACKAGES_INIT_OUTPUT_STORAGE_CLASS: str = "Packages"
"""Name of the storage class for the dataset type that holds software versions.
"""

LOG_OUTPUT_CONNECTION_NAME: str = "_log"
"""Internal task-side name for the log dataset connection.
"""

LOG_OUTPUT_TEMPLATE: str = "{label}" + LOG_OUTPUT_CONNECTION_NAME
"""String template used to form the name for log output dataset type names.
"""

LOG_OUTPUT_STORAGE_CLASS: str = "ButlerLogRecords"
"""Name of the storage class for log output datasets.
"""

METADATA_OUTPUT_CONNECTION_NAME: str = "_metadata"
"""Internal task-side name for the metadata dataset connection.
"""

METADATA_OUTPUT_TEMPLATE: str = "{label}" + METADATA_OUTPUT_CONNECTION_NAME
"""String template used to form the name for task metadata output dataset
type names.
"""

METADATA_OUTPUT_STORAGE_CLASS: str = "TaskMetadata"
"""Name of the storage class for task metadata output datasets.
"""
