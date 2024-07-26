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

from __future__ import annotations

__all__ = ("PexConfigFormatter",)

from typing import Any, BinaryIO

from lsst.daf.butler import FormatterV2
from lsst.pex.config import Config
from lsst.resources import ResourceHandleProtocol, ResourcePath


class PexConfigFormatter(FormatterV2):
    """Formatter implementation for reading and writing
    `lsst.pex.config.Config` instances.
    """

    default_extension = ".py"
    can_read_from_stream = True

    def read_from_stream(
        self, stream: BinaryIO | ResourceHandleProtocol, component: str | None = None, expected_size: int = -1
    ) -> Any:
        # Automatically determine the Config class from the serialized form
        return Config._fromPython(stream.read().decode())

    def write_local_file(self, in_memory_dataset: Any, uri: ResourcePath) -> None:
        in_memory_dataset.save(uri.ospath)
