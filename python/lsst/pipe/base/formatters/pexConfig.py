# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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

__all__ = ("PexConfigFormatter",)

import os.path
from typing import Any, Optional, Type

from lsst.daf.butler.formatters.file import FileFormatter
from lsst.pex.config import Config


class PexConfigFormatter(FileFormatter):
    """Formatter implementation for reading and writing
    `lsst.pex.config.Config` instances."""

    extension = ".py"

    def _readFile(self, path: str, pytype: Optional[Type[Any]] = None) -> Any:
        """Read a pex.config.Config instance from the given file.

        Parameters
        ----------
        path : `str`
            Path to use to open the file.
        pytype : `type`, optional
            Class to use to read the config file.

        Returns
        -------
        data : `lsst.pex.config.Config`
            Instance of class ``pytype`` read from config file. `None`
            if the file could not be opened.
        """
        if not os.path.exists(path):
            return None

        # Automatically determine the Config class from the serialized form
        with open(path, "r") as fd:
            config_py = fd.read()
        return Config._fromPython(config_py)

    def _writeFile(self, inMemoryDataset: Any) -> None:
        """Write the in memory dataset to file on disk.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize.

        Raises
        ------
        Exception
            The file could not be written.
        """
        inMemoryDataset.save(self.fileDescriptor.location.path)
