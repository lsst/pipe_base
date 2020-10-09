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

"""Module which defines ConfigOverrides class and related methods.
"""

__all__ = ["ConfigOverrides"]

import ast

import lsst.pex.exceptions as pexExceptions
from lsst.utils import doImport

from enum import Enum

OverrideTypes = Enum("OverrideTypes", "Value File Python Instrument")


class ConfigOverrides:
    """Defines a set of overrides to be applied to a task config.

    Overrides for task configuration need to be applied by activator when
    creating task instances. This class represents an ordered set of such
    overrides which activator receives from some source (e.g. command line
    or some other configuration).

    Methods
    ----------
    addFileOverride(filename)
        Add overrides from a specified file.
    addValueOverride(field, value)
        Add override for a specific field.
    applyTo(config)
        Apply all overrides to a `config` instance.

    Notes
    -----
    Serialization support for this class may be needed, will add later if
    necessary.
    """

    def __init__(self):
        self._overrides = []

    def addFileOverride(self, filename):
        """Add overrides from a specified file.

        Parameters
        ----------
        filename : str
            Path to the override file.
        """
        self._overrides.append((OverrideTypes.File, filename))

    def addValueOverride(self, field, value):
        """Add override for a specific field.

        This method is not very type-safe as it is designed to support
        use cases where input is given as string, e.g. command line
        activators. If `value` has a string type and setting of the field
        fails with `TypeError` the we'll attempt `eval()` the value and
        set the field with that value instead.

        Parameters
        ----------
        field : str
            Fully-qualified field name.
        value :
            Value to be given to a filed.
        """
        self._overrides.append((OverrideTypes.Value, (field, value)))

    def addPythonOverride(self, python_snippet: str):
        """Add Overrides by running a snippit of python code against a config.

        Parameters
        ----------
        python_snippet: str
            A string which is valid python code to be executed. This is done
            with config as the only local accessible value.
        """
        self._overrides.append((OverrideTypes.Python, python_snippet))

    def addInstrumentOverride(self, instrument: str, task_name: str):
        """Apply any overrides that an instrument has for a task

        Parameters
        ----------
        instrument: str
            A string containing the fully qualified name of an instrument from
            which configs should be loaded and applied
        task_name: str
            The _DefaultName of a task associated with a config, used to look
            up overrides from the instrument.
        """
        instrument_lib = doImport(instrument)()
        self._overrides.append((OverrideTypes.Instrument, (instrument_lib, task_name)))

    def applyTo(self, config):
        """Apply all overrides to a task configuration object.

        Parameters
        ----------
        config : `pex.Config`

        Raises
        ------
        `Exception` is raised if operations on configuration object fail.
        """
        for otype, override in self._overrides:
            if otype is OverrideTypes.File:
                config.load(override)
            elif otype is OverrideTypes.Value:
                field, value = override
                field = field.split('.')
                # find object with attribute to set, throws if we name is wrong
                obj = config
                for attr in field[:-1]:
                    obj = getattr(obj, attr)
                # If input is a string and field type is not a string then we
                # have to convert string to an expected type. Implementing
                # full string parser is non-trivial so we take a shortcut here
                # and `eval` the string and assign the resulting value to a
                # field. Type erroes can happen during both `eval` and field
                # assignment.
                if isinstance(value, str) and obj._fields[field[-1]].dtype is not str:
                    try:
                        # use safer ast.literal_eval, it only supports literals
                        value = ast.literal_eval(value)
                    except Exception:
                        # eval failed, wrap exception with more user-friendly
                        # message
                        raise pexExceptions.RuntimeError(f"Unable to parse `{value}' into a Python object")

                # this can throw in case of type mismatch
                setattr(obj, field[-1], value)
            elif otype is OverrideTypes.Python:
                exec(override, None, {"config": config})
            elif otype is OverrideTypes.Instrument:
                instrument, name = override
                instrument.applyConfigOverrides(name, config)
