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

import lsst.pex.config as pexConfig
import lsst.pex.exceptions as pexExceptions


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
        self._overrides += [('file', filename)]

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
        self._overrides += [('value', (field, value))]

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
            if otype == 'file':
                config.load(override)
            elif otype == 'value':
                field, value = override
                field = field.split('.')
                # find object with attribute to set, throws if we name is wrong
                obj = config
                for attr in field[:-1]:
                    obj = getattr(obj, attr)
                # If the type of the object to set is a list field, the value to assign
                # is most likely a list, and we will eval it to get a python list object
                # which will be used to set the objects value
                # This must be done before the try, as it will otherwise set a string which
                # is a valid iterable object when a list is the intended object
                if isinstance(getattr(obj, field[-1]), pexConfig.listField.List) and isinstance(value, str):
                    try:
                        value = eval(value, {})
                    except Exception:
                        # Something weird happened here, try passing, and seeing if further
                        # code can handle this
                        raise pexExceptions.RuntimeError(f"Unable to parse {value} into a valid list")
                try:
                    setattr(obj, field[-1], value)
                except TypeError:
                    if not isinstance(value, str):
                        raise
                    # this can throw
                    value = eval(value, {})
                    setattr(obj, field[-1], value)
