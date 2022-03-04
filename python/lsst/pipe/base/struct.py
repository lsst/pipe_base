#
# LSST Data Management System
# Copyright 2008, 2009, 2010, 2011 LSST Corporation.
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

from __future__ import annotations

__all__ = ["Struct"]

from typing import Any, Dict


class Struct:
    """A container to which you can add fields as attributes.

    Parameters
    ----------
    keyArgs
        keyword arguments specifying fields and their values.

    Notes
    -----
    Intended to be used for the return value from `~lsst.pipe.base.Task.run`
    and other `~lsst.pipe.base.Task` methods, and useful for any method that
    returns multiple values.

    The intent is to allow accessing returned items by name, instead of
    unpacking a tuple.  This makes the code much more robust and easier to
    read. It allows one to change what values are returned without inducing
    mysterious failures: adding items is completely safe, and removing or
    renaming items causes errors that are caught quickly and reported in a way
    that is easy to understand.

    The primary reason for using Struct instead of dict is that the fields may
    be accessed as attributes, e.g. ``aStruct.foo`` instead of
    ``aDict["foo"]``. Admittedly this only saves a few characters, but it
    makes the code significantly more readable.

    Struct is preferred over named tuples, because named tuples can be used as
    ordinary tuples, thus losing all the safety advantages of Struct. In
    addition, named tuples are clumsy to define and Structs are much more
    mutable (e.g. one can trivially combine Structs and add additional fields).

    Examples
    --------
    >>> myStruct = Struct(
    >>>     strVal = 'the value of the field named "strVal"',
    >>>     intVal = 35,
    >>> )

    """

    def __init__(self, **keyArgs: Any):
        for name, val in keyArgs.items():
            self.__safeAdd(name, val)

    def __safeAdd(self, name: str, val: Any) -> None:
        """Add a field if it does not already exist and name does not start
        with ``__`` (two underscores).

        Parameters
        ----------
        name : `str`
            Name of field to add.
        val : object
            Value of field to add.

        Raises
        ------
        RuntimeError
            Raised if name already exists or starts with ``__`` (two
            underscores).
        """
        if hasattr(self, name):
            raise RuntimeError(f"Item {name!r} already exists")
        if name.startswith("__"):
            raise RuntimeError(f"Item name {name!r} invalid; must not begin with __")
        setattr(self, name, val)

    def getDict(self) -> Dict[str, Any]:
        """Get a dictionary of fields in this struct.

        Returns
        -------
        structDict : `dict`
            Dictionary with field names as keys and field values as values.
            The values are shallow copies.
        """
        return self.__dict__.copy()

    def mergeItems(self, struct: Struct, *nameList: str) -> None:
        """Copy specified fields from another struct, provided they don't
        already exist.

        Parameters
        ----------
        struct : `Struct`
            `Struct` from which to copy.
        *nameList : `str`
            All remaining arguments are names of items to copy.

        Raises
        ------
        RuntimeError
            Raised if any item in nameList already exists in self (but any
            items before the conflicting item in nameList will have been
            copied).

        Examples
        --------
        For example::

            foo.copyItems(other, "itemName1", "itemName2")

        copies ``other.itemName1`` and ``other.itemName2`` into self.
        """
        for name in nameList:
            self.__safeAdd(name, getattr(struct, name))

    def copy(self) -> Struct:
        """Make a one-level-deep copy (values are not copied).

        Returns
        -------
        copy : `Struct`
            One-level-deep copy of this Struct.
        """
        return Struct(**self.getDict())

    def __eq__(self, other: Any) -> bool:
        return self.__dict__ == other.__dict__

    def __len__(self) -> int:
        return len(self.__dict__)

    def __repr__(self) -> str:
        itemsStr = "; ".join(f"{name}={val}" for name, val in self.getDict().items())
        return f"{self.__class__.__name__}({itemsStr})"
