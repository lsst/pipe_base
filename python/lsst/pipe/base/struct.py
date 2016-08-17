from __future__ import absolute_import, division
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
__all__ = ["Struct"]


class Struct(object):
    """!A struct to which you can add any fields

    Intended to be used for the return value from Task.run and other Task methods,
    and useful for any method that returns multiple values.

    The intent is to allow accessing returned items by name, instead of unpacking a tuple.
    This makes the code much more robust and easier to read. It allows one to change what values are returned
    without inducing mysterious failures: adding items is completely safe, and removing or renaming items
    causes errors that are caught quickly and reported in a way that is easy to understand.

    The primary reason for using Struct instead of dict is that the fields may be accessed as attributes,
    e.g. aStruct.foo instead of aDict["foo"]. Admittedly this only saves a few characters, but it makes
    the code significantly more readable.

    Struct is preferred over named tuples, because named tuples can be used as ordinary tuples, thus losing
    all the safety advantages of Struct. In addition, named tuples are clumsy to define and Structs
    are much more mutable (e.g. one can trivially combine Structs and add additional fields).
    """

    def __init__(self, **keyArgs):
        """!Create a Struct with the specified field names and values

        For example:
        @code
        myStruct = Struct(
            strVal = 'the value of the field named "strVal"',
            intVal = 35,
        )
        @endcode

        @param[in] **keyArgs    keyword arguments specifying name=value pairs
        """
        object.__init__(self)
        for name, val in keyArgs.iteritems():
            self.__safeAdd(name, val)

    def __safeAdd(self, name, val):
        """!Add a field if it does not already exist and name does not start with __ (two underscores)

        @param[in] name name of field to add
        @param[in] val  value of field to add

        @throw RuntimeError if name already exists or starts with __ (two underscores)
        """
        if hasattr(self, name):
            raise RuntimeError("Item %s already exists" % (name,))
        if name.startswith("__"):
            raise RuntimeError("Item name %r invalid; must not begin with __" % (name,))
        setattr(self, name, val)

    def getDict(self):
        """!Return a dictionary of attribute name: value

        @warning: the values are shallow copies.
        """
        return self.__dict__.copy()

    def mergeItems(self, struct, *nameList):
        """!Copy specified fields from another struct, provided they don't already exist

        @param[in] struct       struct from which to copy
        @param[in] *nameList    all remaining arguments are names of items to copy

        For example: foo.copyItems(other, "itemName1", "itemName2")
        copies other.itemName1 and other.itemName2 into self.

        @throw RuntimeError if any item in nameList already exists in self
            (but any items before the conflicting item in nameList will have been copied)
        """
        for name in nameList:
            self.__safeAdd(name, getattr(struct, name))

    def copy(self):
        """!Return a one-level-deep copy (values are not copied)
        """
        return Struct(**self.getDict())

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        itemList = ["%s=%r" % (name, val) for name, val in self.getDict().iteritems()]
        return "%s(%s)" % (self.__class__.__name__, "; ".join(itemList))
