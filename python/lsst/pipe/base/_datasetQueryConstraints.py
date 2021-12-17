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

"""Module Defining variants for valid values used to constrain datasets in a
graph building query.
"""

__all__ = ("DatasetQueryConstraintVariant",)

import warnings
from typing import Iterable, Iterator, Protocol, Type


class DatasetQueryConstraintVariant(Iterable, Protocol):
    """This class is the base for all the valid variants for controling
    constraining graph building queries based on dataset type existence.

    ALL variant corresponds to using all input dataset types to constrain
    a query.

    OFF variant corrresponds to not using any dataset types to constrain a
    graph building query.

    LIST variant should be used when one or more specific names should be used
    in constraining a graph building query.

    Normally the ALL and OFF variants are used as as Singletons, attempting to
    instantiate them (i.e. ALL()) will return in singleton class itself.

    LIST is used as a constructor to the contents (i.e. List(['a', 'b'])).
    Using the LIST variant directly as a singleton will behave the same as if
    it were an empty instance.

    Variants can be directly used, or automatically be selected by using the
    `fromExpression` class method given a valid string.
    """

    ALL: "Type[_ALL]"
    OFF: "Type[_OFF]"
    LIST: "Type[_LIST]"

    @classmethod
    def __subclasshook__(cls, subclass):
        if subclass == cls.ALL or subclass == cls.OFF or subclass == cls.LIST:
            return True
        return False

    @classmethod
    def fromExpression(cls, expression: str) -> "DatasetQueryConstraintVariant":
        """Select and return the correct Variant that corresponds to the input
        expression.

        Valid values are `all` for all inputs dataset types in pipeline, `off`
        to not consider dataset type existence as a constraint, single or comma
        seperated list of dataset type names.
        """
        if not isinstance(expression, str):
            raise ValueError("Expression must be a string")
        elif expression == "all":
            return cls.ALL
        elif expression == "off":
            return cls.OFF
        else:
            if " " in expression:
                warnings.warn("Witespace found in expression will be trimmed", RuntimeWarning)
                expression = expression.replace(" ", "")
            members = expression.split(",")
            return cls.LIST(members)


class _ALLMETA(DatasetQueryConstraintVariant, type(Protocol)):
    def __iter__(self) -> Iterator:
        raise NotImplementedError("This variant cannot be iteratted")


class _ALL(metaclass=_ALLMETA):
    def __new__(cls):
        return cls


class _OFFMETA(DatasetQueryConstraintVariant, type(Protocol)):
    def __iter__(self) -> Iterator:
        raise NotImplementedError("This variant cannot be iteratted")


class _OFF(metaclass=_OFFMETA):
    def __new__(cls):
        return cls


class _LISTMETA(type(Protocol)):
    def __iter__(self):
        return iter(tuple())

    def __len__(self):
        return 0

    def __eq__(self, o: object) -> bool:
        if isinstance(o, self):
            return True
        return super().__eq__(o)


class _LIST(DatasetQueryConstraintVariant, metaclass=_LISTMETA):
    def __init__(self, members: Iterable[str]):
        self.members = list(members)

    def __len__(self) -> int:
        return len(self.members)

    def __iter__(self) -> Iterable[str]:
        return iter(self.members)

    def __repr__(self) -> str:
        return repr(self.members)

    def __str__(self) -> str:
        return str(self.members)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, type(self)):
            return self.members == o.members
        return super().__eq__(o)


def suppressInit(self):
    raise NotImplementedError(
        "DatasetQueryConstraintVariants cannot be directly instantiated. "
        "Please use the variants or the fromExpression class method"
    )


DatasetQueryConstraintVariant.__init__ = suppressInit
DatasetQueryConstraintVariant.ALL = _ALL
DatasetQueryConstraintVariant.OFF = _OFF
DatasetQueryConstraintVariant.LIST = _LIST
