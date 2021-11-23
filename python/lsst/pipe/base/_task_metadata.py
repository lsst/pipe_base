# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = ["TaskMetadata"]

from collections.abc import Sequence
from deprecated.sphinx import deprecated

from typing import Dict, List, Union
from pydantic import BaseModel, StrictInt, StrictFloat, StrictBool, StrictStr

_DEPRECATION_REASON = "Dictionary methods should be used. Will be removed after v25."
_DEPRECATION_VERSION = "v24"


def _isListLike(v):
    return isinstance(v, Sequence) and not isinstance(v, str)


class TaskMetadata(BaseModel):
    """Dict-like object for storing task metadata.
    Metadata can be stored at two levels: single task or task plus subtasks.
    The later is called full metadata of a task and has a form

        topLevelTaskName:subtaskName:subsubtaskName.itemName

    Metadata item key of a task (`itemName` above) must not contain `.`,
    which serves as a separator in full metadata keys and turns
    the value into sub-dictionary. `KeyError` is raised if more than one
    separator is detected in a key.

    Deprecated methods are for compatibility with
    the predecessor containers.
    """

    # Metadata is limited -- float must come before int
    __root__: Dict[str, Union["TaskMetadata", StrictFloat, StrictInt, StrictBool, StrictStr,
                              List[StrictFloat], List[StrictInt], List[StrictBool], List[StrictStr]]] = {}

    def add(self, name, value):
        """Store a new value, adding to a list if one already exists.

        Parameters
        ----------
        name : `str`
            Name of the metadata property.
        value
            Metadata property value.
        """
        if self.__contains__(name):
            v = self.__getitem__(name)
            if isinstance(v, TaskMetadata):
                # can not add to TaskMetadata
                raise KeyError(f"Invalid metadata key '{name}' for add.")
            if not isinstance(v, list):
                v = [v]
            if _isListLike(value):
                v.extend(value)
            else:
                v.append(value)
        else:
            v = value
        self.__setitem__(name, v)

    @deprecated(reason=_DEPRECATION_REASON,
                version=_DEPRECATION_VERSION, category=FutureWarning)
    def getAsDouble(self, key):
        return float(self.__getitem__(key))

    def getScalar(self, key):
        """Retrieve a scalar item even if the item is a list.

        Parameters
        ----------
        key : `str`
            Item to retrieve.

        Returns
        -------
        value : Any
            Either the value associated with the key or, if the key
            corresponds to a list, the last item in the list.
        """
        v = self.__getitem__(key)
        if _isListLike(v):
            return v[-1]
        else:
            return v

    def getArray(self, key):
        """Retrieve an item as a list even if it is a scalar.

        Parameters
        ----------
        key : `str`
            Item to retrieve.

        Returns
        -------
        values : `list` of any
            A list containing the value or values associated with this item.
        """
        v = self.__getitem__(key)
        if _isListLike(v):
            return v
        else:
            return [v]

    @deprecated(reason=_DEPRECATION_REASON,
                version=_DEPRECATION_VERSION, category=FutureWarning)
    def names(self, topLevelOnly: bool = True):
        """
        This method exists for backward compatibility with
        `lsst.daf.base.PropertySet`.

        Parameters
        ----------
        topLevelOnly  : `bool`
            If true, return top-level keys, otherwise full metadata item keys

        Returns
        -------
        names : `collection.abc.Set`
            A set of top-level keys or full metadata item keys

        """
        if topLevelOnly:
            return set(self.__root__.keys())
        else:
            names = set()
            for k, v in self.__root__.items():
                if isinstance(v, TaskMetadata):
                    names.update({k+'.'+item for item in v.keys()})
                else:
                    names.add(k)
            return names

    @deprecated(reason=_DEPRECATION_REASON,
                version=_DEPRECATION_VERSION, category=FutureWarning)
    def paramNames(self, topLevelOnly):
        return self.names(topLevelOnly)

    @deprecated(reason=_DEPRECATION_REASON,
                version=_DEPRECATION_VERSION, category=FutureWarning)
    def set(self, key, item):
        self.__setitem__(key, item)

    @deprecated(reason=_DEPRECATION_REASON,
                version=_DEPRECATION_VERSION, category=FutureWarning)
    def remove(self, key):
        self.__delitem__(key)

    @staticmethod
    def _getKeys(key):
        if not isinstance(key, str):
            raise KeyError(f"Invalid key '{key}': only string keys are allowed")
        keys = key.split('.')
        if len(keys) > 2:
            raise KeyError(f"Invalid key '{key}': '.' is only allowed as"
                           " a separator between task and metadata item key.")
        return keys

    def keys(self):
        return self.__root__.keys()

    def __len__(self):
        return len(self.__root__)

    def __iter__(self):
        return iter(self.__root__)

    def __getitem__(self, key):
        keys = self._getKeys(key)
        if len(keys) == 1:
            return self.__root__[key]
        if keys[0] in self.__root__.keys():
            val = self.__root__[keys[0]]
            if isinstance(val, TaskMetadata) and keys[1] in val:
                return val[keys[1]]
        raise KeyError(f"'{key}' not found in TaskMetadata")

    def __setitem__(self, key, item):
        # make sure list-like items are stored as lists
        if _isListLike(item) and not isinstance(item, TaskMetadata):
            item = list(item)
        keys = self._getKeys(key)
        if len(keys) == 1:
            self.__root__[key] = item
        else:
            if keys[0] not in self.__root__.keys():
                self.__root__[keys[0]] = TaskMetadata()
            elif not isinstance(self.__root__[keys[0]], TaskMetadata):
                raise KeyError((f"Key '{key}' can not be set. '{keys[0]}'"
                                f" exists and is not TaskMetadata"))
            self.__root__[keys[0]][keys[1]] = item

    def __contains__(self, key):
        keys = self._getKeys(key)
        if len(keys) == 1:
            return key in self.__root__
        else:
            return \
                keys[0] in self.__root__.keys() and \
                isinstance(self.__root__[keys[0]], TaskMetadata) and \
                keys[1] in self.__root__[keys[0]]

    def __delitem__(self, key):
        keys = self._getKeys(key)
        if len(keys) == 1:
            del self.__root__[key]
        if keys[0] in self.__root__.keys() and \
                isinstance(self.__root__[keys[0]], TaskMetadata) and \
                keys[1] in self.__root__[keys[0]]:
            del self.__root__[keys[0]][keys[1]]

    def __repr__(self):
        # use repr() to format dictionary data
        return f"{type(self).__name__}({self.__root__!r})"


# Needed because a TaskMetadata can contain a TaskMetadata.
TaskMetadata.update_forward_refs()
