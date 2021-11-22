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

from collections import UserDict
from collections.abc import Sequence
from deprecated.sphinx import deprecated
import json


def _isListLike(v):
    return isinstance(v, Sequence) and not isinstance(v, str)


class TaskMetadata(UserDict):
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

    def add(self, name, value):
        """
        This method exists for backward compatibility with
        `lsst.daf.base.PropertySet` and `lsst.daf.base.PropertyList`.
         It should not be used.

        Parameters
        ----------
        name : `str`
            Name of the metadata property
        value
            Metadata property value

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

    @deprecated(reason="Dictionary methods should be used. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def getAsDouble(self, key):
        return float(self.__getitem__(key))

    @deprecated(reason="Dictionary methods should be used. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def getScalar(self, key):
        v = self.__getitem__(key)
        if _isListLike(v):
            return v[-1]
        else:
            return v

    @deprecated(reason="Dictionary methods should be used. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def getArray(self, key):
        v = self.__getitem__(key)
        if _isListLike(v):
            return v
        else:
            return [v]

    @deprecated(reason="Dictionary methods should be used. Will be removed after v23.",
                version="v23", category=FutureWarning)
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
            return self.data.keys()
        else:
            names = set()
            for k, v in self.data.items():
                if isinstance(v, TaskMetadata):
                    names.update({k+'.'+item for item in v.keys()})
                else:
                    names.add(k)
            return names

    @deprecated(reason="Dictionary methods should be used. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def paramNames(self, topLevelOnly):
        return self.names(topLevelOnly)

    @deprecated(reason="Dictionary methods should be used. Will be removed after v23.",
                version="v23", category=FutureWarning)
    def set(self, key, item):
        self.__setitem__(key, item)

    @deprecated(reason="Dictionary methods should be used. Will be removed after v23.",
                version="v23", category=FutureWarning)
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

    def __getitem__(self, key):
        keys = self._getKeys(key)
        if len(keys) == 1:
            return super().__getitem__(key)
        if keys[0] in self.data.keys():
            val = self.data[keys[0]]
            if isinstance(val, TaskMetadata) and keys[1] in val:
                return val[keys[1]]
        raise KeyError(f"'{key}' not found in TaskMetadata")

    def __setitem__(self, key, item):
        # make sure list-like items are stored as lists
        if _isListLike(item) and not isinstance(item, TaskMetadata):
            item = list(item)
        keys = self._getKeys(key)
        if len(keys) == 1:
            super().__setitem__(key, item)
        else:
            if keys[0] not in self.data.keys():
                self.data[keys[0]] = TaskMetadata({})
            elif not isinstance(self.data[keys[0]], TaskMetadata):
                raise KeyError((f"Key '{key}' can not be set. '{keys[0]}'"
                                f" exists and is not TaskMetadata"))
            self.data[keys[0]][keys[1]] = item

    def __contains__(self, key):
        keys = self._getKeys(key)
        if len(keys) == 1:
            return super().__contains__(key)
        else:
            return \
                keys[0] in self.data.keys() and \
                isinstance(self.data[keys[0]], TaskMetadata) and \
                keys[1] in self.data[keys[0]]

    def __delitem__(self, key):
        keys = self._getKeys(key)
        if len(keys) == 1:
            super().__delitem__(key)
        if keys[0] in self.data.keys() and \
                isinstance(self.data[keys[0]], TaskMetadata) and \
                keys[1] in self.data[keys[0]]:
            del self.data[keys[0]][keys[1]]

    def __repr__(self):
        # use repr() to format dictionary data
        return f"{type(self).__name__}({self.data!r})"

    def json(self) -> str:
        """Serialize the task metadata to JSON.

        Returns
        -------
        json_str : `str`
            The contents of the task metadata in JSON string format.
        """
        return json.dumps(self.data)
