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

import itertools
import warnings
from collections.abc import Sequence
from deprecated.sphinx import deprecated

from typing import Dict, List, Union, Any, Mapping
from pydantic import BaseModel, StrictInt, StrictFloat, StrictBool, StrictStr, Field

_DEPRECATION_REASON = "Will be removed after v25."
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

    scalars: Dict[str, Union[StrictFloat, StrictInt, StrictBool, StrictStr]] = Field(default_factory=dict)
    arrays: Dict[str, Union[List[StrictFloat], List[StrictInt], List[StrictBool],
                            List[StrictStr]]] = Field(default_factory=dict)
    metadata: Dict[str, "TaskMetadata"] = Field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "TaskMetadata":
        """Create a TaskMetadata from a dictionary.

        Parameters
        ----------
        d : `Mapping`
            Mapping to convert. Can be hierarchical. Any dictionaries
            in the hierarchy are converted to `TaskMetadata`.

        Returns
        -------
        meta : `TaskMetadata`
            Newly-constructed metadata.
        """
        metadata = cls()
        for k, v in d.items():
            metadata[k] = v
        return metadata

    def add(self, name, value):
        """Store a new value, adding to a list if one already exists.

        Parameters
        ----------
        name : `str`
            Name of the metadata property.
        value
            Metadata property value.
        """
        keys = self._getKeys(name)
        key0 = keys.pop(0)
        if len(keys) == 0:

            # If add() is being used, always store the value in the arrays
            # property as a list. It's likely there will be another call.
            if not _isListLike(value):
                value = [value]
            else:
                # Force deep copy. No validation.
                value = list(value)

            if key0 in self.metadata:
                raise ValueError(f"Can not add() to key '{name}' since that is a TaskMetadata")

            if key0 in self.scalars:
                # Convert scalar to array.
                self.arrays[key0] = [self.scalars.pop(key0)]

            if key0 in self.arrays:
                self.arrays[key0].extend(value)
            else:
                self.arrays[key0] = value

            return

        self.metadata[key0].add(".".join(keys), value)

    @deprecated(reason="Cast the return value to float explicitly. " + _DEPRECATION_REASON,
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
        # Used in pipe_tasks
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            if key0 in self.arrays:
                return self.arrays[key0][-1]
            elif key0 in self.scalars:
                return self.scalars[key0]
            elif key0 in self.metadata:
                return self.metadata[key0]
            raise KeyError(f"'{key}' not found")

        return self.metadata[key0].getScalar(".".join(keys))

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
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            if key0 in self.arrays:
                return self.arrays[key0]
            elif key0 in self.scalars:
                return [self.scalars[key0]]
            elif key0 in self.metadata:
                return [self.metadata[key0]]
            raise KeyError(f"'{key}' not found")

        return self.metadata[key0].getArray(".".join(keys))

    def names(self, topLevelOnly: bool = True):
        """Return the hierarchical keys from the metadata.

        Parameters
        ----------
        topLevelOnly  : `bool`
            If true, return top-level keys, otherwise full metadata item keys.

        Returns
        -------
        names : `collection.abc.Set`
            A set of top-level keys or full metadata item keys, including
            the top-level keys.

        Notes
        -----
        Should never be called in new code with ``topLevelOnly`` set to `True`
        -- this is equivalent to asking for the keys and is the default
        when iterating through the task metadata. In this case a deprecation
        message will be issued and the ability will raise an exception
        in a future release.

        When ``topLevelOnly`` is `False` all keys, including those from the
        hierarchy and the top-level hierarchy, are returned.
        """
        if topLevelOnly:
            warnings.warn("Use keys() instead. " + _DEPRECATION_REASON, FutureWarning)
            return set(self.keys())
        else:
            names = set()
            for k, v in self.items():
                names.add(k)  # Always include the current level
                if isinstance(v, TaskMetadata):
                    names.update({k + '.' + item for item in v.names(topLevelOnly=topLevelOnly)})
            return names

    def paramNames(self, topLevelOnly):
        """Return hierarchical names.

        Parameters
        ----------
        topLevelOnly : `bool`
            Control whether only top-level items are returned or items
            from the hierarchy.

        Returns
        -------
        paramNames : `set` of `str`
            If ``topLevelOnly`` is `True`, returns any keys that are not
            part of a hierarchy. If `False` also returns fully-qualified
            names from the hierarchy. Keys associated with the top
            of a hierarchy are never returned.
        """
        # Currently used by the verify package.
        paramNames = set()
        for k, v in self.items():
            if isinstance(v, TaskMetadata):
                if not topLevelOnly:
                    paramNames.update({k + "." + item for item in v.paramNames(topLevelOnly=topLevelOnly)})
            else:
                paramNames.add(k)
        return paramNames

    @deprecated(reason="Use standard assignment syntax. " + _DEPRECATION_REASON,
                version=_DEPRECATION_VERSION, category=FutureWarning)
    def set(self, key, item):
        self.__setitem__(key, item)

    @deprecated(reason="Use standard del dict syntax. " + _DEPRECATION_REASON,
                version=_DEPRECATION_VERSION, category=FutureWarning)
    def remove(self, key):
        try:
            self.__delitem__(key)
        except KeyError:
            # The PropertySet.remove() should always work.
            pass

    @staticmethod
    def _getKeys(key):
        try:
            keys = key.split('.')
        except Exception:
            raise KeyError(f"Invalid key '{key}': only string keys are allowed") from None
        return keys

    def keys(self):
        return tuple(k for k in self)

    def items(self):
        for k, v in itertools.chain(self.scalars.items(), self.arrays.items(), self.metadata.items()):
            yield (k, v)

    def __len__(self):
        return len(self.scalars) + len(self.arrays) + len(self.metadata)

    def __iter__(self):
        # The order of keys is not preserved since items can move
        # from scalar to array.
        return itertools.chain(iter(self.scalars), iter(self.arrays), iter(self.metadata))

    def __getitem__(self, key):
        # For compatibility with PropertySet, if the key refers to
        # an array, the final element is returned and not the array itself.
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            if key0 in self.scalars:
                return self.scalars[key0]
            if key0 in self.metadata:
                return self.metadata[key0]
            if key0 in self.arrays:
                return self.arrays[key0][-1]
            raise KeyError(f"'{key}' not found")
        # Hierarchical lookup so the top key can only be in the metadata
        # property.
        if key0 in self.metadata:
            # And forward request to that metadata
            return self.metadata[key0][".".join(keys)]
        raise KeyError(f"'{key}' not found")

    def __setitem__(self, key, item):
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            # Currently no type validation.
            slots = {"arrays": self.arrays, "scalars": self.scalars, "metadata": self.metadata}
            primary = None
            if _isListLike(item):
                primary = slots.pop("arrays")
                # Force to a list copy for any sequence.
                item = list(item)
            elif isinstance(item, TaskMetadata):
                primary = slots.pop("metadata")
            elif isinstance(item, Mapping):
                primary = slots.pop("metadata")
                item = self.from_dict(item)
            else:
                primary = slots.pop("scalars")

            # Assign the value to the right place.
            primary[key0] = item
            for property in slots.values():
                # Remove any other entries.
                property.pop(key0, None)
            return

        # This must be hierarchical so forward to the child TaskMetadata.
        if key0 not in self.metadata:
            self.metadata[key0] = TaskMetadata()
        self.metadata[key0][".".join(keys)] = item

        # Ensure we have cleared out anything with the same name elsewhere.
        self.scalars.pop(key0, None)
        self.arrays.pop(key0, None)

    def __contains__(self, key):
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            return key0 in self.scalars or key0 in self.arrays or key0 in self.metadata

        if key0 in self.metadata:
            return ".".join(keys) in self.metadata[key0]
        return False

    def __delitem__(self, key):
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            for property in (self.scalars, self.arrays, self.metadata):
                if key0 in property:
                    del property[key0]
                    return
            raise KeyError(f"'{key} not found'")

        del self.metadata[key0][".".join(keys)]


# Needed because a TaskMetadata can contain a TaskMetadata.
TaskMetadata.update_forward_refs()
