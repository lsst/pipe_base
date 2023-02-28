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
import numbers
import warnings
from collections.abc import Sequence
from typing import Any, Collection, Dict, Iterator, List, Mapping, Optional, Protocol, Set, Tuple, Union

from deprecated.sphinx import deprecated
from pydantic import BaseModel, Field, StrictBool, StrictFloat, StrictInt, StrictStr

_DEPRECATION_REASON = "Will be removed after v25."
_DEPRECATION_VERSION = "v24"

# The types allowed in a Task metadata field are restricted
# to allow predictable serialization.
_ALLOWED_PRIMITIVE_TYPES = (str, float, int, bool)


class PropertySetLike(Protocol):
    """Protocol that looks like a ``lsst.daf.base.PropertySet``

    Enough of the API is specified to support conversion of a
    ``PropertySet`` to a `TaskMetadata`.
    """

    def paramNames(self, topLevelOnly: bool = True) -> Collection[str]:
        ...

    def getArray(self, name: str) -> Any:
        ...


def _isListLike(v: Any) -> bool:
    return isinstance(v, Sequence) and not isinstance(v, str)


class TaskMetadata(BaseModel):
    """Dict-like object for storing task metadata.

    Metadata can be stored at two levels: single task or task plus subtasks.
    The later is called full metadata of a task and has a form

        topLevelTaskName:subtaskName:subsubtaskName.itemName

    Metadata item key of a task (`itemName` above) must not contain `.`,
    which serves as a separator in full metadata keys and turns
    the value into sub-dictionary. Arbitrary hierarchies are supported.

    Deprecated methods are for compatibility with
    the predecessor containers.
    """

    scalars: Dict[str, Union[StrictFloat, StrictInt, StrictBool, StrictStr]] = Field(default_factory=dict)
    arrays: Dict[str, Union[List[StrictFloat], List[StrictInt], List[StrictBool], List[StrictStr]]] = Field(
        default_factory=dict
    )
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

    @classmethod
    def from_metadata(cls, ps: PropertySetLike) -> "TaskMetadata":
        """Create a TaskMetadata from a PropertySet-like object.

        Parameters
        ----------
        ps : `PropertySetLike` or `TaskMetadata`
            A ``PropertySet``-like object to be transformed to a
            `TaskMetadata`. A `TaskMetadata` can be copied using this
            class method.

        Returns
        -------
        tm : `TaskMetadata`
            Newly-constructed metadata.

        Notes
        -----
        Items stored in single-element arrays in the supplied object
        will be converted to scalars in the newly-created object.
        """
        # Use hierarchical names to assign values from input to output.
        # This API exists for both PropertySet and TaskMetadata.
        # from_dict() does not work because PropertySet is not declared
        # to be a Mapping.
        # PropertySet.toDict() is not present in TaskMetadata so is best
        # avoided.
        metadata = cls()
        for key in sorted(ps.paramNames(topLevelOnly=False)):
            value = ps.getArray(key)
            if len(value) == 1:
                value = value[0]
            metadata[key] = value
        return metadata

    def to_dict(self) -> Dict[str, Any]:
        """Convert the class to a simple dictionary.

        Returns
        -------
        d : `dict`
            Simple dictionary that can contain scalar values, array values
            or other dictionary values.

        Notes
        -----
        Unlike `dict()`, this method hides the model layout and combines
        scalars, arrays, and other metadata in the same dictionary. Can be
        used when a simple dictionary is needed.  Use
        `TaskMetadata.from_dict()` to convert it back.
        """
        d: Dict[str, Any] = {}
        d.update(self.scalars)
        d.update(self.arrays)
        for k, v in self.metadata.items():
            d[k] = v.to_dict()
        return d

    def add(self, name: str, value: Any) -> None:
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
            slot_type, value = self._validate_value(value)
            if slot_type == "array":
                pass
            elif slot_type == "scalar":
                value = [value]
            else:
                raise ValueError("add() can only be used for primitive types or sequences of those types.")

            if key0 in self.metadata:
                raise ValueError(f"Can not add() to key '{name}' since that is a TaskMetadata")

            if key0 in self.scalars:
                # Convert scalar to array.
                # MyPy should be able to figure out that List[Union[T1, T2]] is
                # compatible with  Union[List[T1], List[T2]] if the list has
                # only one element, but it can't.
                self.arrays[key0] = [self.scalars.pop(key0)]  # type: ignore

            if key0 in self.arrays:
                # Check that the type is not changing.
                if (curtype := type(self.arrays[key0][0])) is not (newtype := type(value[0])):
                    raise ValueError(f"Type mismatch in add() -- currently {curtype} but adding {newtype}")
                self.arrays[key0].extend(value)
            else:
                self.arrays[key0] = value

            return

        self.metadata[key0].add(".".join(keys), value)

    @deprecated(
        reason="Cast the return value to float explicitly. " + _DEPRECATION_REASON,
        version=_DEPRECATION_VERSION,
        category=FutureWarning,
    )
    def getAsDouble(self, key: str) -> float:
        """Return the value cast to a `float`.

        Parameters
        ----------
        key : `str`
            Item to return. Can be dot-separated hierarchical.

        Returns
        -------
        value : `float`
            The value cast to a `float`.

        Raises
        ------
        KeyError
            Raised if the item is not found.
        """
        return float(self.__getitem__(key))

    def getScalar(self, key: str) -> Union[str, int, float, bool]:
        """Retrieve a scalar item even if the item is a list.

        Parameters
        ----------
        key : `str`
            Item to retrieve.

        Returns
        -------
        value : `str`, `int`, `float`, or `bool`
            Either the value associated with the key or, if the key
            corresponds to a list, the last item in the list.

        Raises
        ------
        KeyError
            Raised if the item is not found.
        """
        # Used in pipe_tasks.
        # getScalar() is the default behavior for __getitem__.
        return self[key]

    def getArray(self, key: str) -> List[Any]:
        """Retrieve an item as a list even if it is a scalar.

        Parameters
        ----------
        key : `str`
            Item to retrieve.

        Returns
        -------
        values : `list` of any
            A list containing the value or values associated with this item.

        Raises
        ------
        KeyError
            Raised if the item is not found.
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

        try:
            return self.metadata[key0].getArray(".".join(keys))
        except KeyError:
            # Report the correct key.
            raise KeyError(f"'{key}' not found") from None

    def names(self, topLevelOnly: bool = True) -> Set[str]:
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
                    names.update({k + "." + item for item in v.names(topLevelOnly=topLevelOnly)})
            return names

    def paramNames(self, topLevelOnly: bool) -> Set[str]:
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

    @deprecated(
        reason="Use standard assignment syntax. " + _DEPRECATION_REASON,
        version=_DEPRECATION_VERSION,
        category=FutureWarning,
    )
    def set(self, key: str, item: Any) -> None:
        """Set the value of the supplied key."""
        self.__setitem__(key, item)

    @deprecated(
        reason="Use standard del dict syntax. " + _DEPRECATION_REASON,
        version=_DEPRECATION_VERSION,
        category=FutureWarning,
    )
    def remove(self, key: str) -> None:
        """Remove the item without raising if absent."""
        try:
            self.__delitem__(key)
        except KeyError:
            # The PropertySet.remove() should always work.
            pass

    @staticmethod
    def _getKeys(key: str) -> List[str]:
        """Return the key hierarchy.

        Parameters
        ----------
        key : `str`
            The key to analyze. Can be dot-separated.

        Returns
        -------
        keys : `list` of `str`
            The key hierarchy that has been split on ``.``.

        Raises
        ------
        KeyError
            Raised if the key is not a string.
        """
        try:
            keys = key.split(".")
        except Exception:
            raise KeyError(f"Invalid key '{key}': only string keys are allowed") from None
        return keys

    def keys(self) -> Tuple[str, ...]:
        """Return the top-level keys."""
        return tuple(k for k in self)

    def items(self) -> Iterator[Tuple[str, Any]]:
        """Yield the top-level keys and values."""
        for k, v in itertools.chain(self.scalars.items(), self.arrays.items(), self.metadata.items()):
            yield (k, v)

    def __len__(self) -> int:
        """Return the number of items."""
        return len(self.scalars) + len(self.arrays) + len(self.metadata)

    # This is actually a Liskov substitution violation, because
    # pydantic.BaseModel says __iter__ should return something else.  But the
    # pydantic docs say to do exactly this to in order to make a mapping-like
    # BaseModel, so that's what we do.
    def __iter__(self) -> Iterator[str]:  # type: ignore
        """Return an iterator over each key."""
        # The order of keys is not preserved since items can move
        # from scalar to array.
        return itertools.chain(iter(self.scalars), iter(self.arrays), iter(self.metadata))

    def __getitem__(self, key: str) -> Any:
        """Retrieve the item associated with the key.

        Parameters
        ----------
        key : `str`
            The key to retrieve. Can be dot-separated hierarchical.

        Returns
        -------
        value : `TaskMetadata`, `float`, `int`, `bool`, `str`
            A scalar value. For compatibility with ``PropertySet``, if the key
             refers to an array, the final element is returned and not the
             array itself.

        Raises
        ------
        KeyError
            Raised if the item is not found.
        """
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
        # property. Trap KeyError and reraise so that the correct key
        # in the hierarchy is reported.
        try:
            # And forward request to that metadata.
            return self.metadata[key0][".".join(keys)]
        except KeyError:
            raise KeyError(f"'{key}' not found") from None

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve the item associated with the key or a default.

        Parameters
        ----------
        key : `str`
            The key to retrieve. Can be dot-separated hierarchical.
        default
            The value to return if the key doesnot exist.

        Returns
        -------
        value : `TaskMetadata`, `float`, `int`, `bool`, `str`
            A scalar value.  If the key refers to an array, the final element
            is returned and not the array itself; this is consistent with
            `__getitem__` and `PropertySet.get`, but not ``to_dict().get``.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __setitem__(self, key: str, item: Any) -> None:
        """Store the given item."""
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            slots: Dict[str, Dict[str, Any]] = {
                "array": self.arrays,
                "scalar": self.scalars,
                "metadata": self.metadata,
            }
            primary: Optional[Dict[str, Any]] = None
            slot_type, item = self._validate_value(item)
            primary = slots.pop(slot_type, None)
            if primary is None:
                raise AssertionError(f"Unknown slot type returned from validator: {slot_type}")

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

    def __contains__(self, key: str) -> bool:
        """Determine if the key exists."""
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            return key0 in self.scalars or key0 in self.arrays or key0 in self.metadata

        if key0 in self.metadata:
            return ".".join(keys) in self.metadata[key0]
        return False

    def __delitem__(self, key: str) -> None:
        """Remove the specified item.

        Raises
        ------
        KeyError
            Raised if the item is not present.
        """
        keys = self._getKeys(key)
        key0 = keys.pop(0)
        if len(keys) == 0:
            # MyPy can't figure out that this way to combine the types in the
            # tuple is the one that matters, and annotating a local variable
            # helps it out.
            properties: Tuple[Dict[str, Any], ...] = (self.scalars, self.arrays, self.metadata)
            for property in properties:
                if key0 in property:
                    del property[key0]
                    return
            raise KeyError(f"'{key}' not found'")

        try:
            del self.metadata[key0][".".join(keys)]
        except KeyError:
            # Report the correct key.
            raise KeyError(f"'{key}' not found'") from None

    def _validate_value(self, value: Any) -> Tuple[str, Any]:
        """Validate the given value.

        Parameters
        ----------
        value : Any
            Value to check.

        Returns
        -------
        slot_type : `str`
            The type of value given. Options are "scalar", "array", "metadata".
        item : Any
            The item that was given but possibly modified to conform to
            the slot type.

        Raises
        ------
        ValueError
            Raised if the value is not a recognized type.
        """
        # Test the simplest option first.
        value_type = type(value)
        if value_type in _ALLOWED_PRIMITIVE_TYPES:
            return "scalar", value

        if isinstance(value, TaskMetadata):
            return "metadata", value
        if isinstance(value, Mapping):
            return "metadata", self.from_dict(value)

        if _isListLike(value):
            # For model consistency, need to check that every item in the
            # list has the same type.
            value = list(value)

            type0 = type(value[0])
            for i in value:
                if type(i) != type0:
                    raise ValueError(
                        "Type mismatch in supplied list. TaskMetadata requires all"
                        f" elements have same type but see {type(i)} and {type0}."
                    )

            if type0 not in _ALLOWED_PRIMITIVE_TYPES:
                # Must check to see if we got numpy floats or something.
                type_cast: type
                if isinstance(value[0], numbers.Integral):
                    type_cast = int
                elif isinstance(value[0], numbers.Real):
                    type_cast = float
                else:
                    raise ValueError(
                        f"Supplied list has element of type '{type0}'. "
                        "TaskMetadata can only accept primitive types in lists."
                    )

                value = [type_cast(v) for v in value]

            return "array", value

        # Sometimes a numpy number is given.
        if isinstance(value, numbers.Integral):
            value = int(value)
            return "scalar", value
        if isinstance(value, numbers.Real):
            value = float(value)
            return "scalar", value

        raise ValueError(f"TaskMetadata does not support values of type {value!r}.")


# Needed because a TaskMetadata can contain a TaskMetadata.
TaskMetadata.update_forward_refs()
