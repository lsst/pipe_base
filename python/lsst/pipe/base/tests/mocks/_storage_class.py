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

__all__ = (
    "MockDataset",
    "MockStorageClass",
    "MockDatasetQuantum",
    "MockStorageClassDelegate",
    "get_mock_name",
    "get_original_name",
    "is_mock_name",
)

from collections.abc import Callable, Iterable, Mapping
from typing import Any, cast

from lsst.daf.butler import (
    DatasetComponent,
    Formatter,
    FormatterFactory,
    LookupKey,
    SerializedDataCoordinate,
    SerializedDatasetRef,
    SerializedDatasetType,
    StorageClass,
    StorageClassDelegate,
    StorageClassFactory,
)
from lsst.daf.butler._compat import _BaseModelCompat
from lsst.daf.butler.formatters.json import JsonFormatter
from lsst.utils.introspection import get_full_type_name

_NAME_PREFIX: str = "_mock_"


def get_mock_name(original: str) -> str:
    """Return the name of the mock storage class, dataset type, or task label
    for the given original name.
    """
    return _NAME_PREFIX + original


def get_original_name(mock: str) -> str:
    """Return the name of the original storage class, dataset type, or task
    label that corresponds to the given mock name.
    """
    assert mock.startswith(_NAME_PREFIX)
    return mock.removeprefix(_NAME_PREFIX)


def is_mock_name(name: str) -> bool:
    """Return whether the given name is that of a mock storage class, dataset
    type, or task label.
    """
    return name.startswith(_NAME_PREFIX)


# Tests for this module are in the ci_middleware package, where we have easy
# access to complex real storage classes (and their pytypes) to test against.


class MockDataset(_BaseModelCompat):
    """The in-memory dataset type used by `MockStorageClass`."""

    ref: SerializedDatasetRef
    """Reference used to read and write this dataset.

    This is a `~lsst.daf.butler.SerializedDatasetRef` instead of a "real" one
    for two reasons:

    - the mock dataset may need to be read from disk in a context in which a
      `~lsst.daf.butler.DimensionUniverse` is unavailable;
    - we don't want the complexity of having a separate
      ``SerializedMockDataset``.

    The downside of this is that we end up effectively reimplementing a few
    fairly trivial DatasetType/DatasetRef methods that override storage classes
    and extract components (in `MockStorageClass` and
    `MockStorageClassDelegate`).
    """

    quantum: MockDatasetQuantum | None = None
    """Description of the quantum that produced this dataset.
    """

    output_connection_name: str | None = None
    """The name of the PipelineTask output connection that produced this
    dataset.
    """

    converted_from: MockDataset | None = None
    """Another `MockDataset` that underwent a storage class conversion to
    produce this one.
    """

    parent: MockDataset | None = None
    """Another `MockDataset` from which a component was extract to form this
    one.
    """

    parameters: dict[str, str] | None = None
    """`repr` of all parameters applied when reading this dataset."""

    @property
    def dataset_type(self) -> SerializedDatasetType:
        return cast(SerializedDatasetType, self.ref.datasetType)

    @property
    def storage_class(self) -> str:
        return cast(str, self.dataset_type.storageClass)

    def make_derived(self, **kwargs: Any) -> MockDataset:
        """Return a new MockDataset that represents applying some storage class
        operation to this one.

        Keyword arguments are fields of `MockDataset` or
        `~lsst.daf.butler.SerializedDatasetType` to override in the result.
        """
        dataset_type_updates = {
            k: kwargs.pop(k) for k in list(kwargs) if k in SerializedDatasetType.model_fields  # type: ignore
        }
        derived_dataset_type = self.dataset_type.copy(update=dataset_type_updates)
        derived_ref = self.ref.copy(update=dict(datasetType=derived_dataset_type))
        # Fields below are those that should not be propagated to the derived
        # dataset, because they're not about the intrinsic on-disk thing.
        kwargs.setdefault("converted_from", None)
        kwargs.setdefault("parent", None)
        kwargs.setdefault("parameters", None)
        # Also use setdefault on the ref in case caller wants to override that
        # directly, but this is expected to be rare enough that it's not worth
        # it to try to optimize out the work above to make derived_ref.
        kwargs.setdefault("ref", derived_ref)
        return self.copy(update=kwargs)


class MockDatasetQuantum(_BaseModelCompat):
    """Description of the quantum that produced a mock dataset."""

    task_label: str
    """Label of the producing PipelineTask in its pipeline."""

    data_id: SerializedDataCoordinate
    """Data ID for the quantum."""

    inputs: dict[str, list[MockDataset]]
    """Mock datasets provided as input to the quantum."""


MockDataset.model_rebuild()


class MockStorageClassDelegate(StorageClassDelegate):
    """Implementation of the StorageClassDelegate interface for mock datasets.

    This class does not implement assembly and disassembly just because it's
    not needed right now.  That could be added in the future with some
    additional tracking attributes in `MockDataset`.
    """

    def assemble(self, components: dict[str, Any], pytype: type | None = None) -> MockDataset:
        # Docstring inherited.
        raise NotImplementedError("Mock storage classes do not implement assembly.")

    def getComponent(self, composite: Any, componentName: str) -> Any:
        # Docstring inherited.
        assert isinstance(
            composite, MockDataset
        ), f"MockStorageClassDelegate given a non-mock dataset {composite!r}."
        return composite.make_derived(
            name=f"{composite.dataset_type.name}.{componentName}",
            storageClass=self.storageClass.allComponents()[componentName].name,
            parentStorageClass=self.storageClass.name,
            parent=composite,
        )

    def disassemble(
        self, composite: Any, subset: Iterable | None = None, override: Any | None = None
    ) -> dict[str, DatasetComponent]:
        # Docstring inherited.
        raise NotImplementedError("Mock storage classes do not implement disassembly.")

    def handleParameters(self, inMemoryDataset: Any, parameters: Mapping[str, Any] | None = None) -> Any:
        # Docstring inherited.
        assert isinstance(
            inMemoryDataset, MockDataset
        ), f"MockStorageClassDelegate given a non-mock dataset {inMemoryDataset!r}."
        if not parameters:
            return inMemoryDataset
        return inMemoryDataset.make_derived(parameters={k: repr(v) for k, v in parameters.items()})


class MockStorageClass(StorageClass):
    """A reimplementation of `lsst.daf.butler.StorageClass` for mock datasets.

    Each `MockStorageClass` instance corresponds to a real "original" storage
    class, with components and conversions that are mocks of the original's
    components and conversions.  The `pytype` for all `MockStorageClass`
    instances is `MockDataset`.
    """

    def __init__(self, original: StorageClass, factory: StorageClassFactory | None = None):
        name = get_mock_name(original.name)
        if factory is None:
            factory = StorageClassFactory()
        super().__init__(
            name=name,
            pytype=MockDataset,
            components={
                k: self.get_or_register_mock(v.name, factory) for k, v in original.components.items()
            },
            derivedComponents={
                k: self.get_or_register_mock(v.name, factory) for k, v in original.derivedComponents.items()
            },
            parameters=frozenset(original.parameters),
            delegate=get_full_type_name(MockStorageClassDelegate),
            # Conversions work differently for mock storage classes, since they
            # all have the same pytype: we use the original storage class being
            # mocked to see if we can convert, then just make a new MockDataset
            # that points back to the original.
            converters={},
        )
        self.original = original
        # Make certain no one tries to use the converters.
        self._converters = None  # type: ignore

    def _get_converters_by_type(self) -> dict[type, Callable[[Any], Any]]:
        # Docstring inherited.
        raise NotImplementedError("MockStorageClass does not use converters.")

    @classmethod
    def get_or_register_mock(
        cls, original: str, factory: StorageClassFactory | None = None
    ) -> MockStorageClass:
        """Return a mock storage class for the given original storage class,
        creating and registering it if necessary.

        Parameters
        ----------
        original : `str`
            Name of the original storage class to be mocked.
        factory : `~lsst.daf.butler.StorageClassFactory`, optional
            Storage class factory singleton instance.

        Returns
        -------
        mock : `MockStorageClass`
            New storage class that mocks ``original``.
        """
        name = get_mock_name(original)
        if factory is None:
            factory = StorageClassFactory()
        if name in factory:
            return cast(MockStorageClass, factory.getStorageClass(name))
        else:
            result = cls(factory.getStorageClass(original), factory)
            factory.registerStorageClass(result)
            return result

    def allComponents(self) -> Mapping[str, MockStorageClass]:
        # Docstring inherited.
        return cast(Mapping[str, MockStorageClass], super().allComponents())

    @property
    def components(self) -> Mapping[str, MockStorageClass]:
        # Docstring inherited.
        return cast(Mapping[str, MockStorageClass], super().components)

    @property
    def derivedComponents(self) -> Mapping[str, MockStorageClass]:
        # Docstring inherited.
        return cast(Mapping[str, MockStorageClass], super().derivedComponents)

    def can_convert(self, other: StorageClass) -> bool:
        # Docstring inherited.
        if not isinstance(other, MockStorageClass):
            return False
        return self.original.can_convert(other.original)

    def coerce_type(self, incorrect: Any) -> Any:
        # Docstring inherited.
        if not isinstance(incorrect, MockDataset):
            raise TypeError(
                f"Mock storage class {self.name!r} can only convert in-memory datasets "
                f"corresponding to other mock storage classes, not {incorrect!r}."
            )
        factory = StorageClassFactory()
        other_storage_class = factory.getStorageClass(incorrect.storage_class)
        assert isinstance(other_storage_class, MockStorageClass), "Should not get a MockDataset otherwise."
        if other_storage_class.name == self.name:
            return incorrect
        if not self.can_convert(other_storage_class):
            raise TypeError(
                f"Mocked storage class {self.original.name!r} cannot convert from "
                f"{other_storage_class.original.name!r}."
            )
        return incorrect.make_derived(storageClass=self.name, converted_from=incorrect)


def _monkeypatch_daf_butler() -> None:
    """Replace methods in daf_butler's StorageClassFactory and FormatterFactory
    classes to automatically recognize mock storage classes.

    This monkey-patching is executed when the `lsst.pipe.base.tests.mocks`
    package is imported, and it affects all butler instances created before or
    after that imported.
    """
    original_get_storage_class = StorageClassFactory.getStorageClass

    def new_get_storage_class(self: StorageClassFactory, storageClassName: str) -> StorageClass:
        try:
            return original_get_storage_class(self, storageClassName)
        except KeyError:
            if is_mock_name(storageClassName):
                return MockStorageClass.get_or_register_mock(get_original_name(storageClassName))
            raise

    StorageClassFactory.getStorageClass = new_get_storage_class  # type: ignore

    del new_get_storage_class

    original_get_formatter_class_with_match = FormatterFactory.getFormatterClassWithMatch

    def new_get_formatter_class_with_match(
        self: FormatterFactory, entity: Any
    ) -> tuple[LookupKey, type[Formatter], dict[str, Any]]:
        try:
            return original_get_formatter_class_with_match(self, entity)
        except KeyError:
            lookup_keys = (LookupKey(name=entity),) if isinstance(entity, str) else entity._lookupNames()
            for key in lookup_keys:
                # This matches mock dataset type names before mock storage
                # classes, and it would even match some regular dataset types
                # that are automatic connections (logs, configs, metadata) of
                # mocked tasks.  The latter would be a problem, except that
                # those should have already matched in the try block above.
                if is_mock_name(key.name):
                    return (key, JsonFormatter, {})
            raise

    FormatterFactory.getFormatterClassWithMatch = new_get_formatter_class_with_match  # type: ignore

    del new_get_formatter_class_with_match

    original_get_formatter_with_match = FormatterFactory.getFormatterWithMatch

    def new_get_formatter_with_match(
        self: FormatterFactory, entity: Any, *args: Any, **kwargs: Any
    ) -> tuple[LookupKey, Formatter]:
        try:
            return original_get_formatter_with_match(self, entity, *args, **kwargs)
        except KeyError:
            lookup_keys = (LookupKey(name=entity),) if isinstance(entity, str) else entity._lookupNames()
            for key in lookup_keys:
                if is_mock_name(key.name):
                    return (key, JsonFormatter(*args, **kwargs))
            raise

    FormatterFactory.getFormatterWithMatch = new_get_formatter_with_match  # type: ignore

    del new_get_formatter_with_match


_monkeypatch_daf_butler()
