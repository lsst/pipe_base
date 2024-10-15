# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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
    "ConvertedUnmockedDataset",
    "MockDataset",
    "MockStorageClass",
    "MockDatasetQuantum",
    "MockStorageClassDelegate",
    "get_mock_name",
    "get_original_name",
    "is_mock_name",
)

import sys
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any, cast

import pydantic
from lsst.daf.butler import (
    DataIdValue,
    DatasetComponent,
    DatasetRef,
    DatasetType,
    Formatter,
    FormatterFactory,
    FormatterV2,
    LookupKey,
    SerializedDatasetType,
    StorageClass,
    StorageClassDelegate,
    StorageClassFactory,
)
from lsst.daf.butler.formatters.json import JsonFormatter
from lsst.utils.introspection import get_full_type_name

_NAME_PREFIX: str = "_mock_"


def get_mock_name(original: str) -> str:
    """Return the name of the mock storage class, dataset type, or task label
    for the given original name.

    Parameters
    ----------
    original : `str`
        Original name.

    Returns
    -------
    name : `str`
        The name of the mocked version.
    """
    return _NAME_PREFIX + original


def get_original_name(mock: str) -> str:
    """Return the name of the original storage class, dataset type, or task
    label that corresponds to the given mock name.

    Parameters
    ----------
    mock : `str`
        The mocked name.

    Returns
    -------
    original : `str`
        The original name.
    """
    assert mock.startswith(_NAME_PREFIX)
    return mock.removeprefix(_NAME_PREFIX)


def is_mock_name(name: str | None) -> bool:
    """Return whether the given name is that of a mock storage class, dataset
    type, or task label.

    Parameters
    ----------
    name : `str` or `None`
        The given name to check.

    Returns
    -------
    is_mock : `bool`
        Whether the name is for a mock or not.
    """
    return name is not None and name.startswith(_NAME_PREFIX)


# Tests for this module are in the ci_middleware package, where we have easy
# access to complex real storage classes (and their pytypes) to test against.


class MockDataset(pydantic.BaseModel):
    """The in-memory dataset type used by `MockStorageClass`."""

    dataset_id: uuid.UUID | None
    """Universal unique identifier for this dataset."""

    dataset_type: SerializedDatasetType
    """Butler dataset type or this dataset.

    See the documentation for ``data_id`` for why this is a
    `~lsst.daf.butler.SerializedDatasetType` instead of a "real" one.
    """

    data_id: dict[str, DataIdValue]
    """Butler data ID for this dataset.

    This is a `~lsst.daf.butler.SerializedDataCoordinate` instead of a "real"
    one for two reasons:

    - the mock dataset may need to be read from disk in a context in which a
      `~lsst.daf.butler.DimensionUniverse` is unavailable;
    - we don't want the complexity of having a separate
      ``SerializedMockDataCoordinate``.
    """

    run: str | None
    """`~lsst.daf.butler.CollectionType.RUN` collection this dataset belongs
    to.
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
    def storage_class(self) -> str:
        return cast(str, self.dataset_type.storageClass)

    def make_derived(self, **kwargs: Any) -> MockDataset:
        """Return a new MockDataset that represents applying some storage class
        operation to this one.

        Parameters
        ----------
        **kwargs : `~typing.Any`
            Keyword arguments are fields of `MockDataset` or
            `~lsst.daf.butler.SerializedDatasetType` to override in the result.

        Returns
        -------
        derived : `MockDataset`
            The newly-mocked dataset.
        """
        dataset_type_updates = {
            k: kwargs.pop(k) for k in list(kwargs) if k in SerializedDatasetType.model_fields
        }
        kwargs.setdefault("dataset_type", self.dataset_type.model_copy(update=dataset_type_updates))
        # Fields below are those that should not be propagated to the derived
        # dataset, because they're not about the intrinsic on-disk thing.
        kwargs.setdefault("converted_from", None)
        kwargs.setdefault("parent", None)
        kwargs.setdefault("parameters", None)
        # Also use setdefault on the ref in case caller wants to override that
        # directly, but this is expected to be rare enough that it's not worth
        # it to try to optimize out the work above to make derived_ref.
        return self.model_copy(update=kwargs)

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)


class ConvertedUnmockedDataset(pydantic.BaseModel):
    """A marker class that represents a conversion from a regular in-memory
    dataset to a mock storage class.
    """

    original_type: str
    """The full Python type of the original unmocked in-memory dataset."""

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)


class MockDatasetQuantum(pydantic.BaseModel):
    """Description of the quantum that produced a mock dataset.

    This is also used to represent task-init operations for init-output mock
    datasets.
    """

    task_label: str
    """Label of the producing PipelineTask in its pipeline."""

    data_id: dict[str, DataIdValue]
    """Data ID for the quantum."""

    inputs: dict[str, list[MockDataset | ConvertedUnmockedDataset]]
    """Mock datasets provided as input to the quantum.

    Keys are task-internal connection names, not dataset type names.
    """

    # Work around the fact that Sphinx chokes on Pydantic docstring formatting,
    # when we inherit those docstrings in our public classes.
    if "sphinx" in sys.modules:

        def copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.copy`."""
            return super().copy(*args, **kwargs)

        def model_dump(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump`."""
            return super().model_dump(*args, **kwargs)

        def model_dump_json(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_dump_json`."""
            return super().model_dump(*args, **kwargs)

        def model_copy(self, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_copy`."""
            return super().model_copy(*args, **kwargs)

        @classmethod
        def model_construct(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_construct`."""
            return super().model_construct(*args, **kwargs)

        @classmethod
        def model_json_schema(cls, *args: Any, **kwargs: Any) -> Any:
            """See `pydantic.BaseModel.model_json_schema`."""
            return super().model_json_schema(*args, **kwargs)


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

    Parameters
    ----------
    original : `~lsst.daf.butler.StorageClass`
        The original storage class.
    factory : `~lsst.daf.butler.StorageClassFactory` or `None`, optional
        Storage class factory to use. If `None` the default factory is used.

    Notes
    -----
    Each `MockStorageClass` instance corresponds to a real "original" storage
    class, with components and conversions that are mocks of the original's
    components and conversions.  The ``pytype`` for all `MockStorageClass`
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
            # Allow conversions from an original type (and others compatible
            # with it) to a mock, to allow for cases where an upstream task
            # did not use a mock to write something but the downstream one is
            # trying to us a mock to read it.
            return self.original.can_convert(other)
        return self.original.can_convert(other.original)

    def coerce_type(self, incorrect: Any) -> Any:
        # Docstring inherited.
        if not isinstance(incorrect, MockDataset):
            if isinstance(incorrect, ConvertedUnmockedDataset):
                return incorrect
            return ConvertedUnmockedDataset(original_type=get_full_type_name(incorrect))
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

    @staticmethod
    def mock_dataset_type(original_type: DatasetType) -> DatasetType:
        """Replace a dataset type with a version that uses a mock storage class
        and name.

        Parameters
        ----------
        original_type : `lsst.daf.butler.DatasetType`
            Original dataset type to be mocked.

        Returns
        -------
        mock_type : `lsst.daf.butler.DatasetType`
            A mock version of the dataset type, with name and storage class
            changed and everything else unchanged.
        """
        mock_storage_class = MockStorageClass.get_or_register_mock(original_type.storageClass_name)
        mock_parent_storage_class = None
        if original_type.parentStorageClass is not None:
            mock_parent_storage_class = MockStorageClass.get_or_register_mock(
                original_type.parentStorageClass.name
            )
        return DatasetType(
            get_mock_name(original_type.name),
            original_type.dimensions,
            mock_storage_class,
            isCalibration=original_type.isCalibration(),
            parentStorageClass=mock_parent_storage_class,
        )

    @staticmethod
    def mock_dataset_refs(original_refs: Iterable[DatasetRef]) -> list[DatasetRef]:
        """Replace dataset references with versions that uses a mock storage
        class and dataset type name.

        Parameters
        ----------
        original_refs : `~collections.abc.Iterable` [ \
                `lsst.daf.butler.DatasetRef` ]
            Original dataset references to be mocked.

        Returns
        -------
        mock_refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            Mocked version of the dataset references, with dataset type name
            and storage class changed and everything else unchanged.
        """
        original_refs = list(original_refs)
        if not original_refs:
            return original_refs
        dataset_type = MockStorageClass.mock_dataset_type(original_refs[0].datasetType)
        return [
            DatasetRef(dataset_type, original_ref.dataId, run=original_ref.run, id=original_ref.id)
            for original_ref in original_refs
        ]

    @staticmethod
    def unmock_dataset_type(mock_type: DatasetType) -> DatasetType:
        """Replace a mock dataset type with the original one it was created
        from.

        Parameters
        ----------
        mock_type : `lsst.daf.butler.DatasetType`
            A dataset type with a mocked name and storage class.

        Returns
        -------
        original_type : `lsst.daf.butler.DatasetType`
            The original dataset type.
        """
        storage_class = mock_type.storageClass
        parent_storage_class = mock_type.parentStorageClass
        if isinstance(storage_class, MockStorageClass):
            storage_class = storage_class.original
        if parent_storage_class is not None and isinstance(parent_storage_class, MockStorageClass):
            parent_storage_class = parent_storage_class.original
        return DatasetType(
            get_original_name(mock_type.name),
            mock_type.dimensions,
            storage_class,
            isCalibration=mock_type.isCalibration(),
            parentStorageClass=parent_storage_class,
        )

    @staticmethod
    def unmock_dataset_refs(mock_refs: Iterable[DatasetRef]) -> list[DatasetRef]:
        """Replace dataset references with versions that do not use a mock
        storage class and dataset type name.

        Parameters
        ----------
        mock_refs : `~collections.abc.Iterable` [ \
                `lsst.daf.butler.DatasetRef` ]
            Dataset references that use a mocked dataset type name and storage
            class.

        Returns
        -------
        original_refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            The original dataset references.
        """
        mock_refs = list(mock_refs)
        if not mock_refs:
            return mock_refs
        dataset_type = MockStorageClass.unmock_dataset_type(mock_refs[0].datasetType)
        return [
            DatasetRef(dataset_type, mock_ref.dataId, run=mock_ref.run, id=mock_ref.id)
            for mock_ref in mock_refs
        ]


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
    ) -> tuple[LookupKey, type[Formatter | FormatterV2], dict[str, Any]]:
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
    ) -> tuple[LookupKey, Formatter | FormatterV2]:
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
