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

import pydantic
from lsst.daf.butler import (
    Config,
    DatasetComponent,
    SerializedDataCoordinate,
    SerializedDatasetRef,
    SerializedDatasetType,
    StorageClass,
    StorageClassDelegate,
    StorageClassFactory,
)
from lsst.resources import ResourcePath, ResourcePathExpression
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
    return mock[len(_NAME_PREFIX) :]


def is_mock_name(name: str) -> bool:
    """Return whether the given name is that of a mock storage class, dataset
    type, or task label.
    """
    return name.startswith(_NAME_PREFIX)


# Tests for this module are in the ci_middleware package, where we have easy
# access to complex real storage classes (and their pytypes) to test against.


class MockDataset(pydantic.BaseModel):
    """The in-memory dataset type used by `MockStorageClass`."""

    ref: SerializedDatasetRef
    """Reference used to read and write this dataset.

    This is a `SerializedDatasetRef` instead of a "real" one for two reasons:

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
        `SerializedDatasetType` to override in the result.
        """
        dataset_type_updates = {
            k: kwargs.pop(k) for k in list(kwargs) if k in SerializedDatasetType.__fields__
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


class MockDatasetQuantum(pydantic.BaseModel):
    """Description of the quantum that produced a mock dataset."""

    task_label: str
    """Label of the producing PipelineTask in its pipeline."""

    data_id: SerializedDataCoordinate
    """Data ID for the quantum."""

    inputs: dict[str, list[MockDataset]]
    """Mock datasets provided as input to the quantum."""


MockDataset.update_forward_refs()


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
        factory : `StorageClassFactory`, optional
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

    @staticmethod
    def make_formatter_config_dir(
        root: ResourcePathExpression, factory: StorageClassFactory | None = None
    ) -> None:
        """Make a directory suitable for inclusion in the butler config
        search path.

        Parameters
        ----------
        root : convertible to `lsst.resources.ResourcePath`
            Root directory that will be passed as any element of the
            ``searchPaths`` list at `~lsst.daf.butler.Butler` construction.
        factory : `StorageClassFactory`, optional
            Storage class factory singleton instance.

        Notes
        -----
        This adds formatter entries to `~lsst.daf.butler.FileDatastore`
        configuration for all mock storage classes that have been registered so
        far.  It does not add the storage class configuration entries
        themselves, because `MockStorageClass` can't be written out as
        configuration in the same way that regular storage classes can. So the
        usual pattern for creating a butler client with storage class mocking
        is:

        - Register all needed mock storage classes with the singleton
          `lsst.daf.butler.StorageClassFactory`, which is constructed on first
          use even when there are no active butler clients.

        - Call this method to create a directory with formatter configuration
          for those storage classes (within the same process, so singleton
          state is preserved).

        - Create a butler client while passing ``searchPaths=[root]``, again
          within the same process.

        There is currently no automatic way to share mock storage class
        definitions across processes, other than to re-run the code that
        registers those mock storage classes.

        Note that the data repository may be created any time before the butler
        client is - the formatter configuration written by this method is
        expected to be used per-client, not at data repository construction,
        and nothing about the mocks is persistent within the data repository
        other than dataset types that use those storage classes (and dataset
        types with unrecognized storage classes are already carefully handled
        by the butler).
        """
        if factory is None:
            factory = StorageClassFactory()
        formatters_config = Config()
        for storage_class_name in factory.keys():
            if is_mock_name(storage_class_name):
                # Have to escape mock storage class names, because
                # config["_mock_X"] gets reinterpreted as a nested config key,
                # i.e. config["mock"]["X"].
                formatters_config["\\" + storage_class_name] = "lsst.daf.butler.formatters.json.JsonFormatter"
        root = ResourcePath(root, forceDirectory=True)
        dir_path = root.join("datastores", forceDirectory=True)
        dir_path.mkdir()
        formatters_config.dumpToUri(dir_path.join("formatters.yaml"))
        with dir_path.join("fileDatastore.yaml").open("w") as stream:
            stream.write(
                "datastore:\n"
                "  cls: lsst.daf.butler.datastores.fileDatastore.FileDatastore\n"
                "  formatters: !include formatters.yaml\n"
            )
        with root.join("datastore.yaml").open("w") as stream:
            stream.write("datastore:\n  cls: lsst.daf.butler.datastores.fileDatastore.FileDatastore\n")

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
