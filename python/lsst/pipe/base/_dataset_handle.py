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

__all__ = ["InMemoryDatasetHandle"]

import dataclasses
from typing import Any, Optional

from lsst.daf.butler import DataCoordinate, DimensionUniverse, StorageClass, StorageClassFactory
from lsst.utils.introspection import get_full_type_name


# Use an empty dataID as a default.
def _default_dataId() -> DataCoordinate:
    return DataCoordinate.makeEmpty(DimensionUniverse())


@dataclasses.dataclass(frozen=True)
class InMemoryDatasetHandle:
    """An in-memory version of a `~lsst.daf.butler.DeferredDatasetHandle`."""

    def get(
        self, *, component: Optional[str] = None, parameters: Optional[dict] = None, **kwargs: dict
    ) -> Any:
        """Retrieves the dataset pointed to by this handle

        This handle may be used multiple times, possibly with different
        parameters.

        Parameters
        ----------
        component : `str` or None
            If the deferred object is a component dataset type, this parameter
            may specify the name of the component to use in the get operation.
        parameters : `dict` or None
            The parameters argument will be passed to the butler get method.
            It defaults to None. If the value is not None, this dict will
            be merged with the parameters dict used to construct the
            `DeferredDatasetHandle` class.
        **kwargs
            This argument is deprecated and only exists to support legacy
            gen2 butler code during migration. It is completely ignored
            and will be removed in the future.

        Returns
        -------
        return : `object`
            The dataset pointed to by this handle. This is the actual object
            that was initially stored and not a copy. Modifying this object
            will modify the stored object. If the stored object is `None` this
            method always returns `None` regardless of any component request or
            parameters.
        """
        if self.inMemoryDataset is None:
            return None

        if self.parameters is not None:
            mergedParameters = self.parameters.copy()
            if parameters is not None:
                mergedParameters.update(parameters)
        elif parameters is not None:
            mergedParameters = parameters
        else:
            mergedParameters = {}

        if component or mergedParameters:
            # This requires a storage class look up to locate the delegate
            # class.
            storageClass = self._getStorageClass()
            inMemoryDataset = self.inMemoryDataset

            # Parameters for derived components are applied against the
            # composite.
            if component in storageClass.derivedComponents:
                storageClass.validateParameters(parameters)

                # Process the parameters (hoping this never modified the
                # original object).
                inMemoryDataset = storageClass.delegate().handleParameters(inMemoryDataset, mergedParameters)
                mergedParameters = {}  # They have now been used

                readStorageClass = storageClass.derivedComponents[component]
            else:
                if component:
                    readStorageClass = storageClass.components[component]
                else:
                    readStorageClass = storageClass
                readStorageClass.validateParameters(mergedParameters)

            if component:
                inMemoryDataset = storageClass.delegate().getComponent(inMemoryDataset, component)

            if mergedParameters:
                inMemoryDataset = readStorageClass.delegate().handleParameters(
                    inMemoryDataset, mergedParameters
                )

            return inMemoryDataset
        else:
            # If there are no parameters or component requests the object
            # can be returned as is.
            return self.inMemoryDataset

    def _getStorageClass(self) -> StorageClass:
        factory = StorageClassFactory()
        if self.storageClass:
            return factory.getStorageClass(self.storageClass)

        # Need to match python type.
        pytype = type(self.inMemoryDataset)
        for storageClass in factory.values():
            # It is possible for a single python type to refer to multiple
            # storage classes such that this could be quite fragile.
            if storageClass.is_type(pytype):
                return storageClass

        raise ValueError(
            "Unable to find a StorageClass with associated with type "
            f"{get_full_type_name(self.inMemoryDataset)}"
        )

    inMemoryDataset: Any
    """The object to store in this dataset handle for later retrieval.
    """

    storageClass: Optional[str] = None
    """The name of the `~lsst.daf.butler.StorageClass` associated with this
    dataset.

    If `None`, the storage class will be looked up from the factory.
    """

    parameters: Optional[dict] = None
    """Optional parameters that may be used to specify a subset of the dataset
    to be loaded (`dict` or `None`).
    """

    dataId: DataCoordinate = dataclasses.field(default_factory=_default_dataId)
    """The `~lsst.daf.butler.DataCoordinate` associated with this dataset
    handle.
    """
