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

__all__ = ["InMemoryDatasetHandle"]

import dataclasses
from typing import Any, cast

from frozendict import frozendict

from lsst.daf.butler import (
    DataCoordinate,
    DataId,
    DimensionUniverse,
    StorageClass,
    StorageClassDelegate,
    StorageClassFactory,
)


# Use an empty dataID as a default.
def _default_dataId() -> DataCoordinate:
    return DataCoordinate.make_empty(DimensionUniverse())


@dataclasses.dataclass(frozen=True, init=False)
class InMemoryDatasetHandle:
    """An in-memory version of a `~lsst.daf.butler.DeferredDatasetHandle`.

    Parameters
    ----------
    inMemoryDataset : `~typing.Any`
        The dataset to be used by this handle.
    storageClass : `~lsst.daf.butler.StorageClass` or `None`, optional
        The storage class associated with the in-memory dataset. If `None`
        and if a storage class is needed, an attempt will be made to work one
        out from the underlying python type.
    parameters : `dict` [`str`, `~typing.Any`]
        Parameters to be used with `get`.
    dataId : `~lsst.daf.butler.DataId` or `None`, optional
        The dataId associated with this dataset. Only used for compatibility
        with the Butler implementation. Can be used for logging messages
        by calling code. If ``dataId`` is not specified, a default empty
        dataId will be constructed.
    copy : `bool`, optional
        Whether to copy on `get` or not.
    **kwargs : `~typing.Any`
        If ``kwargs`` are provided without specifying a ``dataId``, those
        parameters will be converted into a dataId-like entity.
    """

    _empty = DataCoordinate.make_empty(DimensionUniverse())

    def __init__(
        self,
        inMemoryDataset: Any,
        *,
        storageClass: StorageClass | str | None = None,
        parameters: dict[str, Any] | None = None,
        dataId: DataId | None = None,
        copy: bool = False,
        **kwargs: Any,
    ):
        object.__setattr__(self, "inMemoryDataset", inMemoryDataset)
        object.__setattr__(self, "storageClass", storageClass)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "copy", copy)
        # Need to be able to construct a dataId from kwargs for convenience.
        # This will not be a full DataCoordinate.
        if dataId is None:
            if kwargs:
                dataId = frozendict(kwargs)
            else:
                dataId = self._empty
        elif kwargs:
            if isinstance(dataId, DataCoordinate):
                dataId = DataCoordinate.standardize(kwargs, defaults=dataId, universe=dataId.universe)
            else:
                new = dict(dataId)
                new.update(kwargs)
                dataId = frozendict(new)
        object.__setattr__(self, "dataId", dataId)

    def get(
        self,
        *,
        component: str | None = None,
        parameters: dict | None = None,
        storageClass: str | StorageClass | None = None,
    ) -> Any:
        """Retrieve the dataset pointed to by this handle.

        This handle may be used multiple times, possibly with different
        parameters.

        Parameters
        ----------
        component : `str` or None
            If the deferred object is a component dataset type, this parameter
            may specify the name of the component to use in the get operation.
        parameters : `dict` or None
            The parameters argument will be passed to the butler get method.
            It defaults to `None`. If the value is not `None`, this `dict` will
            be merged with the parameters dict used to construct the
            `~lsst.daf.butler.DeferredDatasetHandle` class.
        storageClass : `~lsst.daf.butler.StorageClass` or `str`, optional
            The storage class to be used to override the Python type
            returned by this method. By default the returned type matches
            the type stored. Specifying a read `~lsst.daf.butler.StorageClass`
            can force a different type to be returned.
            This type must be compatible with the original type.

        Returns
        -------
        return : `object`
            The dataset pointed to by this handle. Whether this returns the
            original object or a copy is controlled by the ``copy`` property
            of the handle that is set at handle construction time.
            If the stored object is `None` this method always returns `None`
            regardless of any component request or parameters.

        Raises
        ------
        KeyError
            Raised if a component or parameters are used but no storage
            class can be found.
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

        returnStorageClass: StorageClass | None = None
        if storageClass:
            if isinstance(storageClass, str):
                factory = StorageClassFactory()
                returnStorageClass = factory.getStorageClass(storageClass)
            else:
                returnStorageClass = storageClass

        inMemoryDataset = self.inMemoryDataset

        if self.copy:
            # An optimization might be to defer copying until any components
            # and parameters have been applied. This can be a problem since
            # most component storage classes do not bother to define a
            # storage class delegate and the default delegate uses deepcopy()
            # which can fail if explicit support for deepcopy() is missing
            # or pickle does not work.
            # Copying will require a storage class be determined, which is
            # not normally required for the default case of no parameters and
            # no components.
            thisStorageClass = self._getStorageClass()
            try:
                delegate = thisStorageClass.delegate()
            except TypeError:
                # Try the default copy options if no delegate is available.
                delegate = StorageClassDelegate(thisStorageClass)

            inMemoryDataset = delegate.copy(inMemoryDataset)

        if component or mergedParameters:
            # This requires a storage class look up to locate the delegate
            # class.
            thisStorageClass = self._getStorageClass()

            # Parameters for derived components are applied against the
            # composite.
            if component in thisStorageClass.derivedComponents:
                # For some reason MyPy doesn't see the line above as narrowing
                # 'component' from 'str | None' to 'str'.
                component = cast(str, component)
                thisStorageClass.validateParameters(parameters)

                # Process the parameters (hoping this never modified the
                # original object).
                inMemoryDataset = thisStorageClass.delegate().handleParameters(
                    inMemoryDataset, mergedParameters
                )
                mergedParameters = {}  # They have now been used

                readStorageClass = thisStorageClass.derivedComponents[component]
            else:
                if component:
                    readStorageClass = thisStorageClass.components[component]
                else:
                    readStorageClass = thisStorageClass
                readStorageClass.validateParameters(mergedParameters)

            if component:
                inMemoryDataset = thisStorageClass.delegate().getComponent(inMemoryDataset, component)

            if mergedParameters:
                inMemoryDataset = readStorageClass.delegate().handleParameters(
                    inMemoryDataset, mergedParameters
                )

            if returnStorageClass:
                return returnStorageClass.coerce_type(inMemoryDataset)
            return inMemoryDataset
        else:
            # If there are no parameters or component requests the object
            # can be returned as is, but possibly with conversion.
            if returnStorageClass:
                return returnStorageClass.coerce_type(inMemoryDataset)
            return inMemoryDataset

    def _getStorageClass(self) -> StorageClass:
        """Return the relevant storage class.

        Returns
        -------
        storageClass : `~lsst.daf.butler.StorageClass`
            The storage class associated with this handle, or one derived
            from the python type of the stored object.

        Raises
        ------
        KeyError
            Raised if the storage class could not be found.
        """
        factory = StorageClassFactory()
        if self.storageClass:
            if isinstance(self.storageClass, str):
                return factory.getStorageClass(self.storageClass)
            else:
                return self.storageClass

        # Need to match python type.
        pytype = type(self.inMemoryDataset)
        return factory.findStorageClass(pytype)

    inMemoryDataset: Any
    """The object to store in this dataset handle for later retrieval.
    """

    dataId: DataCoordinate | frozendict
    """The `~lsst.daf.butler.DataCoordinate` associated with this dataset
    handle.
    """

    storageClass: StorageClass | str | None = None
    """The name of the `~lsst.daf.butler.StorageClass` associated with this
    dataset.

    If `None`, the storage class will be looked up from the factory.
    """

    parameters: dict | None = None
    """Optional parameters that may be used to specify a subset of the dataset
    to be loaded (`dict` or `None`).
    """

    copy: bool = False
    """Control whether a copy of the in-memory dataset is returned for every
    call to `get()`."""
