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

"""Module defining connection types to be used within a
`PipelineTaskConnections` class.
"""

__all__ = ["InitInput", "InitOutput", "Input", "PrerequisiteInput", "Output", "BaseConnection"]

import dataclasses
import typing
from collections.abc import Callable, Iterable, Sequence
from typing import Optional, Union

from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionUniverse, Registry, StorageClass


@dataclasses.dataclass(frozen=True)
class BaseConnection:
    """Base class used for declaring PipelineTask connections

    Parameters
    ----------
    name : `str`
        The name used to identify the dataset type
    storageClass : `str`
        The storage class used when (un)/persisting the dataset type
    multiple : `bool`
        Indicates if this connection should expect to contain multiple objects
        of the given dataset type.  Tasks with more than one connection with
        ``multiple=True`` with the same dimensions may want to implement
        `PipelineTaskConnections.adjustQuantum` to ensure those datasets are
        consistent (i.e. zip-iterable) in `PipelineTask.runQuantum` and notify
        the execution system as early as possible of outputs that will not be
        produced because the corresponding input is missing.
    """

    name: str
    storageClass: str
    doc: str = ""
    multiple: bool = False

    def __get__(self, inst, klass):
        """Descriptor method

        This is a method used to turn a connection into a descriptor.
        When a connection is added to a connection class, it is a class level
        variable. This method makes accessing this connection, on the
        instance of the connection class owning this connection, return a
        result specialized for that instance. In the case of connections
        this specifically means names specified in a config instance will
        be visible instead of the default names for the connection.
        """
        # If inst is None, this is being accessed by the class and not an
        # instance, return this connection itself
        if inst is None:
            return self
        # If no object cache exists, create one to track the instances this
        # connection has been accessed by
        if not hasattr(inst, "_connectionCache"):
            object.__setattr__(inst, "_connectionCache", {})
        # Look up an existing cached instance
        idSelf = id(self)
        if idSelf in inst._connectionCache:
            return inst._connectionCache[idSelf]
        # Accumulate the parameters that define this connection
        params = {}
        for field in dataclasses.fields(self):
            params[field.name] = getattr(self, field.name)
        # Get the name override defined by the instance of the connection class
        params["name"] = inst._nameOverrides[self.varName]
        # Return a new instance of this connection specialized with the
        # information provided by the connection class instance
        return inst._connectionCache.setdefault(idSelf, self.__class__(**params))

    def makeDatasetType(
        self, universe: DimensionUniverse, parentStorageClass: Optional[Union[StorageClass, str]] = None
    ) -> DatasetType:
        """Construct a true `DatasetType` instance with normalized dimensions.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Set of all known dimensions to be used to normalize the dimension
            names specified in config.
        parentStorageClass : `lsst.daf.butler.StorageClass` or `str`, optional
            Parent storage class for component datasets; `None` otherwise.

        Returns
        -------
        datasetType : `DatasetType`
            The `DatasetType` defined by this connection.
        """
        return DatasetType(
            self.name, universe.empty, self.storageClass, parentStorageClass=parentStorageClass
        )


@dataclasses.dataclass(frozen=True)
class DimensionedConnection(BaseConnection):
    """Class used for declaring PipelineTask connections that includes
    dimensions

    Parameters
    ----------
    name : `str`
        The name used to identify the dataset type
    storageClass : `str`
        The storage class used when (un)/persisting the dataset type
    multiple : `bool`
        Indicates if this connection should expect to contain multiple objects
        of the given dataset type.  Tasks with more than one connection with
        ``multiple=True`` with the same dimensions may want to implement
        `PipelineTaskConnections.adjustQuantum` to ensure those datasets are
        consistent (i.e. zip-iterable) in `PipelineTask.runQuantum` and notify
        the execution system as early as possible of outputs that will not be
        produced because the corresponding input is missing.
    dimensions : iterable of `str`
        The `lsst.daf.butler.Butler` `lsst.daf.butler.Registry` dimensions used
        to identify the dataset type identified by the specified name
    isCalibration: `bool`, optional
        `True` if this dataset type may be included in CALIBRATION-type
        collections to associate it with a validity range, `False` (default)
        otherwise.
    """

    dimensions: typing.Iterable[str] = ()
    isCalibration: bool = False

    def __post_init__(self):
        if isinstance(self.dimensions, str):
            raise TypeError(
                "Dimensions must be iterable of dimensions, got str, possibly omitted trailing comma"
            )
        if not isinstance(self.dimensions, typing.Iterable):
            raise TypeError("Dimensions must be iterable of dimensions")

    def makeDatasetType(
        self, universe: DimensionUniverse, parentStorageClass: Optional[Union[StorageClass, str]] = None
    ) -> DatasetType:
        """Construct a true `DatasetType` instance with normalized dimensions.

        Parameters
        ----------
        universe : `lsst.daf.butler.DimensionUniverse`
            Set of all known dimensions to be used to normalize the dimension
            names specified in config.
        parentStorageClass : `lsst.daf.butler.StorageClass` or `str`, optional
            Parent storage class for component datasets; `None` otherwise.

        Returns
        -------
        datasetType : `DatasetType`
            The `DatasetType` defined by this connection.
        """
        return DatasetType(
            self.name,
            universe.extract(self.dimensions),
            self.storageClass,
            isCalibration=self.isCalibration,
            parentStorageClass=parentStorageClass,
        )


@dataclasses.dataclass(frozen=True)
class BaseInput(DimensionedConnection):
    """Class used for declaring PipelineTask input connections

    Parameters
    ----------
    name : `str`
        The default name used to identify the dataset type
    storageClass : `str`
        The storage class used when (un)/persisting the dataset type
    multiple : `bool`
        Indicates if this connection should expect to contain multiple objects
        of the given dataset type.  Tasks with more than one connection with
        ``multiple=True`` with the same dimensions may want to implement
        `PipelineTaskConnections.adjustQuantum` to ensure those datasets are
        consistent (i.e. zip-iterable) in `PipelineTask.runQuantum` and notify
        the execution system as early as possible of outputs that will not be
        produced because the corresponding input is missing.
    dimensions : iterable of `str`
        The `lsst.daf.butler.Butler` `lsst.daf.butler.Registry` dimensions used
        to identify the dataset type identified by the specified name
    deferLoad : `bool`
        Indicates that this dataset type will be loaded as a
        `lsst.daf.butler.DeferredDatasetHandle`. PipelineTasks can use this
        object to load the object at a later time.
    minimum : `bool`
        Minimum number of datasets required for this connection, per quantum.
        This is checked in the base implementation of
        `PipelineTaskConnections.adjustQuantum`, which raises `NoWorkFound` if
        the minimum is not met for `Input` connections (causing the quantum to
        be pruned, skipped, or never created, depending on the context), and
        `FileNotFoundError` for `PrerequisiteInput` connections (causing
        QuantumGraph generation to fail).  `PipelineTask` implementations may
        provide custom `~PipelineTaskConnections.adjustQuantum` implementations
        for more fine-grained or configuration-driven constraints, as long as
        they are compatible with this minium.

    Raises
    ------
    TypeError
        Raised if ``minimum`` is greater than one but ``multiple=False``.
    NotImplementedError
        Raised if ``minimum`` is zero for a regular `Input` connection; this
        is not currently supported by our QuantumGraph generation algorithm.
    """

    deferLoad: bool = False
    minimum: int = 1

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.minimum > 1 and not self.multiple:
            raise TypeError(f"Cannot set minimum={self.minimum} if multiple=False.")


@dataclasses.dataclass(frozen=True)
class Input(BaseInput):
    def __post_init__(self) -> None:
        super().__post_init__()
        if self.minimum == 0:
            raise TypeError(f"Cannot set minimum={self.minimum} for regular input.")


@dataclasses.dataclass(frozen=True)
class PrerequisiteInput(BaseInput):
    """Class used for declaring PipelineTask prerequisite connections

    Parameters
    ----------
    name : `str`
        The default name used to identify the dataset type
    storageClass : `str`
        The storage class used when (un)/persisting the dataset type
    multiple : `bool`
        Indicates if this connection should expect to contain multiple objects
        of the given dataset type.  Tasks with more than one connection with
        ``multiple=True`` with the same dimensions may want to implement
        `PipelineTaskConnections.adjustQuantum` to ensure those datasets are
        consistent (i.e. zip-iterable) in `PipelineTask.runQuantum` and notify
        the execution system as early as possible of outputs that will not be
        produced because the corresponding input is missing.
    dimensions : iterable of `str`
        The `lsst.daf.butler.Butler` `lsst.daf.butler.Registry` dimensions used
        to identify the dataset type identified by the specified name
    minimum : `bool`
        Minimum number of datasets required for this connection, per quantum.
        This is checked in the base implementation of
        `PipelineTaskConnections.adjustQuantum`, which raises
        `FileNotFoundError` (causing QuantumGraph generation to fail).
        `PipelineTask` implementations may
        provide custom `~PipelineTaskConnections.adjustQuantum` implementations
        for more fine-grained or configuration-driven constraints, as long as
        they are compatible with this minium.
    lookupFunction: `typing.Callable`, optional
        An optional callable function that will look up PrerequisiteInputs
        using the DatasetType, registry, quantum dataId, and input collections
        passed to it. If no function is specified, the default temporal spatial
        lookup will be used.

    Raises
    ------
    TypeError
        Raised if ``minimum`` is greater than one but ``multiple=False``.

    Notes
    -----
    Prerequisite inputs are used for datasets that must exist in the data
    repository before a pipeline including this is run; they cannot be produced
    by another task in the same pipeline.

    In exchange for this limitation, they have a number of advantages relative
    to regular `Input` connections:

    - The query used to find them then during `QuantumGraph` generation can be
      fully customized by providing a ``lookupFunction``.
    - Failed searches for prerequisites during `QuantumGraph` generation will
      usually generate more helpful diagnostics than those for regular `Input`
      connections.
    - The default query for prerequisite inputs relates the quantum dimensions
      directly to the dimensions of its dataset type, without being constrained
      by any of the other dimensions in the pipeline.  This allows them to be
      used for temporal calibration lookups (which regular `Input` connections
      cannot do at present) and to work around `QuantumGraph` generation
      limitations involving cases where naive spatial overlap relationships
      between dimensions are not desired (e.g. a task that wants all detectors
      in each visit for which the visit overlaps a tract, not just those where
      that detector+visit combination overlaps the tract).
    - Prerequisite inputs may be optional (regular inputs are never optional).

    """

    lookupFunction: Optional[
        Callable[[DatasetType, Registry, DataCoordinate, Sequence[str]], Iterable[DatasetRef]]
    ] = None


@dataclasses.dataclass(frozen=True)
class Output(DimensionedConnection):
    pass


@dataclasses.dataclass(frozen=True)
class InitInput(BaseConnection):
    pass


@dataclasses.dataclass(frozen=True)
class InitOutput(BaseConnection):
    pass
