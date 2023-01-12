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

from __future__ import annotations

__all__ = [
    "assertValidInitOutput",
    "assertValidOutput",
    "getInitInputs",
    "lintConnections",
    "makeQuantum",
    "runTestQuantum",
]


import collections.abc
import itertools
import unittest.mock
from collections import defaultdict
from typing import TYPE_CHECKING, AbstractSet, Any, Dict, Mapping, Optional, Sequence, Set, Union

from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DataId,
    DatasetRef,
    DatasetType,
    Dimension,
    DimensionUniverse,
    Quantum,
    SkyPixDimension,
    StorageClassFactory,
)
from lsst.pipe.base.connectionTypes import BaseConnection, DimensionedConnection

from .butlerQuantumContext import ButlerQuantumContext

if TYPE_CHECKING:
    from .config import PipelineTaskConfig
    from .connections import PipelineTaskConnections
    from .pipelineTask import PipelineTask
    from .struct import Struct


def makeQuantum(
    task: PipelineTask,
    butler: Butler,
    dataId: DataId,
    ioDataIds: Mapping[str, Union[DataId, Sequence[DataId]]],
) -> Quantum:
    """Create a Quantum for a particular data ID(s).

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task whose processing the quantum represents.
    butler : `lsst.daf.butler.Butler`
        The collection the quantum refers to.
    dataId: any data ID type
        The data ID of the quantum. Must have the same dimensions as
        ``task``'s connections class.
    ioDataIds : `collections.abc.Mapping` [`str`]
        A mapping keyed by input/output names. Values must be data IDs for
        single connections and sequences of data IDs for multiple connections.

    Returns
    -------
    quantum : `lsst.daf.butler.Quantum`
        A quantum for ``task``, when called with ``dataIds``.
    """
    # This is a type ignore, because `connections` is a dynamic class, but
    # it for sure will have this property
    connections = task.config.ConnectionsClass(config=task.config)  # type: ignore

    try:
        _checkDimensionsMatch(butler.registry.dimensions, connections.dimensions, dataId.keys())
    except ValueError as e:
        raise ValueError("Error in quantum dimensions.") from e

    inputs = defaultdict(list)
    outputs = defaultdict(list)
    for name in itertools.chain(connections.inputs, connections.prerequisiteInputs):
        try:
            connection = connections.__getattribute__(name)
            _checkDataIdMultiplicity(name, ioDataIds[name], connection.multiple)
            ids = _normalizeDataIds(ioDataIds[name])
            for id in ids:
                ref = _refFromConnection(butler, connection, id)
                inputs[ref.datasetType].append(ref)
        except (ValueError, KeyError) as e:
            raise ValueError(f"Error in connection {name}.") from e
    for name in connections.outputs:
        try:
            connection = connections.__getattribute__(name)
            _checkDataIdMultiplicity(name, ioDataIds[name], connection.multiple)
            ids = _normalizeDataIds(ioDataIds[name])
            for id in ids:
                ref = _refFromConnection(butler, connection, id)
                outputs[ref.datasetType].append(ref)
        except (ValueError, KeyError) as e:
            raise ValueError(f"Error in connection {name}.") from e
    quantum = Quantum(
        taskClass=type(task),
        dataId=DataCoordinate.standardize(dataId, universe=butler.registry.dimensions),
        inputs=inputs,
        outputs=outputs,
    )
    return quantum


def _checkDimensionsMatch(
    universe: DimensionUniverse,
    expected: Union[AbstractSet[str], AbstractSet[Dimension]],
    actual: Union[AbstractSet[str], AbstractSet[Dimension]],
) -> None:
    """Test whether two sets of dimensions agree after conversions.

    Parameters
    ----------
    universe : `lsst.daf.butler.DimensionUniverse`
        The set of all known dimensions.
    expected : `Set` [`str`] or `Set` [`~lsst.daf.butler.Dimension`]
        The dimensions expected from a task specification.
    actual : `Set` [`str`] or `Set` [`~lsst.daf.butler.Dimension`]
        The dimensions provided by input.

    Raises
    ------
    ValueError
        Raised if ``expected`` and ``actual`` cannot be reconciled.
    """
    if _simplify(universe, expected) != _simplify(universe, actual):
        raise ValueError(f"Mismatch in dimensions; expected {expected} but got {actual}.")


def _simplify(
    universe: DimensionUniverse, dimensions: Union[AbstractSet[str], AbstractSet[Dimension]]
) -> Set[str]:
    """Reduce a set of dimensions to a string-only form.

    Parameters
    ----------
    universe : `lsst.daf.butler.DimensionUniverse`
        The set of all known dimensions.
    dimensions : `Set` [`str`] or `Set` [`~lsst.daf.butler.Dimension`]
        A set of dimensions to simplify.

    Returns
    -------
    dimensions : `Set` [`str`]
        A copy of ``dimensions`` reduced to string form, with all spatial
        dimensions simplified to ``skypix``.
    """
    simplified: Set[str] = set()
    for dimension in dimensions:
        # skypix not a real Dimension, handle it first
        if dimension == "skypix":
            simplified.add(dimension)  # type: ignore
        else:
            # Need a Dimension to test spatialness
            fullDimension = universe[dimension] if isinstance(dimension, str) else dimension
            if isinstance(fullDimension, SkyPixDimension):
                simplified.add("skypix")
            else:
                simplified.add(fullDimension.name)
    return simplified


def _checkDataIdMultiplicity(name: str, dataIds: Union[DataId, Sequence[DataId]], multiple: bool) -> None:
    """Test whether data IDs are scalars for scalar connections and sequences
    for multiple connections.

    Parameters
    ----------
    name : `str`
        The name of the connection being tested.
    dataIds : any data ID type or `~collections.abc.Sequence` [data ID]
        The data ID(s) provided for the connection.
    multiple : `bool`
        The ``multiple`` field of the connection.

    Raises
    ------
    ValueError
        Raised if ``dataIds`` and ``multiple`` do not match.
    """
    if multiple:
        if not isinstance(dataIds, collections.abc.Sequence):
            raise ValueError(f"Expected multiple data IDs for {name}, got {dataIds}.")
    else:
        # DataCoordinate is a Mapping
        if not isinstance(dataIds, collections.abc.Mapping):
            raise ValueError(f"Expected single data ID for {name}, got {dataIds}.")


def _normalizeDataIds(dataIds: Union[DataId, Sequence[DataId]]) -> Sequence[DataId]:
    """Represent both single and multiple data IDs as a list.

    Parameters
    ----------
    dataIds : any data ID type or `~collections.abc.Sequence` thereof
        The data ID(s) provided for a particular input or output connection.

    Returns
    -------
    normalizedIds : `~collections.abc.Sequence` [data ID]
        A sequence equal to ``dataIds`` if it was already a sequence, or
        ``[dataIds]`` if it was a single ID.
    """
    if isinstance(dataIds, collections.abc.Sequence):
        return dataIds
    else:
        return [dataIds]


def _refFromConnection(
    butler: Butler, connection: DimensionedConnection, dataId: DataId, **kwargs: Any
) -> DatasetRef:
    """Create a DatasetRef for a connection in a collection.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The collection to point to.
    connection : `lsst.pipe.base.connectionTypes.DimensionedConnection`
        The connection defining the dataset type to point to.
    dataId
        The data ID for the dataset to point to.
    **kwargs
        Additional keyword arguments used to augment or construct
        a `~lsst.daf.butler.DataCoordinate`.

    Returns
    -------
    ref : `lsst.daf.butler.DatasetRef`
        A reference to a dataset compatible with ``connection``, with ID
        ``dataId``, in the collection pointed to by ``butler``.
    """
    universe = butler.registry.dimensions
    # DatasetRef only tests if required dimension is missing, but not extras
    _checkDimensionsMatch(universe, set(connection.dimensions), dataId.keys())
    dataId = DataCoordinate.standardize(dataId, **kwargs, universe=universe)

    datasetType = butler.registry.getDatasetType(connection.name)

    try:
        butler.registry.getDatasetType(datasetType.name)
    except KeyError:
        raise ValueError(f"Invalid dataset type {connection.name}.")
    try:
        ref = DatasetRef(datasetType=datasetType, dataId=dataId)
        return ref
    except KeyError as e:
        raise ValueError(f"Dataset type ({connection.name}) and ID {dataId.byName()} not compatible.") from e


def _resolveTestQuantumInputs(butler: Butler, quantum: Quantum) -> None:
    """Look up all input datasets a test quantum in the `Registry` to resolve
    all `DatasetRef` objects (i.e. ensure they have not-`None` ``id`` and
    ``run`` attributes).

    Parameters
    ----------
    quantum : `~lsst.daf.butler.Quantum`
        Single Quantum instance.
    butler : `~lsst.daf.butler.Butler`
        Data butler.
    """
    # TODO (DM-26819): This function is a direct copy of
    # `lsst.ctrl.mpexec.SingleQuantumExecutor.updateQuantumInputs`, but the
    # `runTestQuantum` function that calls it is essentially duplicating logic
    # in that class as well (albeit not verbatim).  We should probably move
    # `SingleQuantumExecutor` to ``pipe_base`` and see if it is directly usable
    # in test code instead of having these classes at all.
    for refsForDatasetType in quantum.inputs.values():
        newRefsForDatasetType = []
        for ref in refsForDatasetType:
            if ref.id is None:
                resolvedRef = butler.registry.findDataset(
                    ref.datasetType, ref.dataId, collections=butler.collections
                )
                if resolvedRef is None:
                    raise ValueError(
                        f"Cannot find {ref.datasetType.name} with id {ref.dataId} "
                        f"in collections {butler.collections}."
                    )
                newRefsForDatasetType.append(resolvedRef)
            else:
                newRefsForDatasetType.append(ref)
        refsForDatasetType[:] = newRefsForDatasetType


def runTestQuantum(
    task: PipelineTask, butler: Butler, quantum: Quantum, mockRun: bool = True
) -> Optional[unittest.mock.Mock]:
    """Run a PipelineTask on a Quantum.

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task to run on the quantum.
    butler : `lsst.daf.butler.Butler`
        The collection to run on.
    quantum : `lsst.daf.butler.Quantum`
        The quantum to run.
    mockRun : `bool`
        Whether or not to replace ``task``'s ``run`` method. The default of
        `True` is recommended unless ``run`` needs to do real work (e.g.,
        because the test needs real output datasets).

    Returns
    -------
    run : `unittest.mock.Mock` or `None`
        If ``mockRun`` is set, the mock that replaced ``run``. This object can
        be queried for the arguments ``runQuantum`` passed to ``run``.
    """
    _resolveTestQuantumInputs(butler, quantum)
    butlerQc = ButlerQuantumContext.from_full(butler, quantum)
    # This is a type ignore, because `connections` is a dynamic class, but
    # it for sure will have this property
    connections = task.config.ConnectionsClass(config=task.config)  # type: ignore
    inputRefs, outputRefs = connections.buildDatasetRefs(quantum)
    if mockRun:
        with unittest.mock.patch.object(task, "run") as mock, unittest.mock.patch(
            "lsst.pipe.base.ButlerQuantumContext.put"
        ):
            task.runQuantum(butlerQc, inputRefs, outputRefs)
            return mock
    else:
        task.runQuantum(butlerQc, inputRefs, outputRefs)
        return None


def _assertAttributeMatchesConnection(obj: Any, attrName: str, connection: BaseConnection) -> None:
    """Test that an attribute on an object matches the specification given in
    a connection.

    Parameters
    ----------
    obj
        An object expected to contain the attribute ``attrName``.
    attrName : `str`
        The name of the attribute to be tested.
    connection : `lsst.pipe.base.connectionTypes.BaseConnection`
        The connection, usually some type of output, specifying ``attrName``.

    Raises
    ------
    AssertionError:
        Raised if ``obj.attrName`` does not match what's expected
        from ``connection``.
    """
    # name
    try:
        attrValue = obj.__getattribute__(attrName)
    except AttributeError:
        raise AssertionError(f"No such attribute on {obj!r}: {attrName}")
    # multiple
    if connection.multiple:
        if not isinstance(attrValue, collections.abc.Sequence):
            raise AssertionError(f"Expected {attrName} to be a sequence, got {attrValue!r} instead.")
    else:
        # use lazy evaluation to not use StorageClassFactory unless
        # necessary
        if isinstance(attrValue, collections.abc.Sequence) and not issubclass(
            StorageClassFactory().getStorageClass(connection.storageClass).pytype, collections.abc.Sequence
        ):
            raise AssertionError(f"Expected {attrName} to be a single value, got {attrValue!r} instead.")
    # no test for storageClass, as I'm not sure how much persistence
    # depends on duck-typing


def assertValidOutput(task: PipelineTask, result: Struct) -> None:
    """Test that the output of a call to ``run`` conforms to its own
    connections.

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task whose connections need validation. This is a fully-configured
        task object to support features such as optional outputs.
    result : `lsst.pipe.base.Struct`
        A result object produced by calling ``task.run``.

    Raises
    ------
    AssertionError:
        Raised if ``result`` does not match what's expected from ``task's``
        connections.
    """
    # This is a type ignore, because `connections` is a dynamic class, but
    # it for sure will have this property
    connections = task.config.ConnectionsClass(config=task.config)  # type: ignore

    for name in connections.outputs:
        connection = connections.__getattribute__(name)
        _assertAttributeMatchesConnection(result, name, connection)


def assertValidInitOutput(task: PipelineTask) -> None:
    """Test that a constructed task conforms to its own init-connections.

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task whose connections need validation.

    Raises
    ------
    AssertionError:
        Raised if ``task`` does not have the state expected from ``task's``
        connections.
    """
    # This is a type ignore, because `connections` is a dynamic class, but
    # it for sure will have this property
    connections = task.config.ConnectionsClass(config=task.config)  # type: ignore

    for name in connections.initOutputs:
        connection = connections.__getattribute__(name)
        _assertAttributeMatchesConnection(task, name, connection)


def getInitInputs(butler: Butler, config: PipelineTaskConfig) -> Dict[str, Any]:
    """Return the initInputs object that would have been passed to a
    `~lsst.pipe.base.PipelineTask` constructor.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository to search for input datasets. Must have
        pre-configured collections.
    config : `lsst.pipe.base.PipelineTaskConfig`
        The config for the task to be constructed.

    Returns
    -------
    initInputs : `dict` [`str`]
        A dictionary of objects in the format of the ``initInputs`` parameter
        to `lsst.pipe.base.PipelineTask`.
    """
    connections = config.connections.ConnectionsClass(config=config)
    initInputs = {}
    for name in connections.initInputs:
        attribute = getattr(connections, name)
        # Get full dataset type to check for consistency problems
        dsType = DatasetType(
            attribute.name, butler.registry.dimensions.extract(set()), attribute.storageClass
        )
        # All initInputs have empty data IDs
        initInputs[name] = butler.get(dsType)

    return initInputs


def lintConnections(
    connections: PipelineTaskConnections,
    *,
    checkMissingMultiple: bool = True,
    checkUnnecessaryMultiple: bool = True,
) -> None:
    """Inspect a connections class for common errors.

    These tests are designed to detect misuse of connections features in
    standard designs. An unusually designed connections class may trigger
    alerts despite being correctly written; specific checks can be turned off
    using keywords.

    Parameters
    ----------
    connections : `lsst.pipe.base.PipelineTaskConnections`-type
        The connections class to test.
    checkMissingMultiple : `bool`
        Whether to test for single connections that would match multiple
        datasets at run time.
    checkUnnecessaryMultiple : `bool`
        Whether to test for multiple connections that would only match
        one dataset.

    Raises
    ------
    AssertionError
        Raised if any of the selected checks fail for any connection.
    """
    # Since all comparisons are inside the class, don't bother
    # normalizing skypix.
    quantumDimensions = connections.dimensions

    errors = ""
    # connectionTypes.DimensionedConnection is implementation detail,
    # don't use it.
    for name in itertools.chain(connections.inputs, connections.prerequisiteInputs, connections.outputs):
        connection: DimensionedConnection = connections.allConnections[name]  # type: ignore
        connDimensions = set(connection.dimensions)
        if checkMissingMultiple and not connection.multiple and connDimensions > quantumDimensions:
            errors += (
                f"Connection {name} may be called with multiple values of "
                f"{connDimensions - quantumDimensions} but has multiple=False.\n"
            )
        if checkUnnecessaryMultiple and connection.multiple and connDimensions <= quantumDimensions:
            errors += (
                f"Connection {name} has multiple=True but can only be called with one "
                f"value of {connDimensions} for each {quantumDimensions}.\n"
            )
    if errors:
        raise AssertionError(errors)
