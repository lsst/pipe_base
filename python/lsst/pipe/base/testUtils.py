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


__all__ = ["makeQuantum", "runTestQuantum", "assertValidOutput"]


import collections.abc
import itertools
import unittest.mock

from lsst.daf.butler import DataCoordinate, DatasetRef, Quantum, StorageClassFactory
from lsst.pipe.base import ButlerQuantumContext


def makeQuantum(task, butler, dataId, ioDataIds):
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
    quantum = Quantum(taskClass=type(task), dataId=dataId)
    connections = task.config.ConnectionsClass(config=task.config)

    try:
        for name in itertools.chain(connections.inputs, connections.prerequisiteInputs):
            connection = connections.__getattribute__(name)
            _checkDataIdMultiplicity(name, ioDataIds[name], connection.multiple)
            ids = _normalizeDataIds(ioDataIds[name])
            for id in ids:
                quantum.addPredictedInput(_refFromConnection(butler, connection, id))
        for name in connections.outputs:
            connection = connections.__getattribute__(name)
            _checkDataIdMultiplicity(name, ioDataIds[name], connection.multiple)
            ids = _normalizeDataIds(ioDataIds[name])
            for id in ids:
                quantum.addOutput(_refFromConnection(butler, connection, id))
        return quantum
    except KeyError as e:
        raise ValueError("Mismatch in input data.") from e


def _checkDataIdMultiplicity(name, dataIds, multiple):
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


def _normalizeDataIds(dataIds):
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


def _refFromConnection(butler, connection, dataId, **kwargs):
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
    dataId = DataCoordinate.standardize(dataId, **kwargs, universe=universe)

    # skypix is a PipelineTask alias for "some spatial index", Butler doesn't
    # understand it. Code copied from TaskDatasetTypes.fromTaskDef
    if "skypix" in connection.dimensions:
        datasetType = butler.registry.getDatasetType(connection.name)
    else:
        datasetType = connection.makeDatasetType(universe)

    try:
        butler.registry.getDatasetType(datasetType.name)
    except KeyError:
        raise ValueError(f"Invalid dataset type {connection.name}.")
    try:
        ref = DatasetRef(datasetType=datasetType, dataId=dataId)
        return ref
    except KeyError as e:
        raise ValueError(f"Dataset type ({connection.name}) and ID {dataId.byName()} not compatible.") \
            from e


def _resolveTestQuantumInputs(butler, quantum):
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
    for refsForDatasetType in quantum.predictedInputs.values():
        newRefsForDatasetType = []
        for ref in refsForDatasetType:
            if ref.id is None:
                resolvedRef = butler.registry.findDataset(ref.datasetType, ref.dataId,
                                                          collections=butler.collections)
                if resolvedRef is None:
                    raise ValueError(
                        f"Cannot find {ref.datasetType.name} with id {ref.dataId} "
                        f"in collections {butler.collections}."
                    )
                newRefsForDatasetType.append(resolvedRef)
            else:
                newRefsForDatasetType.append(ref)
        refsForDatasetType[:] = newRefsForDatasetType


def runTestQuantum(task, butler, quantum, mockRun=True):
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
    butlerQc = ButlerQuantumContext(butler, quantum)
    connections = task.config.ConnectionsClass(config=task.config)
    inputRefs, outputRefs = connections.buildDatasetRefs(quantum)
    if mockRun:
        with unittest.mock.patch.object(task, "run") as mock, \
                unittest.mock.patch("lsst.pipe.base.ButlerQuantumContext.put"):
            task.runQuantum(butlerQc, inputRefs, outputRefs)
            return mock
    else:
        task.runQuantum(butlerQc, inputRefs, outputRefs)
        return None


def assertValidOutput(task, result):
    """Test that the output of a call to ``run`` conforms to its own connections.

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task whose connections need validation. This is a fully-configured
        task object to support features such as optional outputs.
    result : `lsst.pipe.base.Struct`
        A result object produced by calling ``task.run``.

    Raises
    -------
    AssertionError:
        Raised if ``result`` does not match what's expected from ``task's``
        connections.
    """
    connections = task.config.ConnectionsClass(config=task.config)
    recoveredOutputs = result.getDict()

    for name in connections.outputs:
        connection = connections.__getattribute__(name)
        # name
        try:
            output = recoveredOutputs[name]
        except KeyError:
            raise AssertionError(f"No such output: {name}")
        # multiple
        if connection.multiple:
            if not isinstance(output, collections.abc.Sequence):
                raise AssertionError(f"Expected {name} to be a sequence, got {output} instead.")
        else:
            # use lazy evaluation to not use StorageClassFactory unless necessary
            if isinstance(output, collections.abc.Sequence) \
                    and not issubclass(
                        StorageClassFactory().getStorageClass(connection.storageClass).pytype,
                        collections.abc.Sequence):
                raise AssertionError(f"Expected {name} to be a single value, got {output} instead.")
        # no test for storageClass, as I'm not sure how much persistence depends on duck-typing
