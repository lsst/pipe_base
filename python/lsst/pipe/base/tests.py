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


__all__ = ["makeTestRepo", "makeUniqueButler", "makeDatasetType", "expandUniqueId",
           "makeQuantum", "runQuantum", "validateOutputConnections"]


import collections.abc
import unittest.mock

import numpy as np

from lsst.daf.butler import Butler, DatasetType, DataCoordinate, DatasetRef, Quantum
from lsst.pipe.base import ButlerQuantumContext


def makeTestRepo(root, dataIds):
    """Create an empty repository with default configuration.

    Parameters
    ----------
    root : `str`
        The location of the root directory for the repository.
    dataIds : `~collections.abc.Mapping` [`str`, `iterable`]
        A mapping keyed by the dimensions used in the test. Each value
        is an iterable of names for that dimension (e.g., detector IDs for
        `"detector"`). Related dimensions (e.g., instruments and detectors)
        are linked arbitrarily.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler referring to the new repository. This Butler is provided only
        for additional setup; to keep test cases isolated, it is highly
        recommended that each test create its own Butler with a
        unique run/collection. See `makeUniqueButler`.

    Notes
    -----
    Since the values in ``dataIds`` uniquely determine the repository's
    data IDs, the IDs can be recovered by calling `expandUniqueId` so long as
    no other code has inserted dimensions into the repository registry.
    """
    Butler.makeRepo(root)
    butler = Butler(root, collection="base")
    dimensionRecords = _makeRecords(dataIds, butler.registry.dimensions)
    for dimension, records in dimensionRecords.items():
        butler.registry.insertDimensionData(dimension, *records)
    return butler


def makeUniqueButler(root):
    """Create a read/write Butler to a fresh collection.

    Parameters
    ----------
    root : `str`
        The location of the root directory for the repository. Must have
        already been initialized, e.g., through `makeTestRepo`.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler referring to a new collection in the repository at ``root``.
        The collection is (almost) guaranteed to be new.
    """
    # Create a "random" collection name; speed matters more than cryptographic guarantees
    collection = "test" + "".join((str(i) for i in np.random.randint(0, 10, size=8)))
    return Butler(root, run=collection)


def _makeRecords(dataIds, universe):
    """Create cross-linked dimension records from a collection of
    data ID values.

    Parameters
    ----------
    dataIds : `~collections.abc.Mapping` [`str`, `iterable`]
        A mapping keyed by the dimensions of interest. Each value is an
        iterable of names for that dimension (e.g., detector IDs for
        `"detector"`).
    universe : lsst.daf.butler.DimensionUniverse
        Set of all known dimensions and their relationships.

    Returns
    -------
    dataIds : `~collections.abc.Mapping` [`str`, `iterable` [`~lsst.daf.butler.DimensionRecord`]]
        A mapping keyed by the dimensions of interest, giving one dimension
        record for each input name. Related dimensions (e.g., instruments and
        detectors) are linked arbitrarily.
    """
    expandedIds = {}
    # Provide alternate keys like detector names
    for name, values in dataIds.items():
        expandedIds[name] = []
        dimension = universe[name]
        for value in values:
            expandedValue = {}
            for key in dimension.uniqueKeys:
                if key.nbytes:
                    castType = bytes
                else:
                    castType = key.dtype().python_type
                try:
                    castValue = castType(value)
                except TypeError:
                    castValue = castType()
                expandedValue[key.name] = castValue
            for key in dimension.metadata:
                if not key.nullable:
                    expandedValue[key.name] = key.dtype().python_type(value)
            expandedIds[name].append(expandedValue)

    # Pick cross-relationships arbitrarily
    for name, values in expandedIds.items():
        dimension = universe[name]
        for value in values:
            for other in dimension.graph.required:
                if other != dimension:
                    relation = expandedIds[other.name][0]
                    value[other.name] = relation[other.primaryKey.name]
            # Do not recurse, to keep the user from having to provide irrelevant dimensions
            for other in dimension.implied:
                if other != dimension and other.name in expandedIds and other.viewOf is None:
                    relation = expandedIds[other.name][0]
                    value[other.name] = relation[other.primaryKey.name]

    return {dimension: [universe[dimension].RecordClass.fromDict(value) for value in values]
            for dimension, values in expandedIds.items()}


def expandUniqueId(butler, partialId):
    """Return a complete data ID matching some criterion.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository to query.
    partialId : `~collections.abc.Mapping` [`str`, any]
        A mapping of known dimensions and values.

    Returns
    -------
    dataId : `lsst.daf.butler.DataCoordinate`
        The unique data ID that matches ``partialId``.

    Raises
    ------
    ValueError
        Raised if ``partialId`` does not uniquely identify a data ID.

    Notes
    -----
    This method will only work correctly if all dimensions attached to the
    target dimension (eg., "physical_filter" for "visit") are known to the
    repository, even if they're not needed to identify a dataset.

    Examples
    --------
    .. code-block:: py

       >>> butler = makeTestRepo(
               "testdir", {"instrument": ["notACam"], "detector": [1]})
       >>> expandUniqueId(butler, {"detector": 1})
       DataCoordinate({instrument, detector}, ('notACam', 1))
    """
    # The example is *not* a doctest because it requires dangerous I/O
    registry = butler.registry
    dimensions = registry.dimensions.extract(partialId.keys()).required

    query = " AND ".join(f"{dimension} = {value!r}" for dimension, value in partialId.items())

    dataId = [id for id in registry.queryDimensions(dimensions, where=query, expand=False)]
    if len(dataId) == 1:
        return dataId[0]
    else:
        raise ValueError(f"Found {len(dataId)} matches for {partialId}, expected 1.")


def makeDatasetType(butler, name, dimensions, storageClass):
    """Create a dataset type in a particular repository.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository to update.
    name : `str`
        The name of the dataset type.
    dimensions : `set` [`str`]
        The dimensions of the new dataset type.
    storageClass : `str`
        The storage class the dataset will use.

    Returns
    -------
    datasetType : `lsst.daf.butler.DatasetType`
        The new type.

    Raises
    ------
    ValueError
        Raised if the dimensions or storage class is invalid.

    Notes
    -----
    Dataset types are shared across all collections in a repository, so this
    function does not need to be run for each collection.
    """
    try:
        datasetType = DatasetType(name, dimensions, storageClass,
                                  universe=butler.registry.dimensions)
        butler.registry.registerDatasetType(datasetType)
        return datasetType
    except KeyError as e:
        raise ValueError from e


def makeQuantum(task, butler, dataIds):
    """Create a Quantum for a particular data ID(s).

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task whose processing the quantum represents.
    butler : `lsst.daf.butler.Butler`
        The collection the quantum refers to.
    dataIds : `collections.abc.Mapping` [`str`]
        A mapping keyed by input/output names. Values must be data IDs for
        single connections and sequences of data IDs for multiple connections.

    Returns
    -------
    quantum : `lsst.daf.butler.Quantum`
        A quantum for ``task``, when called with ``dataIds``.
    """
    quantum = Quantum(taskClass=type(task))
    connections = task.config.ConnectionsClass(config=task.config)

    try:
        for name in connections.inputs:
            connection = connections.__getattribute__(name)
            ids = _checkDataIdMultiplicity(dataIds[name], connection.multiple)
            for id in ids:
                quantum.addPredictedInput(_refFromConnection(butler, connection, id))
        for name in connections.outputs:
            connection = connections.__getattribute__(name)
            ids = _checkDataIdMultiplicity(dataIds[name], connection.multiple)
            for id in ids:
                quantum.addOutput(_refFromConnection(butler, connection, id))
        return quantum
    except KeyError as e:
        raise ValueError("Mismatch in input data.") from e


def _checkDataIdMultiplicity(dataIds, multiple):
    if multiple:
        if isinstance(dataIds, collections.abc.Sequence):
            return dataIds
        else:
            raise ValueError(f"Expected multiple data IDs, got {dataIds}.")
    else:
        if isinstance(dataIds, collections.abc.Mapping) or isinstance(dataIds, DataCoordinate):
            return [dataIds]
        else:
            raise ValueError(f"Expected single data ID, got {dataIds}.")


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
    datasetType = connection.makeDatasetType(universe)
    try:
        butler.registry.getDatasetType(datasetType.name)
    except KeyError:
        raise ValueError(f"Invalid dataset type {connection.name}.")
    try:
        return DatasetRef(
            datasetType=datasetType,
            dataId=dataId,
        )
    except KeyError as e:
        raise ValueError(f"Dataset type ({connection.name}) and ID {dataId.byName()} not compatible.") \
            from e


def runQuantum(task, butler, quantum, mockRun=True):
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


def validateOutputConnections(task, result):
    """Test that the output of a call to ``run`` conforms to its own connections.

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task whose connections need validation. This is a fully-configured
        task object to support features such as optional outputs.
    result : `lsst.pipe.base.Struct`
        A result object produced by calling ``task.run``.

    Returns
    -------
    isValid : `bool`
        `True` if ``result`` has the fields promised by ``task``, `False` if
        it does not.
    """
    connections = task.config.ConnectionsClass(config=task.config)
    recoveredOutputs = result.getDict()

    for name in connections.outputs:
        connection = connections.__getattribute__(name)
        # name
        try:
            output = recoveredOutputs[name]
        except KeyError:
            return False
        # multiple
        if connection.multiple:
            if not isinstance(output, collections.abc.Sequence):
                return False
        else:
            if isinstance(output, collections.abc.Sequence) \
                    and not issubclass(connections.storageClass.pytype, collections.abc.Sequence):
                return False
        # no test for storageClass, as I'm not sure how much persistence depends on duck-typing
    return True
