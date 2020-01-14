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


__all__ = ["refFromConnection", "runTestQuantum"]


from lsst.daf.butler import DataCoordinate, DatasetRef
from lsst.pipe.base import ButlerQuantumContext


def refFromConnection(butler, connection, dataId, **kwargs):
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
    return DatasetRef(
        datasetType=connection.makeDatasetType(universe),
        dataId=dataId,
    )


def runTestQuantum(task, butler, quantum):
    """Run a PipelineTask on a Quantum.

    Parameters
    ----------
    task : `lsst.pipe.base.PipelineTask`
        The task to run on the quantum.
    butler : `lsst.daf.butler.Butler`
        The collection to run on.
    quantum : `lsst.daf.butler.Quantum`
        The quantum to run.
    """
    butlerQc = ButlerQuantumContext(butler, quantum)
    connections = task.config.ConnectionsClass(config=task.config)
    inputRefs, outputRefs = connections.buildDatasetRefs(quantum)
    task.runQuantum(butlerQc, inputRefs, outputRefs)
