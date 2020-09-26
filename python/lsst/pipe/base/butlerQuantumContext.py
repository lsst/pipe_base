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

"""Module defining a butler like object specialized to a specific quantum.
"""

__all__ = ("ButlerQuantumContext",)

import types
import typing

from .connections import InputQuantizedConnection, OutputQuantizedConnection, DeferredDatasetRef
from .struct import Struct
from lsst.daf.butler import DatasetRef, Butler, Quantum


class ButlerQuantumContext:
    """Butler like class specialized for a single quantum

    A ButlerQuantumContext class wraps a standard butler interface and
    specializes it to the context of a given quantum. What this means
    in practice is that the only gets and puts that this class allows
    are DatasetRefs that are contained in the quantum.

    In the future this class will also be used to record provenance on
    what was actually get and put. This is in contrast to what the
    preflight expects to be get and put by looking at the graph before
    execution.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler object from/to which datasets will be get/put
    quantum : `lsst.daf.butler.core.Quantum`
        Quantum object that describes the datasets which will
        be get/put by a single execution of this node in the
        pipeline graph.
    """
    def __init__(self, butler: Butler, quantum: Quantum):
        self.quantum = quantum
        self.registry = butler.registry
        self.allInputs = set()
        self.allOutputs = set()
        for refs in quantum.predictedInputs.values():
            for ref in refs:
                self.allInputs.add((ref.datasetType, ref.dataId))
        for refs in quantum.outputs.values():
            for ref in refs:
                self.allOutputs.add((ref.datasetType, ref.dataId))

        # Create closures over butler to discourage anyone from directly
        # using a butler reference
        def _get(self, ref):
            if isinstance(ref, DeferredDatasetRef):
                self._checkMembership(ref.datasetRef, self.allInputs)
                return butler.getDirectDeferred(ref.datasetRef)

            else:
                self._checkMembership(ref, self.allInputs)
                return butler.getDirect(ref)

        def _put(self, value, ref):
            self._checkMembership(ref, self.allOutputs)
            butler.put(value, ref)

        self._get = types.MethodType(_get, self)
        self._put = types.MethodType(_put, self)

    def get(self, dataset: typing.Union[InputQuantizedConnection,
                                        typing.List[DatasetRef],
                                        DatasetRef]) -> object:
        """Fetches data from the butler

        Parameters
        ----------
        dataset
            This argument may either be an `InputQuantizedConnection` which describes
            all the inputs of a quantum, a list of `~lsst.daf.butler.DatasetRef`, or
            a single `~lsst.daf.butler.DatasetRef`. The function will get and return
            the corresponding datasets from the butler.

        Returns
        -------
        return : `object`
            This function returns arbitrary objects fetched from the bulter. The
            structure these objects are returned in depends on the type of the input
            argument. If the input dataset argument is a InputQuantizedConnection, then
            the return type will be a dictionary with keys corresponding to the attributes
            of the `InputQuantizedConnection` (which in turn are the attribute identifiers
            of the connections). If the input argument is of type `list` of
            `~lsst.daf.butler.DatasetRef` then the return type  will be a list of objects.
            If the input argument is a single `~lsst.daf.butler.DatasetRef` then a single
            object will be returned.

        Raises
        ------
        ValueError
            If a `DatasetRef` is passed to get that is not defined in the quantum object
        """
        if isinstance(dataset, InputQuantizedConnection):
            retVal = {}
            for name, ref in dataset:
                if isinstance(ref, list):
                    val = [self._get(r) for r in ref]
                else:
                    val = self._get(ref)
                retVal[name] = val
            return retVal
        elif isinstance(dataset, list):
            return [self._get(x) for x in dataset]
        elif isinstance(dataset, DatasetRef) or isinstance(dataset, DeferredDatasetRef):
            return self._get(dataset)
        else:
            raise TypeError("Dataset argument is not a type that can be used to get")

    def put(self, values: typing.Union[Struct, typing.List[typing.Any], object],
            dataset: typing.Union[OutputQuantizedConnection, typing.List[DatasetRef], DatasetRef]):
        """Puts data into the butler

        Parameters
        ----------
        values : `Struct` or `list` of `object` or `object`
            The data that should be put with the butler. If the type of the dataset
            is `OutputQuantizedConnection` then this argument should be a `Struct`
            with corresponding attribute names. Each attribute should then correspond
            to either a list of object or a single object depending of the type of the
            corresponding attribute on dataset. I.e. if dataset.calexp is [datasetRef1,
            datasetRef2] then values.calexp should be [calexp1, calexp2]. Like wise
            if there is a single ref, then only a single object need be passed. The same
            restriction applies if dataset is directly a `list` of `DatasetRef` or a
            single `DatasetRef`.
        dataset
            This argument may either be an `InputQuantizedConnection` which describes
            all the inputs of a quantum, a list of `lsst.daf.butler.DatasetRef`, or
            a single `lsst.daf.butler.DatasetRef`. The function will get and return
            the corresponding datasets from the butler.

        Raises
        ------
        ValueError
            If a `DatasetRef` is passed to put that is not defined in the quantum object, or
            the type of values does not match what is expected from the type of dataset.
        """
        if isinstance(dataset, OutputQuantizedConnection):
            if not isinstance(values, Struct):
                raise ValueError("dataset is a OutputQuantizedConnection, a Struct with corresponding"
                                 " attributes must be passed as the values to put")
            for name, refs in dataset:
                valuesAttribute = getattr(values, name)
                if isinstance(refs, list):
                    if len(refs) != len(valuesAttribute):
                        raise ValueError(f"There must be a object to put for every Dataset ref in {name}")
                    for i, ref in enumerate(refs):
                        self._put(valuesAttribute[i], ref)
                else:
                    self._put(valuesAttribute, refs)
        elif isinstance(dataset, list):
            if len(dataset) != len(values):
                raise ValueError("There must be a common number of references and values to put")
            for i, ref in enumerate(dataset):
                self._put(values[i], ref)
        elif isinstance(dataset, DatasetRef):
            self._put(values, dataset)
        else:
            raise TypeError("Dataset argument is not a type that can be used to put")

    def _checkMembership(self, ref: typing.Union[typing.List[DatasetRef], DatasetRef], inout: set):
        """Internal function used to check if a DatasetRef is part of the input quantum

        This function will raise an exception if the ButlerQuantumContext is used to
        get/put a DatasetRef which is not defined in the quantum.

        Parameters
        ----------
        ref : `list` of `DatasetRef` or `DatasetRef`
            Either a list or a single `DatasetRef` to check
        inout : `set`
            The connection type to check, e.g. either an input or an output. This prevents
            both types needing to be checked for every operation, which may be important
            for Quanta with lots of `DatasetRef`s.
        """
        if not isinstance(ref, list):
            ref = [ref]
        for r in ref:
            if (r.datasetType, r.dataId) not in inout:
                raise ValueError("DatasetRef is not part of the Quantum being processed")
