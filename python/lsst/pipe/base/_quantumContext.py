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

"""Module defining variants for valid values used to constrain datasets in a
graph building query.
"""

from __future__ import annotations

__all__ = ("ExecutionResources", "QuantumContext")

import numbers
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import astropy.units as u
from lsst.daf.butler import DatasetRef, DimensionUniverse, LimitedButler, Quantum
from lsst.utils.introspection import get_full_type_name
from lsst.utils.logging import PeriodicLogger, getLogger

from .connections import DeferredDatasetRef, InputQuantizedConnection, OutputQuantizedConnection
from .struct import Struct

_LOG = getLogger(__name__)


@dataclass(init=False, frozen=True)
class ExecutionResources:
    """A description of the resources available to a running quantum.

    Parameters
    ----------
    num_cores : `int`, optional
        The number of cores allocated to the task.
    max_mem : `~astropy.units.Quantity`, `numbers.Real`, `str`, or `None`,\
            optional
        The amount of memory allocated to the task. Can be specified
        as byte-compatible `~astropy.units.Quantity`, a plain number,
        a string with a plain number, or a string representing a quantity.
        If `None` no limit is specified.
    default_mem_units : `astropy.units.Unit`, optional
        The default unit to apply when the ``max_mem`` value is given
        as a plain number.
    """

    num_cores: int = 1
    """The maximum number of cores that the task can use."""

    max_mem: u.Quantity | None = None
    """If defined, the amount of memory allocated to the task.
    """

    def __init__(
        self,
        *,
        num_cores: int = 1,
        max_mem: u.Quantity | numbers.Real | str | None = None,
        default_mem_units: u.Unit = u.B,
    ):
        # Create our own __init__ to allow more flexible input parameters
        # but with a constrained dataclass definition.
        if num_cores < 1:
            raise ValueError("The number of cores must be a positive integer")

        object.__setattr__(self, "num_cores", num_cores)

        mem: u.Quantity | None = None

        if max_mem is None or isinstance(max_mem, u.Quantity):
            mem = max_mem
        elif max_mem == "":
            # Some command line tooling can treat no value as empty string.
            pass
        else:
            parsed_mem = None
            try:
                parsed_mem = float(max_mem)
            except ValueError:
                pass
            else:
                mem = parsed_mem * default_mem_units

            if mem is None:
                mem = u.Quantity(max_mem)

        if mem is not None:
            # Force to bytes. This also checks that we can convert to bytes.
            mem = mem.to(u.B)

        object.__setattr__(self, "max_mem", mem)

    def __deepcopy__(self, memo: Any) -> ExecutionResources:
        """Deep copy returns itself because the class is frozen."""
        return self

    def _reduce_kwargs(self) -> dict[str, Any]:
        """Return a dict of the keyword arguments that should be used
        by `__reduce__`.

        This is necessary because the dataclass is defined to be keyword
        only and we wish the default pickling to only store a plain number
        for the memory allocation and not a large Quantity.

        Returns
        -------
        kwargs : `dict`
            Keyword arguments to be used when pickling.
        """
        kwargs: dict[str, Any] = {"num_cores": self.num_cores}
        if self.max_mem is not None:
            # .value is a numpy float. Cast it to a python int since we
            # do not want fractional bytes. The constructor ensures that this
            # uses units of byte so we do not have to convert.
            kwargs["max_mem"] = int(self.max_mem.value)
        return kwargs

    @staticmethod
    def _unpickle_via_factory(
        cls: type[ExecutionResources], args: Sequence[Any], kwargs: dict[str, Any]
    ) -> ExecutionResources:
        """Unpickle something by calling a factory.

        Allows unpickle using `__reduce__` with keyword
        arguments as well as positional arguments.
        """
        return cls(**kwargs)

    def __reduce__(
        self,
    ) -> tuple[
        Callable[[type[ExecutionResources], Sequence[Any], dict[str, Any]], ExecutionResources],
        tuple[type[ExecutionResources], Sequence[Any], dict[str, Any]],
    ]:
        """Pickler."""
        return self._unpickle_via_factory, (self.__class__, [], self._reduce_kwargs())


class QuantumContext:
    """A Butler-like class specialized for a single quantum along with
    context information that can influence how the task is executed.

    Parameters
    ----------
    butler : `lsst.daf.butler.LimitedButler`
        Butler object from/to which datasets will be get/put.
    quantum : `lsst.daf.butler.Quantum`
        Quantum object that describes the datasets which will be get/put by a
        single execution of this node in the pipeline graph.
    resources : `ExecutionResources`, optional
        The resources allocated for executing quanta.

    Notes
    -----
    A `QuantumContext` class wraps a standard butler interface and
    specializes it to the context of a given quantum. What this means
    in practice is that the only gets and puts that this class allows
    are DatasetRefs that are contained in the quantum.

    In the future this class will also be used to record provenance on
    what was actually get and put. This is in contrast to what the
    preflight expects to be get and put by looking at the graph before
    execution.
    """

    resources: ExecutionResources

    def __init__(
        self, butler: LimitedButler, quantum: Quantum, *, resources: ExecutionResources | None = None
    ):
        self.quantum = quantum
        if resources is None:
            resources = ExecutionResources()
        self.resources = resources

        self.allInputs = set()
        self.allOutputs = set()
        for refs in quantum.inputs.values():
            for ref in refs:
                self.allInputs.add((ref.datasetType, ref.dataId))
        for refs in quantum.outputs.values():
            for ref in refs:
                self.allOutputs.add((ref.datasetType, ref.dataId))
        self.__butler = butler

    def _get(self, ref: DeferredDatasetRef | DatasetRef | None) -> Any:
        # Butler methods below will check for unresolved DatasetRefs and
        # raise appropriately, so no need for us to do that here.
        if isinstance(ref, DeferredDatasetRef):
            self._checkMembership(ref.datasetRef, self.allInputs)
            return self.__butler.getDeferred(ref.datasetRef)
        elif ref is None:
            return None
        else:
            self._checkMembership(ref, self.allInputs)
            return self.__butler.get(ref)

    def _put(self, value: Any, ref: DatasetRef) -> None:
        """Store data in butler."""
        self._checkMembership(ref, self.allOutputs)
        self.__butler.put(value, ref)

    def get(
        self,
        dataset: (
            InputQuantizedConnection
            | list[DatasetRef | None]
            | list[DeferredDatasetRef | None]
            | DatasetRef
            | DeferredDatasetRef
            | None
        ),
    ) -> Any:
        """Fetch data from the butler.

        Parameters
        ----------
        dataset : see description
            This argument may either be an `InputQuantizedConnection` which
            describes all the inputs of a quantum, a list of
            `~lsst.daf.butler.DatasetRef`, or a single
            `~lsst.daf.butler.DatasetRef`. The function will get and return
            the corresponding datasets from the butler. If `None` is passed in
            place of a `~lsst.daf.butler.DatasetRef` then the corresponding
            returned object will be `None`.

        Returns
        -------
        return : `object`
            This function returns arbitrary objects fetched from the bulter.
            The structure these objects are returned in depends on the type of
            the input argument. If the input dataset argument is a
            `InputQuantizedConnection`, then the return type will be a
            dictionary with keys corresponding to the attributes of the
            `InputQuantizedConnection` (which in turn are the attribute
            identifiers of the connections). If the input argument is of type
            `list` of `~lsst.daf.butler.DatasetRef` then the return type will
            be a list of objects.  If the input argument is a single
            `~lsst.daf.butler.DatasetRef` then a single object will be
            returned.

        Raises
        ------
        ValueError
            Raised if a `~lsst.daf.butler.DatasetRef` is passed to get that is
            not defined in the quantum object
        """
        # Set up a periodic logger so log messages can be issued if things
        # are taking too long.
        periodic = PeriodicLogger(_LOG)

        if isinstance(dataset, InputQuantizedConnection):
            retVal = {}
            n_connections = len(dataset)
            n_retrieved = 0
            for i, (name, ref) in enumerate(dataset):
                if isinstance(ref, list | tuple):
                    val = []
                    n_refs = len(ref)
                    for j, r in enumerate(ref):
                        val.append(self._get(r))
                        n_retrieved += 1
                        periodic.log(
                            "Retrieved %d out of %d datasets for connection '%s' (%d out of %d)",
                            j + 1,
                            n_refs,
                            name,
                            i + 1,
                            n_connections,
                        )
                else:
                    val = self._get(ref)
                    periodic.log(
                        "Retrieved dataset for connection '%s' (%d out of %d)",
                        name,
                        i + 1,
                        n_connections,
                    )
                    n_retrieved += 1
                retVal[name] = val
            if periodic.num_issued > 0:
                # This took long enough that we issued some periodic log
                # messages, so issue a final confirmation message as well.
                _LOG.verbose(
                    "Completed retrieval of %d datasets from %d connections", n_retrieved, n_connections
                )
            return retVal
        elif isinstance(dataset, list | tuple):
            n_datasets = len(dataset)
            retrieved = []
            for i, x in enumerate(dataset):
                # Mypy is not sure of the type of x because of the union
                # of lists so complains. Ignoring it is more efficient
                # than adding an isinstance assert.
                retrieved.append(self._get(x))
                periodic.log("Retrieved %d out of %d datasets", i + 1, n_datasets)
            if periodic.num_issued > 0:
                _LOG.verbose("Completed retrieval of %d datasets", n_datasets)
            return retrieved
        elif isinstance(dataset, DatasetRef | DeferredDatasetRef) or dataset is None:
            return self._get(dataset)
        else:
            raise TypeError(
                f"Dataset argument ({get_full_type_name(dataset)}) is not a type that can be used to get"
            )

    def put(
        self,
        values: Struct | list[Any] | Any,
        dataset: OutputQuantizedConnection | list[DatasetRef] | DatasetRef,
    ) -> None:
        """Put data into the butler.

        Parameters
        ----------
        values : `Struct` or `list` of `object` or `object`
            The data that should be put with the butler. If the type of the
            dataset is `OutputQuantizedConnection` then this argument should be
            a `Struct` with corresponding attribute names. Each attribute
            should then correspond to either a list of object or a single
            object depending of the type of the corresponding attribute on
            dataset. I.e. if ``dataset.calexp`` is
            ``[datasetRef1, datasetRef2]`` then ``values.calexp`` should be
            ``[calexp1, calexp2]``. Like wise if there is a single ref, then
            only a single object need be passed. The same restriction applies
            if dataset is directly a `list` of `~lsst.daf.butler.DatasetRef`
            or a single `~lsst.daf.butler.DatasetRef`. If ``values.NAME`` is
            None, no output is written.
        dataset : `OutputQuantizedConnection` or `list`[`DatasetRef`] \
                or `DatasetRef`
            This argument may either be an `InputQuantizedConnection` which
            describes all the inputs of a quantum, a list of
            `lsst.daf.butler.DatasetRef`, or a single
            `lsst.daf.butler.DatasetRef`. The function will get and return
            the corresponding datasets from the butler.

        Raises
        ------
        ValueError
            Raised if a `~lsst.daf.butler.DatasetRef` is passed to put that is
            not defined in the `~lsst.daf.butler.Quantum` object, or the type
            of values does not match what is expected from the type of dataset.
        """
        if isinstance(dataset, OutputQuantizedConnection):
            if not isinstance(values, Struct):
                raise ValueError(
                    "dataset is a OutputQuantizedConnection, a Struct with corresponding"
                    " attributes must be passed as the values to put"
                )
            for name, refs in dataset:
                if (valuesAttribute := getattr(values, name, None)) is None:
                    continue
                if isinstance(refs, list | tuple):
                    if len(refs) != len(valuesAttribute):
                        raise ValueError(f"There must be a object to put for every Dataset ref in {name}")
                    for i, ref in enumerate(refs):
                        self._put(valuesAttribute[i], ref)
                else:
                    self._put(valuesAttribute, refs)
        elif isinstance(dataset, list | tuple):
            if not isinstance(values, Sequence):
                raise ValueError("Values to put must be a sequence")
            if len(dataset) != len(values):
                raise ValueError("There must be a common number of references and values to put")
            for i, ref in enumerate(dataset):
                self._put(values[i], ref)
        elif isinstance(dataset, DatasetRef):
            self._put(values, dataset)
        else:
            raise TypeError("Dataset argument is not a type that can be used to put")

    def _checkMembership(self, ref: list[DatasetRef] | DatasetRef, inout: set) -> None:
        """Check if a `~lsst.daf.butler.DatasetRef` is part of the input
        `~lsst.daf.butler.Quantum`.

        This function will raise an exception if the `QuantumContext` is
        used to get/put a `~lsst.daf.butler.DatasetRef` which is not defined
        in the quantum.

        Parameters
        ----------
        ref : `list` [ `~lsst.daf.butler.DatasetRef` ] or \
                `~lsst.daf.butler.DatasetRef`
            Either a `list` or a single `~lsst.daf.butler.DatasetRef` to check
        inout : `set`
            The connection type to check, e.g. either an input or an output.
            This prevents both types needing to be checked for every operation,
            which may be important for Quanta with lots of
            `~lsst.daf.butler.DatasetRef`.
        """
        if not isinstance(ref, list | tuple):
            ref = [ref]
        for r in ref:
            if (r.datasetType, r.dataId) not in inout:
                raise ValueError("DatasetRef is not part of the Quantum being processed")

    @property
    def dimensions(self) -> DimensionUniverse:
        """Structure managing all dimensions recognized by this data
        repository (`~lsst.daf.butler.DimensionUniverse`).
        """
        return self.__butler.dimensions
