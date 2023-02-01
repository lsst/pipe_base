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

"""Module defining connection classes for PipelineTask.
"""

from __future__ import annotations

__all__ = [
    "AdjustQuantumHelper",
    "DeferredDatasetRef",
    "InputQuantizedConnection",
    "OutputQuantizedConnection",
    "PipelineTaskConnections",
    "ScalarError",
    "iterConnections",
    "ScalarError",
]

import itertools
import string
import typing
from collections import UserDict
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, ClassVar, Dict, Iterable, List, Set, Union

from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, NamedKeyDict, NamedKeyMapping, Quantum

from ._status import NoWorkFound
from .connectionTypes import (
    BaseConnection,
    BaseInput,
    InitInput,
    InitOutput,
    Input,
    Output,
    PrerequisiteInput,
)

if typing.TYPE_CHECKING:
    from .config import PipelineTaskConfig


class ScalarError(TypeError):
    """Exception raised when dataset type is configured as scalar
    but there are multiple data IDs in a Quantum for that dataset.
    """


class PipelineTaskConnectionDict(UserDict):
    """This is a special dict class used by PipelineTaskConnectionMetaclass

    This dict is used in PipelineTaskConnection class creation, as the
    dictionary that is initially used as __dict__. It exists to
    intercept connection fields declared in a PipelineTaskConnection, and
    what name is used to identify them. The names are then added to class
    level list according to the connection type of the class attribute. The
    names are also used as keys in a class level dictionary associated with
    the corresponding class attribute. This information is a duplicate of
    what exists in __dict__, but provides a simple place to lookup and
    iterate on only these variables.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        # Initialize class level variables used to track any declared
        # class level variables that are instances of
        # connectionTypes.BaseConnection
        self.data["inputs"] = []
        self.data["prerequisiteInputs"] = []
        self.data["outputs"] = []
        self.data["initInputs"] = []
        self.data["initOutputs"] = []
        self.data["allConnections"] = {}

    def __setitem__(self, name: str, value: Any) -> None:
        if isinstance(value, Input):
            self.data["inputs"].append(name)
        elif isinstance(value, PrerequisiteInput):
            self.data["prerequisiteInputs"].append(name)
        elif isinstance(value, Output):
            self.data["outputs"].append(name)
        elif isinstance(value, InitInput):
            self.data["initInputs"].append(name)
        elif isinstance(value, InitOutput):
            self.data["initOutputs"].append(name)
        # This should not be an elif, as it needs tested for
        # everything that inherits from BaseConnection
        if isinstance(value, BaseConnection):
            object.__setattr__(value, "varName", name)
            self.data["allConnections"][name] = value
        # defer to the default behavior
        super().__setitem__(name, value)


class PipelineTaskConnectionsMetaclass(type):
    """Metaclass used in the declaration of PipelineTaskConnections classes"""

    def __prepare__(name, bases, **kwargs):  # noqa: 805
        # Create an instance of our special dict to catch and track all
        # variables that are instances of connectionTypes.BaseConnection
        # Copy any existing connections from a parent class
        dct = PipelineTaskConnectionDict()
        for base in bases:
            if isinstance(base, PipelineTaskConnectionsMetaclass):
                for name, value in base.allConnections.items():
                    dct[name] = value
        return dct

    def __new__(cls, name, bases, dct, **kwargs):
        dimensionsValueError = TypeError(
            "PipelineTaskConnections class must be created with a dimensions "
            "attribute which is an iterable of dimension names"
        )

        if name != "PipelineTaskConnections":
            # Verify that dimensions are passed as a keyword in class
            # declaration
            if "dimensions" not in kwargs:
                for base in bases:
                    if hasattr(base, "dimensions"):
                        kwargs["dimensions"] = base.dimensions
                        break
                if "dimensions" not in kwargs:
                    raise dimensionsValueError
            try:
                if isinstance(kwargs["dimensions"], str):
                    raise TypeError(
                        "Dimensions must be iterable of dimensions, got str,possibly omitted trailing comma"
                    )
                if not isinstance(kwargs["dimensions"], typing.Iterable):
                    raise TypeError("Dimensions must be iterable of dimensions")
                dct["dimensions"] = set(kwargs["dimensions"])
            except TypeError as exc:
                raise dimensionsValueError from exc
            # Lookup any python string templates that may have been used in the
            # declaration of the name field of a class connection attribute
            allTemplates = set()
            stringFormatter = string.Formatter()
            # Loop over all connections
            for obj in dct["allConnections"].values():
                nameValue = obj.name
                # add all the parameters to the set of templates
                for param in stringFormatter.parse(nameValue):
                    if param[1] is not None:
                        allTemplates.add(param[1])

            # look up any template from base classes and merge them all
            # together
            mergeDict = {}
            for base in bases[::-1]:
                if hasattr(base, "defaultTemplates"):
                    mergeDict.update(base.defaultTemplates)
            if "defaultTemplates" in kwargs:
                mergeDict.update(kwargs["defaultTemplates"])

            if len(mergeDict) > 0:
                kwargs["defaultTemplates"] = mergeDict

            # Verify that if templated strings were used, defaults were
            # supplied as an argument in the declaration of the connection
            # class
            if len(allTemplates) > 0 and "defaultTemplates" not in kwargs:
                raise TypeError(
                    "PipelineTaskConnection class contains templated attribute names, but no "
                    "defaut templates were provided, add a dictionary attribute named "
                    "defaultTemplates which contains the mapping between template key and value"
                )
            if len(allTemplates) > 0:
                # Verify all templates have a default, and throw if they do not
                defaultTemplateKeys = set(kwargs["defaultTemplates"].keys())
                templateDifference = allTemplates.difference(defaultTemplateKeys)
                if templateDifference:
                    raise TypeError(f"Default template keys were not provided for {templateDifference}")
                # Verify that templates do not share names with variable names
                # used for a connection, this is needed because of how
                # templates are specified in an associated config class.
                nameTemplateIntersection = allTemplates.intersection(set(dct["allConnections"].keys()))
                if len(nameTemplateIntersection) > 0:
                    raise TypeError(
                        "Template parameters cannot share names with Class attributes"
                        f" (conflicts are {nameTemplateIntersection})."
                    )
            dct["defaultTemplates"] = kwargs.get("defaultTemplates", {})

        # Convert all the connection containers into frozensets so they cannot
        # be modified at the class scope
        for connectionName in ("inputs", "prerequisiteInputs", "outputs", "initInputs", "initOutputs"):
            dct[connectionName] = frozenset(dct[connectionName])
        # our custom dict type must be turned into an actual dict to be used in
        # type.__new__
        return super().__new__(cls, name, bases, dict(dct))

    def __init__(cls, name, bases, dct, **kwargs):
        # This overrides the default init to drop the kwargs argument. Python
        # metaclasses will have this argument set if any kwargs are passes at
        # class construction time, but should be consumed before calling
        # __init__ on the type metaclass. This is in accordance with python
        # documentation on metaclasses
        super().__init__(name, bases, dct)


class QuantizedConnection(SimpleNamespace):
    """A Namespace to map defined variable names of connections to the
    associated `lsst.daf.butler.DatasetRef` objects.

    This class maps the names used to define a connection on a
    PipelineTaskConnectionsClass to the corresponding
    `lsst.daf.butler.DatasetRef`s provided by a `lsst.daf.butler.Quantum`
    instance.  This will be a quantum of execution based on the graph created
    by examining all the connections defined on the
    `PipelineTaskConnectionsClass`.
    """

    def __init__(self, **kwargs):
        # Create a variable to track what attributes are added. This is used
        # later when iterating over this QuantizedConnection instance
        object.__setattr__(self, "_attributes", set())

    def __setattr__(self, name: str, value: typing.Union[DatasetRef, typing.List[DatasetRef]]) -> None:
        # Capture the attribute name as it is added to this object
        self._attributes.add(name)
        super().__setattr__(name, value)

    def __delattr__(self, name):
        object.__delattr__(self, name)
        self._attributes.remove(name)

    def __len__(self) -> int:
        return len(self._attributes)

    def __iter__(
        self,
    ) -> typing.Generator[typing.Tuple[str, typing.Union[DatasetRef, typing.List[DatasetRef]]], None, None]:
        """Make an Iterator for this QuantizedConnection

        Iterating over a QuantizedConnection will yield a tuple with the name
        of an attribute and the value associated with that name. This is
        similar to dict.items() but is on the namespace attributes rather than
        dict keys.
        """
        yield from ((name, getattr(self, name)) for name in self._attributes)

    def keys(self) -> typing.Generator[str, None, None]:
        """Returns an iterator over all the attributes added to a
        QuantizedConnection class
        """
        yield from self._attributes


class InputQuantizedConnection(QuantizedConnection):
    pass


class OutputQuantizedConnection(QuantizedConnection):
    pass


@dataclass(frozen=True)
class DeferredDatasetRef:
    """A wrapper class for `DatasetRef` that indicates that a `PipelineTask`
    should receive a `DeferredDatasetHandle` instead of an in-memory dataset.

    Parameters
    ----------
    datasetRef : `lsst.daf.butler.DatasetRef`
        The `lsst.daf.butler.DatasetRef` that will be eventually used to
        resolve a dataset
    """

    datasetRef: DatasetRef

    @property
    def datasetType(self) -> DatasetType:
        """The dataset type for this dataset."""
        return self.datasetRef.datasetType

    @property
    def dataId(self) -> DataCoordinate:
        """The data ID for this dataset."""
        return self.datasetRef.dataId


class PipelineTaskConnections(metaclass=PipelineTaskConnectionsMetaclass):
    """PipelineTaskConnections is a class used to declare desired IO when a
    PipelineTask is run by an activator

    Parameters
    ----------
    config : `PipelineTaskConfig`
        A `PipelineTaskConfig` class instance whose class has been configured
        to use this `PipelineTaskConnectionsClass`

    See also
    --------
    iterConnections

    Notes
    -----
    ``PipelineTaskConnection`` classes are created by declaring class
    attributes of types defined in `lsst.pipe.base.connectionTypes` and are
    listed as follows:

    * ``InitInput`` - Defines connections in a quantum graph which are used as
      inputs to the ``__init__`` function of the `PipelineTask` corresponding
      to this class
    * ``InitOuput`` - Defines connections in a quantum graph which are to be
      persisted using a butler at the end of the ``__init__`` function of the
      `PipelineTask` corresponding to this class. The variable name used to
      define this connection should be the same as an attribute name on the
      `PipelineTask` instance. E.g. if an ``InitOutput`` is declared with
      the name ``outputSchema`` in a ``PipelineTaskConnections`` class, then
      a `PipelineTask` instance should have an attribute
      ``self.outputSchema`` defined. Its value is what will be saved by the
      activator framework.
    * ``PrerequisiteInput`` - An input connection type that defines a
      `lsst.daf.butler.DatasetType` that must be present at execution time,
      but that will not be used during the course of creating the quantum
      graph to be executed. These most often are things produced outside the
      processing pipeline, such as reference catalogs.
    * ``Input`` - Input `lsst.daf.butler.DatasetType` objects that will be used
      in the ``run`` method of a `PipelineTask`.  The name used to declare
      class attribute must match a function argument name in the ``run``
      method of a `PipelineTask`. E.g. If the ``PipelineTaskConnections``
      defines an ``Input`` with the name ``calexp``, then the corresponding
      signature should be ``PipelineTask.run(calexp, ...)``
    * ``Output`` - A `lsst.daf.butler.DatasetType` that will be produced by an
      execution of a `PipelineTask`. The name used to declare the connection
      must correspond to an attribute of a `Struct` that is returned by a
      `PipelineTask` ``run`` method.  E.g. if an output connection is
      defined with the name ``measCat``, then the corresponding
      ``PipelineTask.run`` method must return ``Struct(measCat=X,..)`` where
      X matches the ``storageClass`` type defined on the output connection.

    The process of declaring a ``PipelineTaskConnection`` class involves
    parameters passed in the declaration statement.

    The first parameter is ``dimensions`` which is an iterable of strings which
    defines the unit of processing the run method of a corresponding
    `PipelineTask` will operate on. These dimensions must match dimensions that
    exist in the butler registry which will be used in executing the
    corresponding `PipelineTask`.

    The second parameter is labeled ``defaultTemplates`` and is conditionally
    optional. The name attributes of connections can be specified as python
    format strings, with named format arguments. If any of the name parameters
    on connections defined in a `PipelineTaskConnections` class contain a
    template, then a default template value must be specified in the
    ``defaultTemplates`` argument. This is done by passing a dictionary with
    keys corresponding to a template identifier, and values corresponding to
    the value to use as a default when formatting the string. For example if
    ``ConnectionClass.calexp.name = '{input}Coadd_calexp'`` then
    ``defaultTemplates`` = {'input': 'deep'}.

    Once a `PipelineTaskConnections` class is created, it is used in the
    creation of a `PipelineTaskConfig`. This is further documented in the
    documentation of `PipelineTaskConfig`. For the purposes of this
    documentation, the relevant information is that the config class allows
    configuration of connection names by users when running a pipeline.

    Instances of a `PipelineTaskConnections` class are used by the pipeline
    task execution framework to introspect what a corresponding `PipelineTask`
    will require, and what it will produce.

    Examples
    --------
    >>> from lsst.pipe.base import connectionTypes as cT
    >>> from lsst.pipe.base import PipelineTaskConnections
    >>> from lsst.pipe.base import PipelineTaskConfig
    >>> class ExampleConnections(PipelineTaskConnections,
    ...                          dimensions=("A", "B"),
    ...                          defaultTemplates={"foo": "Example"}):
    ...     inputConnection = cT.Input(doc="Example input",
    ...                                dimensions=("A", "B"),
    ...                                storageClass=Exposure,
    ...                                name="{foo}Dataset")
    ...     outputConnection = cT.Output(doc="Example output",
    ...                                  dimensions=("A", "B"),
    ...                                  storageClass=Exposure,
    ...                                  name="{foo}output")
    >>> class ExampleConfig(PipelineTaskConfig,
    ...                     pipelineConnections=ExampleConnections):
    ...    pass
    >>> config = ExampleConfig()
    >>> config.connections.foo = Modified
    >>> config.connections.outputConnection = "TotallyDifferent"
    >>> connections = ExampleConnections(config=config)
    >>> assert(connections.inputConnection.name == "ModifiedDataset")
    >>> assert(connections.outputConnection.name == "TotallyDifferent")
    """

    dimensions: ClassVar[Set[str]]

    def __init__(self, *, config: "PipelineTaskConfig" | None = None):
        self.inputs: Set[str] = set(self.inputs)
        self.prerequisiteInputs: Set[str] = set(self.prerequisiteInputs)
        self.outputs: Set[str] = set(self.outputs)
        self.initInputs: Set[str] = set(self.initInputs)
        self.initOutputs: Set[str] = set(self.initOutputs)
        self.allConnections: Dict[str, BaseConnection] = dict(self.allConnections)

        from .config import PipelineTaskConfig  # local import to avoid cycle

        if config is None or not isinstance(config, PipelineTaskConfig):
            raise ValueError(
                "PipelineTaskConnections must be instantiated with a PipelineTaskConfig instance"
            )
        self.config = config
        # Extract the template names that were defined in the config instance
        # by looping over the keys of the defaultTemplates dict specified at
        # class declaration time
        templateValues = {
            name: getattr(config.connections, name) for name in getattr(self, "defaultTemplates").keys()
        }
        # Extract the configured value corresponding to each connection
        # variable. I.e. for each connection identifier, populate a override
        # for the connection.name attribute
        self._nameOverrides = {
            name: getattr(config.connections, name).format(**templateValues)
            for name in self.allConnections.keys()
        }

        # connections.name corresponds to a dataset type name, create a reverse
        # mapping that goes from dataset type name to attribute identifier name
        # (variable name) on the connection class
        self._typeNameToVarName = {v: k for k, v in self._nameOverrides.items()}

    def buildDatasetRefs(
        self, quantum: Quantum
    ) -> typing.Tuple[InputQuantizedConnection, OutputQuantizedConnection]:
        """Builds QuantizedConnections corresponding to input Quantum

        Parameters
        ----------
        quantum : `lsst.daf.butler.Quantum`
            Quantum object which defines the inputs and outputs for a given
            unit of processing

        Returns
        -------
        retVal : `tuple` of (`InputQuantizedConnection`,
            `OutputQuantizedConnection`) Namespaces mapping attribute names
            (identifiers of connections) to butler references defined in the
            input `lsst.daf.butler.Quantum`
        """
        inputDatasetRefs = InputQuantizedConnection()
        outputDatasetRefs = OutputQuantizedConnection()
        # operate on a reference object and an interable of names of class
        # connection attributes
        for refs, names in zip(
            (inputDatasetRefs, outputDatasetRefs),
            (itertools.chain(self.inputs, self.prerequisiteInputs), self.outputs),
        ):
            # get a name of a class connection attribute
            for attributeName in names:
                # get the attribute identified by name
                attribute = getattr(self, attributeName)
                # Branch if the attribute dataset type is an input
                if attribute.name in quantum.inputs:
                    # if the dataset is marked to load deferred, wrap it in a
                    # DeferredDatasetRef
                    quantumInputRefs: Union[List[DatasetRef], List[DeferredDatasetRef]]
                    if attribute.deferLoad:
                        quantumInputRefs = [
                            DeferredDatasetRef(datasetRef=ref) for ref in quantum.inputs[attribute.name]
                        ]
                    else:
                        quantumInputRefs = list(quantum.inputs[attribute.name])
                    # Unpack arguments that are not marked multiples (list of
                    # length one)
                    if not attribute.multiple:
                        if len(quantumInputRefs) > 1:
                            raise ScalarError(
                                "Received multiple datasets "
                                f"{', '.join(str(r.dataId) for r in quantumInputRefs)} "
                                f"for scalar connection {attributeName} "
                                f"({quantumInputRefs[0].datasetType.name}) "
                                f"of quantum for {quantum.taskName} with data ID {quantum.dataId}."
                            )
                        if len(quantumInputRefs) == 0:
                            continue
                        setattr(refs, attributeName, quantumInputRefs[0])
                    else:
                        # Add to the QuantizedConnection identifier
                        setattr(refs, attributeName, quantumInputRefs)
                # Branch if the attribute dataset type is an output
                elif attribute.name in quantum.outputs:
                    value = quantum.outputs[attribute.name]
                    # Unpack arguments that are not marked multiples (list of
                    # length one)
                    if not attribute.multiple:
                        setattr(refs, attributeName, value[0])
                    else:
                        setattr(refs, attributeName, value)
                # Specified attribute is not in inputs or outputs dont know how
                # to handle, throw
                else:
                    raise ValueError(
                        f"Attribute with name {attributeName} has no counterpoint in input quantum"
                    )
        return inputDatasetRefs, outputDatasetRefs

    def adjustQuantum(
        self,
        inputs: typing.Dict[str, typing.Tuple[BaseInput, typing.Collection[DatasetRef]]],
        outputs: typing.Dict[str, typing.Tuple[Output, typing.Collection[DatasetRef]]],
        label: str,
        data_id: DataCoordinate,
    ) -> typing.Tuple[
        typing.Mapping[str, typing.Tuple[BaseInput, typing.Collection[DatasetRef]]],
        typing.Mapping[str, typing.Tuple[Output, typing.Collection[DatasetRef]]],
    ]:
        """Override to make adjustments to `lsst.daf.butler.DatasetRef` objects
        in the `lsst.daf.butler.core.Quantum` during the graph generation stage
        of the activator.

        Parameters
        ----------
        inputs : `dict`
            Dictionary whose keys are an input (regular or prerequisite)
            connection name and whose values are a tuple of the connection
            instance and a collection of associated `DatasetRef` objects.
            The exact type of the nested collections is unspecified; it can be
            assumed to be multi-pass iterable and support `len` and ``in``, but
            it should not be mutated in place.  In contrast, the outer
            dictionaries are guaranteed to be temporary copies that are true
            `dict` instances, and hence may be modified and even returned; this
            is especially useful for delegating to `super` (see notes below).
        outputs : `Mapping`
            Mapping of output datasets, with the same structure as ``inputs``.
        label : `str`
            Label for this task in the pipeline (should be used in all
            diagnostic messages).
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for this quantum in the pipeline (should be used in all
            diagnostic messages).

        Returns
        -------
        adjusted_inputs : `Mapping`
            Mapping of the same form as ``inputs`` with updated containers of
            input `DatasetRef` objects.  Connections that are not changed
            should not be returned at all.  Datasets may only be removed, not
            added.  Nested collections may be of any multi-pass iterable type,
            and the order of iteration will set the order of iteration within
            `PipelineTask.runQuantum`.
        adjusted_outputs : `Mapping`
            Mapping of updated output datasets, with the same structure and
            interpretation as ``adjusted_inputs``.

        Raises
        ------
        ScalarError
            Raised if any `Input` or `PrerequisiteInput` connection has
            ``multiple`` set to `False`, but multiple datasets.
        NoWorkFound
            Raised to indicate that this quantum should not be run; not enough
            datasets were found for a regular `Input` connection, and the
            quantum should be pruned or skipped.
        FileNotFoundError
            Raised to cause QuantumGraph generation to fail (with the message
            included in this exception); not enough datasets were found for a
            `PrerequisiteInput` connection.

        Notes
        -----
        The base class implementation performs important checks.  It always
        returns an empty mapping (i.e. makes no adjustments).  It should
        always called be via `super` by custom implementations, ideally at the
        end of the custom implementation with already-adjusted mappings when
        any datasets are actually dropped, e.g.::

            def adjustQuantum(self, inputs, outputs, label, data_id):
                # Filter out some dataset refs for one connection.
                connection, old_refs = inputs["my_input"]
                new_refs = [ref for ref in old_refs if ...]
                adjusted_inputs = {"my_input", (connection, new_refs)}
                # Update the original inputs so we can pass them to super.
                inputs.update(adjusted_inputs)
                # Can ignore outputs from super because they are guaranteed
                # to be empty.
                super().adjustQuantum(inputs, outputs, label_data_id)
                # Return only the connections we modified.
                return adjusted_inputs, {}

        Removing outputs here is guaranteed to affect what is actually
        passed to `PipelineTask.runQuantum`, but its effect on the larger
        graph may be deferred to execution, depending on the context in
        which `adjustQuantum` is being run: if one quantum removes an output
        that is needed by a second quantum as input, the second quantum may not
        be adjusted (and hence pruned or skipped) until that output is actually
        found to be missing at execution time.

        Tasks that desire zip-iteration consistency between any combinations of
        connections that have the same data ID should generally implement
        `adjustQuantum` to achieve this, even if they could also run that
        logic during execution; this allows the system to see outputs that will
        not be produced because the corresponding input is missing as early as
        possible.
        """
        for name, (input_connection, refs) in inputs.items():
            dataset_type_name = input_connection.name
            if not input_connection.multiple and len(refs) > 1:
                raise ScalarError(
                    f"Found multiple datasets {', '.join(str(r.dataId) for r in refs)} "
                    f"for non-multiple input connection {label}.{name} ({dataset_type_name}) "
                    f"for quantum data ID {data_id}."
                )
            if len(refs) < input_connection.minimum:
                if isinstance(input_connection, PrerequisiteInput):
                    # This branch should only be possible during QG generation,
                    # or if someone deleted the dataset between making the QG
                    # and trying to run it.  Either one should be a hard error.
                    raise FileNotFoundError(
                        f"Not enough datasets ({len(refs)}) found for non-optional connection {label}.{name} "
                        f"({dataset_type_name}) with minimum={input_connection.minimum} for quantum data ID "
                        f"{data_id}."
                    )
                else:
                    # This branch should be impossible during QG generation,
                    # because that algorithm can only make quanta whose inputs
                    # are either already present or should be created during
                    # execution.  It can trigger during execution if the input
                    # wasn't actually created by an upstream task in the same
                    # graph.
                    raise NoWorkFound(label, name, input_connection)
        for name, (output_connection, refs) in outputs.items():
            dataset_type_name = output_connection.name
            if not output_connection.multiple and len(refs) > 1:
                raise ScalarError(
                    f"Found multiple datasets {', '.join(str(r.dataId) for r in refs)} "
                    f"for non-multiple output connection {label}.{name} ({dataset_type_name}) "
                    f"for quantum data ID {data_id}."
                )
        return {}, {}


def iterConnections(
    connections: PipelineTaskConnections, connectionType: Union[str, Iterable[str]]
) -> typing.Generator[BaseConnection, None, None]:
    """Creates an iterator over the selected connections type which yields
    all the defined connections of that type.

    Parameters
    ----------
    connections: `PipelineTaskConnections`
        An instance of a `PipelineTaskConnections` object that will be iterated
        over.
    connectionType: `str`
        The type of connections to iterate over, valid values are inputs,
        outputs, prerequisiteInputs, initInputs, initOutputs.

    Yields
    ------
    connection: `BaseConnection`
        A connection defined on the input connections object of the type
        supplied.  The yielded value Will be an derived type of
        `BaseConnection`.
    """
    if isinstance(connectionType, str):
        connectionType = (connectionType,)
    for name in itertools.chain.from_iterable(getattr(connections, ct) for ct in connectionType):
        yield getattr(connections, name)


@dataclass
class AdjustQuantumHelper:
    """Helper class for calling `PipelineTaskConnections.adjustQuantum`.

    This class holds `input` and `output` mappings in the form used by
    `Quantum` and execution harness code, i.e. with `DatasetType` keys,
    translating them to and from the connection-oriented mappings used inside
    `PipelineTaskConnections`.
    """

    inputs: NamedKeyMapping[DatasetType, typing.List[DatasetRef]]
    """Mapping of regular input and prerequisite input datasets, grouped by
    `DatasetType`.
    """

    outputs: NamedKeyMapping[DatasetType, typing.List[DatasetRef]]
    """Mapping of output datasets, grouped by `DatasetType`.
    """

    inputs_adjusted: bool = False
    """Whether any inputs were removed in the last call to `adjust_in_place`.
    """

    outputs_adjusted: bool = False
    """Whether any outputs were removed in the last call to `adjust_in_place`.
    """

    def adjust_in_place(
        self,
        connections: PipelineTaskConnections,
        label: str,
        data_id: DataCoordinate,
    ) -> None:
        """Call `~PipelineTaskConnections.adjustQuantum` and update ``self``
        with its results.

        Parameters
        ----------
        connections : `PipelineTaskConnections`
            Instance on which to call `~PipelineTaskConnections.adjustQuantum`.
        label : `str`
            Label for this task in the pipeline (should be used in all
            diagnostic messages).
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for this quantum in the pipeline (should be used in all
            diagnostic messages).
        """
        # Translate self's DatasetType-keyed, Quantum-oriented mappings into
        # connection-keyed, PipelineTask-oriented mappings.
        inputs_by_connection: typing.Dict[str, typing.Tuple[BaseInput, typing.Tuple[DatasetRef, ...]]] = {}
        outputs_by_connection: typing.Dict[str, typing.Tuple[Output, typing.Tuple[DatasetRef, ...]]] = {}
        for name in itertools.chain(connections.inputs, connections.prerequisiteInputs):
            connection = getattr(connections, name)
            dataset_type_name = connection.name
            inputs_by_connection[name] = (connection, tuple(self.inputs.get(dataset_type_name, ())))
        for name in itertools.chain(connections.outputs):
            connection = getattr(connections, name)
            dataset_type_name = connection.name
            outputs_by_connection[name] = (connection, tuple(self.outputs.get(dataset_type_name, ())))
        # Actually call adjustQuantum.
        # MyPy correctly complains that this call is not quite legal, but the
        # method docs explain exactly what's expected and it's the behavior we
        # want.  It'd be nice to avoid this if we ever have to change the
        # interface anyway, but not an immediate problem.
        adjusted_inputs_by_connection, adjusted_outputs_by_connection = connections.adjustQuantum(
            inputs_by_connection,  # type: ignore
            outputs_by_connection,  # type: ignore
            label,
            data_id,
        )
        # Translate adjustments to DatasetType-keyed, Quantum-oriented form,
        # installing new mappings in self if necessary.
        if adjusted_inputs_by_connection:
            adjusted_inputs = NamedKeyDict[DatasetType, typing.List[DatasetRef]](self.inputs)
            for name, (connection, updated_refs) in adjusted_inputs_by_connection.items():
                dataset_type_name = connection.name
                if not set(updated_refs).issubset(self.inputs[dataset_type_name]):
                    raise RuntimeError(
                        f"adjustQuantum implementation for task with label {label} returned {name} "
                        f"({dataset_type_name}) input datasets that are not a subset of those "
                        f"it was given for data ID {data_id}."
                    )
                adjusted_inputs[dataset_type_name] = list(updated_refs)
            self.inputs = adjusted_inputs.freeze()
            self.inputs_adjusted = True
        else:
            self.inputs_adjusted = False
        if adjusted_outputs_by_connection:
            adjusted_outputs = NamedKeyDict[DatasetType, typing.List[DatasetRef]](self.outputs)
            for name, (connection, updated_refs) in adjusted_outputs_by_connection.items():
                if not set(updated_refs).issubset(self.outputs[dataset_type_name]):
                    raise RuntimeError(
                        f"adjustQuantum implementation for task with label {label} returned {name} "
                        f"({dataset_type_name}) output datasets that are not a subset of those "
                        f"it was given for data ID {data_id}."
                    )
                adjusted_outputs[dataset_type_name] = list(updated_refs)
            self.outputs = adjusted_outputs.freeze()
            self.outputs_adjusted = True
        else:
            self.outputs_adjusted = False
