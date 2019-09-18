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

__all__ = ["PipelineTaskConnections", "InputQuantizedConnection", "OutputQuantizedConnection",
           "DeferredDatasetRef", "iterConnections"]

from collections import UserDict, namedtuple
from types import SimpleNamespace
import typing

import itertools
import string

from . import config as configMod
from .connectionTypes import (InitInput, InitOutput, Input, PrerequisiteInput,
                              Output, BaseConnection)
from lsst.daf.butler import DatasetRef, Quantum

if typing.TYPE_CHECKING:
    from .config import PipelineTaskConfig


class ScalarError(TypeError):
    """Exception raised when dataset type is configured as scalar
    but there are multiple DataIds in a Quantum for that dataset.

    Parameters
    ----------
    key : `str`
        Name of the configuration field for dataset type.
    numDataIds : `int`
        Actual number of DataIds in a Quantum for this dataset type.
    """
    def __init__(self, key, numDataIds):
        super().__init__((f"Expected scalar for output dataset field {key}, "
                          "received {numDataIds} DataIds"))


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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Initialize class level variables used to track any declared
        # class level variables that are instances of
        # connectionTypes.BaseConnection
        self.data['inputs'] = []
        self.data['prerequisiteInputs'] = []
        self.data['outputs'] = []
        self.data['initInputs'] = []
        self.data['initOutputs'] = []
        self.data['allConnections'] = {}

    def __setitem__(self, name, value):
        if isinstance(value, Input):
            self.data['inputs'].append(name)
        elif isinstance(value, PrerequisiteInput):
            self.data['prerequisiteInputs'].append(name)
        elif isinstance(value, Output):
            self.data['outputs'].append(name)
        elif isinstance(value, InitInput):
            self.data['initInputs'].append(name)
        elif isinstance(value, InitOutput):
            self.data['initOutputs'].append(name)
        # This should not be an elif, as it needs tested for
        # everything that inherits from BaseConnection
        if isinstance(value, BaseConnection):
            object.__setattr__(value, 'varName', name)
            self.data['allConnections'][name] = value
        # defer to the default behavior
        super().__setitem__(name, value)


class PipelineTaskConnectionsMetaclass(type):
    """Metaclass used in the declaration of PipelineTaskConnections classes
    """
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
        dimensionsValueError = TypeError("PipelineTaskConnections class must be created with a dimensions "
                                         "attribute which is an iterable of dimension names")

        if name != 'PipelineTaskConnections':
            # Verify that dimensions are passed as a keyword in class
            # declaration
            if 'dimensions' not in kwargs:
                for base in bases:
                    if hasattr(base, 'dimensions'):
                        kwargs['dimensions'] = base.dimensions
                        break
                if 'dimensions' not in kwargs:
                    raise dimensionsValueError
            try:
                dct['dimensions'] = set(kwargs['dimensions'])
            except TypeError as exc:
                raise dimensionsValueError from exc
            # Lookup any python string templates that may have been used in the
            # declaration of the name field of a class connection attribute
            allTemplates = set()
            stringFormatter = string.Formatter()
            # Loop over all connections
            for obj in dct['allConnections'].values():
                nameValue = obj.name
                # add all the parameters to the set of templates
                for param in stringFormatter.parse(nameValue):
                    if param[1] is not None:
                        allTemplates.add(param[1])

            # look up any template from base classes and merge them all
            # together
            mergeDict = {}
            for base in bases[::-1]:
                if hasattr(base, 'defaultTemplates'):
                    mergeDict.update(base.defaultTemplates)
            if 'defaultTemplates' in kwargs:
                mergeDict.update(kwargs['defaultTemplates'])

            if len(mergeDict) > 0:
                kwargs['defaultTemplates'] = mergeDict

            # Verify that if templated strings were used, defaults were
            # supplied as an argument in the declaration of the connection
            # class
            if len(allTemplates) > 0 and 'defaultTemplates' not in kwargs:
                raise TypeError("PipelineTaskConnection class contains templated attribute names, but no "
                                "defaut templates were provided, add a dictionary attribute named "
                                "defaultTemplates which contains the mapping between template key and value")
            if len(allTemplates) > 0:
                # Verify all templates have a default, and throw if they do not
                defaultTemplateKeys = set(kwargs['defaultTemplates'].keys())
                templateDifference = allTemplates.difference(defaultTemplateKeys)
                if templateDifference:
                    raise TypeError(f"Default template keys were not provided for {templateDifference}")
                # Verify that templates do not share names with variable names
                # used for a connection, this is needed because of how
                # templates are specified in an associated config class.
                nameTemplateIntersection = allTemplates.intersection(set(dct['allConnections'].keys()))
                if len(nameTemplateIntersection) > 0:
                    raise TypeError(f"Template parameters cannot share names with Class attributes")
            dct['defaultTemplates'] = kwargs.get('defaultTemplates', {})

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
    """A Namespace to map defined variable names of connections to their
    `lsst.daf.buter.DatasetRef`s

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

    def __setattr__(self, name: str, value: typing.Union[DatasetRef, typing.List[DatasetRef]]):
        # Capture the attribute name as it is added to this object
        self._attributes.add(name)
        super().__setattr__(name, value)

    def __delattr__(self, name):
        object.__delattr__(self, name)
        self._attributes.remove(name)

    def __iter__(self) -> typing.Generator[typing.Tuple[str, typing.Union[DatasetRef,
                                           typing.List[DatasetRef]]], None, None]:
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


class DeferredDatasetRef(namedtuple("DeferredDatasetRefBase", "datasetRef")):
    """Class which denotes that a datasetRef should be treated as deferred when
    interacting with the butler

    Parameters
    ----------
    datasetRef : `lsst.daf.butler.DatasetRef`
        The `lsst.daf.butler.DatasetRef` that will be eventually used to
        resolve a dataset
    """
    __slots__ = ()


class PipelineTaskConnections(metaclass=PipelineTaskConnectionsMetaclass):
    """PipelineTaskConnections is a class used to declare desired IO when a
    PipelineTask is run by an activator

    Parameters
    ----------
    config : `PipelineTaskConfig` A `PipelineTaskConfig` class instance who's
    class has been configured to use this `PipelineTaskConnectionsClass`

    Notes ----- PipelineTaskConnection classes are created by declaring class
    attributes of types defined in lsst.pipe.base.connectionTypes and are
    listed as follows:

    * InitInput - Defines connections in a quantum graph which are used as
    inputs to the __init__ function of the PipelineTask corresponding to this
    class
    * InitOuput - Defines connections in a quantum graph which are to be
    persisted using a butler at the end of the __init__ function of the
    PipelineTask corresponding to this class. The variable name used to define
    this connection should be the same as an attribute name on the PipelineTask
    instance. E.g. if a InitOutput is declared with the name outputSchema in a
    PipelineTaskConnections class, then a PipelineTask instance should have an
    attribute self.outputSchema defined. Its value is what will be saved by the
    activator framework.
    * PrerequisiteInput - An input connection type that defines a
    `lsst.daf.butler.DatasetType` that must be present at execution time, but
    that will not be used during the course of creating the quantum graph to be
    executed. These most often are things produced outside the processing
    pipeline, such as reference catalogs.
    * Input - Input `lsst.daf.butler.DatasetType`s that will be used in the run
    method of a PipelineTask.  The name used to declare class attribute must
    match a function argument name in the run method of a PipelineTask. E.g. If
    the PipelineTaskConnections defines an Input with the name calexp, then the
    corresponding signature should be PipelineTask.run(calexp, ...)
    * Output - A `lsst.daf.butler.DatasetType` that will be produced by an
    execution of a PipelineTask. The name used to declare the connection must
    correspond to an attribute of a `Struct` that is returned by a
    `PipelineTask` run method.  E.g. if an output connection is defined with
    the name measCat, then the corresponding PipelineTask.run method must
    return Struct(measCat=X,..) where X matches the storageClass type defined
    on the output connection.

    The process of declaring a PipelineTaskConnection class involves parameters
    passed in the declaration statement.

    The first parameter is dimensions which is an iterable of strings which
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
    ConnectionClass.calexp.name = '{input}Coadd_calexp' then
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
    >>> assert(connections.outputCOnnection.name == "TotallyDifferent")
    """

    def __init__(self, *, config: 'PipelineTaskConfig' = None):
        self.inputs = set(self.inputs)
        self.prerequisiteInputs = set(self.prerequisiteInputs)
        self.outputs = set(self.outputs)
        self.initInputs = set(self.initInputs)
        self.initOutputs = set(self.initOutputs)

        if config is None or not isinstance(config, configMod.PipelineTaskConfig):
            raise ValueError("PipelineTaskConnections must be instantiated with"
                             " a PipelineTaskConfig instance")
        self.config = config
        # Extract the template names that were defined in the config instance
        # by looping over the keys of the defaultTemplates dict specified at
        # class declaration time
        templateValues = {name: getattr(config.connections, name) for name in getattr(self,
                          'defaultTemplates').keys()}
        # Extract the configured value corresponding to each connection
        # variable. I.e. for each connection identifier, populate a override
        # for the connection.name attribute
        self._nameOverrides = {name: getattr(config.connections, name).format(**templateValues)
                               for name in self.allConnections.keys()}

        # connections.name corresponds to a dataset type name, create a reverse
        # mapping that goes from dataset type name to attribute identifier name
        # (variable name) on the connection class
        self._typeNameToVarName = {v: k for k, v in self._nameOverrides.items()}

    def buildDatasetRefs(self, quantum: Quantum) -> typing.Tuple[InputQuantizedConnection,
                                                                 OutputQuantizedConnection]:
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
        for refs, names in zip((inputDatasetRefs, outputDatasetRefs),
                               (itertools.chain(self.inputs, self.prerequisiteInputs), self.outputs)):
            # get a name of a class connection attribute
            for attributeName in names:
                # get the attribute identified by name
                attribute = getattr(self, attributeName)
                # Branch if the attribute dataset type is an input
                if attribute.name in quantum.predictedInputs:
                    # Get the DatasetRefs
                    quantumInputRefs = quantum.predictedInputs[attribute.name]
                    # if the dataset is marked to load deferred, wrap it in a
                    # DeferredDatasetRef
                    if attribute.deferLoad:
                        quantumInputRefs = [DeferredDatasetRef(datasetRef=ref) for ref in quantumInputRefs]
                    # Unpack arguments that are not marked multiples (list of
                    # length one)
                    if not attribute.multiple:
                        if len(quantumInputRefs) != 1:
                            raise ScalarError(attributeName, len(quantumInputRefs))
                        quantumInputRefs = quantumInputRefs[0]
                    # Add to the QuantizedConnection identifier
                    setattr(refs, attributeName, quantumInputRefs)
                # Branch if the attribute dataset type is an output
                elif attribute.name in quantum.outputs:
                    value = quantum.outputs[attribute.name]
                    # Unpack arguments that are not marked multiples (list of
                    # length one)
                    if not attribute.multiple:
                        value = value[0]
                    # Add to the QuantizedConnection identifier
                    setattr(refs, attributeName, value)
                # Specified attribute is not in inputs or outputs dont know how
                # to handle, throw
                else:
                    raise ValueError(f"Attribute with name {attributeName} has no counterpoint "
                                     "in input quantum")
        return inputDatasetRefs, outputDatasetRefs

    def adjustQuantum(self, datasetRefMap: InputQuantizedConnection):
        """Override to make adjustments to `lsst.daf.butler.DatasetRef`s in the
        `lsst.daf.butler.core.Quantum` during the graph generation stage of the
        activator.

        Parameters
        ----------
        datasetRefMap : `dict`
            Mapping with keys of dataset type name to `list` of
            `lsst.daf.butler.DatasetRef`s

        Returns
        -------
        datasetRefMap : `dict`
            Modified mapping of input with possible adjusted
            `lsst.daf.butler.DatasetRef`s

        Raises
        ------
        Exception
            Overrides of this function have the option of raising and Exception
            if a field in the input does not satisfy a need for a corresponding
            pipelineTask, i.e. no reference catalogs are found.
        """
        return datasetRefMap


def iterConnections(connections: PipelineTaskConnections, connectionType: str) -> typing.Generator:
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
    -------
    connection: `BaseConnection`
        A connection defined on the input connections object of the type
        supplied.  The yielded value Will be an derived type of
        `BaseConnection`.
    """
    for name in getattr(connections, connectionType):
        yield getattr(connections, name)
