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

"""Module defining connection classes for PipelineTask."""

from __future__ import annotations

__all__ = [
    "AdjustQuantumHelper",
    "DeferredDatasetRef",
    "InputQuantizedConnection",
    "OutputQuantizedConnection",
    "PipelineTaskConnections",
    "QuantizedConnection",
    "ScalarError",
    "ScalarError",
    "iterConnections",
]

import dataclasses
import itertools
import string
import warnings
from collections import UserDict
from collections.abc import Collection, Generator, Iterable, Mapping, Sequence, Set
from dataclasses import dataclass
from types import MappingProxyType, SimpleNamespace
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, NamedKeyDict, NamedKeyMapping, Quantum

from ._status import NoWorkFound
from .connectionTypes import BaseConnection, BaseInput, Output, PrerequisiteInput

if TYPE_CHECKING:
    from .config import PipelineTaskConfig


class ScalarError(TypeError):
    """Exception raised when dataset type is configured as scalar
    but there are multiple data IDs in a Quantum for that dataset.
    """


class PipelineTaskConnectionDict(UserDict):
    """A special dict class used by `PipelineTaskConnectionMetaclass`.

    This dict is used in `PipelineTaskConnection` class creation, as the
    dictionary that is initially used as ``__dict__``. It exists to
    intercept connection fields declared in a `PipelineTaskConnection`, and
    what name is used to identify them. The names are then added to class
    level list according to the connection type of the class attribute. The
    names are also used as keys in a class level dictionary associated with
    the corresponding class attribute. This information is a duplicate of
    what exists in ``__dict__``, but provides a simple place to lookup and
    iterate on only these variables.

    Parameters
    ----------
    *args : `~typing.Any`
        Passed to `dict` constructor.
    **kwargs : `~typing.Any`
        Passed to `dict` constructor.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        # Initialize class level variables used to track any declared
        # class level variables that are instances of
        # connectionTypes.BaseConnection
        self.data["inputs"] = set()
        self.data["prerequisiteInputs"] = set()
        self.data["outputs"] = set()
        self.data["initInputs"] = set()
        self.data["initOutputs"] = set()
        self.data["allConnections"] = {}

    def __setitem__(self, name: str, value: Any) -> None:
        if isinstance(value, BaseConnection):
            if name in {
                "dimensions",
                "inputs",
                "prerequisiteInputs",
                "outputs",
                "initInputs",
                "initOutputs",
                "allConnections",
            }:
                # Guard against connections whose names are reserved.
                raise AttributeError(f"Connection name {name!r} is reserved for internal use.")
            if (previous := self.data.get(name)) is not None:
                # Guard against changing the type of an in inherited connection
                # by first removing it from the set it's current in.
                self.data[previous._connection_type_set].discard(name)
            object.__setattr__(value, "varName", name)
            self.data["allConnections"][name] = value
            self.data[value._connection_type_set].add(name)
        # defer to the default behavior
        super().__setitem__(name, value)


class PipelineTaskConnectionsMetaclass(type):
    """Metaclass used in the declaration of PipelineTaskConnections classes.

    Parameters
    ----------
    name : `str`
        Name of connection.
    bases : `~collections.abc.Collection`
        Base classes.
    dct : `~collections.abc.Mapping`
        Connections dict.
    **kwargs : `~typing.Any`
        Additional parameters.
    """

    # We can annotate these attributes as `collections.abc.Set` to discourage
    # undesirable modifications in type-checked code, since the internal code
    # modifying them is in `PipelineTaskConnectionDict` and that doesn't see
    # these annotations anyway.

    dimensions: Set[str]
    """Set of dimension names that define the unit of work for this task.

    Required and implied dependencies will automatically be expanded later and
    need not be provided.

    This is shadowed by an instance-level attribute on
    `PipelineTaskConnections` instances.
    """

    inputs: Set[str]
    """Set with the names of all `~connectionTypes.Input` connection
    attributes.

    This is updated automatically as class attributes are added.  Note that
    this attribute is shadowed by an instance-level attribute on
    `PipelineTaskConnections` instances.
    """

    prerequisiteInputs: Set[str]
    """Set with the names of all `~connectionTypes.PrerequisiteInput`
    connection attributes.

    See `inputs` for additional information.
    """

    outputs: Set[str]
    """Set with the names of all `~connectionTypes.Output` connection
    attributes.

    See `inputs` for additional information.
    """

    initInputs: Set[str]
    """Set with the names of all `~connectionTypes.InitInput` connection
    attributes.

    See `inputs` for additional information.
    """

    initOutputs: Set[str]
    """Set with the names of all `~connectionTypes.InitOutput` connection
    attributes.

    See `inputs` for additional information.
    """

    allConnections: Mapping[str, BaseConnection]
    """Mapping containing all connection attributes.

    See `inputs` for additional information.
    """

    def __prepare__(name, bases, **kwargs):  # noqa: N804
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
                if not isinstance(kwargs["dimensions"], Iterable):
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
            mergeDeprecationsDict = {}
            for base in bases[::-1]:
                if hasattr(base, "defaultTemplates"):
                    mergeDict.update(base.defaultTemplates)
                if hasattr(base, "deprecatedTemplates"):
                    mergeDeprecationsDict.update(base.deprecatedTemplates)
            if "defaultTemplates" in kwargs:
                mergeDict.update(kwargs["defaultTemplates"])
            if "deprecatedTemplates" in kwargs:
                mergeDeprecationsDict.update(kwargs["deprecatedTemplates"])
            if len(mergeDict) > 0:
                kwargs["defaultTemplates"] = mergeDict
            if len(mergeDeprecationsDict) > 0:
                kwargs["deprecatedTemplates"] = mergeDeprecationsDict

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
            dct["deprecatedTemplates"] = kwargs.get("deprecatedTemplates", {})

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

    def __call__(cls, *, config: PipelineTaskConfig | None = None) -> PipelineTaskConnections:
        # MyPy appears not to really understand metaclass.__call__ at all, so
        # we need to tell it to ignore __new__ and __init__ calls here.
        instance: PipelineTaskConnections = cls.__new__(cls)  # type: ignore

        # Make mutable copies of all set-like class attributes so derived
        # __init__ implementations can modify them in place.
        instance.dimensions = set(cls.dimensions)
        instance.inputs = set(cls.inputs)
        instance.prerequisiteInputs = set(cls.prerequisiteInputs)
        instance.outputs = set(cls.outputs)
        instance.initInputs = set(cls.initInputs)
        instance.initOutputs = set(cls.initOutputs)

        # Set self.config.  It's a bit strange that we claim to accept None but
        # really just raise here, but it's not worth changing now.
        from .config import PipelineTaskConfig  # local import to avoid cycle

        if config is None or not isinstance(config, PipelineTaskConfig):
            raise ValueError(
                "PipelineTaskConnections must be instantiated with a PipelineTaskConfig instance"
            )
        instance.config = config

        # Extract the template names that were defined in the config instance
        # by looping over the keys of the defaultTemplates dict specified at
        # class declaration time.
        templateValues = {
            name: getattr(config.connections, name)
            for name in cls.defaultTemplates  # type: ignore
        }

        # We now assemble a mapping of all connection instances keyed by
        # internal name, applying the configuration and templates to make new
        # configurations from the class-attribute defaults.  This will be
        # private, but with a public read-only view.  This mapping is what the
        # descriptor interface of the class-level attributes will return when
        # they are accessed on an instance.  This is better than just assigning
        # regular instance attributes as it makes it so removed connections
        # cannot be accessed on instances, instead of having access to them
        # silent fall through to the not-removed class connection instance.
        instance._allConnections = {}
        instance.allConnections = MappingProxyType(instance._allConnections)
        for internal_name, connection in cls.allConnections.items():
            dataset_type_name = getattr(config.connections, internal_name).format(**templateValues)
            instance_connection = dataclasses.replace(
                connection,
                name=dataset_type_name,
                doc=(
                    connection.doc
                    if connection.deprecated is None
                    else f"{connection.doc}\n{connection.deprecated}"
                ),
                _deprecation_context=connection._deprecation_context,
            )
            instance._allConnections[internal_name] = instance_connection

        # Finally call __init__.  The base class implementation does nothing;
        # we could have left some of the above implementation there (where it
        # originated), but putting it here instead makes it hard for derived
        # class implementors to get things into a weird state by delegating to
        # super().__init__ in the wrong place, or by forgetting to do that
        # entirely.
        instance.__init__(config=config)  # type: ignore

        # Derived-class implementations may have changed the contents of the
        # various kinds-of-connection sets; update allConnections to have keys
        # that are a union of all those.  We get values for the new
        # allConnections from the attributes, since any dynamically added new
        # ones will not be present in the old allConnections.  Typically those
        # getattrs will invoke the descriptors and get things from the old
        # allConnections anyway.  After processing each set we replace it with
        # a frozenset.
        updated_all_connections = {}
        for attrName in ("initInputs", "prerequisiteInputs", "inputs", "initOutputs", "outputs"):
            updated_connection_names = getattr(instance, attrName)
            updated_all_connections.update(
                {name: getattr(instance, name) for name in updated_connection_names}
            )
            # Setting these to frozenset is at odds with the type annotation,
            # but MyPy can't tell because we're using setattr, and we want to
            # lie to it anyway to get runtime guards against post-__init__
            # mutation.
            setattr(instance, attrName, frozenset(updated_connection_names))
        # Update the existing dict in place, since we already have a view of
        # that.
        instance._allConnections.clear()
        instance._allConnections.update(updated_all_connections)

        for connection_name, obj in instance._allConnections.items():
            if obj.deprecated is not None:
                warnings.warn(
                    f"Connection {connection_name} with datasetType {obj.name} "
                    f"(from {obj._deprecation_context}): {obj.deprecated}",
                    FutureWarning,
                    stacklevel=1,  # Report from this location.
                )

        # Freeze the connection instance dimensions now.  This at odds with the
        # type annotation, which says [mutable] `set`, just like the connection
        # type attributes (e.g. `inputs`, `outputs`, etc.), though MyPy can't
        # tell with those since we're using setattr for them.
        instance.dimensions = frozenset(instance.dimensions)  # type: ignore

        return instance


class QuantizedConnection(SimpleNamespace):
    r"""A Namespace to map defined variable names of connections to the
    associated `lsst.daf.butler.DatasetRef` objects.

    This class maps the names used to define a connection on a
    `PipelineTaskConnections` class to the corresponding
    `~lsst.daf.butler.DatasetRef`\s provided by a `~lsst.daf.butler.Quantum`
    instance.  This will be a quantum of execution based on the graph created
    by examining all the connections defined on the
    `PipelineTaskConnections` class.

    Parameters
    ----------
    **kwargs : `~typing.Any`
        Not used.
    """

    def __init__(self, **kwargs):
        # Create a variable to track what attributes are added. This is used
        # later when iterating over this QuantizedConnection instance
        object.__setattr__(self, "_attributes", set())

    def __setattr__(self, name: str, value: DatasetRef | list[DatasetRef]) -> None:
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
    ) -> Generator[tuple[str, DatasetRef | list[DatasetRef]], None, None]:
        """Make an iterator for this `QuantizedConnection`.

        Iterating over a `QuantizedConnection` will yield a tuple with the name
        of an attribute and the value associated with that name. This is
        similar to dict.items() but is on the namespace attributes rather than
        dict keys.
        """
        yield from ((name, getattr(self, name)) for name in self._attributes)

    def keys(self) -> Generator[str, None, None]:
        """Return an iterator over all the attributes added to a
        `QuantizedConnection` class.
        """
        yield from self._attributes


class InputQuantizedConnection(QuantizedConnection):
    """Input variant of a `QuantizedConnection`."""

    pass


class OutputQuantizedConnection(QuantizedConnection):
    """Output variant of a `QuantizedConnection`."""

    pass


@dataclass(frozen=True)
class DeferredDatasetRef:
    """A wrapper class for `~lsst.daf.butler.DatasetRef` that indicates that a
    `PipelineTask` should receive a `~lsst.daf.butler.DeferredDatasetHandle`
    instead of an in-memory dataset.

    Attributes
    ----------
    datasetRef : `lsst.daf.butler.DatasetRef`
        The `lsst.daf.butler.DatasetRef` that will be eventually used to
        resolve a dataset.
    """

    datasetRef: DatasetRef

    def __getattr__(self, name: str) -> Any:
        return getattr(self.datasetRef, name)


class PipelineTaskConnections(metaclass=PipelineTaskConnectionsMetaclass):
    """PipelineTaskConnections is a class used to declare desired IO when a
    PipelineTask is run by an activator.

    Parameters
    ----------
    config : `PipelineTaskConfig`
        A `PipelineTaskConfig` class instance whose class has been configured
        to use this `PipelineTaskConnections` class.

    See Also
    --------
    iterConnections : Iterator over selected connections.

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

    Attributes of these types can also be created, replaced, or deleted on the
    `PipelineTaskConnections` instance in the ``__init__`` method, if more than
    just the name depends on the configuration.  It is preferred to define them
    in the class when possible (even if configuration may cause the connection
    to be removed from the instance).

    The process of declaring a ``PipelineTaskConnection`` class involves
    parameters passed in the declaration statement.

    The first parameter is ``dimensions`` which is an iterable of strings which
    defines the unit of processing the run method of a corresponding
    `PipelineTask` will operate on. These dimensions must match dimensions that
    exist in the butler registry which will be used in executing the
    corresponding `PipelineTask`.  The dimensions may be also modified in
    subclass ``__init__`` methods if they need to depend on configuration.

    The second parameter is labeled ``defaultTemplates`` and is conditionally
    optional. The name attributes of connections can be specified as python
    format strings, with named format arguments. If any of the name parameters
    on connections defined in a `PipelineTaskConnections` class contain a
    template, then a default template value must be specified in the
    ``defaultTemplates`` argument. This is done by passing a dictionary with
    keys corresponding to a template identifier, and values corresponding to
    the value to use as a default when formatting the string. For example if
    ``ConnectionsClass.calexp.name = '{input}Coadd_calexp'`` then
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
    >>> class ExampleConnections(
    ...     PipelineTaskConnections,
    ...     dimensions=("A", "B"),
    ...     defaultTemplates={"foo": "Example"},
    ... ):
    ...     inputConnection = cT.Input(
    ...         doc="Example input",
    ...         dimensions=("A", "B"),
    ...         storageClass=Exposure,
    ...         name="{foo}Dataset",
    ...     )
    ...     outputConnection = cT.Output(
    ...         doc="Example output",
    ...         dimensions=("A", "B"),
    ...         storageClass=Exposure,
    ...         name="{foo}output",
    ...     )
    >>> class ExampleConfig(
    ...     PipelineTaskConfig, pipelineConnections=ExampleConnections
    ... ):
    ...     pass
    >>> config = ExampleConfig()
    >>> config.connections.foo = Modified
    >>> config.connections.outputConnection = "TotallyDifferent"
    >>> connections = ExampleConnections(config=config)
    >>> assert connections.inputConnection.name == "ModifiedDataset"
    >>> assert connections.outputConnection.name == "TotallyDifferent"
    """

    # We annotate these attributes as mutable sets because that's what they are
    # inside derived ``__init__`` implementations and that's what matters most
    # After that's done, the metaclass __call__ makes them into frozensets, but
    # relatively little code interacts with them then, and that code knows not
    # to try to modify them without having to be told that by mypy.

    dimensions: set[str]
    """Set of dimension names that define the unit of work for this task.

    Required and implied dependencies will automatically be expanded later and
    need not be provided.

    This may be replaced or modified in ``__init__`` to change the dimensions
    of the task.  After ``__init__`` it will be a `frozenset` and may not be
    replaced.
    """

    inputs: set[str]
    """Set with the names of all `connectionTypes.Input` connection attributes.

    This is updated automatically as class attributes are added, removed, or
    replaced in ``__init__``.  Removing entries from this set will cause those
    connections to be removed after ``__init__`` completes, but this is
    supported only for backwards compatibility; new code should instead just
    delete the collection attributed directly.  After ``__init__`` this will be
    a `frozenset` and may not be replaced.
    """

    prerequisiteInputs: set[str]
    """Set with the names of all `~connectionTypes.PrerequisiteInput`
    connection attributes.

    See `inputs` for additional information.
    """

    outputs: set[str]
    """Set with the names of all `~connectionTypes.Output` connection
    attributes.

    See `inputs` for additional information.
    """

    initInputs: set[str]
    """Set with the names of all `~connectionTypes.InitInput` connection
    attributes.

    See `inputs` for additional information.
    """

    initOutputs: set[str]
    """Set with the names of all `~connectionTypes.InitOutput` connection
    attributes.

    See `inputs` for additional information.
    """

    allConnections: Mapping[str, BaseConnection]
    """Mapping holding all connection attributes.

    This is a read-only view that is automatically updated when connection
    attributes are added, removed, or replaced in ``__init__``.  It is also
    updated after ``__init__`` completes to reflect changes in `inputs`,
    `prerequisiteInputs`, `outputs`, `initInputs`, and `initOutputs`.
    """

    _allConnections: dict[str, BaseConnection]

    def __init__(self, *, config: PipelineTaskConfig | None = None):
        pass

    def __setattr__(self, name: str, value: Any) -> None:
        if isinstance(value, BaseConnection):
            previous = self._allConnections.get(name)
            try:
                getattr(self, value._connection_type_set).add(name)
            except AttributeError:
                # Attempt to call add on a frozenset, which is what these sets
                # are after __init__ is done.
                raise TypeError("Connections objects are frozen after construction.") from None
            if previous is not None and value._connection_type_set != previous._connection_type_set:
                # Connection has changed type, e.g. Input to PrerequisiteInput;
                # update the sets accordingly. To be extra defensive about
                # multiple assignments we use the type of the previous instance
                # instead of assuming that's the same as the type of the self,
                # which is just the default.  Use discard instead of remove so
                # manually removing from these sets first is never an error.
                getattr(self, previous._connection_type_set).discard(name)
            self._allConnections[name] = value
            if hasattr(self.__class__, name):
                # Don't actually set the attribute if this was a connection
                # declared in the class; in that case we let the descriptor
                # return the value we just added to allConnections.
                return
        # Actually add the attribute.
        super().__setattr__(name, value)

    def __delattr__(self, name):
        """Descriptor delete method."""
        previous = self._allConnections.get(name)
        if previous is not None:
            # Delete this connection's name from the appropriate set, which we
            # have to get from the previous instance instead of assuming it's
            # the same set that was appropriate for the class-level default.
            # Use discard instead of remove so manually removing from these
            # sets first is never an error.
            try:
                getattr(self, previous._connection_type_set).discard(name)
            except AttributeError:
                # Attempt to call discard on a frozenset, which is what these
                # sets are after __init__ is done.
                raise TypeError("Connections objects are frozen after construction.") from None
            del self._allConnections[name]
            if hasattr(self.__class__, name):
                # Don't actually delete the attribute if this was a connection
                # declared in the class; in that case we let the descriptor
                # see that it's no longer present in allConnections.
                return
        # Actually delete the attribute.
        super().__delattr__(name)

    def buildDatasetRefs(
        self, quantum: Quantum
    ) -> tuple[InputQuantizedConnection, OutputQuantizedConnection]:
        """Build `QuantizedConnection` corresponding to input
        `~lsst.daf.butler.Quantum`.

        Parameters
        ----------
        quantum : `lsst.daf.butler.Quantum`
            Quantum object which defines the inputs and outputs for a given
            unit of processing.

        Returns
        -------
        retVal : `tuple` of (`InputQuantizedConnection`, \
                `OutputQuantizedConnection`)
            Namespaces mapping attribute names
            (identifiers of connections) to butler references defined in the
            input `~lsst.daf.butler.Quantum`.
        """
        inputDatasetRefs = InputQuantizedConnection()
        outputDatasetRefs = OutputQuantizedConnection()

        # populate inputDatasetRefs from quantum inputs
        for attributeName in itertools.chain(self.inputs, self.prerequisiteInputs):
            # get the attribute identified by name
            attribute = getattr(self, attributeName)
            # if the dataset is marked to load deferred, wrap it in a
            # DeferredDatasetRef
            quantumInputRefs: list[DatasetRef] | list[DeferredDatasetRef]
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
                setattr(inputDatasetRefs, attributeName, quantumInputRefs[0])
            else:
                # Add to the QuantizedConnection identifier
                setattr(inputDatasetRefs, attributeName, quantumInputRefs)

        # populate outputDatasetRefs from quantum outputs
        for attributeName in self.outputs:
            # get the attribute identified by name
            attribute = getattr(self, attributeName)
            value = quantum.outputs[attribute.name]
            # Unpack arguments that are not marked multiples (list of
            # length one)
            if not attribute.multiple:
                setattr(outputDatasetRefs, attributeName, value[0])
            else:
                setattr(outputDatasetRefs, attributeName, value)

        return inputDatasetRefs, outputDatasetRefs

    def adjustQuantum(
        self,
        inputs: dict[str, tuple[BaseInput, Collection[DatasetRef]]],
        outputs: dict[str, tuple[Output, Collection[DatasetRef]]],
        label: str,
        data_id: DataCoordinate,
    ) -> tuple[
        Mapping[str, tuple[BaseInput, Collection[DatasetRef]]],
        Mapping[str, tuple[Output, Collection[DatasetRef]]],
    ]:
        """Override to make adjustments to `lsst.daf.butler.DatasetRef` objects
        in the `lsst.daf.butler.Quantum` during the graph generation stage
        of the activator.

        Parameters
        ----------
        inputs : `dict`
            Dictionary whose keys are an input (regular or prerequisite)
            connection name and whose values are a tuple of the connection
            instance and a collection of associated
            `~lsst.daf.butler.DatasetRef` objects.
            The exact type of the nested collections is unspecified; it can be
            assumed to be multi-pass iterable and support `len` and ``in``, but
            it should not be mutated in place.  In contrast, the outer
            dictionaries are guaranteed to be temporary copies that are true
            `dict` instances, and hence may be modified and even returned; this
            is especially useful for delegating to `super` (see notes below).
        outputs : `~collections.abc.Mapping`
            Mapping of output datasets, with the same structure as ``inputs``.
        label : `str`
            Label for this task in the pipeline (should be used in all
            diagnostic messages).
        data_id : `lsst.daf.butler.DataCoordinate`
            Data ID for this quantum in the pipeline (should be used in all
            diagnostic messages).

        Returns
        -------
        adjusted_inputs : `~collections.abc.Mapping`
            Mapping of the same form as ``inputs`` with updated containers of
            input `~lsst.daf.butler.DatasetRef` objects.  Connections that are
            not changed should not be returned at all. Datasets may only be
            removed, not added.  Nested collections may be of any multi-pass
            iterable type, and the order of iteration will set the order of
            iteration within `PipelineTask.runQuantum`.
        adjusted_outputs : `~collections.abc.Mapping`
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
        any datasets are actually dropped, e.g.:

        .. code-block:: python

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

    def getSpatialBoundsConnections(self) -> Iterable[str]:
        """Return the names of regular input and output connections whose data
        IDs should be used to compute the spatial bounds of this task's quanta.

        The spatial bound for a quantum is defined as the union of the regions
        of all data IDs of all connections returned here, along with the region
        of the quantum data ID (if the task has spatial dimensions).

        Returns
        -------
        connection_names : `collections.abc.Iterable` [ `str` ]
            Names of collections with spatial dimensions.  These are the
            task-internal connection names, not butler dataset type names.

        Notes
        -----
        The spatial bound is used to search for prerequisite inputs that have
        skypix dimensions. The default implementation returns an empty
        iterable, which is usually sufficient for tasks with spatial
        dimensions, but if a task's inputs or outputs are associated with
        spatial regions that extend beyond the quantum data ID's region, this
        method may need to be overridden to expand the set of prerequisite
        inputs found.

        Tasks that do not have spatial dimensions that have skypix prerequisite
        inputs should always override this method, as the default spatial
        bounds otherwise cover the full sky.
        """
        return ()

    def getTemporalBoundsConnections(self) -> Iterable[str]:
        """Return the names of regular input and output connections whose data
        IDs should be used to compute the temporal bounds of this task's
        quanta.

        The temporal bound for a quantum is defined as the union of the
        timespans of all data IDs of all connections returned here, along with
        the timespan of the quantum data ID (if the task has temporal
        dimensions).

        Returns
        -------
        connection_names : `collections.abc.Iterable` [ `str` ]
            Names of collections with temporal dimensions.  These are the
            task-internal connection names, not butler dataset type names.

        Notes
        -----
        The temporal bound is used to search for prerequisite inputs that are
        calibration datasets. The default implementation returns an empty
        iterable, which is usually sufficient for tasks with temporal
        dimensions, but if a task's inputs or outputs are associated with
        timespans that extend beyond the quantum data ID's timespan, this
        method may need to be overridden to expand the set of prerequisite
        inputs found.

        Tasks that do not have temporal dimensions that do not implement this
        method will use an infinite timespan for any calibration lookups.
        """
        return ()


def iterConnections(
    connections: PipelineTaskConnections, connectionType: str | Iterable[str]
) -> Generator[BaseConnection, None, None]:
    """Create an iterator over the selected connections type which yields
    all the defined connections of that type.

    Parameters
    ----------
    connections : `PipelineTaskConnections`
        An instance of a `PipelineTaskConnections` object that will be iterated
        over.
    connectionType : `str`
        The type of connections to iterate over, valid values are inputs,
        outputs, prerequisiteInputs, initInputs, initOutputs.

    Yields
    ------
    connection: `~.connectionTypes.BaseConnection`
        A connection defined on the input connections object of the type
        supplied.  The yielded value Will be an derived type of
        `~.connectionTypes.BaseConnection`.
    """
    if isinstance(connectionType, str):
        connectionType = (connectionType,)
    for name in itertools.chain.from_iterable(getattr(connections, ct) for ct in connectionType):
        yield getattr(connections, name)


@dataclass
class AdjustQuantumHelper:
    """Helper class for calling `PipelineTaskConnections.adjustQuantum`.

    This class holds `input` and `output` mappings in the form used by
    `Quantum` and execution harness code, i.e. with
    `~lsst.daf.butler.DatasetType` keys, translating them to and from the
    connection-oriented mappings used inside `PipelineTaskConnections`.
    """

    inputs: NamedKeyMapping[DatasetType, Sequence[DatasetRef]]
    """Mapping of regular input and prerequisite input datasets, grouped by
    `~lsst.daf.butler.DatasetType`.
    """

    outputs: NamedKeyMapping[DatasetType, Sequence[DatasetRef]]
    """Mapping of output datasets, grouped by `~lsst.daf.butler.DatasetType`.
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
        inputs_by_connection: dict[str, tuple[BaseInput, tuple[DatasetRef, ...]]] = {}
        outputs_by_connection: dict[str, tuple[Output, tuple[DatasetRef, ...]]] = {}
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
            adjusted_inputs = NamedKeyDict[DatasetType, tuple[DatasetRef, ...]](self.inputs)
            for name, (connection, updated_refs) in adjusted_inputs_by_connection.items():
                dataset_type_name = connection.name
                if not set(updated_refs).issubset(self.inputs[dataset_type_name]):
                    raise RuntimeError(
                        f"adjustQuantum implementation for task with label {label} returned {name} "
                        f"({dataset_type_name}) input datasets that are not a subset of those "
                        f"it was given for data ID {data_id}."
                    )
                adjusted_inputs[dataset_type_name] = tuple(updated_refs)
            self.inputs = adjusted_inputs.freeze()
            self.inputs_adjusted = True
        else:
            self.inputs_adjusted = False
        if adjusted_outputs_by_connection:
            adjusted_outputs = NamedKeyDict[DatasetType, tuple[DatasetRef, ...]](self.outputs)
            for name, (connection, updated_refs) in adjusted_outputs_by_connection.items():
                dataset_type_name = connection.name
                if not set(updated_refs).issubset(self.outputs[dataset_type_name]):
                    raise RuntimeError(
                        f"adjustQuantum implementation for task with label {label} returned {name} "
                        f"({dataset_type_name}) output datasets that are not a subset of those "
                        f"it was given for data ID {data_id}."
                    )
                adjusted_outputs[dataset_type_name] = tuple(updated_refs)
            self.outputs = adjusted_outputs.freeze()
            self.outputs_adjusted = True
        else:
            self.outputs_adjusted = False
