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

"""Module defining config classes for PipelineTask."""

from __future__ import annotations

__all__ = ["PipelineTaskConfig"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import os
from collections.abc import Iterable
from numbers import Number
from typing import TYPE_CHECKING, Any, TypeVar

# -----------------------------
#  Imports for other modules --
# -----------------------------
import lsst.pex.config as pexConfig

from ._instrument import Instrument
from .configOverrides import ConfigOverrides
from .connections import PipelineTaskConnections
from .pipelineIR import ConfigIR, ParametersIR

if TYPE_CHECKING:
    from lsst.pex.config.callStack import StackFrame

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_S = TypeVar("_S", bound="PipelineTaskConfigMeta")

# ------------------------
#  Exported definitions --
# ------------------------


class TemplateField(pexConfig.Field):
    """Field specialized for use with connection templates.

    Specifically it treats strings or numbers as valid input, as occasionally
    numbers are used as a cycle counter in templates.

    The reason for the specialized field, is that when numbers are involved
    with the config override system through pipelines or from the command line,
    sometimes the quoting to get appropriate values as strings gets
    complicated. This will simplify the process greatly.
    """

    def _validateValue(self, value: Any) -> None:
        if value is None:
            return

        if not (isinstance(value, str | Number)):
            raise TypeError(
                f"Value {value} is of incorrect type {pexConfig.config._typeStr(value)}."
                " Expected type str or a number"
            )
        if self.check is not None and not self.check(value):
            ValueError("Value {value} is not a valid value")

    def __set__(
        self,
        instance: pexConfig.Config,
        value: Any,
        at: StackFrame | None = None,
        label: str = "assignment",
    ) -> None:
        # validate first, even though validate will be called in super
        self._validateValue(value)
        # now, explicitly make it into a string
        value = str(value)
        super().__set__(instance, value, at, label)


class PipelineTaskConfigMeta(pexConfig.ConfigMeta):
    """Metaclass used in the creation of PipelineTaskConfig classes.

    This metaclass ensures a `PipelineTaskConnections` class is specified in
    the class construction parameters with a parameter name of
    pipelineConnections. Using the supplied connection class, this metaclass
    constructs a `lsst.pex.config.Config` instance which can be used to
    configure the connections class. This config is added to the config class
    under declaration with the name "connections" used as an identifier. The
    connections config also has a reference to the connections class used in
    its construction associated with an atttribute named ``ConnectionsClass``.
    Finally the newly constructed config class (not an instance of it) is
    assigned to the Config class under construction with the attribute name
    ``ConnectionsConfigClass``.

    Parameters
    ----------
    name : `str`
        Name of config.
    bases : `~collections.abc.Collection`
        Base classes.
    dct : `~collections.abc.Mapping`
        Parameter dict.
    **kwargs : `~typing.Any`
        Additional parameters.
    """

    def __new__(
        cls: type[_S],
        name: str,
        bases: tuple[type[PipelineTaskConfig], ...],
        dct: dict[str, Any],
        **kwargs: Any,
    ) -> _S:
        if name != "PipelineTaskConfig":
            # Verify that a connection class was specified and the argument is
            # an instance of PipelineTaskConfig
            if "pipelineConnections" not in kwargs:
                for base in bases:
                    if hasattr(base, "connections"):
                        kwargs["pipelineConnections"] = base.connections.dtype.ConnectionsClass
                        break
            if "pipelineConnections" not in kwargs:
                raise NameError("PipelineTaskConfig or a base class must be defined with connections class")
            connectionsClass = kwargs["pipelineConnections"]
            if not issubclass(connectionsClass, PipelineTaskConnections):
                raise ValueError("Can only assign a PipelineTaskConnections Class to pipelineConnections")

            # Create all the fields that will be used in the newly created sub
            # config (under the attribute name "connections")
            configConnectionsNamespace: dict[str, pexConfig.Field] = {}
            for fieldName, obj in connectionsClass.allConnections.items():
                configConnectionsNamespace[fieldName] = pexConfig.Field[str](
                    doc=f"name for connection {fieldName}", default=obj.name, deprecated=obj.deprecated
                )
            # If there are default templates also add them as fields to
            # configure the template values
            if hasattr(connectionsClass, "defaultTemplates"):
                docString = "Template parameter used to format corresponding field template parameter"
                for templateName, default in connectionsClass.defaultTemplates.items():
                    configConnectionsNamespace[templateName] = TemplateField(
                        dtype=str,
                        doc=docString,
                        default=default,
                        deprecated=connectionsClass.deprecatedTemplates.get(templateName),
                    )
            # add a reference to the connection class used to create this sub
            # config
            configConnectionsNamespace["ConnectionsClass"] = connectionsClass

            # Create a new config class with the fields defined above
            Connections = type(f"{name}Connections", (pexConfig.Config,), configConnectionsNamespace)
            # add it to the Config class that is currently being declared
            dct["connections"] = pexConfig.ConfigField(
                dtype=Connections,
                doc="Configurations describing the connections of the PipelineTask to datatypes",
            )
            dct["ConnectionsConfigClass"] = Connections
            dct["ConnectionsClass"] = connectionsClass
        inst = super().__new__(cls, name, bases, dct)
        return inst

    def __init__(
        self, name: str, bases: tuple[type[PipelineTaskConfig], ...], dct: dict[str, Any], **kwargs: Any
    ):
        # This overrides the default init to drop the kwargs argument. Python
        # metaclasses will have this argument set if any kwargs are passes at
        # class construction time, but should be consumed before calling
        # __init__ on the type metaclass. This is in accordance with python
        # documentation on metaclasses
        super().__init__(name, bases, dct)

    ConnectionsClass: type[PipelineTaskConnections]
    ConnectionsConfigClass: type[pexConfig.Config]


class PipelineTaskConfig(pexConfig.Config, metaclass=PipelineTaskConfigMeta):
    """Configuration class for `PipelineTask`.

    This Configuration class functions in largely the same manner as any other
    derived from `lsst.pex.config.Config`. The only difference is in how it is
    declared. `PipelineTaskConfig` children need to be declared with a
    pipelineConnections argument. This argument should specify a child class of
    `PipelineTaskConnections`. During the declaration of a `PipelineTaskConfig`
    a config class is created with information from the supplied connections
    class to allow configuration of the connections class. This dynamically
    created config class is then attached to the `PipelineTaskConfig` via a
    `~lsst.pex.config.ConfigField` with the attribute name ``connections``.
    """

    connections: pexConfig.ConfigField
    """Field which refers to a dynamically added configuration class which is
    based on a PipelineTaskConnections class.
    """

    saveLogOutput = pexConfig.Field[bool](
        default=True,
        optional=False,
        doc="Flag to enable/disable saving of log output for a task, enabled by default.",
    )

    def applyConfigOverrides(
        self,
        instrument: Instrument | None,
        taskDefaultName: str,
        pipelineConfigs: Iterable[ConfigIR] | None,
        parameters: ParametersIR,
        label: str,
    ) -> None:
        """Apply config overrides to this config instance.

        Parameters
        ----------
        instrument : `Instrument` or `None`
            An instance of the `Instrument` specified in a pipeline.
            If `None` then the pipeline did not specify and instrument.
        taskDefaultName : `str`
            The default name associated with the `Task` class. This
            may be used with instrumental overrides.
        pipelineConfigs : `~collections.abc.Iterable` \
                of `~.pipelineIR.ConfigIR`
            An iterable of `~.pipelineIR.ConfigIR` objects that contain
            overrides to apply to this config instance.
        parameters : `~.pipelineIR.ParametersIR`
            Parameters defined in a Pipeline which are used in formatting
            of config values across multiple `Task` in a pipeline.
        label : `str`
            The label associated with this class's Task in a pipeline.
        """
        overrides = ConfigOverrides()
        overrides.addParameters(parameters)
        if instrument is not None:
            overrides.addInstrumentOverride(instrument, taskDefaultName)
        if pipelineConfigs is not None:
            for subConfig in (configIr.formatted(parameters) for configIr in pipelineConfigs):
                if subConfig.dataId is not None:
                    raise NotImplementedError(
                        "Specializing a config on a partial data id is not yet "
                        "supported in Pipeline definition"
                    )
                # only apply override if it applies to everything
                if subConfig.dataId is None:
                    if subConfig.file:
                        for configFile in subConfig.file:
                            overrides.addFileOverride(os.path.expandvars(configFile))
                    if subConfig.python is not None:
                        overrides.addPythonOverride(subConfig.python)
                    for key, value in subConfig.rest.items():
                        overrides.addValueOverride(key, value)
        overrides.applyTo(self)
