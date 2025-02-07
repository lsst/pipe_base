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

"""Module which defines ConfigOverrides class and related methods."""

from __future__ import annotations

__all__ = ["ConfigOverrides"]

import ast
import contextlib
import inspect
from enum import Enum
from operator import attrgetter
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from lsst.resources import ResourcePath

from ._instrument import Instrument

if TYPE_CHECKING:
    from .pipelineIR import ParametersIR

OverrideTypes = Enum("OverrideTypes", "Value File Python Instrument")


class _FrozenSimpleNamespace(SimpleNamespace):
    """SimpleNamespace subclass which disallows setting after construction"""

    def __init__(self, **kwargs: Any) -> None:
        object.__setattr__(self, "_frozen", False)
        super().__init__(**kwargs)
        self._frozen = True

    def __setattr__(self, __name: str, __value: Any) -> None:
        if self._frozen:
            raise ValueError("Cannot set attributes on parameters")
        else:
            return super().__setattr__(__name, __value)


class ConfigExpressionParser(ast.NodeVisitor):
    """An expression parser that will be used to transform configuration
    strings supplied from the command line or a pipeline into a python
    object.

    This is roughly equivalent to ast.literal_parser, but with the ability to
    transform strings that are valid variable names into the value associated
    with the name. Variables that should be considered valid are supplied to
    the constructor as a dictionary that maps a string to its corresponding
    value.

    This class in an internal implementation detail, and should not be exposed
    outside this module.

    Parameters
    ----------
    namespace : `dict` of `str` to variable
        This is a mapping of strings corresponding to variable names, to the
        object that is associated with that name.
    """

    def __init__(self, namespace):
        self.variables = namespace

    def visit_Name(self, node):
        """Handle a node corresponding to a variable name.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        # If the id (name) of the variable is in the dictionary of valid names,
        # load and return the corresponding variable.
        if node.id in self.variables:
            return self.variables[node.id]
        # If the node does not correspond to a valid variable, turn the name
        # into a string, as the user likely intended it as such.
        return f"{node.id}"

    def visit_List(self, node):
        """Build a list out of the sub nodes when a list node is
        encountered.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        return [self.visit(elm) for elm in node.elts]

    def visit_Tuple(self, node):
        """Build a list out of the sub nodes and then turn it into a
        tuple.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        return tuple(self.visit_List(node))

    def visit_Constant(self, node):
        """Return constant from node.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        return node.value

    def visit_Dict(self, node):
        """Build dict out of component nodes if dict node encountered.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        return {self.visit(key): self.visit(value) for key, value in zip(node.keys, node.values, strict=True)}

    def visit_Set(self, node):
        """Build set out of node is set encountered.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        return {self.visit(el) for el in node.elts}

    def visit_UnaryOp(self, node):
        """Handle unary operators.

        This method is visited if the node is a unary operator. Currently
        The only operator we support is the negative (-) operator, all others
        are passed to generic_visit method.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        if isinstance(node.op, ast.USub):
            value = self.visit(node.operand)
            return -1 * value
        self.generic_visit(node)

    def generic_visit(self, node):
        """Handle other node types.

        This method is called for all other node types. It will just raise
        a value error, because this is a type of expression that we do not
        support.

        Parameters
        ----------
        node : `ast.Name`
            Node to use.
        """
        raise ValueError("Unable to parse string into literal expression")


class ConfigOverrides:
    """Defines a set of overrides to be applied to a task config.

    Overrides for task configuration need to be applied by activator when
    creating task instances. This class represents an ordered set of such
    overrides which activator receives from some source (e.g. command line
    or some other configuration).

    Methods
    -------
    addFileOverride(filename)
        Add overrides from a specified file.
    addValueOverride(field, value)
        Add override for a specific field.
    applyTo(config)
        Apply all overrides to a `config` instance.

    Notes
    -----
    Serialization support for this class may be needed, will add later if
    necessary.
    """

    def __init__(self) -> None:
        self._overrides: list[tuple[OverrideTypes, Any]] = []
        self._parameters: SimpleNamespace | None = None

    def addParameters(self, parameters: ParametersIR) -> None:
        """Add parameters which will be substituted when applying overrides.

        Parameters
        ----------
        parameters : `ParametersIR`
            Override parameters in the form as read from a Pipeline file.

        Notes
        -----
        This method may be called more than once, but each call will overwrite
        any previous parameter defined with the same name.
        """
        if self._parameters is None:
            self._parameters = SimpleNamespace()

        for key, value in parameters.mapping.items():
            setattr(self._parameters, key, value)

    def addFileOverride(self, filename):
        """Add overrides from a specified file.

        Parameters
        ----------
        filename : convertible to `~lsst.resources.ResourcePath`
            Path or URI to the override file.  All URI schemes supported by
            `~lsst.resources.ResourcePath` are supported.
        """
        self._overrides.append((OverrideTypes.File, ResourcePath(filename)))

    def addValueOverride(self, field, value):
        """Add override for a specific field.

        This method is not very type-safe as it is designed to support
        use cases where input is given as string, e.g. command line
        activators. If `value` has a string type and setting of the field
        fails with `TypeError` the we'll attempt `eval()` the value and
        set the field with that value instead.

        Parameters
        ----------
        field : str
            Fully-qualified field name.
        value : `~typing.Any`
            Value to be given to a filed.
        """
        self._overrides.append((OverrideTypes.Value, (field, value)))

    def addPythonOverride(self, python_snippet: str) -> None:
        """Add Overrides by running a snippit of python code against a config.

        Parameters
        ----------
        python_snippet : str
            A string which is valid python code to be executed. This is done
            with config as the only local accessible value.
        """
        self._overrides.append((OverrideTypes.Python, python_snippet))

    def addInstrumentOverride(self, instrument: Instrument, task_name: str) -> None:
        """Apply any overrides that an instrument has for a task.

        Parameters
        ----------
        instrument : `Instrument`
            An instrument instance which will apply configs.
        task_name : str
            The _DefaultName of a task associated with a config, used to look
            up overrides from the instrument.
        """
        self._overrides.append((OverrideTypes.Instrument, (instrument, task_name)))

    def _parser(self, value, configParser):
        # Exception probably means it is a specific user string such as a URI.
        # Let the value return as a string to attempt to continue to
        # process as a string, another error will be raised in downstream
        # code if that assumption is wrong
        with contextlib.suppress(Exception):
            value = configParser.visit(ast.parse(value, mode="eval").body)
        return value

    def applyTo(self, config):
        """Apply all overrides to a task configuration object.

        Parameters
        ----------
        config : `pex.Config`
            Configuration to apply to; modified in place.

        Raises
        ------
        `Exception` is raised if operations on configuration object fail.
        """
        # Look up a stack of variables people may be using when setting
        # configs. Create a dictionary that will be used akin to a namespace
        # for the duration of this function.
        localVars = {}
        # pull in the variables that are declared in module scope of the config
        mod = inspect.getmodule(config)
        localVars.update({k: v for k, v in mod.__dict__.items() if not k.startswith("__")})
        # put the supplied config in the variables dictionary
        localVars["config"] = config
        extraLocals = None

        # If any parameters are supplied add them to the variables dictionary
        if self._parameters is not None:
            # make a copy of the params and "freeze" it
            localParams = _FrozenSimpleNamespace(**vars(self._parameters))
            localVars["parameters"] = localParams
            extraLocals = {"parameters": localParams}

        # Create a parser for config expressions that may be strings
        configParser = ConfigExpressionParser(namespace=localVars)

        for otype, override in self._overrides:
            if otype is OverrideTypes.File:
                with override.open("r") as buffer:
                    config.loadFromStream(buffer, filename=override.ospath, extraLocals=extraLocals)
            elif otype is OverrideTypes.Value:
                field, value = override
                if isinstance(value, str):
                    value = self._parser(value, configParser)
                # checking for dicts and lists here is needed because {} and []
                # are valid yaml syntax so they get converted before hitting
                # this method, so we must parse the elements.
                #
                # The same override would remain a string if specified on the
                # command line, and is handled above.
                if isinstance(value, dict):
                    new = {}
                    for k, v in value.items():
                        if isinstance(v, str):
                            new[k] = self._parser(v, configParser)
                        else:
                            new[k] = v
                    value = new
                elif isinstance(value, list):
                    new = []
                    for v in value:
                        if isinstance(v, str):
                            new.append(self._parser(v, configParser))
                        else:
                            new.append(v)
                    value = new
                # The field might be a string corresponding to a attribute
                # hierarchy, attempt to split off the last field which
                # will then be set.
                parent, *child = field.rsplit(".", maxsplit=1)
                if child:
                    # This branch means there was a hierarchy, get the
                    # field to set, and look up the sub config for which
                    # it is to be set
                    finalField = child[0]
                    tmpConfig = attrgetter(parent)(config)
                else:
                    # There is no hierarchy, the config is the base config
                    # and the field is exactly what was passed in
                    finalField = parent
                    tmpConfig = config
                # set the specified config
                setattr(tmpConfig, finalField, value)

            elif otype is OverrideTypes.Python:
                # exec python string with the context of all vars known. This
                # both lets people use a var they know about (maybe a bit
                # dangerous, but not really more so than arbitrary python exec,
                # and there are so many other places to worry about security
                # before we start changing this) and any imports that are done
                # in a python block will be put into this scope. This means
                # other config setting branches can make use of these
                # variables.
                exec(override, None, localVars)
            elif otype is OverrideTypes.Instrument:
                instrument, name = override
                instrument.applyConfigOverrides(name, config)
