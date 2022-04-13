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

"""Module which defines ConfigOverrides class and related methods.
"""

__all__ = ["ConfigOverrides"]

import ast
import inspect
from enum import Enum
from operator import attrgetter

from lsst.resources import ResourcePath
from lsst.utils import doImportType

OverrideTypes = Enum("OverrideTypes", "Value File Python Instrument")


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
        object that is associated with that name
    """

    def __init__(self, namespace):
        self.variables = namespace

    def visit_Name(self, node):
        """This method gets called when the parser has determined a node
        corresponds to a variable name.
        """
        # If the id (name) of the variable is in the dictionary of valid names,
        # load and return the corresponding variable.
        if node.id in self.variables:
            return self.variables[node.id]
        # If the node does not correspond to a valid variable, turn the name
        # into a string, as the user likely intended it as such.
        return f"{node.id}"

    def visit_List(self, node):
        """This method is visited if the node is a list. Constructs a list out
        of the sub nodes.
        """
        return [self.visit(elm) for elm in node.elts]

    def visit_Tuple(self, node):
        """This method is visited if the node is a tuple. Constructs a list out
        of the sub nodes, and then turns it into a tuple.
        """
        return tuple(self.visit_List(node))

    def visit_Constant(self, node):
        """This method is visited if the node is a constant"""
        return node.value

    def visit_Dict(self, node):
        """This method is visited if the node is a dict. It builds a dict out
        of the component nodes.
        """
        return {self.visit(key): self.visit(value) for key, value in zip(node.keys, node.values)}

    def visit_Set(self, node):
        """This method is visited if the node is a set. It builds a set out
        of the component nodes.
        """
        return {self.visit(el) for el in node.elts}

    def visit_UnaryOp(self, node):
        """This method is visited if the node is a unary operator. Currently
        The only operator we support is the negative (-) operator, all others
        are passed to generic_visit method.
        """
        if isinstance(node.op, ast.USub):
            value = self.visit(node.operand)
            return -1 * value
        self.generic_visit(node)

    def generic_visit(self, node):
        """This method is called for all other node types. It will just raise
        a value error, because this is a type of expression that we do not
        support.
        """
        raise ValueError("Unable to parse string into literal expression")


class ConfigOverrides:
    """Defines a set of overrides to be applied to a task config.

    Overrides for task configuration need to be applied by activator when
    creating task instances. This class represents an ordered set of such
    overrides which activator receives from some source (e.g. command line
    or some other configuration).

    Methods
    ----------
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

    def __init__(self):
        self._overrides = []

    def addFileOverride(self, filename):
        """Add overrides from a specified file.

        Parameters
        ----------
        filename : convertible to `ResourcePath`
            Path or URI to the override file.  All URI schemes supported by
            `ResourcePath` are supported.
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
        value :
            Value to be given to a filed.
        """
        self._overrides.append((OverrideTypes.Value, (field, value)))

    def addPythonOverride(self, python_snippet: str) -> None:
        """Add Overrides by running a snippit of python code against a config.

        Parameters
        ----------
        python_snippet: str
            A string which is valid python code to be executed. This is done
            with config as the only local accessible value.
        """
        self._overrides.append((OverrideTypes.Python, python_snippet))

    def addInstrumentOverride(self, instrument: str, task_name: str) -> None:
        """Apply any overrides that an instrument has for a task

        Parameters
        ----------
        instrument: str
            A string containing the fully qualified name of an instrument from
            which configs should be loaded and applied
        task_name: str
            The _DefaultName of a task associated with a config, used to look
            up overrides from the instrument.
        """
        instrument_cls: type = doImportType(instrument)
        instrument_lib = instrument_cls()
        self._overrides.append((OverrideTypes.Instrument, (instrument_lib, task_name)))

    def _parser(self, value, configParser):
        try:
            value = configParser.visit(ast.parse(value, mode="eval").body)
        except Exception:
            # This probably means it is a specific user string such as a URI.
            # Let the value return as a string to attempt to continue to
            # process as a string, another error will be raised in downstream
            # code if that assumption is wrong
            pass

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
        vars = {}
        # pull in the variables that are declared in module scope of the config
        mod = inspect.getmodule(config)
        vars.update({k: v for k, v in mod.__dict__.items() if not k.startswith("__")})
        # put the supplied config in the variables dictionary
        vars["config"] = config

        # Create a parser for config expressions that may be strings
        configParser = ConfigExpressionParser(namespace=vars)

        for otype, override in self._overrides:
            if otype is OverrideTypes.File:
                with override.open("r") as buffer:
                    config.loadFromStream(buffer, filename=override.ospath)
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
                exec(override, None, vars)
            elif otype is OverrideTypes.Instrument:
                instrument, name = override
                instrument.applyConfigOverrides(name, config)
