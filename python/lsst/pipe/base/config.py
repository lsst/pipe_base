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

"""Module defining config classes for PipelineTask.
"""

__all__ = ["InputDatasetConfig", "InputDatasetField",
           "OutputDatasetConfig", "OutputDatasetField",
           "InitInputDatasetConfig", "InitInputDatasetField",
           "InitOutputDatasetConfig", "InitOutputDatasetField",
           "ResourceConfig", "QuantumConfig", "PipelineTaskConfig"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from textwrap import dedent, indent

# -----------------------------
#  Imports for other modules --
# -----------------------------
import lsst.pex.config as pexConfig

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

# ------------------------
#  Exported definitions --
# ------------------------

PIPELINETASK_CONFIG_TEMPLATE_DICT = {}


def _makeDatasetField(name, dtype):
    """ Function to make callables which produce ConfigField objects

    This is factory function which produces factory functions. The factories
    returned by this function are used to simplify the process of creating
    `lsst.pex.config.ConfigField` which have dtypes derived from either
    `_DatasetTypeConfig`, or `_GlobalDatasetTypeConfig`. These functions can
    then be used in a mannor similar to other `~lsst.pex.config.ConfigField`
    constructors.

    Below is a flow diagram to explain the use of this function visually,
    where arrows indicate processing flow.

    Make a ConfigField factory:
    _makeDatasetField() -> return wrappedFunc -> assign to variable corresponding
        to name

    Use a ConfigField factory:
    name() -> factory() -> return pexConfig instance

    Example
    -------
    FooField = _makeDatasetField("FooField", FooConfig)
    fooFieldInstance = FooField("An example Foo ConfigField",
                                "fooConfigurable",
                                ("tract", "patch"),
                                "Exposure")

    Parameters
    ----------
    name : `str`
        The name to use as the final output `~lsst.pex.config.Field`
        constructor
    dtype : Configuration Object
        This is the python type to set as the dtype in the ConfigField
        construction

    Returns
    -------
    func : function
        Python callable function which can be used to produce instances of
        `~lsst.pex.config.ConfigField` of type dtype.

    Raises
    ------
    TypeError
        Possibly raises a TypeError if attempting to create a factory function
        from an incompatible type
    """

    def factory(**kwargs):
        """ This is the innermost function in the closure, and does the work
        of actually producing the ConfigField
        """
        # kwargs contain all the variables needed to construct the ConfigField
        # The doc and check variables are used to construct the ConfigField,
        # while the rest are used in the construction of the dtype object,
        # which is why doc and check are filted out before unpacking the
        # dictionary to the dtype constructor.
        return pexConfig.ConfigField(doc=kwargs['doc'],
                                     dtype=dtype,
                                     default=dtype(
                                         **{k: v for k, v in kwargs.items()
                                             if k not in ('doc', 'check')}),
                                     check=kwargs['check'])

    # here the dtype is checked against its baseclass type. This is due to the fact
    # the code nesseary to make ConfigField factories is shared, but the arguments
    # the factories take differ between base class types
    if issubclass(dtype, _GlobalDatasetTypeConfig):
        # Handle global dataset types like InitInputDatasetConfig, these types have
        # a function signature with no dimensions variable
        def wrappedFunc(*, doc, storageClass, check=None, name="", nameTemplate=''):
            return factory(**{k: v for k, v in locals().items() if k != 'factory'})
        # This factory does not take a dimensions argument, so set the
        # variables for the dimensions documentation to empty python strings
        extraDoc = ""
        extraFields = ""
    elif issubclass(dtype, _DatasetTypeConfig):
        # Handle dataset types like InputDatasetConfig, note these take a dimensions argument
        def wrappedFunc(*, doc, dimensions, storageClass, name="", scalar=False, check=None, nameTemplate='',
                        manualLoad=False):
            return factory(**{k: v for k, v in locals().items() if k != 'factory'})
        # Set the string corresponding to the dimensions parameter documentation
        # formatting is to support final output of the docstring variable
        extraDoc = """
            dimensions : iterable of `str`
                Iterable of Dimensions for this `~lsst.daf.butler.DatasetType`
            scalar : `bool`, optional
                If set to True then only a single dataset is expected on input or
                produced on output. In that case list of objects/DataIds will be
                unpacked before calling task methods, returned data is expected
                to contain single objects as well.
            nameTemplate : `str`, optional
                Template for the `name` field which is specified as a python formattable
                string. The template is formatted during the configuration of a Config
                class with a user defined string. Defaults to empty string, in which
                case no formatting is done.
            manualLoad : `bool`
                Indicates runQuantum will not load the data from the butler, and that
                the task intends to do the loading itself. Defaults to False
            """
        # Set a string to add the dimensions argument to the list of arguments in the
        # docstring explanation section formatting is to support final output
        # of the docstring variable
        extraFields = ", dimensions, scalar, nameTemplate, manualLoad"
    else:
        # if someone tries to create a config factory for a type that is not
        # handled raise and exception
        raise TypeError(f"Cannot create a factory for dtype {dtype}")

    # Programatically create a docstring to use in the factory function
    docstring = f"""    Factory function to create `~lsst.pex.config.Config` class instances
    of `{dtype.__name__}`

    This function servers as syntactic sugar for creating
    `~lsst.pex.config.ConfigField` which are `{dtype.__name__}`. The naming of
    this function violates the normal convention of a lowercase first letter
    in the function name, as this function is intended to sit in the same
    place as `~lsst.pex.config.ConfigField` classes, and consistency in
    declaration syntax is important.

    The input arguments for this class are a combination of the arguments for
    `~lsst.pex.config.ConfigField` and `{dtype.__name__}`. The arguments
    doc and check come from `~lsst.pex.config.ConfigField`, while name{extraFields}
    and storageClass come from `{dtype.__name__}`.

    Parameters
    ----------
    doc : `str`
        Documentation string for the `{dtype.__name__}`
    name : `str`
        Name of the `~lsst.daf.butler.DatasetType` in the returned
        `{dtype.__name__}`{indent(dedent(extraDoc), " " * 4)}
    storageClass : `str`
        Name of the `~lsst.daf.butler.StorageClass` in the `{dtype.__name__}`
    check : callable
        A callable to be called with the field value that returns
        False if the value is invalid.

    Returns
    -------
    result : `~lsst.pex.config.ConfigField`
        Instance of a `~lsst.pex.config.ConfigField` with `InputDatasetConfig` as a dtype
    """
    # Set the name to be used for the returned ConfigField factory function
    wrappedFunc.__name__ = name
    # Set the name to be used for the returned ConfigField factory function, and unindent
    # the docstring as it was indednted to corrispond to this factory functions indention
    wrappedFunc.__doc__ = dedent(docstring)
    return wrappedFunc


class QuantumConfig(pexConfig.Config):
    """Configuration class which defines PipelineTask quanta dimensions.

    In addition to a list of dataUnit names this also includes optional list of
    SQL statements to be executed against Registry database. Exact meaning and
    format of SQL will be determined at later point.
    """
    dimensions = pexConfig.ListField(dtype=str,
                                     doc="list of Dimensions which define quantum")
    sql = pexConfig.ListField(dtype=str,
                              doc="sequence of SQL statements",
                              optional=True)


class _BaseDatasetTypeConfig(pexConfig.Config):
    """Intermediate base class for dataset type configuration in PipelineTask.
    """
    name = pexConfig.Field(dtype=str,
                           doc="name of the DatasetType")
    storageClass = pexConfig.Field(dtype=str,
                                   doc="name of the StorageClass")
    nameTemplate = pexConfig.Field(dtype=str,
                                   default='',
                                   optional=True,
                                   doc=("Templated name of string, used to set name "
                                        "field according to a shared substring when "
                                        "`~PipelineTaskConfig.formatTemplateNames` "
                                        "is called"))


class _DatasetTypeConfig(_BaseDatasetTypeConfig):
    """Configuration class which defines dataset type used by PipelineTask.

    Consists of DatasetType name, list of Dimension names and StorageCass name.
    PipelineTasks typically define one or more input and output datasets. This
    class should not be used directly, instead one of `InputDatasetConfig` or
    `OutputDatasetConfig` should be used in PipelineTask config.
    """
    dimensions = pexConfig.ListField(dtype=str,
                                     doc="list of Dimensions for this DatasetType")
    scalar = pexConfig.Field(dtype=bool,
                             default=False,
                             optional=True,
                             doc=("If set to True then only a single dataset is expected "
                                  "on input or produced on output. In that case list of "
                                  "objects/DataIds will be unpacked before calling task "
                                  "methods, returned data is expected to contain single "
                                  "objects as well."))
    manualLoad = pexConfig.Field(dtype=bool,
                                 default=False,
                                 optional=True,
                                 doc=("If this is set to True, the class intends to load "
                                      "the data associated with this Configurable Field "
                                      "manually, and runQuantum should not load it. Should "
                                      "not be set by configuration override"))


class InputDatasetConfig(_DatasetTypeConfig):
    pass


class OutputDatasetConfig(_DatasetTypeConfig):
    pass


class _GlobalDatasetTypeConfig(_BaseDatasetTypeConfig):
    """Configuration class which defines dataset types used in PipelineTask
    initialization.

    Consists of DatasetType name and StorageCass name, with a read-only
    ``dimensions`` property that returns an empty tuple, enforcing the
    constraint that datasets used in initialization are not associated with
    any Dimensions. This class should not be used directly, instead one of
    `InitInputDatasetConfig` or `InitOutputDatasetConfig` should be used in
    PipelineTask config.
    """
    @property
    def dimensions(self):
        """Dimensions associated with this DatasetType (always empty)."""
        return ()


class InitInputDatasetConfig(_GlobalDatasetTypeConfig):
    pass


class InitOutputDatasetConfig(_GlobalDatasetTypeConfig):
    pass


class ResourceConfig(pexConfig.Config):
    """Configuration for resource requirements.

    This configuration class will be used by some activators to estimate
    resource use by pipeline. Additionally some tasks could use it to adjust
    their resource use (e.g. reduce the number of threads).

    For some resources their limit can be estimated by corresponding task,
    in that case task could set the field value. For many fields defined in
    this class their associated resource used by a task will depend on the
    size of the data and is not known in advance. For these resources their
    value will be configured through overrides based on some external
    estimates.
    """
    minMemoryMB = pexConfig.Field(dtype=int, default=None, optional=True,
                                  doc="Minimal memory needed by task, can be None if estimate is unknown.")
    minNumCores = pexConfig.Field(dtype=int, default=1,
                                  doc="Minimal number of cores needed by task.")


class PipelineTaskConfig(pexConfig.Config):
    """Base class for all PipelineTask configurations.

    This class defines fields that must be defined for every
    `~lsst.pipe.base.PipelineTask`. It will be used as a base class for all
    `~lsst.pipe.base.PipelineTask` configurations instead of
    `pex.config.Config`.
    """
    quantum = pexConfig.ConfigField(dtype=QuantumConfig,
                                    doc="configuration for PipelineTask quantum")

    def formatTemplateNames(self, templateParamsDict):
        # Look up the stored parameters for the specific instance of this config
        # class
        storedParamsDict = PIPELINETASK_CONFIG_TEMPLATE_DICT.setdefault(id(self), {})
        storedParamsDict.update(templateParamsDict)
        for key, value in self.items():
            if isinstance(value, _BaseDatasetTypeConfig) and value.nameTemplate != '':
                value.name = value.nameTemplate.format(**storedParamsDict)


InputDatasetField = _makeDatasetField("InputDatasetField", InputDatasetConfig)
OutputDatasetField = _makeDatasetField("OutputDatasetField", OutputDatasetConfig)
InitInputDatasetField = _makeDatasetField("InitInputDatasetField", InitInputDatasetConfig)
InitOutputDatasetField = _makeDatasetField("InitOutputDatasetField", InitOutputDatasetConfig)
