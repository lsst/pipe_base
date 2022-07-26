#
# LSST Data Management System
# Copyright 2008-2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#
__all__ = [
    "ArgumentParser",
    "ConfigFileAction",
    "ConfigValueAction",
    "DataIdContainer",
    "DatasetArgument",
    "ConfigDatasetType",
    "InputOnlyArgumentParser",
]

import argparse

from deprecated.sphinx import deprecated


@deprecated(
    reason="Gen2 DataIdContainer is no longer supported. This functionality has been disabled.",
    version="v23.0",
    category=FutureWarning,
)
class DataIdContainer:
    """Container for data IDs and associated data references.

    Parameters
    ----------
    level : `str`
        The lowest hierarchy level to descend to for this dataset type,
        for example `"amp"` for `"raw"` or `"ccd"` for `"calexp"`.
        Use `""` to use the mapper's default for the dataset type.
        This class does not support `None`, but if it did, `None`
        would mean the level should not be restricted.

    Notes
    -----
    Override this class for data IDs that require special handling to be
    converted to ``data references``, and specify the override class
    as ``ContainerClass`` for ``add_id_argument``.

    If you don't want the argument parser to compute data references,
    specify ``doMakeDataRefList=False`` in ``add_id_argument``.
    """

    def __init__(self, level=None):
        pass


@deprecated(
    reason="Gen2 DatasetArgument is no longer supported. This functionality has been disabled.",
    version="v23.0",
    category=FutureWarning,
)
class DatasetArgument:
    """Dataset type specified by a command-line argument.

    Parameters
    ----------
    name : `str`, optional
        Name of command-line argument (including leading "--",
        if appropriate) whose value is the dataset type.
        If `None`, uses ``--idName_dstype`` where idName
        is the name of the data ID argument (e.g. "id").
    help : `str`, optional
        Help string for the command-line argument.
    default : `object`, optional
        Default value. If `None`, then the command-line option is required.
        This argument isignored if the command-line argument is positional
        (name does not start with "-") because positional arguments do
        not support default values.
    """

    def __init__(
        self,
        name=None,
        help="dataset type to process from input data repository",
        default=None,
    ):
        pass


@deprecated(
    reason="Gen2 ConfigDatasetType is no longer supported. This functionality has been disabled.",
    version="v23.0",
    category=FutureWarning,
)
class ConfigDatasetType:
    """Dataset type specified by a config parameter.

    Parameters
    ----------
    name : `str`
        Name of config option whose value is the dataset type.
    """

    def __init__(self, name):
        pass


@deprecated(
    reason="Gen2 ArgumentParser is no longer supported. This functionality has been disabled.",
    version="v23.0",
    category=FutureWarning,
)
class ArgumentParser(argparse.ArgumentParser):
    """Argument parser for command-line tasks that is based on
    `argparse.ArgumentParser`.

    Parameters
    ----------
    name : `str`
        Name of top-level task; used to identify camera-specific override
        files.
    usage : `str`, optional
        Command-line usage signature.
    **kwargs
        Additional keyword arguments for `argparse.ArgumentParser`.

    Notes
    -----
    Users may wish to add additional arguments before calling `parse_args`.
    """

    def __init__(self, name, usage="%(prog)s input [options]", **kwargs):
        pass


class InputOnlyArgumentParser(ArgumentParser):
    """`ArgumentParser` for command-line tasks that don't write any output."""


class ConfigValueAction(argparse.Action):
    """argparse action callback to override config parameters using
    name=value pairs from the command-line.
    """

    def __call__(self, parser, namespace, values, option_string):
        pass


class ConfigFileAction(argparse.Action):
    """argparse action to load config overrides from one or more files."""

    def __call__(self, parser, namespace, values, option_string=None):
        """Load one or more files of config overrides.

        Parameters
        ----------
        parser : `argparse.ArgumentParser`
            Argument parser.
        namespace : `argparse.Namespace`
            Parsed command. The following attributes are updated by this
            method: ``namespace.config``.
        values : `list`
            A list of data config file paths.
        option_string : `str`, optional
            Option value specified by the user.
        """
        pass
