# This file is part of task_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""This module defines YAML I/O for key lsst.pipe.base classes."""
from .task_metadata import TaskMetadata

try:
    import yaml
except ImportError:
    yaml = None

if yaml:
    try:
        # CLoader is not always available
        from yaml import CLoader
    except ImportError:
        CLoader = None

    yaml_loaders = (yaml.Loader, yaml.FullLoader, yaml.SafeLoader, yaml.UnsafeLoader)
    if CLoader is not None:
        yaml_loaders += (CLoader,)


def tm_representer(dumper, data):
    """Represent an lsst.pipe.base.TaskMetadata as a mapping.
    """
    return dumper.represent_mapping('lsst.pipe.base.TaskMetadata', data)


if yaml:
    yaml.add_representer(TaskMetadata, tm_representer)


def tm_constructor(loader, node):
    """Construct an lsst.pipe.base.TaskMetadata from a YAML serialization.
    """
    meta = loader.construct_mapping(node, deep=True)
    return TaskMetadata(meta)


if yaml:
    for loader in yaml_loaders:
        yaml.add_constructor('lsst.pipe.base.TaskMetadata', tm_constructor, Loader=loader)
