__all__ = ("ConfigIR", "ContractError", "ContractIR", "InheritIR", "PipelineIR", "TaskIR")
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

from collections import Counter
from dataclasses import dataclass, field
from typing import List, Union, Generator

import os
import yaml
import warnings


class PipelineYamlLoader(yaml.SafeLoader):
    """This is a specialized version of yaml's SafeLoader. It checks and raises
    an exception if it finds that there are multiple instances of the same key
    found inside a pipeline file at a given scope.
    """
    def construct_mapping(self, node, deep=False):
        # do the call to super first so that it can do all the other forms of
        # checking on this node. If you check the uniqueness of keys first
        # it would save the work that super does in the case of a failure, but
        # it might fail in the case that the node was the incorrect node due
        # to a parsing error, and the resulting exception would be difficult to
        # understand.
        mapping = super().construct_mapping(node, deep)
        # Check if there are any duplicate keys
        all_keys = Counter(key_node.value for key_node, _ in node.value)
        duplicates = {k for k, i in all_keys.items() if i != 1}
        if duplicates:
            raise KeyError("Pipeline files must not have duplicated keys, "
                           f"{duplicates} appeared multiple times")
        return mapping


class ContractError(Exception):
    """An exception that is raised when a pipeline contract is not satisfied
    """
    pass


@dataclass
class ContractIR:
    """Intermediate representation of contracts read from a pipeline yaml file.
    """
    contract: str
    """A string of python code representing one or more conditions on configs
    in a pipeline. This code-as-string should, once evaluated, should be True
    if the configs are fine, and False otherwise.
    """
    msg: Union[str, None] = None
    """An optional message to be shown to the user if a contract fails
    """

    def to_primitives(self) -> dict:
        """Convert to a representation used in yaml serialization
        """
        accumulate = {"contract": self.contract}
        if self.msg is not None:
            accumulate['msg'] = self.msg
        return accumulate

    def __eq__(self, other: "ContractIR"):
        if not isinstance(other, ContractIR):
            return False
        elif self.contract == other.contract and self.msg == other.msg:
            return True
        else:
            return False


@dataclass
class ConfigIR:
    """Intermediate representation of configurations read from a pipeline yaml
    file.
    """
    python: Union[str, None] = None
    """A string of python code that is used to modify a configuration. This can
    also be None if there are no modifications to do.
    """
    dataId: Union[dict, None] = None
    """A dataId that is used to constrain these config overrides to only quanta
    with matching dataIds. This field can be None if there is no constraint.
    This is currently an unimplemented feature, and is placed here for future
    use.
    """
    file: List[str] = field(default_factory=list)
    """A list of paths which points to a file containing config overrides to be
    applied. This value may be an empty list if there are no overrides to apply.
    """
    rest: dict = field(default_factory=dict)
    """This is a dictionary of key value pairs, where the keys are strings
    corresponding to qualified fields on a config to override, and the values
    are strings representing the values to apply.
    """

    def to_primitives(self) -> dict:
        """Convert to a representation used in yaml serialization
        """
        accumulate = {}
        for name in ("python", "dataId", "file"):
            # if this attribute is thruthy add it to the accumulation dictionary
            if getattr(self, name):
                accumulate[name] = getattr(self, name)
        # Add the dictionary containing the rest of the config keys to the
        # # accumulated dictionary
        accumulate.update(self.rest)
        return accumulate

    def maybe_merge(self, other_config: "ConfigIR") -> Generator["ConfigIR", None, None]:
        """Merges another instance of a `ConfigIR` into this instance if
        possible. This function returns a generator that is either self
        if the configs were merged, or self, and other_config if that could
        not be merged.

        Parameters
        ----------
        other_config : `ConfigIR`
            An instance of `ConfigIR` to merge into this instance.

        Returns
        -------
        Generator : `ConfigIR`
            A generator containing either self, or self and other_config if
            the configs could be merged or not respectively.
        """
        # Verify that the config blocks can be merged
        if self.dataId != other_config.dataId or self.python or other_config.python or\
                self.file or other_config.file:
            yield from (self, other_config)
            return

        # create a set of all keys, and verify two keys do not have different
        # values
        key_union = self.rest.keys() & other_config.rest.keys()
        for key in key_union:
            if self.rest[key] != other_config.rest[key]:
                yield from (self, other_config)
                return
        self.rest.update(other_config.rest)

        # Combine the lists of override files to load
        self_file_set = set(self.file)
        other_file_set = set(other_config.file)
        self.file = list(self_file_set.union(other_file_set))

        yield self

    def __eq__(self, other: "ConfigIR"):
        if not isinstance(other, ConfigIR):
            return False
        elif all(getattr(self, attr) == getattr(other, attr) for attr in
                 ("python", "dataId", "file", "rest")):
            return True
        else:
            return False


@dataclass
class TaskIR:
    """Intermediate representation of tasks read from a pipeline yaml file.
    """
    label: str
    """An identifier used to refer to a task.
    """
    klass: str
    """A string containing a fully qualified python class to be run in a
    pipeline.
    """
    config: Union[List[ConfigIR], None] = None
    """List of all configs overrides associated with this task, and may be
    `None` if there are no config overrides.
    """

    def to_primitives(self) -> dict:
        """Convert to a representation used in yaml serialization
        """
        accumulate = {'class': self.klass}
        if self.config:
            accumulate['config'] = [c.to_primitives() for c in self.config]
        return accumulate

    def add_or_update_config(self, other_config: ConfigIR):
        """Adds a `ConfigIR` to this task if one is not present. Merges configs
        if there is a `ConfigIR` present and the dataId keys of both configs
        match, otherwise adds a new entry to the config list. The exception to
        the above is that if either the last config or other_config has a python
        block, then other_config is always added, as python blocks can modify
        configs in ways that cannot be predicted.

        Parameters
        ----------
        other_config : `ConfigIR`
            A `ConfigIR` instance to add or merge into the config attribute of
            this task.
        """
        if not self.config:
            self.config = [other_config]
            return
        self.config.extend(self.config.pop().maybe_merge(other_config))

    def __eq__(self, other: "TaskIR"):
        if not isinstance(other, TaskIR):
            return False
        elif all(getattr(self, attr) == getattr(other, attr) for attr in
                 ("label", "klass", "config")):
            return True
        else:
            return False


@dataclass
class InheritIR:
    """An intermediate representation of inherited pipelines
    """
    location: str
    """This is the location of the pipeline to inherit. The path should be
    specified as an absolute path. Environment variables may be used in the path
    and should be specified as a python string template, with the name of the
    environment variable inside braces.
    """
    include: Union[List[str], None] = None
    """List of tasks that should be included when inheriting this pipeline.
    Either the include or exclude attributes may be specified, but not both.
    """
    exclude: Union[List[str], None] = None
    """List of tasks that should be excluded when inheriting this pipeline.
    Either the include or exclude attributes may be specified, but not both.
    """
    importContracts: bool = True
    """Boolean attribute to dictate if contracts should be inherited with the
    pipeline or not.
    """

    def toPipelineIR(self) -> "PipelineIR":
        """Convert to a representation used in yaml serialization
        """
        if self.include and self.exclude:
            raise ValueError("Both an include and an exclude list cant be specified"
                             " when declaring a pipeline import")
        tmp_pipeline = PipelineIR.from_file(os.path.expandvars(self.location))
        if tmp_pipeline.instrument is not None:
            warnings.warn("Any instrument definitions in imported pipelines are ignored. "
                          "if an instrument is desired please define it in the top most pipeline")

        new_tasks = {}
        for label, task in tmp_pipeline.tasks.items():
            if (self.include and label in self.include) or (self.exclude and label not in self.exclude)\
                    or (self.include is None and self.exclude is None):
                new_tasks[label] = task
        tmp_pipeline.tasks = new_tasks

        if not self.importContracts:
            tmp_pipeline.contracts = []

        return tmp_pipeline

    def __eq__(self, other: "InheritIR"):
        if not isinstance(other, InheritIR):
            return False
        elif all(getattr(self, attr) == getattr(other, attr) for attr in
                 ("location", "include", "exclude", "importContracts")):
            return True
        else:
            return False


class PipelineIR:
    """Intermediate representation of a pipeline definition

    Parameters
    ----------
    loaded_yaml : `dict`
        A dictionary which matches the structure that would be produced by a
        yaml reader which parses a pipeline definition document

    Raises
    ------
    ValueError :
        - If a pipeline is declared without a description
        - If no tasks are declared in a pipeline, and no pipelines are to be
          inherited
        - If more than one instrument is specified
        - If more than one inherited pipeline share a label
    """
    def __init__(self, loaded_yaml):
        # Check required fields are present
        if "description" not in loaded_yaml:
            raise ValueError("A pipeline must be declared with a description")
        if "tasks" not in loaded_yaml and "inherits" not in loaded_yaml:
            raise ValueError("A pipeline must be declared with one or more tasks")

        # Process pipeline description
        self.description = loaded_yaml.pop("description")

        # Process tasks
        self._read_tasks(loaded_yaml)

        # Process instrument keys
        inst = loaded_yaml.pop("instrument", None)
        if isinstance(inst, list):
            raise ValueError("Only one top level instrument can be defined in a pipeline")
        self.instrument = inst

        # Process any contracts
        self._read_contracts(loaded_yaml)

        # Process any inherited pipelines
        self._read_inherits(loaded_yaml)

    def _read_contracts(self, loaded_yaml):
        """Process the contracts portion of the loaded yaml document

        Parameters
        ---------
        loaded_yaml : `dict`
            A dictionary which matches the structure that would be produced by a
            yaml reader which parses a pipeline definition document
        """
        loaded_contracts = loaded_yaml.pop("contracts", [])
        if isinstance(loaded_contracts, str):
            loaded_contracts = [loaded_contracts]
        self.contracts = []
        for contract in loaded_contracts:
            if isinstance(contract, dict):
                self.contracts.append(ContractIR(**contract))
            if isinstance(contract, str):
                self.contracts.append(ContractIR(contract=contract))

    def _read_inherits(self, loaded_yaml):
        """Process the inherits portion of the loaded yaml document

        Parameters
        ---------
        loaded_yaml : `dict`
            A dictionary which matches the structure that would be produced by a
            yaml reader which parses a pipeline definition document
        """
        def process_args(argument: Union[str, dict]) -> dict:
            if isinstance(argument, str):
                return {"location": argument}
            elif isinstance(argument, dict):
                if "exclude" in argument and isinstance(argument["exclude"], str):
                    argument["exclude"] = [argument["exclude"]]
                if "include" in argument and isinstance(argument["include"], str):
                    argument["include"] = [argument["include"]]
                return argument
        tmp_inherit = loaded_yaml.pop("inherits", None)
        if tmp_inherit is None:
            self.inherits = []
        elif isinstance(tmp_inherit, list):
            self.inherits = [InheritIR(**process_args(args)) for args in tmp_inherit]
        else:
            self.inherits = [InheritIR(**process_args(tmp_inherit))]

        # integrate any imported pipelines
        accumulate_tasks = {}
        for other_pipeline in self.inherits:
            tmp_IR = other_pipeline.toPipelineIR()
            if accumulate_tasks.keys() & tmp_IR.tasks.keys():
                raise ValueError("Task labels in the imported pipelines must "
                                 "be unique")
            accumulate_tasks.update(tmp_IR.tasks)
            self.contracts.extend(tmp_IR.contracts)

        # merge the dict of label:TaskIR objects, preserving any configs in the
        # imported pipeline if the labels point to the same class
        for label, task in self.tasks.items():
            if label not in accumulate_tasks:
                accumulate_tasks[label] = task
            elif accumulate_tasks[label].klass == task.klass:
                if task.config is not None:
                    for config in task.config:
                        accumulate_tasks[label].add_or_update_config(config)
            else:
                accumulate_tasks[label] = task
        self.tasks = accumulate_tasks

    def _read_tasks(self, loaded_yaml):
        """Process the tasks portion of the loaded yaml document

        Parameters
        ---------
        loaded_yaml : `dict`
            A dictionary which matches the structure that would be produced by a
            yaml reader which parses a pipeline definition document
        """
        self.tasks = {}
        tmp_tasks = loaded_yaml.pop("tasks", None)
        if tmp_tasks is None:
            tmp_tasks = {}

        for label, definition in tmp_tasks.items():
            if isinstance(definition, str):
                definition = {"class": definition}
            config = definition.get('config', None)
            if config is None:
                task_config_ir = None
            else:
                if isinstance(config, dict):
                    config = [config]
                task_config_ir = []
                for c in config:
                    file = c.pop("file", None)
                    if file is None:
                        file = []
                    elif not isinstance(file, list):
                        file = [file]
                    task_config_ir.append(ConfigIR(python=c.pop("python", None),
                                                   dataId=c.pop("dataId", None),
                                                   file=file,
                                                   rest=c))
            self.tasks[label] = TaskIR(label, definition["class"], task_config_ir)

    @classmethod
    def from_string(cls, pipeline_string: str):
        """Create a `PipelineIR` object from a string formatted like a pipeline
        document

        Parameters
        ----------
        pipeline_string : `str`
            A string that is formatted according like a pipeline document
        """
        loaded_yaml = yaml.load(pipeline_string, Loader=PipelineYamlLoader)
        return cls(loaded_yaml)

    @classmethod
    def from_file(cls, filename: str):
        """Create a `PipelineIR` object from the document specified by the
        input path.

        Parameters
        ----------
        filename : `str`
            Location of document to use in creating a `PipelineIR` object.
        """
        with open(filename, 'r') as f:
            loaded_yaml = yaml.load(f, Loader=PipelineYamlLoader)
        return cls(loaded_yaml)

    def to_file(self, filename: str):
        """Serialize this `PipelineIR` object into a yaml formatted string and
        write the output to a file at the specified path.

        Parameters
        ----------
        filename : `str`
            Location of document to write a `PipelineIR` object.
        """
        with open(filename, 'w') as f:
            yaml.dump(self.to_primitives(), f, sort_keys=False)

    def to_primitives(self):
        """Convert to a representation used in yaml serialization
        """
        accumulate = {"description": self.description}
        if self.instrument is not None:
            accumulate['instrument'] = self.instrument
        accumulate['tasks'] = {m: t.to_primitives() for m, t in self.tasks.items()}
        if len(self.contracts) > 0:
            accumulate['contracts'] = [c.to_primitives() for c in self.contracts]
        return accumulate

    def __str__(self) -> str:
        """Instance formatting as how it would look in yaml representation
        """
        return yaml.dump(self.to_primitives(), sort_keys=False)

    def __repr__(self) -> str:
        """Instance formatting as how it would look in yaml representation
        """
        return str(self)

    def __eq__(self, other: "PipelineIR"):
        if not isinstance(other, PipelineIR):
            return False
        elif all(getattr(self, attr) == getattr(other, attr) for attr in
                 ("contracts", "tasks", "instrument")):
            return True
        else:
            return False
