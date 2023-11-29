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
from __future__ import annotations

__all__ = (
    "ConfigIR",
    "ContractError",
    "ContractIR",
    "ImportIR",
    "LabeledSubset",
    "ParametersIR",
    "PipelineIR",
    "TaskIR",
)

import copy
import enum
import os
import re
import warnings
from collections import Counter
from collections.abc import Generator, Hashable, Iterable, MutableMapping
from dataclasses import dataclass, field
from typing import Any, Literal

import yaml
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.utils.introspection import find_outside_stacklevel


class PipelineSubsetCtrl(enum.Enum):
    """An Enumeration of the various ways a pipeline subsetting operation will
    handle labeled subsets when task labels they defined are missing.
    """

    DROP = enum.auto()
    """Drop any subsets that contain labels which are no longer in the set of
    task labels when subsetting an entire pipeline
    """
    EDIT = enum.auto()
    """Edit any subsets that contain labels which are no longer in the set of
    task labels to remove the missing label, but leave the subset when
    subsetting a pipeline.
    """


class _Tags(enum.Enum):
    KeepInstrument = enum.auto()


class PipelineYamlLoader(yaml.SafeLoader):
    """Specialized version of yaml's SafeLoader.

    It checks and raises an exception if it finds that there are multiple
    instances of the same key found inside a pipeline file at a given scope.
    """

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict[Hashable, Any]:
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
            raise KeyError(
                f"Pipeline files must not have duplicated keys, {duplicates} appeared multiple times"
            )
        return mapping


class MultilineStringDumper(yaml.Dumper):
    """Custom YAML dumper that makes multi-line strings use the '|'
    continuation style instead of unreadable newlines and tons of quotes.

    Basic approach is taken from
    https://stackoverflow.com/questions/8640959/how-can-i-control-what-scalar-form-pyyaml-uses-for-my-data,
    but is written as a Dumper subclass to make its effects non-global (vs
    `yaml.add_representer`).
    """

    def represent_scalar(self, tag: str, value: Any, style: str | None = None) -> yaml.ScalarNode:
        if style is None and tag == "tag:yaml.org,2002:str" and len(value.splitlines()) > 1:
            style = "|"
        return super().represent_scalar(tag, value, style)


class ContractError(Exception):
    """An exception that is raised when a pipeline contract is not
    satisfied.
    """

    pass


@dataclass
class ContractIR:
    """Intermediate representation of configuration contracts read from a
    pipeline yaml file.
    """

    contract: str
    """A string of python code representing one or more conditions on configs
    in a pipeline. This code-as-string should, once evaluated, should be True
    if the configs are fine, and False otherwise.
    """
    msg: str | None = None
    """An optional message to be shown to the user if a contract fails
    """

    def to_primitives(self) -> dict[str, str]:
        """Convert to a representation used in yaml serialization."""
        accumulate = {"contract": self.contract}
        if self.msg is not None:
            accumulate["msg"] = self.msg
        return accumulate

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ContractIR):
            return False
        return self.contract == other.contract and self.msg == other.msg


@dataclass
class LabeledSubset:
    """Intermediate representation of named subset of task labels read from
    a pipeline yaml file.
    """

    label: str
    """The label used to identify the subset of task labels.
    """
    subset: set[str]
    """A set of task labels contained in this subset.
    """
    description: str | None
    """A description of what this subset of tasks is intended to do
    """

    @staticmethod
    def from_primitives(label: str, value: list[str] | dict) -> LabeledSubset:
        """Generate `LabeledSubset` objects given a properly formatted object
        that as been created by a yaml loader.

        Parameters
        ----------
        label : `str`
            The label that will be used to identify this labeled subset.
        value : `list` of `str` or `dict`
            Object returned from loading a labeled subset section from a yaml
            document.

        Returns
        -------
        labeledSubset : `LabeledSubset`
            A `LabeledSubset` object build from the inputs.

        Raises
        ------
        ValueError
            Raised if the value input is not properly formatted for parsing
        """
        if isinstance(value, MutableMapping):
            subset = value.pop("subset", None)
            if subset is None:
                raise ValueError(
                    "If a labeled subset is specified as a mapping, it must contain the key 'subset'"
                )
            description = value.pop("description", None)
        elif isinstance(value, Iterable):
            subset = value
            description = None
        else:
            raise ValueError(
                f"There was a problem parsing the labeled subset {label}, make sure the "
                "definition is either a valid yaml list, or a mapping with keys "
                "(subset, description) where subset points to a yaml list, and description is "
                "associated with a string"
            )
        return LabeledSubset(label, set(subset), description)

    def to_primitives(self) -> dict[str, list[str] | str]:
        """Convert to a representation used in yaml serialization."""
        accumulate: dict[str, list[str] | str] = {"subset": list(self.subset)}
        if self.description is not None:
            accumulate["description"] = self.description
        return accumulate


@dataclass
class ParametersIR:
    """Intermediate representation of parameters that are global to a pipeline.

    Attributes
    ----------
    mapping : `dict` [`str`, `str`]
        A mutable mapping of identifiers as keys, and shared configuration
        as values.

    Notes
    -----
    These parameters are specified under a top level key named ``parameters``
    and are declared as a yaml mapping. These entries can then be used inside
    task configuration blocks to specify configuration values. They may not be
    used in the special ``file`` or ``python`` blocks.

    Examples
    --------
    .. code-block:: yaml

        \u200bparameters:
            shared_value: 14
            tasks:
                taskA:
                    class: modA
                    config:
                        field1: parameters.shared_value
                taskB:
                    class: modB
                    config:
                        field2: parameters.shared_value
    """

    mapping: MutableMapping[str, Any]
    """A mutable mapping of identifiers as keys, and shared configuration
    as values.
    """

    def update(self, other: ParametersIR | None) -> None:
        if other is not None:
            self.mapping.update(other.mapping)

    def to_primitives(self) -> MutableMapping[str, str]:
        """Convert to a representation used in yaml serialization."""
        return self.mapping

    def __contains__(self, value: str) -> bool:
        return value in self.mapping

    def __getitem__(self, item: str) -> Any:
        return self.mapping[item]

    def __bool__(self) -> bool:
        return bool(self.mapping)


@dataclass
class ConfigIR:
    """Intermediate representation of configurations read from a pipeline yaml
    file.
    """

    python: str | None = None
    """A string of python code that is used to modify a configuration. This can
    also be None if there are no modifications to do.
    """
    dataId: dict | None = None
    """A dataId that is used to constrain these config overrides to only quanta
    with matching dataIds. This field can be None if there is no constraint.
    This is currently an unimplemented feature, and is placed here for future
    use.
    """
    file: list[str] = field(default_factory=list)
    """A list of paths which points to a file containing config overrides to be
    applied. This value may be an empty list if there are no overrides to
    apply.
    """
    rest: dict = field(default_factory=dict)
    """This is a dictionary of key value pairs, where the keys are strings
    corresponding to qualified fields on a config to override, and the values
    are strings representing the values to apply.
    """

    def to_primitives(self) -> dict[str, str | dict | list[str]]:
        """Convert to a representation used in yaml serialization."""
        accumulate = {}
        for name in ("python", "dataId", "file"):
            # if this attribute is thruthy add it to the accumulation
            # dictionary
            if getattr(self, name):
                accumulate[name] = getattr(self, name)
        # Add the dictionary containing the rest of the config keys to the
        # # accumulated dictionary
        accumulate.update(self.rest)
        return accumulate

    def formatted(self, parameters: ParametersIR) -> ConfigIR:
        """Return a new ConfigIR object that is formatted according to the
        specified parameters.

        Parameters
        ----------
        parameters : `ParametersIR`
            Object that contains variable mappings used in substitution.

        Returns
        -------
        config : `ConfigIR`
            A new ConfigIR object formatted with the input parameters.
        """
        new_config = copy.deepcopy(self)
        for key, value in new_config.rest.items():
            if not isinstance(value, str):
                continue
            match = re.match("parameters[.](.*)", value)
            if match and match.group(1) in parameters:
                new_config.rest[key] = parameters[match.group(1)]
            if match and match.group(1) not in parameters:
                warnings.warn(
                    f"config {key} contains value {match.group(0)} which is formatted like a "
                    "Pipeline parameter but was not found within the Pipeline, if this was not "
                    "intentional, check for a typo",
                    stacklevel=find_outside_stacklevel("lsst.pipe.base"),
                )
        return new_config

    def maybe_merge(self, other_config: "ConfigIR") -> Generator["ConfigIR", None, None]:
        """Merge another instance of a `ConfigIR` into this instance if
        possible. This function returns a generator that is either self
        if the configs were merged, or self, and other_config if that could
        not be merged.

        Parameters
        ----------
        other_config : `ConfigIR`
            An instance of `ConfigIR` to merge into this instance.

        Yields
        ------
        Generator : `ConfigIR`
            A generator containing either self, or self and other_config if
            the configs could be merged or not respectively.
        """
        # Verify that the config blocks can be merged
        if (
            self.dataId != other_config.dataId
            or self.python
            or other_config.python
            or self.file
            or other_config.file
        ):
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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ConfigIR):
            return False
        return all(
            getattr(self, attr) == getattr(other, attr) for attr in ("python", "dataId", "file", "rest")
        )


@dataclass
class TaskIR:
    """Intermediate representation of tasks read from a pipeline yaml file."""

    label: str
    """An identifier used to refer to a task.
    """
    klass: str
    """A string containing a fully qualified python class to be run in a
    pipeline.
    """
    config: list[ConfigIR] | None = None
    """list of all configs overrides associated with this task, and may be
    `None` if there are no config overrides.
    """

    def to_primitives(self) -> dict[str, str | list[dict]]:
        """Convert to a representation used in yaml serialization."""
        accumulate: dict[str, str | list[dict]] = {"class": self.klass}
        if self.config:
            accumulate["config"] = [c.to_primitives() for c in self.config]
        return accumulate

    def add_or_update_config(self, other_config: ConfigIR) -> None:
        """Add a `ConfigIR` to this task if one is not present. Merges configs
        if there is a `ConfigIR` present and the dataId keys of both configs
        match, otherwise adds a new entry to the config list. The exception to
        the above is that if either the last config or other_config has a
        python block, then other_config is always added, as python blocks can
        modify configs in ways that cannot be predicted.

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskIR):
            return False
        return all(getattr(self, attr) == getattr(other, attr) for attr in ("label", "klass", "config"))


@dataclass
class ImportIR:
    """An intermediate representation of imported pipelines."""

    location: str
    """This is the location of the pipeline to inherit. The path should be
    specified as an absolute path. Environment variables may be used in the
    path and should be specified as a python string template, with the name of
    the environment variable inside braces.
    """
    include: list[str] | None = None
    """list of tasks that should be included when inheriting this pipeline.
    Either the include or exclude attributes may be specified, but not both.
    """
    exclude: list[str] | None = None
    """list of tasks that should be excluded when inheriting this pipeline.
    Either the include or exclude attributes may be specified, but not both.
    """
    importContracts: bool = True
    """Boolean attribute to dictate if contracts should be inherited with the
    pipeline or not.
    """
    importSteps: bool = True
    """Boolean attribute to dictate if steps should be inherited with the
    pipeline or not.
    """
    labeledSubsetModifyMode: PipelineSubsetCtrl = PipelineSubsetCtrl.DROP
    """Controls how labeled subsets are handled when an import ends up not
    including (either through an include or exclusion list) a task label that
    is defined in the `Pipeline` being imported. DROP will remove any
    subsets which contain a missing label. EDIT will change any subsets to not
    include the missing label.
    """
    instrument: Literal[_Tags.KeepInstrument] | str | None = _Tags.KeepInstrument
    """Instrument to assign to the Pipeline at import. The default value of
    `_Tags.KeepInstrument`` indicates that whatever instrument the pipeline is
    declared with will not be modified. setting this value to None will drop
    any declared instrument prior to import.
    """

    def toPipelineIR(self) -> "PipelineIR":
        """Load in the Pipeline specified by this object, and turn it into a
        PipelineIR instance.

        Returns
        -------
        pipeline : `PipelineIR`
            A pipeline generated from the imported pipeline file.
        """
        if self.include and self.exclude:
            raise ValueError(
                "An include list and an exclude list cannot both be specified"
                " when declaring a pipeline import."
            )
        tmp_pipeline = PipelineIR.from_uri(os.path.expandvars(self.location))
        if self.instrument is not _Tags.KeepInstrument:
            tmp_pipeline.instrument = self.instrument

        included_labels = set()
        for label in tmp_pipeline.tasks:
            if (
                (self.include and label in self.include)
                or (self.exclude and label not in self.exclude)
                or (self.include is None and self.exclude is None)
            ):
                included_labels.add(label)

        # Handle labeled subsets being specified in the include or exclude
        # list, adding or removing labels.
        if self.include is not None:
            subsets_in_include = tmp_pipeline.labeled_subsets.keys() & self.include
            for label in subsets_in_include:
                included_labels.update(tmp_pipeline.labeled_subsets[label].subset)

        elif self.exclude is not None:
            subsets_in_exclude = tmp_pipeline.labeled_subsets.keys() & self.exclude
            for label in subsets_in_exclude:
                included_labels.difference_update(tmp_pipeline.labeled_subsets[label].subset)

        if not self.importSteps:
            tmp_pipeline.steps = []

        tmp_pipeline = tmp_pipeline.subset_from_labels(included_labels, self.labeledSubsetModifyMode)

        if not self.importContracts:
            tmp_pipeline.contracts = []

        return tmp_pipeline

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ImportIR):
            return False
        return all(
            getattr(self, attr) == getattr(other, attr)
            for attr in ("location", "include", "exclude", "importContracts")
        )


@dataclass
class StepIR:
    """Intermediate representation of a step definition."""

    label: str
    """The label associated with this step."""
    sharding_dimensions: list[str]
    """The dimensions to use when sharding this step."""


class PipelineIR:
    """Intermediate representation of a pipeline definition.

    Parameters
    ----------
    loaded_yaml : `dict`
        A dictionary which matches the structure that would be produced by a
        yaml reader which parses a pipeline definition document.

    Raises
    ------
    ValueError
        Raised if:

        - a pipeline is declared without a description;
        - no tasks are declared in a pipeline, and no pipelines are to be
          inherited;
        - more than one instrument is specified;
        - more than one inherited pipeline share a label.
    """

    def __init__(self, loaded_yaml: dict[str, Any]):
        # Check required fields are present
        if "description" not in loaded_yaml:
            raise ValueError("A pipeline must be declared with a description")
        if "tasks" not in loaded_yaml and len({"imports", "inherits"} - loaded_yaml.keys()) == 2:
            raise ValueError("A pipeline must be declared with one or more tasks")

        # These steps below must happen in this call order

        # Process pipeline description
        self.description = loaded_yaml.pop("description")

        # Process tasks
        self._read_tasks(loaded_yaml)

        # Process instrument keys
        inst = loaded_yaml.pop("instrument", None)
        if isinstance(inst, list):
            raise ValueError("Only one top level instrument can be defined in a pipeline")
        self.instrument: str | None = inst

        # Process any contracts
        self._read_contracts(loaded_yaml)

        # Process any defined parameters
        self._read_parameters(loaded_yaml)

        # Process any named label subsets
        self._read_labeled_subsets(loaded_yaml)

        # Process defined sets
        self._read_step_declaration(loaded_yaml)

        # Process any inherited pipelines
        self._read_imports(loaded_yaml)

        # verify named subsets, must be done after inheriting
        self._verify_labeled_subsets()

        # verify steps, must be done after inheriting
        self._verify_steps()

    def _read_contracts(self, loaded_yaml: dict[str, Any]) -> None:
        """Process the contracts portion of the loaded yaml document

        Parameters
        ----------
        loaded_yaml : `dict`
            A dictionary which matches the structure that would be produced by
            a yaml reader which parses a pipeline definition document
        """
        loaded_contracts = loaded_yaml.pop("contracts", [])
        if isinstance(loaded_contracts, str):
            loaded_contracts = [loaded_contracts]
        self.contracts: list[ContractIR] = []
        for contract in loaded_contracts:
            if isinstance(contract, dict):
                self.contracts.append(ContractIR(**contract))
            if isinstance(contract, str):
                self.contracts.append(ContractIR(contract=contract))

    def _read_parameters(self, loaded_yaml: dict[str, Any]) -> None:
        """Process the parameters portion of the loaded yaml document

        Parameters
        ----------
        loaded_yaml : `dict`
            A dictionary which matches the structure that would be produced by
            a yaml reader which parses a pipeline definition document
        """
        loaded_parameters = loaded_yaml.pop("parameters", {})
        if not isinstance(loaded_parameters, dict):
            raise ValueError("The parameters section must be a yaml mapping")
        self.parameters = ParametersIR(loaded_parameters)

    def _read_labeled_subsets(self, loaded_yaml: dict[str, Any]) -> None:
        """Process the subsets portion of the loaded yaml document

        Parameters
        ----------
        loaded_yaml : `MutableMapping`
            A dictionary which matches the structure that would be produced
            by a yaml reader which parses a pipeline definition document
        """
        loaded_subsets = loaded_yaml.pop("subsets", {})
        self.labeled_subsets: dict[str, LabeledSubset] = {}
        if not loaded_subsets and "subset" in loaded_yaml:
            raise ValueError("Top level key should be subsets and not subset, add an s")
        for key, value in loaded_subsets.items():
            self.labeled_subsets[key] = LabeledSubset.from_primitives(key, value)

    def _read_step_declaration(self, loaded_yaml: dict[str, Any]) -> None:
        """Process the steps portion of the loaded yaml document

        Steps are subsets that are declared to be normal parts of the overall
        processing of the pipeline. Not all subsets need to be a step, as they
        can exist for certain targeted processing, such as debugging.

        Parameters
        ----------
        loaded_yaml: `dict`
            A dictionary which matches the structure that would be produced
            by a yaml reader which parses a pipeline definition document
        """
        loaded_steps = loaded_yaml.pop("steps", [])
        temp_steps: dict[str, StepIR] = {}
        for declaration in loaded_steps:
            new_step = StepIR(**declaration)
            existing = temp_steps.setdefault(new_step.label, new_step)
            if existing is not new_step:
                raise ValueError(f"Step {existing.label} was declared twice.")
        self.steps = [step for step in temp_steps.values()]

    def _verify_labeled_subsets(self) -> None:
        """Verify that all the labels in each named subset exist within the
        pipeline.
        """
        # Verify that all labels defined in a labeled subset are in the
        # Pipeline
        for labeled_subset in self.labeled_subsets.values():
            if not labeled_subset.subset.issubset(self.tasks.keys()):
                raise ValueError(
                    f"Labels {labeled_subset.subset - self.tasks.keys()} were not found in the "
                    "declared pipeline"
                )
        # Verify subset labels are not already task labels
        label_intersection = self.labeled_subsets.keys() & self.tasks.keys()
        if label_intersection:
            raise ValueError(f"Labeled subsets can not use the same label as a task: {label_intersection}")

    def _verify_steps(self) -> None:
        """Verify that all step definitions have a corresponding labeled
        subset.
        """
        for step in self.steps:
            if step.label not in self.labeled_subsets:
                raise ValueError(
                    f"{step.label} was declared to be a step, but was not declared to be a labeled subset"
                )

    def _read_imports(self, loaded_yaml: dict[str, Any]) -> None:
        """Process the inherits portion of the loaded yaml document

        Parameters
        ----------
        loaded_yaml : `dict`
            A dictionary which matches the structure that would be produced by
            a yaml reader which parses a pipeline definition document
        """

        def process_args(argument: str | dict) -> dict:
            if isinstance(argument, str):
                return {"location": argument}
            elif isinstance(argument, dict):
                if "exclude" in argument and isinstance(argument["exclude"], str):
                    argument["exclude"] = [argument["exclude"]]
                if "include" in argument and isinstance(argument["include"], str):
                    argument["include"] = [argument["include"]]
                if "instrument" in argument and argument["instrument"] == "None":
                    argument["instrument"] = None
                if "labeledSubsetModifyMode" in argument:
                    match argument["labeledSubsetModifyMode"]:
                        case "DROP":
                            argument["labeledSubsetModifyMode"] = PipelineSubsetCtrl.DROP
                        case "EDIT":
                            argument["labeledSubsetModifyMode"] = PipelineSubsetCtrl.EDIT
                        case unknown:
                            raise ValueError(f"{unknown} is not a valid mode for labeledSubsetModifyMode")
                return argument

        if not {"inherits", "imports"} - loaded_yaml.keys():
            raise ValueError("Cannot define both inherits and imports sections, use imports")
        tmp_import = loaded_yaml.pop("inherits", None)
        if tmp_import is None:
            tmp_import = loaded_yaml.pop("imports", None)
        else:
            raise ValueError("The 'inherits' key is not supported. Please use the key 'imports' instead")
        if tmp_import is None:
            self.imports: list[ImportIR] = []
        elif isinstance(tmp_import, list):
            self.imports = [ImportIR(**process_args(args)) for args in tmp_import]
        else:
            self.imports = [ImportIR(**process_args(tmp_import))]

        self.merge_pipelines([fragment.toPipelineIR() for fragment in self.imports])

    def merge_pipelines(self, pipelines: Iterable[PipelineIR]) -> None:
        """Merge one or more other `PipelineIR` objects into this object.

        Parameters
        ----------
        pipelines : `~collections.abc.Iterable` of `PipelineIR` objects
            An `~collections.abc.Iterable` that contains one or more
            `PipelineIR` objects to merge into this object.

        Raises
        ------
        ValueError
            Raised if there is a conflict in instrument specifications.
            Raised if a task label appears in more than one of the input
            `PipelineIR` objects which are to be merged.
            Raised if a labeled subset appears in more than one of the input
            `PipelineIR` objects which are to be merged, and with any subset
            existing in this object.
        """
        # integrate any imported pipelines
        accumulate_tasks: dict[str, TaskIR] = {}
        accumulate_labeled_subsets: dict[str, LabeledSubset] = {}
        accumulated_parameters = ParametersIR({})
        accumulated_steps: dict[str, StepIR] = {}

        for tmp_IR in pipelines:
            if self.instrument is None:
                self.instrument = tmp_IR.instrument
            elif self.instrument != tmp_IR.instrument and tmp_IR.instrument is not None:
                msg = (
                    "Only one instrument can be declared in a pipeline or its imports. "
                    f"Top level pipeline defines {self.instrument} but pipeline to merge "
                    f"defines {tmp_IR.instrument}."
                )
                raise ValueError(msg)
            if duplicate_labels := accumulate_tasks.keys() & tmp_IR.tasks.keys():
                msg = (
                    "Task labels in the imported pipelines must be unique. "
                    f"These labels appear multiple times: {duplicate_labels}"
                )
                raise ValueError(msg)
            accumulate_tasks.update(tmp_IR.tasks)
            self.contracts.extend(tmp_IR.contracts)
            # verify that tmp_IR has unique labels for named subset among
            # existing labeled subsets, and with existing task labels.
            overlapping_subsets = accumulate_labeled_subsets.keys() & tmp_IR.labeled_subsets.keys()
            task_subset_overlap = (
                accumulate_labeled_subsets.keys() | tmp_IR.labeled_subsets.keys()
            ) & accumulate_tasks.keys()
            if overlapping_subsets or task_subset_overlap:
                raise ValueError(
                    "Labeled subset names must be unique amongst imports in both labels and "
                    f" named Subsets. Duplicate: {overlapping_subsets | task_subset_overlap}"
                )
            accumulate_labeled_subsets.update(tmp_IR.labeled_subsets)
            accumulated_parameters.update(tmp_IR.parameters)
            for tmp_step in tmp_IR.steps:
                existing = accumulated_steps.setdefault(tmp_step.label, tmp_step)
                if existing != tmp_step:
                    raise ValueError(
                        f"There were conflicting step definitions in import {tmp_step}, {existing}"
                    )

        for tmp_step in self.steps:
            existing = accumulated_steps.setdefault(tmp_step.label, tmp_step)
            if existing != tmp_step:
                raise ValueError(f"There were conflicting step definitions in import {tmp_step}, {existing}")

        # verify that any accumulated labeled subsets dont clash with a label
        # from this pipeline
        if accumulate_labeled_subsets.keys() & self.tasks.keys():
            raise ValueError(
                "Labeled subset names must be unique amongst imports in both labels and  named Subsets"
            )
        # merge in the named subsets for self so this document can override any
        # that have been delcared
        accumulate_labeled_subsets.update(self.labeled_subsets)
        self.labeled_subsets = accumulate_labeled_subsets

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
        self.tasks: dict[str, TaskIR] = accumulate_tasks
        accumulated_parameters.update(self.parameters)
        self.parameters = accumulated_parameters
        self.steps = list(accumulated_steps.values())

    def _read_tasks(self, loaded_yaml: dict[str, Any]) -> None:
        """Process the tasks portion of the loaded yaml document

        Parameters
        ----------
        loaded_yaml : `dict`
            A dictionary which matches the structure that would be produced by
            a yaml reader which parses a pipeline definition document
        """
        self.tasks = {}
        tmp_tasks = loaded_yaml.pop("tasks", None)
        if tmp_tasks is None:
            tmp_tasks = {}

        if "parameters" in tmp_tasks:
            raise ValueError("parameters is a reserved word and cannot be used as a task label")

        for label, definition in tmp_tasks.items():
            if isinstance(definition, str):
                definition = {"class": definition}
            config = definition.get("config", None)
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
                    task_config_ir.append(
                        ConfigIR(
                            python=c.pop("python", None), dataId=c.pop("dataId", None), file=file, rest=c
                        )
                    )
            self.tasks[label] = TaskIR(label, definition["class"], task_config_ir)

    def _remove_contracts(self, label: str) -> None:
        """Remove any contracts that contain the given label

        String comparison used in this way is not the most elegant and may
        have issues, but it is the only feasible way when users can specify
        contracts with generic strings.
        """
        new_contracts = []
        for contract in self.contracts:
            # match a label that is not preceded by an ASCII identifier, or
            # is the start of a line and is followed by a dot
            if re.match(f".*([^A-Za-z0-9_]|^){label}[.]", contract.contract):
                continue
            new_contracts.append(contract)
        self.contracts = new_contracts

    def subset_from_labels(
        self, labelSpecifier: set[str], subsetCtrl: PipelineSubsetCtrl = PipelineSubsetCtrl.DROP
    ) -> PipelineIR:
        """Subset a pipelineIR to contain only labels specified in
        labelSpecifier.

        Parameters
        ----------
        labelSpecifier : `set` of `str`
            Set containing labels that describes how to subset a pipeline.
        subsetCtrl : `PipelineSubsetCtrl`
            Control object which decides how subsets with missing labels are
            handled. Setting to `PipelineSubsetCtrl.DROP` (the default) will
            cause any subsets that have labels which are not in the set of all
            task labels to be dropped. Setting to `PipelineSubsetCtrl.EDIT`
            will cause the subset to instead be edited to remove the
            nonexistent label.

        Returns
        -------
        pipeline : `PipelineIR`
            A new pipelineIR object that is a subset of the old pipelineIR.

        Raises
        ------
        ValueError
            Raised if there is an issue with specified labels.

        Notes
        -----
        This method attempts to prune any contracts that contain labels which
        are not in the declared subset of labels. This pruning is done using a
        string based matching due to the nature of contracts and may prune more
        than it should.
        """
        pipeline = copy.deepcopy(self)

        # update the label specifier to expand any named subsets
        toRemove = set()
        toAdd = set()
        for label in labelSpecifier:
            if label in pipeline.labeled_subsets:
                toRemove.add(label)
                toAdd.update(pipeline.labeled_subsets[label].subset)
        labelSpecifier.difference_update(toRemove)
        labelSpecifier.update(toAdd)
        # verify all the labels are in the pipeline
        if not labelSpecifier.issubset(pipeline.tasks.keys() | pipeline.labeled_subsets):
            difference = labelSpecifier.difference(pipeline.tasks.keys())
            raise ValueError(
                "Not all supplied labels (specified or named subsets) are in the pipeline "
                f"definition, extra labels: {difference}"
            )
        # copy needed so as to not modify while iterating
        pipeline_labels = set(pipeline.tasks.keys())
        # Remove the labels from the pipelineIR, and any contracts that contain
        # those labels (see docstring on _remove_contracts for why this may
        # cause issues)
        for label in pipeline_labels:
            if label not in labelSpecifier:
                pipeline.tasks.pop(label)
                pipeline._remove_contracts(label)

        # create a copy of the object to iterate over
        labeled_subsets = copy.copy(pipeline.labeled_subsets)
        # remove or edit any labeled subsets that no longer have a complete set
        for label, labeled_subset in labeled_subsets.items():
            if extraTaskLabels := (labeled_subset.subset - pipeline.tasks.keys()):
                match subsetCtrl:
                    case PipelineSubsetCtrl.DROP:
                        pipeline.labeled_subsets.pop(label)
                    case PipelineSubsetCtrl.EDIT:
                        for extra in extraTaskLabels:
                            labeled_subset.subset.discard(extra)

        # remove any steps that correspond to removed subsets
        new_steps = []
        for step in pipeline.steps:
            if step.label not in pipeline.labeled_subsets:
                continue
            new_steps.append(step)
        pipeline.steps = new_steps

        return pipeline

    @classmethod
    def from_string(cls, pipeline_string: str) -> PipelineIR:
        """Create a `PipelineIR` object from a string formatted like a pipeline
        document.

        Parameters
        ----------
        pipeline_string : `str`
            A string that is formatted according like a pipeline document.
        """
        loaded_yaml = yaml.load(pipeline_string, Loader=PipelineYamlLoader)
        return cls(loaded_yaml)

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> PipelineIR:
        """Create a `PipelineIR` object from the document specified by the
        input uri.

        Parameters
        ----------
        uri : convertible to `~lsst.resources.ResourcePath`
            Location of document to use in creating a `PipelineIR` object.

        Returns
        -------
        pipelineIR : `PipelineIR`
            The loaded pipeline.
        """
        loaded_uri = ResourcePath(uri)
        with loaded_uri.open("r") as buffer:
            loaded_yaml = yaml.load(buffer, Loader=PipelineYamlLoader)
            return cls(loaded_yaml)

    def write_to_uri(self, uri: ResourcePathExpression) -> None:
        """Serialize this `PipelineIR` object into a yaml formatted string and
        write the output to a file at the specified uri.

        Parameters
        ----------
        uri : convertible to `~lsst.resources.ResourcePath`
            Location of document to write a `PipelineIR` object.
        """
        with ResourcePath(uri).open("w") as buffer:
            yaml.dump(self.to_primitives(), buffer, sort_keys=False, Dumper=MultilineStringDumper)

    def to_primitives(self) -> dict[str, Any]:
        """Convert to a representation used in yaml serialization.

        Returns
        -------
        primitives : `dict`
            Dictionary that maps directly to the serialized YAML form.
        """
        accumulate = {"description": self.description}
        if self.instrument is not None:
            accumulate["instrument"] = self.instrument
        if self.parameters:
            accumulate["parameters"] = self.parameters.to_primitives()
        accumulate["tasks"] = {m: t.to_primitives() for m, t in self.tasks.items()}
        if len(self.contracts) > 0:
            # sort contracts lexicographical order by the contract string in
            # absence of any other ordering principle
            contracts_list = [c.to_primitives() for c in self.contracts]
            contracts_list.sort(key=lambda x: x["contract"])
            accumulate["contracts"] = contracts_list
        if self.labeled_subsets:
            accumulate["subsets"] = {k: v.to_primitives() for k, v in self.labeled_subsets.items()}
        return accumulate

    def __str__(self) -> str:
        """Instance formatting as how it would look in yaml representation"""
        return yaml.dump(self.to_primitives(), sort_keys=False, Dumper=MultilineStringDumper)

    def __repr__(self) -> str:
        """Instance formatting as how it would look in yaml representation"""
        return str(self)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PipelineIR):
            return False
        # special case contracts because it is a list, but order is not
        # important
        return (
            all(
                getattr(self, attr) == getattr(other, attr)
                for attr in ("tasks", "instrument", "labeled_subsets", "parameters")
            )
            and len(self.contracts) == len(other.contracts)
            and all(c in self.contracts for c in other.contracts)
        )
