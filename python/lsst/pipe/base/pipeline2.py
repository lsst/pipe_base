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

__all__ = ()


import functools
from collections.abc import Callable, Iterable, Set
from typing import TYPE_CHECKING, Protocol, cast

from lsst.daf.butler import DataIdValue
from lsst.utils.introspection import doImportType

from ._instrument import Instrument

if TYPE_CHECKING:
    from lsst.resources import ResourcePathExpression

    from .config import PipelineTaskConfig
    from .pipeline_graph import PipelineGraph
    from .pipelineIR import PipelineIR
    from .pipelineTask import PipelineTask


class PipelineProtocol(Protocol):
    @property
    def is_concrete(self) -> bool:
        """Whether this pipeline can be run directly (vs. just imported by
        another pipeline).
        """
        ...

    @property
    def is_importable(self) -> bool:
        """Whether this pipeline can safely be imported by another pipeline.

        Implementations should generally set this to `False` if they customize
        the `to_graph` method to do something other than delegate to
        `make_pipeline_graph` (like dynamically adding tasks to the pipeline
        graph based on what's in the pipeline graph).
        """
        ...

    @property
    def instrument(self) -> str | None:
        """Fully-qualified Python import path to an `Instrument` class that
        provides config overrides and minimal data ID constraint.
        """
        ...

    @property
    def name(self) -> str:
        """Short name for the pipeline for use in error messages."""
        ...

    @property
    def description(self) -> str:
        """Complete description of the pipeline."""
        ...

    @property
    def tasks(self) -> Set[str]:
        """The labels of all tasks in the pipeline."""
        ...

    @property
    def subsets(self) -> Set[str]:
        """The labels of all subsets in the pipeline."""
        ...

    def get_task_class_name(self, label: str) -> str:
        """Return the fully-qualified Python import path for the task with
        the given label.
        """
        ...

    def override_task_config(self, label: str, config: PipelineTaskConfig) -> None:
        """Apply this pipeline's overrides for the given task to the given
        config object.

        This should not include instrument-provided overrides.
        """
        ...

    def get_subset_members(self, label: str) -> Set[str]:
        """Return the task labels that are in the given subset."""
        ...

    def get_subset_description(self, label: str) -> str:
        """Return the full description of the given subset."""
        ...

    def to_graph(self) -> PipelineGraph:
        """Transform this pipeline into a graph.

        If `is_importable` is `False`, this is the only method in the protocol
        that needs to be implemented.  If all other protocol methods in the
        protocol *are* implemented, the `make_pipeline_graph` free function
        provides a complete implementation.
        """
        ...


def make_pipeline_graph(pipeline: PipelineProtocol) -> PipelineGraph:
    """Make a pipeline graph from a pipeline.

    This is the standard implementation of `PipelineProtocol.to_graph`.
    """
    from .pipeline_graph import PipelineGraph

    if not pipeline.is_concrete:
        raise RuntimeError(
            f"Pipeline {pipeline.name!r} is not concrete; it can only be imported by other pipelines."
        )

    instrument: Instrument | None = None
    data_id: dict[str, DataIdValue] = {}
    if pipeline.instrument is not None:
        instrument = Instrument.from_string(pipeline.instrument)
        data_id["instrument"] = instrument.getName()
    graph = PipelineGraph(description=pipeline.description, data_id=data_id)
    for task_label in pipeline.tasks:
        task_type: type[PipelineTask] = doImportType(pipeline.get_task_class_name(task_label))
        config = task_type.ConfigClass()
        if instrument is not None:
            instrument.applyConfigOverrides(task_type._DefaultName, config)
        pipeline.override_task_config(task_label, config)
        graph.add_task(task_label, task_type, config)
    for subset_label in pipeline.subsets:
        graph.add_task_subset(
            subset_label,
            pipeline.get_subset_members(subset_label),
            pipeline.get_subset_description(subset_label),
        )
    return graph


class RawTaskDeclaration(Protocol):
    def __call__(self, config: PipelineTaskConfig, /) -> None: ...


class TaskDeclaration(Protocol):
    def __call__(self, config: PipelineTaskConfig, /) -> None: ...

    is_task_declaration: bool
    task_type_name: str | None
    reset: bool


class RawSubsetDeclaration(Protocol):
    def __call__(self) -> Set[str]: ...


class SubsetDeclaration(Protocol):
    def __call__(self) -> Set[str]: ...

    is_subset_declaration: bool


@functools.wraps
def declare_task(
    type_name: str, /, *, reset: bool = False
) -> Callable[[RawTaskDeclaration], TaskDeclaration]:
    """Decorate a method to providing a task declaration in a `BasePipeline`
    subclass.

    Parameters
    ----------
    type_name : `str`
        Fully-qualified Python import path for the task class.
    reset : `bool`, optional
        If `True`, drop all imported config overrides before applying the
        decorated function, and allow the task class to be modified.

    The decorated function should take a single `PipelineTaskConfig` parameter,
    and apply any config overrides for this task.
    """

    def decorate(func: RawTaskDeclaration) -> TaskDeclaration:
        func = cast(TaskDeclaration, func)
        func.is_task_declaration = True
        func.task_type_name = type_name
        func.reset = reset
        return func

    return decorate


@functools.wraps
def override_task(func: RawTaskDeclaration) -> TaskDeclaration:
    func = cast(TaskDeclaration, func)
    func.is_task_declaration = True
    func.task_type_name = None
    func.reset = False
    return func


@functools.wraps
def declare_subset(func: RawSubsetDeclaration, /) -> SubsetDeclaration:
    func = cast(SubsetDeclaration, func)
    func.is_subset_declaration = True
    return func


class BasePipeline:
    def __init__(
        self,
        name: str,
        imports: Iterable[PipelineProtocol] = (),
        instrument: str | None = None,
        description: str = "",
        is_importable: bool = True,
        is_concrete: bool = True,
    ) -> None:
        self.name = name
        self.description = description
        self.instrument = instrument
        self.is_importable = is_importable
        self.is_concrete = is_concrete
        self.tasks: set[str] = set()
        self.subsets: set[str] = set()
        self.imports = list(imports)
        for imported_pipeline in self.imports:
            if not imported_pipeline.is_importable:
                raise RuntimeError(f"Pipeline {name!r} is not importable (imported by {self.name}.")
            self.tasks.update(imported_pipeline.tasks)
            self.subsets.update(imported_pipeline.subsets)
            if imported_pipeline.instrument is not None:
                if self.instrument is None:
                    self.instrument = imported_pipeline.instrument
                elif self.instrument != imported_pipeline.instrument:
                    raise RuntimeError(
                        f"Instrument mismatch: {self.instrument!r} vs {imported_pipeline.instrument!r} "
                        f"(from import {imported_pipeline.name!r})."
                    )
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if getattr(attr, "is_task_declaration", False):
                self.tasks.add(attr_name)
            elif getattr(attr, "is_subset_declaration", False):
                self.subsets.add(attr_name)

    def get_task_class_name(self, label: str) -> str:
        result: str | None = None
        func: TaskDeclaration | None
        if (func := getattr(self, label, None)) is not None and getattr(func, "is_task_declaration", False):
            result = func.task_type_name
            if func.reset and result is not None:
                return result
        for imported_pipeline in self.imports:
            if label in imported_pipeline.tasks:
                imported_result = imported_pipeline.get_task_class_name(label)
                if result is None:
                    result = imported_result
                elif result != imported_result:
                    raise RuntimeError(
                        f"Inconsistent task class for {label!r} with no reset: "
                        f"{result!r} vs {imported_result!r}."
                    )
        if result is None:
            raise RuntimeError(
                f"Task {label!r} is not associated with a class.  Note that @override_task "
                "can only be used on imported tasks, not inherited tasks."
            )
        return result

    def override_task_config(self, label: str, config: PipelineTaskConfig) -> None:
        reset: bool = False
        func: TaskDeclaration | None
        if (func := getattr(self, label, None)) is not None and getattr(func, "is_task_declaration", False):
            reset = func.reset
        if not reset:
            # Iterate in reverse order to apply higher-priority configs last.
            for imported_pipeline in reversed(self.imports):
                if label in imported_pipeline.tasks:
                    imported_pipeline.override_task_config(label, config)
        if func is not None:
            func(config)

    def get_subset_members(self, label: str) -> Set[str]:
        func: SubsetDeclaration | None
        if (func := getattr(self, label, None)) is not None and getattr(func, "is_subset_declaration", False):
            return func()
        for imported_pipeline in self.imports:
            if label in imported_pipeline.subsets:
                return imported_pipeline.get_subset_members(label) & self.tasks
        raise LookupError(f"No subset {label!r} in this pipeline.")

    def get_subset_description(self, label: str) -> str:
        func: SubsetDeclaration | None
        if (func := getattr(self, label, None)) is not None and getattr(func, "is_subset_declaration", False):
            return func.__doc__
        for imported_pipeline in self.imports:
            if label in imported_pipeline.subsets:
                return imported_pipeline.get_subset_description(label)
        raise LookupError(f"No subset {label!r} in this pipeline.")

    def to_graph(self) -> PipelineGraph:
        return make_pipeline_graph(self)


class YamlPipeline:
    def __init__(self, ir: PipelineIR, name: str):
        self._ir = ir
        self.name = name

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> YamlPipeline:
        return PipelineIR.from_uri(uri)

    is_concrete: bool = True
    is_importable: bool = True

    @property
    def instrument(self) -> str | None:
        return self._ir.instrument

    @property
    def description(self) -> str:
        return self._ir.description

    @property
    def tasks(self) -> Set[str]:
        return self._ir.tasks.keys()

    @property
    def subsets(self) -> Set[str]:
        return self._ir.labeled_subsets.keys()

    def get_task_class_name(self, label: str) -> str:
        return self._ir.tasks[label].klass

    def override_task_config(self, label: str, config: PipelineTaskConfig) -> None:
        config.applyConfigOverrides(
            None,  # instrument overrides are handled by to_graph.
            "",
            config,
            self._ir.parameters,
            label,
        )

    def get_subset_members(self, label: str) -> Set[str]:
        return self._ir.labeled_subsets[label].subset

    def get_subset_description(self, label: str) -> str:
        return self._ir.labeled_subsets[label].description or ""

    def to_graph(self) -> PipelineGraph:
        return make_pipeline_graph(self)


# EXAMPLES; TEMPORARY!


class DRP(BasePipeline):
    def __init__(self, **kwargs):
        super().__init__(
            name="DRPv2",
            imports=[YamlPipeline.from_uri("$ANALYSIS_TOOLS_DIR/pipelines/visitQualityCore.yaml")],
            is_concrete=False,
            description="Base DRP pipelines.",
        )
        # Only keep one task from the imported pipeline.
        self.tasks.difference_update(self.imports[0].tasks)
        self.tasks.add("analyzeCalibrateImageMetadata")

    @declare_task("lsst.ip_isr.IsrTask")
    def isr(self, config: PipelineTaskConfig) -> None:
        pass

    @declare_task("lsst.pipe.tasks.calibrateTask")
    def calibrateImage(self, config: PipelineTaskConfig) -> None:
        config.connections.stars_footprints = "initial_stars_footprints_detector"
        config.connections.stars = "initial_stars_detector_raw"
        config.connections.exposure = "initial_visit_image"
        config.connections.background = "initial_visit_image_background"
        config.do_calibrate_pixels = False

    @declare_task("lsst.pipe.tasks.postprocess.TransformSourceTableTask")
    def transformInitialStars(self, config: PipelineTaskConfig) -> None:
        config.connections.stars_footprints = "initial_stars_footprints_detector"
        config.connections.stars = "initial_stars_detector_raw"
        config.connections.exposure = "initial_visit_image"
        config.connections.background = "initial_visit_image_background"
        config.do_calibrate_pixels = False

    @declare_subset
    def step1(self) -> set[str]:
        """The first step in the pipeline."""  # noqa: D401
        return {"isr", "calibrateImage", "transformInitialStars", "analyzeCalibrateImageMetadata"}


class ComCamQuickLook(BasePipeline):
    def __init__(self, **kwargs) -> None:
        super().__init__(
            name="LSSTComCam/quickLook",
            imports=[DRP(**kwargs)],
            is_concrete=True,
            instrument="lsst.obs.lsst.LsstComCam",
        )
        self.tasks.remove("transformInitialStars")

    @declare_task("something.imaginary")
    def doQuickLookStuff(self, config: PipelineTaskConfig) -> None:
        pass

    @declare_subset
    def step1(self) -> set[str]:
        return self.imports[0].get_subset_members("step1") | {"doQuickLookStuff"} - {"transformInitialStars"}
