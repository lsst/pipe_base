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

"""Tools for generating Sphinx documentation from Pipeline definitions.

The tools in this module were first developed as something to be run by the
SCons build system, generating files that are later used in ``documenteer``
builds, but it was designed to be usable from other Python code as well (e.g.
some future version of ``documenteer`` itself).  See
`PackagePipelinesDocBuilder.scons_generate` for notes on how interfacing with a
SCons build works, as a few quirks of SCons' behavior need to be worked around.
"""

from __future__ import annotations

__all__ = ("PackagePipelinesDocBuilder", "PipelineDocBuilder")

import argparse
import contextlib
import dataclasses
import os
import textwrap
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from typing import TextIO

from .dot_tools import pipeline2dot
from .pipeline import Pipeline, TaskDef


@dataclasses.dataclass
class _DocPaths:
    """A base class providing utility methods for structs that maintain a path
    to a reStructuredText file.
    """

    rst_path: Path
    """Path to a reStructuredText file (`Path`).
    """

    def _relative_to_rst(self, target: Path) -> Path:
        """Compute a version of the given path that is relative to the
        directory containing `rst_path`.

        Parameters
        ----------
        target : `Path`
            Path to compute a relative version of.

        Returns
        -------
        relative_target : `Path`
            A relative version of ``target``.

        Notes
        -----
        Unlike `Path.relative_to`, this method can backtrack by including
        ``..`` terms where appropriate, provided ``target`` and `rst_path`
        have some common root directory.
        """
        common = os.path.commonpath([target, self.rst_path.parent])
        target_to_common = target.relative_to(common)
        rst_to_common = self.rst_path.parent.relative_to(common)
        terms = [".."] * len(rst_to_common.parts)
        terms.extend(target_to_common.parts)
        return Path(os.path.join(*terms))

    @staticmethod
    def _sanitize_for_rst(*args: str) -> str:
        """Combine the given strings and replace any ``/`` characters into
        a string suitable for use as a reStructuredText label.

        Parameters
        ----------
        *args : `str`
            Strings to combine.

        Returns
        -------
        sanitized : `str`
            Sanitized, combined string.

        Notes
        -----
        This method does not attempt to replace all possible problematic
        characters, just those common in pipeline names derived from
        the directory hierarchy in a typical ``pipelines`` subdirectory.
        """
        return ".".join(s.replace("/", "-") for s in args)

    @staticmethod
    @contextlib.contextmanager
    def _mkdir_and_open(filename: Path) -> Iterator[TextIO]:
        """Return a context manager that opens a file for writing after first
        ensuring its parent directory exists.

        Parameters
        ----------
        filename : `Path`
            File to open.

        Returns
        -------
        cm : `contextlib.ContextManager` [ `typing.TextIO` ]
            Context manager wrapping a text file open for writing.
        """
        filename.parent.mkdir(parents=True, exist_ok=True)
        with open(filename, "w") as buffer:
            yield buffer


@dataclasses.dataclass
class _TaskInPipelineDocBuilder(_DocPaths):
    """Struct containing paths relevant for building the docs for a task
    within a pipeline.

    This class is intended to be used only by `PipelineDocBuilder`.
    """

    sanitized_name: str
    """Name that combines the task label and pipeline name, sanitized for
    use as a reStructuredText label.
    """

    config_path: Path
    """Path to the config file for this label.
    """

    dot_path: Path
    """Path to the GraphViz dot file for a graph that includes just this task
    and its inputs and outputs.
    """

    graph_path: Path
    """Path to the rendered graph that includes just this task and its inputs
    and outputs.
    """

    @classmethod
    def from_pipeline_dirs(
        cls,
        pipeline_name: str,
        label: str,
        *,
        rst_dir: Path,
        config_dir: Path,
        dot_dir: Path,
        graph_dir: Path,
        graph_suffix: str,
    ) -> _TaskInPipelineDocBuilder:
        """Construct from base directories.

        Parameters
        ----------
        pipeline_name : `str`
            Display name of the pipeline to which this task belongs.
        label : `str`
            Label of the task within the pipeline.
        rst_dir : `Path`
            Path to the directory that will contain all reStructuredText files
            for the pipeline.
        config_dir : `Path`
            Path to the directory that will contain all `lsst.pex.config` files
            for the pipeline.
        dot_dir : `Path`
            Path to the directory that will contain all GraphViz DOT files for
            the pipeline.
        graph_dir : `Path`
            Path to the directory that will contain all rendered graphs for
            the pipeline.
        graph_suffix : `str`
            File extension (including the ``.``) for rendered graph files.

        Returns
        -------
        instance : `_TaskInPipelineDocBuilder`
            New instance of this class.
        """
        return cls(
            sanitized_name=cls._sanitize_for_rst(pipeline_name, label),
            rst_path=rst_dir.joinpath("tasks", label + ".rst"),
            config_path=config_dir.joinpath("config", label + ".py"),
            dot_path=dot_dir.joinpath("dot", label + ".dot"),
            graph_path=graph_dir.joinpath("graph", label + graph_suffix),
        )

    def write_dot(self, task_def: TaskDef) -> None:
        """Write the GraphViz DOT file for this task.

        Parameters
        ----------
        task_def : `TaskDef`
            Expanded `TaskDef` for this task in its pipeline.
        """
        with self._mkdir_and_open(self.dot_path) as buffer:
            pipeline2dot([task_def], buffer)

    def write_rst(self, pipeline_name: str, task_def: TaskDef) -> None:
        """Write the reStructuredText file for this task.

        Parameters
        ----------
        pipeline_name : `str`
            Display name of the pipeline to which this task belongs.
        task_def : `TaskDef`
            Expanded `TaskDef` for this task in its pipeline.
        """
        with self._mkdir_and_open(self.rst_path) as buffer:
            title = f"{pipeline_name}.{task_def.label}: `~{task_def.taskName}`"
            buffer.write(
                textwrap.dedent(
                    f"""\
                    .. _{self.sanitized_name}:

                    {title}
                    {'"' * len(title)}

                    `{task_def.taskName}`


                    (open graph in a separate tab/window to zoom and pan)

                    .. image:: {self._relative_to_rst(self.graph_path)}

                    .. literalinclude:: {self._relative_to_rst(self.config_path)}

                    """
                )
            )


@dataclasses.dataclass
class PipelineDocBuilder(_DocPaths):
    """A Sphinx documentation builder for a single `Pipeline`.

    This should generally be constructed via the `from_dirs` factory method,
    not a direct call to the constructor.

    The function call operator can be used to write all outputs.  It optionally
    takes a sequence of `TaskDef` (the result of a call to
    `Pipeline.toExpandedPipeline`) as its only argument; this can be ignored
    to expand the pipeline internally, and is only useful as an optimization if
    calling code already has access to the expanded pipeline.

    Notes
    -----
    The documentation build for a pipeline includes expanding the pipeline
    itself (applying all config defaults and overrides) and generating GraphViz
    DOT diagrams for both the full pipeline and each task.  ReStructuredText
    files are generated for the pipeline as well as each of its tasks,
    referencing that content.

    Transforming ``.dot`` files into images is not handled directly by this
    class; it merely manages the paths to those rendered diagrams.  See
    `PackagePipelinesDocBuilder.scons_generate` for an example of how to
    invoke the ``dot`` tool to do this.
    """

    pipeline: Pipeline
    """Pipeline to document."""

    name: str
    """Display name and relative filesystem path for the pipeline in
    documentation.

    This is usually the same as the path to the pipeline definition ``yaml``
    file relative to a ``pipelines/`` directory; it is normal for it to contain
    ``/`` characters.
    """

    sanitized_name: str
    """Name for the pipeline that is safe for use as a reStructuredText label.
    """

    yaml_path: Path
    """Path to the YAML definition file for the expanded pipeline.
    """

    dot_path: Path
    """Path to the GraphViz DOT file for the pipeline.
    """

    graph_path: Path
    """Path to the rendered graph for the pipeline.
    """

    tasks: dict[str, _TaskInPipelineDocBuilder] = dataclasses.field(default_factory=dict)
    """Mapping of associated builders for each task in the pipeline.

    Keys are task labels.
    """

    @classmethod
    def from_dirs(
        cls,
        name: str,
        pipeline: Pipeline,
        *,
        rst_dir: Path,
        yaml_dir: Path,
        dot_dir: Path,
        graph_dir: Path,
        graph_suffix: str,
    ) -> PipelineDocBuilder:
        """Construct a builder from the directories that will contain its
        outputs.

        Parameters
        ----------
        name : `str`
            Display name and relative filesystem path for the pipeline.
        sanitized_name : `str`
            Name for the pipeline that is safe for us as a reStructuredText
            label.
        rst_dir : `Path`
            Path to the directory that will contain all reStructuredText files
            for the pipeline.
        config_dir : `Path`
            Path to the directory that will contain all `lsst.pex.config` files
            for the pipeline.
        dot_dir : `Path`
            Path to the directory that will contain all GraphViz DOT files for
            the pipeline.
        graph_dir : `Path`
            Path to the directory that will contain all rendered graphs for
            the pipeline.
        graph_suffix : `str`
            File extension (including the ``.``) for rendered graph files.
        """
        return cls(
            pipeline=pipeline,
            name=name,
            sanitized_name=cls._sanitize_for_rst(name),
            rst_path=rst_dir.joinpath("pipeline.rst"),
            yaml_path=yaml_dir.joinpath("pipeline.yaml"),
            dot_path=dot_dir.joinpath("pipeline.dot"),
            graph_path=graph_dir.joinpath("pipeline" + graph_suffix),
            tasks={
                label: _TaskInPipelineDocBuilder.from_pipeline_dirs(
                    pipeline_name=name,
                    label=label,
                    rst_dir=rst_dir,
                    config_dir=yaml_dir,
                    dot_dir=dot_dir,
                    graph_dir=graph_dir,
                    graph_suffix=graph_suffix,
                )
                for label in pipeline.tasks
            },
        )

    def __call__(self, task_defs: Sequence[TaskDef] | None = None) -> None:
        if task_defs is None:
            task_defs = list(self.pipeline)
        self.pipeline.write_to_uri(self.yaml_path.parent)
        self.write_dot(task_defs)
        self.write_rst(task_defs)

    def iter_write_paths(self) -> Iterator[Path]:
        """Iterate over the paths of all files written by this object's
        function call operator.

        This does not include `graph_path` or the similar graph paths for each
        task, as those are not actually produced by this class.
        """
        yield self.rst_path
        yield self.yaml_path
        yield self.dot_path
        for task_paths in self.tasks.values():
            yield task_paths.rst_path
            yield task_paths.config_path
            yield task_paths.dot_path

    def iter_graph_dot_paths(self) -> Iterator[tuple[Path, Path]]:
        """Iterate over pairs of ``(graph_path, dot_path)`` for the pipeline
        and all of its tasks.

        This is intended to be used to contruct calls to the ``dot`` tool (or
        some other GraphViz interpreter) that build the rendered graph files.
        """
        yield (self.graph_path, self.dot_path)
        for task_paths in self.tasks.values():
            yield (task_paths.graph_path, task_paths.dot_path)

    def write_dot(self, task_defs: Sequence[TaskDef] | None = None) -> None:
        """Write the GraphViz DOT representations of the pipeline and its
        tasks.

        Parameters
        ----------
        task_defs : `Sequence` [ `TaskDef` ], optional
            The result of a call to `Pipeline.toExpandedPipeline`, captured in
            a sequence.  May be `None` (default) to expand internally; provided
            as a way for calling code to only expand the pipeline once.
        """
        if task_defs is None:
            task_defs = list(self.pipeline)
        with self._mkdir_and_open(self.dot_path) as buffer:
            pipeline2dot(task_defs, buffer)
        for task_def in task_defs:
            self.tasks[task_def.label].write_dot(task_def)

    def write_rst(self, task_defs: Sequence[TaskDef] | None = None) -> None:
        """Write the reStructuredText files for the pipeline and its tasks.

        Parameters
        ----------
        task_defs : `Sequence` [ `TaskDef` ], optional
            The result of a call to `Pipeline.toExpandedPipeline`, captured in
            a sequence.  May be `None` (default) to expand internally; provided
            as a way for calling code to only expand the pipeline once.
        """
        if task_defs is None:
            task_defs = list(self.pipeline)
        with self._mkdir_and_open(self.rst_path) as buffer:
            buffer.write(
                textwrap.dedent(
                    f"""\
                    .. _{self.sanitized_name}:

                    {self.name}
                    {'-' * len(self.name)}

                    {self.pipeline.description}

                    Tasks
                    ^^^^^
                    .. toctree::
                       :maxdepth: 1

                    """
                )
            )
            for task_def in task_defs:
                buffer.write(
                    f"   {task_def.label} <{self._relative_to_rst(self.tasks[task_def.label].rst_path)}>\n"
                )
            buffer.write("\n")
            buffer.write(
                textwrap.dedent(
                    f"""\
                    Graph
                    ^^^^^

                    (open in a separate tab/window to zoom and pan)

                    .. image:: {self._relative_to_rst(self.graph_path)}

                    Definition
                    ^^^^^^^^^^

                    .. literalinclude:: {self._relative_to_rst(self.yaml_path)}

                    """
                )
            )
        for task_def in task_defs:
            self.tasks[task_def.label].write_rst(self.name, task_def)

    @classmethod
    def scons_script(cls, args: argparse.Namespace) -> None:
        """Command-line script used to invoke the builder by SCons.

        This script builds the docs for a single pipeline.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command-line arguments.  Run this module with
            ``python -m <module> pipeline --help`` for details.

        See Also
        --------
        PackagePipelinesDocBuilder.scons_generate
        """
        pipeline = Pipeline.from_uri(args.source_yaml)
        builder = PipelineDocBuilder.from_dirs(
            args.name,
            pipeline,
            rst_dir=Path(args.rst_dir),
            yaml_dir=Path(args.yaml_dir),
            dot_dir=Path(args.dot_dir),
            graph_dir=Path(args.graph_dir),
            graph_suffix=args.graph_suffix,
        )
        builder()


@dataclasses.dataclass
class PackagePipelinesDocBuilder(_DocPaths):
    """A Sphinx documentation builder for all Pipelines in a single package.

    This should generally be constructed via the `from_source` factory method,
    not a direct call to the constructor.
    """

    pipelines: dict[Path, PipelineDocBuilder]
    """Builders for each pipeline, keyed by the path to the ``yaml`` source
    file for it (i.e. by convention a path in the packages ``pipelines``
    directory).
    """

    @classmethod
    def from_source(
        cls,
        source_root: Path,
        *,
        rst_root: Path,
        pipeline_root: Path,
        dot_root: Path,
        graph_root: Path,
        graph_suffix: str = ".svg",
        rst_path: Path | None = None,
    ) -> PackagePipelinesDocBuilder:
        """Construct by walking a directory tree containing source ``yaml``
        pipeline files.

        Parameters
        ----------
        source_root : `Path`
            Directory path to walk for source ``yaml`` pipeline files.
        rst_dir : `Path`
            Path to the directory that will contain all reStructuredText files
            for all pipelines.
        config_dir : `Path`
            Path to the directory that will contain all `lsst.pex.config` files
            for all pipelines.
        dot_dir : `Path`
            Path to the directory that will contain all GraphViz DOT files for
            all pipelines.
        graph_dir : `Path`
            Path to the directory that will contain all rendered graphs for
            all pipelines.
        graph_suffix : `str`, optional
            File extension (including the ``.``) for rendered graph files.
            Defaults to ``.svg``.
        rst_path : `Path`, optional
            Path to the reStructuredText index file.  This file must be
            included in the package's Sphinx documentation manually, via
            a ``toctree`` or ``include`` directive.  Defaults to
            ``{rst_root}/index.rst``.
        """
        pipelines = {}
        for dir_path, _, file_names in os.walk(source_root):
            for file_name in file_names:
                file_path = Path(dir_path).joinpath(file_name)
                if file_path.suffix == ".yaml":
                    name = cls._name_from_source(file_path, source_root)
                    pipeline = Pipeline.from_uri(file_path)
                    pipelines[file_path] = PipelineDocBuilder.from_dirs(
                        name=name,
                        pipeline=pipeline,
                        rst_dir=rst_root.joinpath(name),
                        yaml_dir=pipeline_root.joinpath(name),
                        dot_dir=dot_root.joinpath(name),
                        graph_dir=graph_root.joinpath(name),
                        graph_suffix=graph_suffix,
                    )
        return cls(
            rst_path=rst_path if rst_path is not None else rst_root.joinpath("index.rst"),
            pipelines=pipelines,
        )

    def write_index_rst(self) -> None:
        """Write the index reStructuredText file for all pipelines in the
        package.
        """
        self._write_index_rst_standalone(
            self.rst_path,
            [self._relative_to_rst(pipeline.rst_path) for pipeline in self.pipelines.values()],
        )

    @classmethod
    def _write_index_rst_standalone(
        cls, target_path: Path, relative_pipeline_rst_paths: Iterable[Path]
    ) -> None:
        """Write Sphinx index files.

        Implementation of `write_index_rst`.

        This is a classmethod so it can also be called by `scons_script`
        without reconstructing all nested `PipelineDocBuilder` instances, with
        just the state needed for this method passed in from the command-line.
        """
        with cls._mkdir_and_open(target_path) as buffer:
            buffer.write(
                textwrap.dedent(
                    """\
                    Pipelines
                    =========

                    .. toctree::
                       :maxdepth: 1

                    """
                )
            )
            for path in relative_pipeline_rst_paths:
                buffer.write(f"   {path}\n")

    @classmethod
    def scons_script(cls, args: argparse.Namespace) -> None:
        """Command-line script used to invoke the builder by SCons.

        This script builds only the index reStructuredText file, not the
        per-pipeline content.

        Parameters
        ----------
        args : `argparse.Namespace`
            Parsed command-line arguments.  Run this module with
            ``python -m <module> index --help`` for details.

        See Also
        --------
        PackagePipelinesDocBuilder.scons_generate
        """
        cls._write_index_rst_standalone(Path(args.target), [Path(p) for p in args.relative])

    @staticmethod
    def _name_from_source(source_yaml: Path, source_root: Path) -> str:
        """Construct the name for a pipeline from the path to its source
        ``yaml`` file and the root for those files.

        Parameters
        ----------
        source_yaml : `Path`
            Path to a source pipeline ``yaml`` file.
        source_root : `Path
            Directory path for all source pipeline ``yaml`` files in this
            package (usually the package ``pipelines`` directory).
        """
        return str(source_yaml.relative_to(source_root).with_suffix(""))

    def scons_generate(self, env, graph_action="dot ${SOURCE} -Tsvg -o ${TARGET}"):  # type: ignore
        """Build documentation for all pipelines in a package using SCons.

        Parameters
        ----------
        env : `SCons.Environment`
            SCons build environment instance.
        graph_action : `str` or `Callable`, optional
            A string command-line (or more rarely, a Python callable) that
            renders a GraphViz DOT into an graphics file consistent with the
            ``graph_suffix`` passed to `from_source`. satisfying the SCons
            "Action" interface.  The default runs ``dot -Tsvg``.

        Yields
        ------
        node : `SCons.Node.Node`
            An SCons build node for a documentation file generated by this
            class.

        Notes
        -----
        SCons is Python-based, but it makes a strong distinction between code
        that is run when its scripts are merely executed vs. code that runs
        when targets are actually built.  This method is an example of the
        former; it yields SCon objects that run this module on the command-line
        via ``python -m`` to achieve the latter.  It would be more natural to
        just instantiate a `PackagePipelinesDocBuilder` once, and then invoke
        each of its nested `PipelineDocBuilder` instances and call
        `write_index_rst` directly, but this isn't possible for two reasons:

        - In parallel builds (i.e. ``scons -j``) the (apparent) use of
          multithreading causes problems with (apparent) globals in reading and
          expanding `Pipelines`.  By making each action a separate command-line
          invocation, we ensure they are run in their own processes.

        - SCons executes its `SConscript` files with the current directory set
          to the directory that `SConscript` file is in, but then builds
          targets with the `SConstruct` directory current; it really wants
          actions that depend on paths to utilize the targets and sources they
          are passed (which are corrected for this shift) instead of
          remembering them internally (as this class and those nested within it
          do).  To work around this, we use relative paths in the script phase
          (constructing a `PackagePipelinesDocBuilder` and calling this method)
          so SCons can correctly reason about dependencies, and then passing
          absolute paths on the command-line so the change of working directory
          is relevant.

        Examples
        --------
        Usage in a ``doc/SConscript`` file, where ``lsst.drp.pipe`` is the
        name of the package, and the environment object and management of
        top-level tarets comes from `lsst.sconsUtils`::

            from lsst.sconsUtils.state import env, targets
            from pathlib import Path
            from lsst.pipe.base.pipeline_doc_builder import (
                PackagePipelinesDocBuilder
            )

            target_root = Path(str(env.Dir("lsst.drp.pipe/pipelines")))
            artifacts = list(
                PackagePipelinesDocBuilder.from_source(
                    Path(str(env.Dir("#pipelines"))),
                    rst_root=target_root,
                    pipeline_root=target_root,
                    dot_root=target_root,
                    graph_root=target_root,
                    graph_suffix=".svg",
                    rst_path=Path(str(env.File("lsst.drp.pipe/pipelines_index.rst"))),
                ).scons_generate(env)
            )

            env.AlwaysBuild(artifacts)
            env.Clean("doc", artifacts)

            targets["doc"].extend(artifacts)

        We use ``AlwaysBuild`` because SCons has no way of knowing when some
        modification to an upstream configuration file or pipeline ``yaml``
        ingredient file could change the outputs, so it is safest to rebuild
        whenever ``scons`` is run.
        """
        source_files = []
        for source_yaml_path, pipeline_builder in self.pipelines.items():
            source_file = env.File(source_yaml_path)
            source_files.append(source_file)
            target_files = [env.File(p) for p in pipeline_builder.iter_write_paths()]
            yield from env.Command(
                target_files,
                [source_file],
                action=(
                    f"python -m {__name__} pipeline "
                    f"{pipeline_builder.name} --source-yaml $SOURCE "
                    f"--rst-dir {pipeline_builder.rst_path.parent.resolve()} "
                    f"--yaml-dir {pipeline_builder.yaml_path.parent.resolve()} "
                    f"--dot-dir {pipeline_builder.dot_path.parent.resolve()} "
                    f"--graph-dir {pipeline_builder.graph_path.parent.resolve()} "
                    f"--graph-suffix={pipeline_builder.graph_path.suffix} "
                ),
            )
            if graph_action:
                for graph_path, dot_path in pipeline_builder.iter_graph_dot_paths():
                    yield from env.Command(
                        [env.File(graph_path)],
                        [env.File(dot_path)],
                        action=graph_action,
                    )
        relative_pipeline_rst_paths = " ".join(
            str(self._relative_to_rst(p.rst_path)) for p in self.pipelines.values()
        )
        yield from env.Command(
            [env.File(str(self.rst_path))],
            source_files,
            action=(f"python -m {__name__} index " f"$TARGET {relative_pipeline_rst_paths}"),
        )


def main(argv: Sequence[str]) -> None:
    """Entry point for command-line invocations used as SCons actions.

    Parameters
    ----------
    argv : `Sequence` [ `str` ]
        Command-line arguments to parse; generally ``sys.argv[1:]``.

    See Also
    --------
    PackagePipelinesDocBuilder.scons_generate
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    index_parser = subparsers.add_parser("index")
    index_parser.add_argument("target", type=str)
    index_parser.add_argument("relative", type=str, nargs="*")
    index_parser.set_defaults(func=PackagePipelinesDocBuilder.scons_script)
    pipeline_parser = subparsers.add_parser("pipeline")
    pipeline_parser.add_argument("name", type=str)
    pipeline_parser.add_argument("--source-yaml", type=str)
    pipeline_parser.add_argument("--rst-dir", type=str)
    pipeline_parser.add_argument("--yaml-dir", type=str)
    pipeline_parser.add_argument("--dot-dir", type=str)
    pipeline_parser.add_argument("--graph-dir", type=str)
    pipeline_parser.add_argument("--graph-suffix", type=str, default=".svg")
    pipeline_parser.set_defaults(func=PipelineDocBuilder.scons_script)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
