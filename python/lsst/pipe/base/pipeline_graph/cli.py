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
from __future__ import annotations

import argparse
import dataclasses
import sys
from collections.abc import Sequence
from contextlib import ExitStack

from lsst.daf.butler import Butler
from lsst.resources import ResourcePath

from ..pipeline import Pipeline
from ._pipeline_graph import MutablePipelineGraph, PipelineGraph, ResolvedPipelineGraph
from .visualization._options import NodeAttributeOptions
from .visualization._show import show


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Expand, resolve, and display and resolve pipelines as graphs."
    )
    Arguments.add_args_to_parser(parser)
    args = Arguments.from_parsed_args(parser.parse_args(argv))
    if args.resolve:
        # If we've been asked to resolve the pipeline, make sure we read it as
        # unresolved.
        pipeline_graph = read_input_pipeline(args.input_pipeline, try_resolved=False)
        butler = Butler(args.resolve, writeable=False)
        pipeline_graph = pipeline_graph.resolved(butler.registry)
    else:
        pipeline_graph = read_input_pipeline(args.input_pipeline)
    if args.save:
        pipeline_graph.write_to_uri(ResourcePath(args.save))
    if args.show:
        with ExitStack() as stack:
            if args.show == "-":
                stream = sys.stdout
            else:
                path = ResourcePath(args.show)
                # ResourcePath.open doesn't have the kind of mode overloading
                # necessary to make the return type clear enough for MyPy.
                stream = stack.enter_context(path.open("r"))  # type: ignore
            show(
                pipeline_graph,
                stream,
                tasks=args.display.tasks,
                dataset_types=args.display.dataset_types,
                init=args.display.init,
                color=args.display.color,
                dimensions=args.display.node_attributes.dimensions,
                task_classes=args.display.node_attributes.task_classes,
                storage_classes=args.display.node_attributes.storage_classes,
                merge_input_trees=args.display.merge_input_trees,
                merge_output_trees=args.display.merge_output_trees,
                merge_intermediates=args.display.merge_intermediates,
                include_automatic_connections=args.display.include_automatic_connections,
                width=args.display.width,
                column_crossing_penalty=args.display.column_crossing_penalty,
                column_insertion_penalty=args.display.column_insertion_penalty,
                column_interior_penalty=args.display.column_interior_penalty,
            )
    return 0


def read_input_pipeline(uri: str, try_resolved: bool = True) -> PipelineGraph:
    path = ResourcePath(uri)
    match path.getExtension():
        case ".yaml":
            pipeline = Pipeline.from_uri(path)
            return pipeline.to_graph()
        case ".tar.gz":
            if try_resolved:
                # It'd be better to refactor the read logic to avoid
                # exception-based control flow here.
                try:
                    return ResolvedPipelineGraph.read_uri(path)
                except KeyError as err:
                    if str(err) != "dimensions":
                        raise
            return MutablePipelineGraph.read_uri(path)
        case other:
            raise ValueError(f"Unexpected extension for pipeline file: {other!r}.")


@dataclasses.dataclass
class Arguments:
    input_pipeline: str
    save: str
    show: str | None
    resolve: str | None
    display: DisplayArguments

    @classmethod
    def from_parsed_args(cls, args: argparse.Namespace) -> Arguments:
        return cls(
            input_pipeline=args.input_pipeline,
            save=args.save,
            show=args.show,
            resolve=args.resolve,
            display=DisplayArguments.from_parsed_args(args),
        )

    @classmethod
    def add_args_to_parser(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "input_pipeline",
            type=str,
            metavar="URI",
            help="""
                Filename or URI for the input pipeline specification (.yaml) or
                graph (.tar.gz) file to read.
            """,
        )
        parser.add_argument(
            "--save",
            type=str,
            metavar="FILE",
            help="""
                Save the pipeline graph content to this file.  Should have a
                .tar.gz extension or no extension (in which case .tar.gz will
                be added).
            """,
            default=None,
        )
        parser.add_argument(
            "--show",
            type=str,
            nargs="?",
            metavar="FILE",
            help="""
                Print the pipeline graph in human-readable form using unicode
                lines and symbols.  May be '-' or have no value for STDOUT.
            """,
            default=None,
            const="-",
        )
        parser.add_argument(
            "--resolve",
            type=str,
            nargs=1,
            metavar="REPO",
            help="""
                A butler data repository to use to resolve the graph's dataset
                types and dimensions.
            """,
            default=None,
        )
        DisplayArguments.add_args_to_parser(parser)


@dataclasses.dataclass
class DisplayArguments:
    tasks: bool
    dataset_types: bool
    init: bool | None
    color: Sequence[str] | bool | None
    node_attributes: NodeAttributeOptions
    merge_input_trees: int
    merge_output_trees: int
    merge_intermediates: bool
    include_automatic_connections: bool
    width: int
    column_crossing_penalty: int
    column_insertion_penalty: int
    column_interior_penalty: int

    def __post_init__(self) -> None:
        if self.node_attributes.storage_classes and not self.dataset_types:
            raise ValueError(
                "--storage-classes does nothing unless --dataset-types or --only-dataset-types is passed."
            )
        if self.node_attributes.task_classes and not self.tasks:
            raise ValueError("--task-classes does nothing if --dataset-types-only is passed.")

    @classmethod
    def from_parsed_args(cls, args: argparse.Namespace) -> DisplayArguments:
        return cls(
            tasks=not args.dataset_types_only,
            dataset_types=args.dataset_types or args.dataset_types_only,
            init=args.init,
            color=args.color,
            node_attributes=NodeAttributeOptions(
                dimensions=args.dimensions,
                task_classes=args.task_classes,
                storage_classes=args.storage_classes,
            ),
            merge_input_trees=args.merge_input_trees,
            merge_output_trees=args.merge_output_trees,
            merge_intermediates=args.merge_intermediates,
            include_automatic_connections=args.include_automatic_connections,
            width=args.width,
            column_crossing_penalty=args.column_crossing_penalty,
            column_insertion_penalty=args.column_insertion_penalty,
            column_interior_penalty=args.column_interior_penalty,
        )

    @classmethod
    def add_args_to_parser(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group("additional options for --show")
        dataset_type_inclusion = group.add_mutually_exclusive_group()
        dataset_type_inclusion.add_argument(
            "--dataset-types-only",
            action="store_true",
            help="Show a graph containing only dataset types. Default is a task-only graph.",
        )
        dataset_type_inclusion.add_argument(
            "--dataset-types",
            action="store_true",
            help="Show a graph containing both dataset types and tasks. Default is a task-only graph.",
        )
        group.add_argument(
            "--init",
            action="store_true",
            help="""
                Show a graph of init-input and init-output dataset types and/or
                task inializations instead of the usual runtime graph.
            """,
        )
        color_group = group.add_mutually_exclusive_group()
        color_group.add_argument(
            "--color",
            action="store_true",
            help="""
                Always use terminal escape codes to add color to the graph.
                Default is to use color only if an interactive terminal is
                detected.
            """,
            default=None,
        )
        color_group.add_argument(
            "--no-color",
            action="store_false",
            help="""
                Never use terminal escape codes to add color to the graph.
                Default is to use color only if an interactive terminal is
                detected.
            """,
            default=None,
        )
        color_group.add_argument(
            "--palette",
            type=str,
            nargs="+",
            metavar="COLORS",
            help="""
                A list of colors to use for nodes.  Options include 'red',
                'green', 'blue', 'cyan', 'yellow', 'magenta', and any of these
                preceded by 'light' (case insensitive).  Implies --color.
            """,
            dest="color",
        )
        dimensions_group = group.add_mutually_exclusive_group()
        dimensions_group.add_argument(
            "--no-dimensions",
            action="store_false",
            help="""
                Do not include dimensions in node descriptions or merge
                comparisons at all.  This is the default if the loaded graph
                was not resolved and --resolve was not passed.
            """,
            dest="dimensions",
            default=None,
        )
        dimensions_group.add_argument(
            "--full-dimensions",
            action="store_const",
            help="""
                Show full dimensions in node descriptions, including those that
                are implied or required by another dimension in the set.
            """,
            dest="dimensions",
            const="full",
        )
        dimensions_group.add_argument(
            "--concise-dimensions",
            action="store_const",
            help="""
                Show concise dimensions in node descriptions, removing those
                that are implied or required by another dimension in the set.
                This is the default if the loaded graph was already resolved
                or --resolve is passed.
            """,
            dest="dimensions",
            const="concise",
        )
        task_classes_group = group.add_mutually_exclusive_group()
        dimensions_group.add_argument(
            "--no-task-classes",
            action="store_false",
            help="""
                Do not include task classes in node descriptions or merge
                comparisons at all.  This is the default if the loaded graph
                was not resolved and --resolve was not passed.
            """,
            dest="task_classes",
            default=None,
        )
        task_classes_group.add_argument(
            "--full-task-classes",
            action="store_const",
            help="""
                Show fully-qualified task classes in task node descriptions,
                and use task classes in merge comparisons.
            """,
            dest="task_classes",
            const="full",
        )
        task_classes_group.add_argument(
            "--concise-task-classes",
            action="store_const",
            help="""
                Show unqualified task classes in task node descriptions, and
                use task classes in merge comparisons.  This is the default if
                the loaded graph was already resolved or --resolve is passed.
            """,
            const="concise",
            dest="task_classes",
        )
        storage_classes_group = group.add_mutually_exclusive_group()
        storage_classes_group.add_argument(
            "--no-storage-classes",
            action="store_false",
            help="""
                Show storage classes in dataset type node descriptions, and use
                storage classes in merge comparisons.  This is the default if
                the loaded graph was not resolved and --resolve was not passed.
            """,
            dest="storage_classes",
            default=None,
        )
        storage_classes_group.add_argument(
            "--storage-classes",
            action="store_true",
            help="""
                Show storage classes in dataset type node descriptions, and use
                storage classes in merge comparisons.  This is the default if
                the loaded graph was already resolved or --resolve is passed.
            """,
            dest="storage_classes",
            default=None,
        )
        group.add_argument(
            "--merge-input-trees",
            type=int,
            default=4,
            help="""
                Depth at which to merge input trees with the same outputs,
                dimensions, task classes, and storage classes.  Zero disables
                merging.
            """,
        )
        group.add_argument(
            "--merge-output-trees",
            type=int,
            default=4,
            help="""
                Depth at which to merge output trees with the same inputs,
                dimensions, task classes, and storage classes.  Zero disables
                merging.
            """,
        )
        group.add_argument(
            "--no-merge-intermediates",
            action="store_false",
            dest="merge_intermediates",
            help="""
                Disable merging of intermediate nodes that share the same
                inputs, outputs, dimensions, task classes, and storage classes.
            """,
        )
        group.add_argument(
            "--include-automatic-connections",
            action="store_true",
            help="""
                Include output datasets added by the execution system, such
                as configs, metadata, and logs.
            """,
        )
        group.add_argument(
            "--width",
            type=int,
            default=-1,
            help="""
                Width in characters for the graph and node descriptions.
                Default (-1) is to use the terminal width.  May be 0 to put no
                limit on the width.  This only sets whether node descriptions
                are truncated and moved below the graph, so it may be exceeded
                by the graph itself.
            """,
        )
        group.add_argument(
            "--column-crossing-penalty",
            type=int,
            default=1,
            help="""
                When selecting the column for a new node, penalize a
                candidate column by multiplying the number of ongoing vertical
                edges this node's horizontal incoming edges would have to 'hop'
                by this value.
            """,
        )
        group.add_argument(
            "--column-insertion-penalty",
            type=int,
            default=2,
            help="""
                When selecting the column for a new node, penalize adding new
                columns by this amount.
            """,
        )
        group.add_argument(
            "--column-interior-penalty",
            type=int,
            default=1,
            help="""
                When selecting the column for a new node, penalize adding new
                columns between two existing columns by this amount (in
                addition to the --column-insertion-penalty applied to all new
                columns).
            """,
        )


if __name__ == "__main__":
    main(sys.argv[1:])
