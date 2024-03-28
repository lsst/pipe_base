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

"""Module defining GraphBuilder class and related methods.
"""

from __future__ import annotations

__all__ = ["GraphBuilder"]


import warnings
from collections.abc import Iterable, Mapping
from typing import Any

from deprecated.sphinx import deprecated
from lsst.daf.butler import Butler, DataCoordinate, Datastore, Registry
from lsst.daf.butler.registry.wildcards import CollectionWildcard
from lsst.utils.introspection import find_outside_stacklevel

from ._datasetQueryConstraints import DatasetQueryConstraintVariant
from .all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from .graph import QuantumGraph
from .pipeline import Pipeline, TaskDef
from .pipeline_graph import PipelineGraph

# Re-exports for backwards-compatibility.
from .quantum_graph_builder import GraphBuilderError  # noqa: F401
from .quantum_graph_builder import OutputExistsError  # noqa: F401
from .quantum_graph_builder import PrerequisiteMissingError  # noqa: F401

# TODO: remove this module on DM-40443.
warnings.warn(
    "The graphBuilder module is deprecated in favor of quantum_graph_builder, and will be removed after v27.",
    category=FutureWarning,
    stacklevel=find_outside_stacklevel("lsst.pipe.base"),
)


@deprecated(
    "Deprecated in favor of QuantumGraphBuilder and will be removed after v27.",
    version="v27.0",
    category=FutureWarning,
)
class GraphBuilder:
    """GraphBuilder class is responsible for building task execution graph from
    a Pipeline.

    Parameters
    ----------
    registry : `~lsst.daf.butler.Registry`
        Data butler instance.
    skipExistingIn : `~typing.Any`
        Expressions representing the collections to search for existing
        output datasets that should be skipped.  See
        :ref:`daf_butler_ordered_collection_searches`.
    clobberOutputs : `bool`, optional
        If `True` (default), allow quanta to created even if partial outputs
        exist; this requires the same behavior behavior to be enabled when
        executing.
    datastore : `~lsst.daf.butler.Datastore`, optional
        If not `None` then fill datastore records in each generated Quantum.
    """

    def __init__(
        self,
        registry: Registry,
        skipExistingIn: Any = None,
        clobberOutputs: bool = True,
        datastore: Datastore | None = None,
    ):
        self.registry = registry
        self.dimensions = registry.dimensions
        self.skipExistingIn = skipExistingIn
        self.clobberOutputs = clobberOutputs
        self.datastore = datastore

    def makeGraph(
        self,
        pipeline: Pipeline | Iterable[TaskDef],
        collections: Any,
        run: str,
        userQuery: str | None,
        datasetQueryConstraint: DatasetQueryConstraintVariant = DatasetQueryConstraintVariant.ALL,
        metadata: Mapping[str, Any] | None = None,
        bind: Mapping[str, Any] | None = None,
        dataId: DataCoordinate | None = None,
    ) -> QuantumGraph:
        """Create execution graph for a pipeline.

        Parameters
        ----------
        pipeline : `Pipeline` or `~collections.abc.Iterable` [ `TaskDef` ]
            Pipeline definition, task names/classes and their configs.
        collections : `~typing.Any`
            Expressions representing the collections to search for input
            datasets.  See :ref:`daf_butler_ordered_collection_searches`.
        run : `str`
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection for
            output datasets. Collection does not have to exist and it will be
            created when graph is executed.
        userQuery : `str`
            String which defines user-defined selection for registry, should be
            empty or `None` if there is no restrictions on data selection.
        datasetQueryConstraint : `DatasetQueryConstraintVariant`, optional
            The query constraint variant that should be used to constraint the
            query based on dataset existance, defaults to
            `DatasetQueryConstraintVariant.ALL`.
        metadata : Optional Mapping of `str` to primitives
            This is an optional parameter of extra data to carry with the
            graph.  Entries in this mapping should be able to be serialized in
            JSON.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``userQuery`` expression, keyed by the identifiers they replace.
        dataId : `lsst.daf.butler.DataCoordinate`, optional
            Data ID that should also be included in the query constraint.
            Ignored if ``pipeline`` is a `Pipeline` instance (which has its own
            data ID).

        Returns
        -------
        graph : `QuantumGraph`
            The constructed graph.

        Raises
        ------
        UserExpressionError
            Raised when user expression cannot be parsed.
        OutputExistsError
            Raised when output datasets already exist.
        Exception
            Other exceptions types may be raised by underlying registry
            classes.
        """
        if isinstance(pipeline, Pipeline):
            pipeline_graph = pipeline.to_graph()
        else:
            pipeline_graph = PipelineGraph(data_id=dataId)
            for task_def in pipeline:
                pipeline_graph.add_task(
                    task_def.label,
                    task_def.taskClass,
                    config=task_def.config,
                    connections=task_def.connections,
                )
        # We assume `registry` is actually a RegistryShim that has a butler
        # inside it, since that's now the only kind of Registry code outside
        # Butler should be able to get, and we assert that the datastore came
        # from the same place.  Soon this interface will be deprecated in favor
        # of QuantumGraphBuilder (which takes a Butler directly, as all new
        # code should) anyway.
        butler: Butler = self.registry._butler  # type: ignore
        assert butler._datastore is self.datastore or self.datastore is None
        qgb = AllDimensionsQuantumGraphBuilder(
            pipeline_graph,
            butler,
            input_collections=CollectionWildcard.from_expression(collections).require_ordered(),
            output_run=run,
            skip_existing_in=self.skipExistingIn if self.skipExistingIn is not None else (),
            clobber=self.clobberOutputs,
            where=userQuery if userQuery is not None else "",
            dataset_query_constraint=datasetQueryConstraint,
            bind=bind,
        )
        return qgb.build(metadata, attach_datastore_records=(self.datastore is not None))
