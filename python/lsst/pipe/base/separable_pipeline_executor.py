# This file is part of ctrl_mpexec.
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

__all__ = [
    "SeparablePipelineExecutor",
]


import datetime
import getpass
import logging
from collections.abc import Iterable
from typing import Any

import lsst.pipe.base
import lsst.resources
from lsst.daf.butler import Butler
from lsst.pipe.base.all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from lsst.pipe.base.quantum_graph_builder import QuantumGraphBuilder

from .mpGraphExecutor import MPGraphExecutor
from .quantumGraphExecutor import QuantumGraphExecutor
from .singleQuantumExecutor import SingleQuantumExecutor
from .taskFactory import TaskFactory

_LOG = logging.getLogger(__name__)


class SeparablePipelineExecutor:
    """An executor that allows each step of pipeline execution to be
    run independently.

    The executor can run any or all of the following steps:

        * pre-execution initialization
        * pipeline building
        * quantum graph generation
        * quantum graph execution

    Any of these steps can also be handed off to external code without
    compromising the remaining ones.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        A Butler whose ``collections`` and ``run`` attributes contain the input
        and output collections to use for processing.
    clobber_output : `bool`, optional
        If set, the pipeline execution overwrites existing output files.
        Otherwise, any conflict between existing and new outputs is an error.
    skip_existing_in : iterable [`str`], optional
        If not empty, the pipeline execution searches the listed collections
        for existing outputs, and skips any quanta that have run to completion
        (or have no work to do). Otherwise, all tasks are attempted (subject to
        ``clobber_output``).
    task_factory : `lsst.pipe.base.TaskFactory`, optional
        A custom task factory for use in pre-execution and execution. By
        default, a new instance of `lsst.ctrl.mpexec.TaskFactory` is used.
    resources : `~lsst.pipe.base.ExecutionResources`
        The resources available to each quantum being executed.
    raise_on_partial_outputs : `bool`, optional
        If `True` raise exceptions chained by
        `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead of
        considering the partial result a success and continuing to run
        downstream tasks.
    """

    def __init__(
        self,
        butler: Butler,
        clobber_output: bool = False,
        skip_existing_in: Iterable[str] | None = None,
        task_factory: lsst.pipe.base.TaskFactory | None = None,
        resources: lsst.pipe.base.ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
    ):
        self._butler = Butler.from_config(
            butler=butler, collections=butler.collections.defaults, run=butler.run
        )
        if not self._butler.collections.defaults:
            raise ValueError("Butler must specify input collections for pipeline.")
        if not self._butler.run:
            raise ValueError("Butler must specify output run for pipeline.")

        self._clobber_output = clobber_output
        self._skip_existing_in = list(skip_existing_in) if skip_existing_in else []

        self._task_factory = task_factory if task_factory else TaskFactory()
        self.resources = resources
        self.raise_on_partial_outputs = raise_on_partial_outputs

    def pre_execute_qgraph(
        self,
        graph: lsst.pipe.base.QuantumGraph,
        register_dataset_types: bool = False,
        save_init_outputs: bool = True,
        save_versions: bool = True,
    ) -> None:
        """Run pre-execution initialization.

        This method will be deprecated after DM-38041, to be replaced with a
        method that takes either a `~lsst.pipe.base.Pipeline` or a
        ``ResolvedPipelineGraph`` instead of a `~lsst.pipe.base.QuantumGraph`.

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
            The quantum graph defining the pipeline and datasets to
            be initialized.
        register_dataset_types : `bool`, optional
            If `True`, register all output dataset types from the pipeline
            represented by ``graph``.
        save_init_outputs : `bool`, optional
            If `True`, create init-output datasets in this object's output run.
        save_versions : `bool`, optional
            If `True`, save a package versions dataset.
        """
        if register_dataset_types:
            graph.pipeline_graph.register_dataset_types(self._butler, include_packages=save_versions)
        if save_init_outputs:
            graph.write_init_outputs(self._butler, skip_existing=(self._butler.run in self._skip_existing_in))
            graph.write_configs(self._butler)
        if save_versions:
            graph.write_packages(self._butler)

    def make_pipeline(self, pipeline_uri: str | lsst.resources.ResourcePath) -> lsst.pipe.base.Pipeline:
        """Build a pipeline from pipeline and configuration information.

        Parameters
        ----------
        pipeline_uri : `str` or `lsst.resources.ResourcePath`
            URI to a file containing a pipeline definition. A URI fragment may
            be used to specify a subset of the pipeline, as described in
            :ref:`pipeline-running-intro`.

        Returns
        -------
        pipeline : `lsst.pipe.base.Pipeline`
            The fully-built pipeline.
        """
        return lsst.pipe.base.Pipeline.from_uri(pipeline_uri)

    def make_quantum_graph(
        self,
        pipeline: lsst.pipe.base.Pipeline,
        where: str = "",
        *,
        builder_class: type[QuantumGraphBuilder] = AllDimensionsQuantumGraphBuilder,
        attach_datastore_records: bool = False,
        **kwargs: Any,
    ) -> lsst.pipe.base.QuantumGraph:
        """Build a quantum graph from a pipeline and input datasets.

        Parameters
        ----------
        pipeline : `lsst.pipe.base.Pipeline`
            The pipeline for which to generate a quantum graph.
        where : `str`, optional
            A data ID query that constrains the quanta generated.  Must not be
            provided if a custom ``builder_class`` is given and that class does
            not accept ``where`` as a construction argument.
        builder_class : `type` [ \
                `lsst.pipe.base.quantum_graph_builder.QuantumGraphBuilder` ], \
                optional
            Quantum graph builder implementation.  Ignored if ``builder`` is
            provided.
        attach_datastore_records : `bool`, optional
            Whether to attach datastore records.  These are currently used only
            by `lsst.daf.butler.QuantumBackedButler`, which is not used by
            `SeparablePipelineExecutor` for execution.
        **kwargs
            Additional keyword arguments are forwarded to ``builder_class``
            when a quantum graph builder instance is constructed.  All
            arguments accepted by the
            `~lsst.pipe.base.quantum_graph_builder.QuantumGraphBuilder` base
            class are provided automatically (from explicit arguments to this
            method and executor attributes) and do not need to be included
            as keyword arguments.

        Returns
        -------
        graph : `lsst.pipe.base.QuantumGraph`
            The quantum graph for ``pipeline`` as run on the datasets
            identified by ``where``.

        Notes
        -----
        This method does no special handling of empty quantum graphs. If
        needed, clients can use `len` to test if the returned graph is empty.
        """
        metadata = {
            "input": self._butler.collections.defaults,
            "output_run": self._butler.run,
            "skip_existing_in": self._skip_existing_in,
            "skip_existing": bool(self._skip_existing_in),
            "data_query": where,
            "user": getpass.getuser(),
            "time": str(datetime.datetime.now()),
        }
        if where:
            # Only pass 'where' if it's actually provided, since some
            # QuantumGraphBuilder subclasses may not accept it.
            kwargs["where"] = where
        qg_builder = builder_class(
            pipeline.to_graph(),
            self._butler,
            skip_existing_in=self._skip_existing_in,
            clobber=self._clobber_output,
            **kwargs,
        )
        graph = qg_builder.build(metadata=metadata, attach_datastore_records=attach_datastore_records)
        _LOG.info(
            "QuantumGraph contains %d quanta for %d tasks, graph ID: %r",
            len(graph),
            len(graph.taskGraph),
            graph.graphID,
        )
        return graph

    def run_pipeline(
        self,
        graph: lsst.pipe.base.QuantumGraph,
        fail_fast: bool = False,
        graph_executor: QuantumGraphExecutor | None = None,
        num_proc: int = 1,
    ) -> None:
        """Run a pipeline in the form of a prepared quantum graph.

        Pre-execution initialization must have already been run;
        see `pre_execute_qgraph`.

        Parameters
        ----------
        graph : `lsst.pipe.base.QuantumGraph`
            The pipeline and datasets to execute.
        fail_fast : `bool`, optional
            If `True`, abort all execution if any task fails when
            running with multiple processes. Only used with the default graph
            executor).
        graph_executor : `lsst.ctrl.mpexec.QuantumGraphExecutor`, optional
            A custom graph executor. By default, a new instance of
            `lsst.ctrl.mpexec.MPGraphExecutor` is used.
        num_proc : `int`, optional
            The number of processes that can be used to run the pipeline. The
            default value ensures that no subprocess is created. Only used with
            the default graph executor.
        """
        if not graph_executor:
            quantum_executor = SingleQuantumExecutor(
                self._butler,
                self._task_factory,
                skipExistingIn=self._skip_existing_in,
                clobberOutputs=self._clobber_output,
                resources=self.resources,
                raise_on_partial_outputs=self.raise_on_partial_outputs,
            )
            graph_executor = MPGraphExecutor(
                numProc=num_proc,
                timeout=2_592_000.0,  # In practice, timeout is never helpful; set to 30 days.
                quantumExecutor=quantum_executor,
                failFast=fail_fast,
            )
            # Have to reset connection pool to avoid sharing connections with
            # forked processes.
            self._butler.registry.resetConnectionPool()

        graph_executor.execute(graph)
