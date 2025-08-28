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

__all__ = ("SimplePipelineExecutor",)

import datetime
import getpass
import itertools
import os
from collections.abc import Iterable, Iterator, Mapping
from typing import Any, cast

from lsst.daf.butler import (
    Butler,
    CollectionType,
    DataCoordinate,
    DatasetRef,
    DimensionDataExtractor,
    DimensionGroup,
    Quantum,
)
from lsst.pex.config import Config

from ._instrument import Instrument
from ._quantumContext import ExecutionResources
from .all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from .graph import QuantumGraph
from .pipeline import Pipeline
from .pipeline_graph import PipelineGraph
from .pipelineTask import PipelineTask
from .single_quantum_executor import SingleQuantumExecutor
from .taskFactory import TaskFactory


class SimplePipelineExecutor:
    """A simple, high-level executor for pipelines.

    Parameters
    ----------
    quantum_graph : `.QuantumGraph`
        Graph to be executed.
    butler : `~lsst.daf.butler.Butler`
        Object that manages all I/O.  Must be initialized with `collections`
        and `run` properties that correspond to the input and output
        collections, which must be consistent with those used to create
        ``quantum_graph``.
    resources : `.ExecutionResources`
        The resources available to each quantum being executed.
    raise_on_partial_outputs : `bool`, optional
        If `True` raise exceptions chained by `.AnnotatedPartialOutputsError`
        immediately, instead of considering the partial result a success and
        continuing to run downstream tasks.

    Notes
    -----
    Most callers should use one of the `classmethod` factory functions
    (`from_pipeline_filename`, `from_task_class`, `from_pipeline`) instead of
    invoking the constructor directly; these guarantee that the
    `~lsst.daf.butler.Butler` and `.QuantumGraph` are created consistently.

    This class is intended primarily to support unit testing and small-scale
    integration testing of `.PipelineTask` classes.  It deliberately lacks many
    features present in the command-line-only ``pipetask`` tool in order to
    keep the implementation simple.  Python callers that need more
    sophistication should call lower-level tools like
    `~.quantum_graph_builder.QuantumGraphBuilder` and
    `.single_quantum_executor.SingleQuantumExecutor` directly.
    """

    def __init__(
        self,
        quantum_graph: QuantumGraph,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
    ):
        self.quantum_graph = quantum_graph
        self.butler = butler
        self.resources = resources
        self.raise_on_partial_outputs = raise_on_partial_outputs

    @classmethod
    def prep_butler(
        cls,
        root: str,
        inputs: Iterable[str],
        output: str,
        output_run: str | None = None,
    ) -> Butler:
        """Return configured `~lsst.daf.butler.Butler`.

        Helper method for creating `~lsst.daf.butler.Butler` instances with
        collections appropriate for processing.

        Parameters
        ----------
        root : `str`
            Root of the butler data repository; must already exist, with all
            necessary input data.
        inputs : `~collections.abc.Iterable` [ `str` ]
            Collections to search for all input datasets, in search order.
        output : `str`
            Name of a new output `~lsst.daf.butler.CollectionType.CHAINED`
            collection to create that will combine both inputs and outputs.
        output_run : `str`, optional
            Name of the output `~lsst.daf.butler.CollectionType.RUN` that will
            directly hold all output datasets.  If not provided, a name will be
            created from ``output`` and a timestamp.

        Returns
        -------
        butler : `~lsst.daf.butler.Butler`
            Butler client instance compatible with all `classmethod` factories.
            Always writeable.
        """
        if output_run is None:
            output_run = f"{output}/{Instrument.makeCollectionTimestamp()}"
        # Make initial butler with no collections, since we haven't created
        # them yet.
        butler = Butler.from_config(root, writeable=True)
        butler.registry.registerCollection(output_run, CollectionType.RUN)
        butler.registry.registerCollection(output, CollectionType.CHAINED)
        collections = [output_run]
        collections.extend(inputs)
        butler.registry.setCollectionChain(output, collections)
        # Remake butler to let it infer default data IDs from collections, now
        # that those collections exist.
        return Butler.from_config(butler=butler, collections=[output], run=output_run)

    @classmethod
    def from_pipeline_filename(
        cls,
        pipeline_filename: str,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
        attach_datastore_records: bool = False,
        output: str | None = None,
        output_run: str | None = None,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an on-disk
        pipeline YAML file.

        Parameters
        ----------
        pipeline_filename : `str`
            Name of the YAML file to load the pipeline definition from.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.
        resources : `.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `.AnnotatedPartialOutputsError` immediately, instead of considering
            the partial result a success and continuing to run downstream
            tasks.
        attach_datastore_records : `bool`, optional
            Whether to attach datastore records to the quantum graph.  This is
            usually unnecessary, unless the executor is used to test behavior
            that depends on datastore records.
        output : `str`, optional
            Name of a new output `~lsst.daf.butler.CollectionType.CHAINED`
            collection to create that will combine both inputs and outputs.
        output_run : `str`, optional
            Name of the output `~lsst.daf.butler.CollectionType.RUN` that will
            directly hold all output datasets.  If not provided, a name will be
            created from ``output`` and a timestamp.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed `.QuantumGraph` and
            `~lsst.daf.butler.Butler`, ready for `run` to be called.
        """
        pipeline = Pipeline.fromFile(pipeline_filename)
        return cls.from_pipeline(
            pipeline,
            butler=butler,
            where=where,
            bind=bind,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
            attach_datastore_records=attach_datastore_records,
            output=output,
            output_run=output_run,
        )

    @classmethod
    def from_task_class(
        cls,
        task_class: type[PipelineTask],
        config: Config | None = None,
        label: str | None = None,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
        attach_datastore_records: bool = False,
        output: str | None = None,
        output_run: str | None = None,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from a pipeline
        containing a single task.

        Parameters
        ----------
        task_class : `type`
            A concrete `.PipelineTask` subclass.
        config : `~lsst.pex.config.Config`, optional
            Configuration for the task.  If not provided, task-level defaults
            will be used (no per-instrument overrides).
        label : `str`, optional
            Label for the task in its pipeline; defaults to
            ``task_class._DefaultName``.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.
        resources : `.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `.AnnotatedPartialOutputsError` immediately, instead of considering
            the partial result a success and continuing to run downstream
            tasks.
        attach_datastore_records : `bool`, optional
            Whether to attach datastore records to the quantum graph.  This is
            usually unnecessary, unless the executor is used to test behavior
            that depends on datastore records.
        output : `str`, optional
            Name of a new output `~lsst.daf.butler.CollectionType.CHAINED`
            collection to create that will combine both inputs and outputs.
        output_run : `str`, optional
            Name of the output `~lsst.daf.butler.CollectionType.RUN` that will
            directly hold all output datasets.  If not provided, a name will be
            created from ``output`` and a timestamp.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed `.QuantumGraph` and
            `~lsst.daf.butler.Butler`, ready for `run` to be called.
        """
        if config is None:
            config = task_class.ConfigClass()
        if label is None:
            label = task_class._DefaultName
        if not isinstance(config, task_class.ConfigClass):
            raise TypeError(
                f"Invalid config class type: expected {task_class.ConfigClass.__name__}, "
                f"got {type(config).__name__}."
            )
        pipeline_graph = PipelineGraph()
        pipeline_graph.add_task(label=label, task_class=task_class, config=config)
        return cls.from_pipeline_graph(
            pipeline_graph,
            butler=butler,
            where=where,
            bind=bind,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
            attach_datastore_records=attach_datastore_records,
            output=output,
            output_run=output_run,
        )

    @classmethod
    def from_pipeline(
        cls,
        pipeline: Pipeline,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
        attach_datastore_records: bool = False,
        output: str | None = None,
        output_run: str | None = None,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an in-memory
        pipeline.

        Parameters
        ----------
        pipeline : `.Pipeline` or `~collections.abc.Iterable` [ `.TaskDef` ]
            A Python object describing the tasks to run, along with their
            labels and configuration.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.
        resources : `.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `.AnnotatedPartialOutputsError` immediately, instead of considering
            the partial result a success and continuing to run downstream
            tasks.
        attach_datastore_records : `bool`, optional
            Whether to attach datastore records to the quantum graph.  This is
            usually unnecessary, unless the executor is used to test behavior
            that depends on datastore records.
        output : `str`, optional
            Name of a new output `~lsst.daf.butler.CollectionType.CHAINED`
            collection to create that will combine both inputs and outputs.
        output_run : `str`, optional
            Name of the output `~lsst.daf.butler.CollectionType.RUN` that will
            directly hold all output datasets.  If not provided, a name will
            be created from ``output`` and a timestamp.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed `.QuantumGraph` and
            `~lsst.daf.butler.Butler`, ready for `run` to be called.
        """
        pipeline_graph = pipeline.to_graph()
        return cls.from_pipeline_graph(
            pipeline_graph,
            where=where,
            bind=bind,
            butler=butler,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
            attach_datastore_records=attach_datastore_records,
            output=output,
            output_run=output_run,
        )

    @classmethod
    def from_pipeline_graph(
        cls,
        pipeline_graph: PipelineGraph,
        *,
        where: str = "",
        bind: Mapping[str, Any] | None = None,
        butler: Butler,
        resources: ExecutionResources | None = None,
        raise_on_partial_outputs: bool = True,
        attach_datastore_records: bool = False,
        output: str | None = None,
        output_run: str | None = None,
    ) -> SimplePipelineExecutor:
        """Create an executor by building a QuantumGraph from an in-memory
        pipeline graph.

        Parameters
        ----------
        pipeline_graph : `~.pipeline_graph.PipelineGraph`
            A Python object describing the tasks to run, along with their
            labels and configuration, in graph form.  Will be resolved against
            the given ``butler``, with any existing resolutions ignored.
        where : `str`, optional
            Data ID query expression that constraints the quanta generated.
        bind : `~collections.abc.Mapping`, optional
            Mapping containing literal values that should be injected into the
            ``where`` expression, keyed by the identifiers they replace.
        butler : `~lsst.daf.butler.Butler`
            Butler that manages all I/O.  `prep_butler` can be used to create
            one.  Must have its `~lsst.daf.butler.Butler.run` and
            ``butler.collections.defaults`` not empty and not `None`.
        resources : `.ExecutionResources`
            The resources available to each quantum being executed.
        raise_on_partial_outputs : `bool`, optional
            If `True` raise exceptions chained by
            `.AnnotatedPartialOutputsError` immediately, instead
            of considering the partial result a success and continuing to run
            downstream tasks.
        attach_datastore_records : `bool`, optional
            Whether to attach datastore records to the quantum graph.  This is
            usually unnecessary, unless the executor is used to test behavior
            that depends on datastore records.
        output : `str`, optional
            Name of a new output `~lsst.daf.butler.CollectionType.CHAINED`
            collection to create that will combine both inputs and outputs.
        output_run : `str`, optional
            Name of the output `~lsst.daf.butler.CollectionType.RUN` that will
            directly hold all output datasets.  If not provided, a name will
            be created from ``output`` and a timestamp.

        Returns
        -------
        executor : `SimplePipelineExecutor`
            An executor instance containing the constructed
            `.QuantumGraph` and `~lsst.daf.butler.Butler`, ready
            for `run` to be called.
        """
        if output_run is None:
            output_run = butler.run
            if output_run is None:
                if output is None:
                    raise TypeError("At least one of output or output_run must be provided.")
                output_run = f"{output}/{Instrument.makeCollectionTimestamp()}"

        quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
            pipeline_graph, butler, where=where, bind=bind, output_run=output_run
        )
        metadata = {
            "input": list(butler.collections.defaults),
            "output": output,
            "output_run": output_run,
            "skip_existing_in": [],
            "skip_existing": False,
            "data_query": where,
            "user": getpass.getuser(),
            "time": str(datetime.datetime.now()),
        }
        quantum_graph = quantum_graph_builder.build(
            metadata=metadata, attach_datastore_records=attach_datastore_records
        )
        return cls(
            quantum_graph=quantum_graph,
            butler=butler,
            resources=resources,
            raise_on_partial_outputs=raise_on_partial_outputs,
        )

    def use_local_butler(
        self, root: str, register_dataset_types: bool = True, transfer_dimensions: bool = True
    ) -> Butler:
        """Transfer all inputs to a local data repository. and set the executor
        to write outputs to it.

        Parameters
        ----------
        root : `str`
            Path to the local data repository; created if it does not exist.
        register_dataset_types : `bool`, optional
            Whether to register dataset types in the new repository.  If
            `False`, the local data repository must already exist and already
            have all input dataset types registered.
        transfer_dimensions : `bool`, optional
            Whether to transfer dimension records to the new repository.  If
            `False`, the local data repository must already exist and already
            have all needed dimension records.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            Writeable butler for local data repository.

        Notes
        -----
        The input collection structure from the original data repository is not
        preserved by this method (it cannot be reconstructed from the quantum
        graph).  Instead, a `~lsst.daf.butler.CollectionType.TAGGED` collection
        is created to gather all inputs, and appended to the output
        `~lsst.daf.butler.CollectionType.CHAINED` collection after the output
        `~lsst.daf.butler.CollectionType.RUN` collection.  Calibration inputs
        with the same data ID but multiple validity ranges are *not* included
        in that `~lsst.daf.butler.CollectionType.TAGGED`; they are still
        transferred to the local data repository, but can only be found via the
        quantum graph or their original `~lsst.daf.butler.CollectionType.RUN`
        collections.
        """
        if not os.path.exists(root):
            Butler.makeRepo(root)
        out_butler = Butler.from_config(root, writeable=True)

        output_run = self.quantum_graph.metadata["output_run"]
        out_butler.collections.register(output_run, CollectionType.RUN)
        output = self.quantum_graph.metadata["output"]
        inputs: str | None = None
        if output is not None:
            inputs = f"{output}/inputs"
            out_butler.collections.register(output, CollectionType.CHAINED)
            out_butler.collections.register(inputs, CollectionType.TAGGED)
            out_butler.collections.redefine_chain(output, [output_run, inputs])

        if transfer_dimensions:
            # We can't just let the transfer_from call below take care of this
            # because we need dimensions for outputs as well as inputs.  And if
            # we have to do the outputs explicitly, it's more efficient to do
            # the inputs at the same time since a lot of those dimensions will
            # be the same.
            self._transfer_qg_dimension_records(out_butler)

        # Extract overall-input DatasetRefs to transfer and possibly insert
        # into a TAGGED collection.
        refs: set[DatasetRef] = set()
        to_tag_by_type: dict[str, dict[DataCoordinate, DatasetRef | None]] = {}
        pipeline_graph = self.quantum_graph.pipeline_graph
        for name, dataset_type_node in pipeline_graph.iter_overall_inputs():
            assert dataset_type_node is not None, "PipelineGraph should be resolved."
            to_tag_for_type = to_tag_by_type.setdefault(name, {})
            for task_node in pipeline_graph.consumers_of(name):
                for quantum in self.quantum_graph.get_task_quanta(task_node.label).values():
                    for ref in quantum.inputs[name]:
                        ref = dataset_type_node.generalize_ref(ref)
                        refs.add(ref)
                        if to_tag_for_type.setdefault(ref.dataId, ref) != ref:
                            # There is already a dataset with the same data ID
                            # and dataset type, but a different UUID/run.  This
                            # can only happen for calibrations found in
                            # calibration collections, and for now we have no
                            # choice but to leave them out of the TAGGED inputs
                            # collection in the local butler.
                            to_tag_for_type[ref.dataId] = None

        out_butler.transfer_from(
            self.butler,
            refs,
            register_dataset_types=register_dataset_types,
            transfer_dimensions=False,
        )

        if inputs is not None:
            to_tag_flat: list[DatasetRef] = []
            for ref_map in to_tag_by_type.values():
                for tag_ref in ref_map.values():
                    if tag_ref is not None:
                        to_tag_flat.append(tag_ref)
            out_butler.registry.associate(inputs, to_tag_flat)

        out_butler.registry.defaults = self.butler.registry.defaults.clone(collections=output, run=output_run)
        self.butler = out_butler
        return self.butler

    def run(self, register_dataset_types: bool = False, save_versions: bool = True) -> list[Quantum]:
        """Run all the quanta in the `.QuantumGraph` in topological order.

        Use this method to run all quanta in the graph.  Use
        `as_generator` to get a generator to run the quanta one at
        a time.

        Parameters
        ----------
        register_dataset_types : `bool`, optional
            If `True`, register all output dataset types before executing any
            quanta.
        save_versions : `bool`, optional
            If `True` (default), save a package versions dataset.

        Returns
        -------
        quanta : `list` [ `~lsst.daf.butler.Quantum` ]
            Executed quanta.

        Notes
        -----
        A topological ordering is not in general unique, but no other
        guarantees are made about the order in which quanta are processed.
        """
        return list(
            self.as_generator(register_dataset_types=register_dataset_types, save_versions=save_versions)
        )

    def as_generator(
        self, register_dataset_types: bool = False, save_versions: bool = True
    ) -> Iterator[Quantum]:
        """Yield quanta in the `.QuantumGraph` in topological order.

        These quanta will be run as the returned generator is iterated
        over.  Use this method to run the quanta one at a time.
        Use `run` to run all quanta in the graph.

        Parameters
        ----------
        register_dataset_types : `bool`, optional
            If `True`, register all output dataset types before executing any
            quanta.
        save_versions : `bool`, optional
            If `True` (default), save a package versions dataset.

        Returns
        -------
        quanta : `~collections.abc.Iterator` [ `~lsst.daf.butler.Quantum` ]
            Executed quanta.

        Notes
        -----
        Global initialization steps (see `.QuantumGraph.init_output_run`) are
        performed immediately when this method is called, but individual quanta
        are not actually executed until the returned iterator is iterated over.

        A topological ordering is not in general unique, but no other
        guarantees are made about the order in which quanta are processed.
        """
        if register_dataset_types:
            self.quantum_graph.pipeline_graph.register_dataset_types(self.butler)
        self.quantum_graph.write_configs(self.butler, compare_existing=False)
        self.quantum_graph.write_init_outputs(self.butler, skip_existing=False)
        if save_versions:
            self.quantum_graph.write_packages(self.butler, compare_existing=False)
        task_factory = TaskFactory()
        single_quantum_executor = SingleQuantumExecutor(
            butler=self.butler,
            task_factory=task_factory,
            resources=self.resources,
            raise_on_partial_outputs=self.raise_on_partial_outputs,
        )
        # Important that this returns a generator expression rather than being
        # a generator itself; that is what makes the init stuff above happen
        # immediately instead of when the first quanta is executed, which might
        # be useful for callers who want to check the state of the repo in
        # between.
        return (
            single_quantum_executor.execute(qnode.task_node, qnode.quantum, qnode.nodeId)[0]
            for qnode in self.quantum_graph
        )

    def _transfer_qg_dimension_records(self, out_butler: Butler) -> None:
        """Transfer all dimension records from the quantum graph to a butler.

        Parameters
        ----------
        out_butler : `lsst.daf.butler.Butler`
            Butler to transfer records to.
        """
        pipeline_graph = self.quantum_graph.pipeline_graph
        all_dimensions = DimensionGroup.union(
            *pipeline_graph.group_by_dimensions(prerequisites=True).keys(),
            universe=self.butler.dimensions,
        )
        dimension_data_extractor = DimensionDataExtractor.from_dimension_group(all_dimensions)
        for task_node in pipeline_graph.tasks.values():
            task_quanta = self.quantum_graph.get_task_quanta(task_node.label)
            for quantum in task_quanta.values():
                dimension_data_extractor.update([cast(DataCoordinate, quantum.dataId)])
                for refs in itertools.chain(quantum.inputs.values(), quantum.outputs.values()):
                    dimension_data_extractor.update(ref.dataId for ref in refs)
        for element_name in all_dimensions.elements:
            record_set = dimension_data_extractor.records.get(element_name)
            if record_set and record_set.element.has_own_table:
                out_butler.registry.insertDimensionData(
                    record_set.element,
                    *record_set,
                    skip_existing=True,
                )
