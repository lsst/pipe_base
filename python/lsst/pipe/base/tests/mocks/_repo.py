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

__all__ = ("DirectButlerRepo", "InMemoryRepo", "MockRepo")

import logging
import tempfile
from abc import abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from typing import Any, Literal, Self

from lsst.daf.butler import (
    Butler,
    CollectionType,
    DataCoordinate,
    DatasetRef,
    DatasetType,
    DimensionConfig,
    LimitedButler,
    RegistryConfig,
)
from lsst.daf.butler.tests.utils import create_populated_sqlite_registry
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.sphgeom import RangeSet

from ...all_dimensions_quantum_graph_builder import AllDimensionsQuantumGraphBuilder
from ...pipeline_graph import PipelineGraph
from ...quantum_graph import PredictedQuantumGraph
from ...single_quantum_executor import SingleQuantumExecutor
from ..in_memory_limited_butler import InMemoryLimitedButler
from ._pipeline_task import (
    DynamicConnectionConfig,
    DynamicTestPipelineTask,
    DynamicTestPipelineTaskConfig,
)
from ._storage_class import MockDataset, is_mock_name

_LOG = logging.getLogger(__name__)


class MockRepo(AbstractContextManager):
    """A test helper that populates a butler repository for task execution.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler to use for at least quantum graph building.  Must be writeable.
    input_run : `str`, optional
        Name of a `~lsst.daf.butler.CollectionType.RUN` collection that will be
        used as an input to quantum graph generation.  Input datasets created
        by the helper are added to this collection.
    input_chain : `str`, optional
        Name of a `~lsst.daf.butler.CollectionType.CHAINED` collection that
        will be the direct input to quantum graph generation.  This always
        includes ``input_run``.
    input_children : `str` or `~collections.abc.Iterable` [ `str`], optional
        Additional collections to include in ``input_chain``.
    """

    def __init__(
        self,
        butler: Butler,
        input_run: str = "input_run",
        input_chain: str = "input_chain",
        input_children: Iterable[str] = (),
    ):
        self.butler = butler
        input_chain_definition = [input_run]
        input_chain_definition.extend(input_children)
        self.input_run = input_run
        self.input_chain = input_chain
        self.butler.collections.register(self.input_run)
        self.butler.collections.register(self.input_chain, CollectionType.CHAINED)
        self.butler.collections.redefine_chain(self.input_chain, input_chain_definition)
        self.pipeline_graph = PipelineGraph()
        self.last_auto_dataset_type_index = 0
        self.last_auto_task_index = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        try:
            self.close()
        except Exception:
            _LOG.exception("An exception occurred during MockRepo.close()")
        return False

    def close(self) -> None:
        """Release all resources associated with this mock instance.  The
        instance may no longer be used after this is called.

        Notes
        -----
        Instead of calling ``close()`` directly, you can use the mock object
        as a context manager.  For example::

          with MockRepo(...) as butler:
              butler.get(...)
          # butler is closed after exiting the block.
        """
        self.butler.close()

    def add_task(
        self,
        label: str | None = None,
        *,
        task_class: type[DynamicTestPipelineTask] = DynamicTestPipelineTask,
        config: DynamicTestPipelineTaskConfig | None = None,
        dimensions: Iterable[str] | None = None,
        inputs: Mapping[str, DynamicConnectionConfig] | None = None,
        outputs: Mapping[str, DynamicConnectionConfig] | None = None,
        prerequisite_inputs: Mapping[str, DynamicConnectionConfig] | None = None,
        init_inputs: Mapping[str, DynamicConnectionConfig] | None = None,
        init_outputs: Mapping[str, DynamicConnectionConfig] | None = None,
    ) -> None:
        """Add a task to the helper's pipeline graph.

        Parameters
        ----------
        label : `str`, optional
            Label for the task.  If not provided, the task name will be
            ``task_auto{self.last_auto_task_index}``, with that variable
            incremented.
        task_class : `type`, optional
            Subclass of `DynamicTestPipelineTask` to use.
        config : `DynamicTestPipelineTaskConfig`, optional
            Task configuration to use.  Note that the dimensions are always
            overridden by the ``dimensions`` argument and ``inputs`` and
            ``outputs`` are updated by those arguments unless they are
            explicitly set to empty dictionaries.
        dimensions : `~collections.abc.Iterable` [ `str` ], optional
            Dimensions of the task and any automatically-added input or output
            connection.
        inputs : `~collections.abc.Mapping` [ `str`, \
                `DynamicConnectionConfig` ], optional
            Input connections to add.  If not provided, a single connection is
            added with the same dimensions as the task and dataset type name
            ``dataset_auto{self.last_auto_dataset_type_index}``.
        outputs : `~collections.abc.Mapping` [ `str`, \
                `DynamicConnectionConfig` ], optional
            Output connections to add.  If not provided, a single connection is
            added with the same dimensions as the task and dataset type name
            ``dataset_auto{self.last_auto_dataset_type_index}``, with that
            variable incremented first.
        prerequisite_inputs : `~collections.abc.Mapping` [ `str`, \
                `DynamicConnectionConfig` ], optional
            Prerequisite input connections to add.  Defaults to an empty
            mapping.
        init_inputs : `~collections.abc.Mapping` [ `str`, \
                `DynamicConnectionConfig` ], optional
            Init input connections to add.  Defaults to an empty mapping.
        init_outputs : `~collections.abc.Mapping` [ `str`, \
                `DynamicConnectionConfig` ], optional
            Init output connections to add.  Defaults to an empty mapping.

        Notes
        -----
        The defaults for this method's arguments are designed to allow it to be
        called in succession to create a sequence of "one-to-one" tasks in
        which each consumes the output of the last.
        """
        if config is None:
            config = DynamicTestPipelineTaskConfig()
        if dimensions is not None:
            config.dimensions = list(dimensions)
        if inputs is not None:
            config.inputs.update(inputs)
        else:
            config.inputs["input_connection"] = DynamicConnectionConfig(
                dataset_type_name=f"dataset_auto{self.last_auto_dataset_type_index}",
                dimensions=list(config.dimensions),
            )
        if outputs is not None:
            config.outputs.update(outputs)
        else:
            self.last_auto_dataset_type_index += 1
            config.outputs["output_connection"] = DynamicConnectionConfig(
                dataset_type_name=f"dataset_auto{self.last_auto_dataset_type_index}",
                dimensions=list(config.dimensions),
            )
        if prerequisite_inputs is not None:
            config.prerequisite_inputs.update(prerequisite_inputs)
        if init_inputs is not None:
            config.init_inputs.update(init_inputs)
        if init_outputs is not None:
            config.init_outputs.update(init_outputs)
        if label is None:
            self.last_auto_task_index += 1
            label = f"task_auto{self.last_auto_task_index}"
        self.pipeline_graph.add_task(label, task_class=task_class, config=config)

    def make_quantum_graph(
        self,
        *,
        output: str | None = None,
        output_run: str = "output_run",
        insert_mocked_inputs: bool = True,
        register_output_dataset_types: bool = True,
    ) -> PredictedQuantumGraph:
        """Make a quantum graph from the pipeline task and internal data
        repository.

        Parameters
        ----------
        output : `str` or `None`, optional
            Name of the output chained collection to embed within the quantum
            graph.  Note that this does not actually create this collection.
        output_run : `str`, optional
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection for
            execution outputs.  Note that this does not actually create this
            collection.
        insert_mocked_inputs : `bool`, optional
            Whether to automatically insert datasets for all overall inputs to
            the pipeline graph whose dataset types have not already been
            registered.  If set to `False`, inputs must be provided by imported
            YAML files or explicit calls to `insert_datasets`, which provides
            more fine-grained control over the data IDs of the datasets.
        register_output_dataset_types : `bool`, optional
            If `True`, register all output dataset types.

        Returns
        -------
        qg : `..quantum_graph.PredictedQuantumGraph`
            Quantum graph.  Datastore records will not be attached, since the
            test helper does not actually have a datastore.
        """
        return (
            self.make_quantum_graph_builder(
                insert_mocked_inputs=insert_mocked_inputs,
                register_output_dataset_types=register_output_dataset_types,
                output_run=output_run,
            )
            .finish(output=output, attach_datastore_records=False)
            .assemble()
        )

    def make_quantum_graph_builder(
        self,
        *,
        output_run: str = "output_run",
        insert_mocked_inputs: bool = True,
        register_output_dataset_types: bool = True,
    ) -> AllDimensionsQuantumGraphBuilder:
        """Make a quantum graph builder from the pipeline task and internal
        data repository.

        Parameters
        ----------
        output_run : `str`, optional
            Name of the `~lsst.daf.butler.CollectionType.RUN` collection for
            execution outputs.  Note that this does not actually create this
            collection.
        insert_mocked_inputs : `bool`, optional
            Whether to automatically insert datasets for all overall inputs to
            the pipeline graph whose dataset types have not already been
            registered.  If set to `False`, inputs must be provided by imported
            YAML files or explicit calls to `insert_datasets`, which provides
            more fine-grained control over the data IDs of the datasets.
        register_output_dataset_types : `bool`, optional
            If `True`, register all output dataset types.

        Returns
        -------
        builder : \
                `..all_dimensions_quantum_graph_builder.AllDimensionsQuantumGraphBuilder`
            Quantum graph builder.  Note that
            ``attach_datastore_records=False`` must be passed to `build`, since
            the helper's butler does not have a datastore.
        """
        if insert_mocked_inputs:
            self.pipeline_graph.resolve(self.butler.registry)
            for _, dataset_type_node in self.pipeline_graph.iter_overall_inputs():
                assert dataset_type_node is not None, "pipeline graph is resolved."
                if self.butler.registry.registerDatasetType(dataset_type_node.dataset_type):
                    self.insert_datasets(dataset_type_node.dataset_type, register=False)
        builder = AllDimensionsQuantumGraphBuilder(
            self.pipeline_graph,
            self.butler,
            input_collections=[self.input_chain],
            output_run=output_run,
        )
        if register_output_dataset_types:
            self.pipeline_graph.register_dataset_types(self.butler)
        return builder

    def insert_datasets(
        self, dataset_type: DatasetType | str, register: bool = True, *args: Any, **kwargs: Any
    ) -> list[DatasetRef]:
        """Insert input datasets into the test repository.

        Parameters
        ----------
        dataset_type : `~lsst.daf.butler.DatasetType` or `str`
            Dataset type or name.  If a name, it must be included in the
            pipeline graph.
        register : `bool`, optional
            Whether to register the dataset type.  If `False`, the dataset type
            must already be registered.
        *args : `object`
            Forwarded to `~lsst.daf.butler.query_data_ids`.
        **kwargs : `object`
            Forwarded to `~lsst.daf.butler.query_data_ids`.

        Returns
        -------
        refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            References to the inserted datasets.

        Notes
        -----
        For dataset types with dimensions that are queryable, this queries for
        all data IDs in the repository (forwarding ``*args`` and ``**kwargs``
        for e.g. ``where`` strings).  For skypix dimensions, this queries for
        both patches and visit-detector regions (forwarding `*args`` and
        ``**kwargs`` to both) and uses all overlapping sky pixels.  Dataset
        types with a mix of skypix and queryable dimensions are not supported.
        """
        if isinstance(dataset_type, str):
            self.pipeline_graph.resolve(self.butler.registry)
            dataset_type = self.pipeline_graph.dataset_types[dataset_type].dataset_type
        if register:
            self.butler.registry.registerDatasetType(dataset_type)
        dimensions = dataset_type.dimensions
        if dataset_type.dimensions.skypix:
            if len(dimensions) == 1:
                (skypix_name,) = dimensions.skypix
                pixelization = dimensions.universe.skypix_dimensions[skypix_name].pixelization
                ranges = RangeSet()
                for patch_record in self.butler.query_dimension_records(
                    "patch", *args, **kwargs, explain=False
                ):
                    ranges |= pixelization.envelope(patch_record.region)
                for vdr_record in self.butler.query_dimension_records(
                    "visit_detector_region", *args, **kwargs, explain=False
                ):
                    ranges |= pixelization.envelope(vdr_record.region)
                data_ids = []
                for begin, end in ranges:
                    for index in range(begin, end):
                        data_ids.append(DataCoordinate.from_required_values(dimensions, (index,)))
            else:
                raise NotImplementedError(
                    "Can only generate data IDs for queryable dimensions and isolated skypix."
                )
        else:
            data_ids = self.butler.query_data_ids(dimensions, *args, **kwargs, explain=False)
        return self._insert_datasets_impl(dataset_type, data_ids)

    @abstractmethod
    def _insert_datasets_impl(
        self, dataset_type: DatasetType, data_ids: list[DataCoordinate]
    ) -> list[DatasetRef]:
        """Insert datasets after their data IDs have been generated.

        Parameters
        ----------
        dataset_type : `lsst.daf.butler.DatasetType`
            Type of the datasets.
        data_ids : `list` [ `lsst.daf.butler.DataCoordinate` ]
            Data IDs of all datasets.

        Returns
        -------
        refs : `list` [ `lsst.daf.butler.DatasetRef` ]
            References to the new datasets.
        """
        raise NotImplementedError()

    @abstractmethod
    def make_single_quantum_executor(
        self, qg: PredictedQuantumGraph
    ) -> tuple[SingleQuantumExecutor, LimitedButler]:
        """Make a single-quantum executor.

        Parameters
        ----------
        qg : `..quantum_graph.PredictedQuantumGraph`
            Graph whose quanta the executor must be capable of executing.

        Returns
        -------
        executor : `..single_quantum_executor.SingleQuantumExecutor`
            An executor for a single quantum.
        butler : `lsst.daf.butler.LimitedButler`
            The butler that the executor will write to.
        """
        raise NotImplementedError()


class InMemoryRepo(MockRepo):
    """A test helper that simulates a butler repository for task execution
    without any disk I/O.

    Parameters
    ----------
    *args : `str` or `lsst.resources.ResourcePath`
        Butler YAML import files to load into the test repository.
    registry_config : `lsst.daf.butler.RegistryConfig`, optional
        Registry configuration for the repository.
    dimension_config : `lsst.daf.butler.DimensionConfig`, optional
        Dimension universe configuration for the repository.
    input_run : `str`, optional
        Name of a `~lsst.daf.butler.CollectionType.RUN` collection that will be
        used as an input to quantum graph generation.  Input datasets created
        by the helper are added to this collection.
    input_chain : `str`, optional
        Name of a `~lsst.daf.butler.CollectionType.CHAINED` collection that
        will be the direct input to quantum graph generation.  This always
        includes ``input_run``.
    use_import_collections_as_input : `bool`, `str`, or \
            `~collections.abc.Iterable` [ `str`], optional
        Additional collections from YAML import files to include in
        ``input_chain``, or `True` to include all such collections (in
        chain-flattened lexicographical order).
    data_root : convertible to `lsst.resources.ResourcePath`, optional
        Root directory to join to each element in ``*args``.  Defaults to
        the `lsst.daf.butler.tests.registry_data` package.

    Notes
    -----
    This helper maintains an `..pipeline_graph.PipelineGraph` and a
    no-datastore butler backed by an in-memory SQLite database for use in
    quantum graph generation.  It creates a separate in-memory limited butler
    for execution as needed.
    """

    def __init__(
        self,
        *args: str | ResourcePath,
        registry_config: RegistryConfig | None = None,
        dimension_config: DimensionConfig | None = None,
        input_run: str = "input_run",
        input_chain: str = "input_chain",
        use_import_collections_as_input: bool | str | Iterable[str] = True,
        data_root: ResourcePathExpression | None = "resource://lsst.daf.butler/tests/registry_data",
    ):
        if data_root is not None:
            data_root = ResourcePath(data_root, forceDirectory=True)
            args = tuple(data_root.join(arg) for arg in args)
        butler = create_populated_sqlite_registry(
            *args, registry_config=registry_config, dimension_config=dimension_config
        )
        if use_import_collections_as_input:
            if use_import_collections_as_input is True:
                use_import_collections_as_input = sorted(butler.collections.query("*", flatten_chains=True))
        else:
            use_import_collections_as_input = ()
        super().__init__(
            butler,
            input_run=input_run,
            input_chain=input_chain,
            input_children=list(use_import_collections_as_input),
        )

    def _insert_datasets_impl(
        self, dataset_type: DatasetType, data_ids: list[DataCoordinate]
    ) -> list[DatasetRef]:
        return self.butler.registry.insertDatasets(dataset_type, data_ids, run=self.input_run)

    def make_limited_butler(self) -> InMemoryLimitedButler:
        """Make a test limited butler for execution.

        Returns
        -------
        limited_butler : `.InMemoryLimitedButler`
            A limited butler that can be used for task execution.

        Notes
        -----
        This queries the database-only butler used for quantum-graph generation
        for all datasets in the ``input_chain`` collection, and populates the
        limited butler with those that have a mock storage class.  Other
        datasets are ignored, so they will appear as though they were present
        during quantum graph generation but absent during execution.
        """
        butler = InMemoryLimitedButler(self.butler.dimensions, self.butler.registry.queryDatasetTypes())
        for ref in self.butler.query_all_datasets(self.input_chain):
            if is_mock_name(ref.datasetType.storageClass_name):
                butler.put(
                    MockDataset(
                        dataset_id=ref.id,
                        dataset_type=ref.datasetType.to_simple(),
                        data_id=dict(ref.dataId.mapping),
                        run=ref.run,
                    ),
                    ref,
                )
        return butler

    def make_single_quantum_executor(
        self, qg: PredictedQuantumGraph | None = None
    ) -> tuple[SingleQuantumExecutor, InMemoryLimitedButler]:
        """Make a single-quantum executor backed by a new limited butler.

        Parameters
        ----------
        qg : `..quantum_graph.PredictedQuantumGraph`
            Ignored by this implementation.

        Returns
        -------
        executor : `..single_quantum_executor.SingleQuantumExecutor`
            An executor for a single quantum.
        butler : `.InMemoryLimitedButler`
            The butler that the executor will write to.
        """
        butler = self.make_limited_butler()
        return SingleQuantumExecutor(limited_butler_factory=butler.factory), butler


class DirectButlerRepo(MockRepo):
    """A test helper for task execution backed by a local direct butler.

    Parameters
    ----------
    butler : `lsst.daf.butler.direct_butler.DirectButler`
        Butler to write to.
    *args : `str` or `lsst.resources.ResourcePath`
        Butler YAML import files to load into the test repository.
    input_run : `str`, optional
        Name of a `~lsst.daf.butler.CollectionType.RUN` collection that will be
        used as an input to quantum graph generation.  Input datasets created
        by the helper are added to this collection.
    input_chain : `str`, optional
        Name of a `~lsst.daf.butler.CollectionType.CHAINED` collection that
        will be the direct input to quantum graph generation.  This always
        includes ``input_run``.
    use_import_collections_as_input : `bool`, `str`, or \
            `~collections.abc.Iterable` [ `str`], optional
        Additional collections from YAML import files to include in
        ``input_chain``, or `True` to include all such collections (in
        chain-flattened lexicographical order).
    data_root : convertible to `lsst.resources.ResourcePath`, optional
        Root directory to join to each element in ``*args``.  Defaults to
        the `lsst.daf.butler.tests.registry_data` package.

    Notes
    -----
    This helper maintains an `..pipeline_graph.PipelineGraph` and a
    no-datastore butler backed by an in-memory SQLite database for use in
    quantum graph generation.  It creates a separate in-memory limited butler
    for execution as needed.
    """

    def __init__(
        self,
        butler: Butler,
        *args: str | ResourcePath,
        input_run: str = "input_run",
        input_chain: str = "input_chain",
        use_import_collections_as_input: bool | str | Iterable[str] = True,
        data_root: ResourcePathExpression | None = "resource://lsst.daf.butler/tests/registry_data",
    ):
        if data_root is not None:
            data_root = ResourcePath(data_root, forceDirectory=True)
            args = tuple(data_root.join(arg) for arg in args)
        for arg in args:
            butler.import_(filename=arg)
        if use_import_collections_as_input:
            if use_import_collections_as_input is True:
                use_import_collections_as_input = sorted(butler.collections.query("*", flatten_chains=True))
        else:
            use_import_collections_as_input = ()
        super().__init__(
            butler,
            input_run=input_run,
            input_chain=input_chain,
            input_children=list(use_import_collections_as_input),
        )

    @classmethod
    @contextmanager
    def make_temporary(
        cls,
        *args: str | ResourcePath,
        input_run: str = "input_run",
        input_chain: str = "input_chain",
        use_import_collections_as_input: bool | str | Iterable[str] = True,
        data_root: ResourcePathExpression | None = "resource://lsst.daf.butler/tests/registry_data",
        **kwargs: Any,
    ) -> Iterator[tuple[DirectButlerRepo, str]]:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as root:
            config = Butler.makeRepo(root, **kwargs)
            with Butler.from_config(config, writeable=True) as butler:
                yield (
                    cls(
                        butler,
                        *args,
                        input_run=input_run,
                        input_chain=input_chain,
                        use_import_collections_as_input=use_import_collections_as_input,
                        data_root=data_root,
                    ),
                    root,
                )

    def _insert_datasets_impl(
        self, dataset_type: DatasetType, data_ids: list[DataCoordinate]
    ) -> list[DatasetRef]:
        if is_mock_name(dataset_type.storageClass_name):
            refs: list[DatasetRef] = []
            for data_id in data_ids:
                data_id = self.butler.registry.expandDataId(data_id)
                ref = DatasetRef(dataset_type, data_id, run=self.input_run)
                self.butler.put(
                    MockDataset(
                        dataset_id=ref.id,
                        dataset_type=ref.datasetType.to_simple(),
                        data_id=dict(ref.dataId.mapping),
                        run=ref.run,
                    ),
                    ref,
                )
                refs.append(ref)
            return refs
        else:
            return self.butler.registry.insertDatasets(dataset_type, data_ids, run=self.input_run)

    def make_single_quantum_executor(
        self, qg: PredictedQuantumGraph | None = None
    ) -> tuple[SingleQuantumExecutor, Butler]:
        """Make a single-quantum executor backed by a new limited butler.

        Parameters
        ----------
        qg : `..quantum_graph.PredictedQuantumGraph`
            Ignored by this implementation.

        Returns
        -------
        executor : `..single_quantum_executor.SingleQuantumExecutor`
            An executor for a single quantum.
        butler : `lsst.daf.butler.Butler`
            The butler that the executor will write to.
        """
        return SingleQuantumExecutor(limited_butler_factory=None, butler=self.butler), self.butler
