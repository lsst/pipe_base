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

from lsst.pipe.base.connectionTypes import BaseInput, Output

__all__ = (
    "DynamicConnectionConfig",
    "DynamicTestPipelineTask",
    "DynamicTestPipelineTaskConfig",
    "ForcedFailure",
    "MockAlgorithmError",
    "MockPipelineTask",
    "MockPipelineTaskConfig",
    "mock_pipeline_graph",
)

import dataclasses
import logging
from collections.abc import Collection, Iterable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from astropy.units import Quantity

from lsst.daf.butler import DataCoordinate, DatasetRef, DeferredDatasetHandle, Quantum, SerializedDatasetType
from lsst.pex.config import Config, ConfigDictField, ConfigurableField, Field, ListField
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name
from lsst.utils.iteration import ensure_iterable

from ... import connectionTypes as cT
from ..._status import AlgorithmError, AnnotatedPartialOutputsError
from ...config import PipelineTaskConfig
from ...connections import InputQuantizedConnection, OutputQuantizedConnection, PipelineTaskConnections
from ...pipeline_graph import PipelineGraph
from ...pipelineTask import PipelineTask
from ._data_id_match import DataIdMatch
from ._storage_class import (
    ConvertedUnmockedDataset,
    MockDataset,
    MockDatasetQuantum,
    MockStorageClass,
    get_mock_name,
)

_LOG = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..._quantumContext import QuantumContext


_T = TypeVar("_T", bound=cT.BaseConnection)


@dataclasses.dataclass
class ForcedFailure:
    """Information about an exception that should be raised by one or more
    quanta.
    """

    condition: str
    """Butler expression-language string that matches the data IDs that should
    raise.
    """

    exception_type: type[BaseException] | None = None
    """The type of exception to raise."""

    memory_required: Quantity | None = None
    """If not `None`, this failure simulates an out-of-memory failure by
    raising only if this value exceeds `ExecutionResources.max_mem`.
    """

    def set_config(self, config: MockPipelineTaskConfig) -> None:
        config.fail_condition = self.condition
        if self.exception_type:
            config.fail_exception = get_full_type_name(self.exception_type)
        config.memory_required = self.memory_required


class MockAlgorithmError(AlgorithmError):
    """A subclass of `..AlgorithmError` chained to
    `..AnnotatedPartialOutputsError` when the latter is configured to be raised
    by `MockPipelineTask`.
    """

    @property
    def metadata(self) -> dict[str, int]:
        return {"badness": 12}


def mock_pipeline_graph(
    original_graph: PipelineGraph,
    unmocked_dataset_types: Iterable[str] = (),
    force_failures: Mapping[str, ForcedFailure] | None = None,
) -> PipelineGraph:
    """Create mocks for a full pipeline graph.

    Parameters
    ----------
    original_graph : `~..pipeline_graph.PipelineGraph`
        Original tasks and configuration to mock.
    unmocked_dataset_types : `~collections.abc.Iterable` [ `str` ], optional
        Names of overall-input dataset types that should not be replaced with
        mocks.  "Automatic" datasets written by the execution framework such
        as configs, logs, and metadata are implicitly included.
    force_failures : `~collections.abc.Mapping` [ `str`, `ForcedFailure` ]
        Mapping from original task label to information about an exception one
        or more quanta for this task should raise.

    Returns
    -------
    mocked : `~..pipeline_graph.PipelineGraph`
        Pipeline graph using `MockPipelineTask` configurations that target the
        original tasks.  Never resolved.
    """
    unmocked_dataset_types = list(unmocked_dataset_types)
    if force_failures is None:
        force_failures = {}
    result = PipelineGraph(description=original_graph.description)
    for task_node in original_graph.tasks.values():
        unmocked_dataset_types.append(task_node.init.config_output.dataset_type_name)
        if task_node.log_output is not None:
            unmocked_dataset_types.append(task_node.log_output.dataset_type_name)
        unmocked_dataset_types.append(task_node.metadata_output.dataset_type_name)
    for original_task_node in original_graph.tasks.values():
        config = MockPipelineTaskConfig()
        config.original.retarget(original_task_node.task_class)
        config.original = original_task_node.config
        config.unmocked_dataset_types.extend(unmocked_dataset_types)
        if original_task_node.label in force_failures:
            force_failures[original_task_node.label].set_config(config)
        result.add_task(get_mock_name(original_task_node.label), MockPipelineTask, config=config)
    return result


class BaseTestPipelineTaskConnections(PipelineTaskConnections, dimensions=()):
    pass


class BaseTestPipelineTaskConfig(PipelineTaskConfig, pipelineConnections=BaseTestPipelineTaskConnections):
    fail_condition = Field[str](
        dtype=str,
        default="",
        doc=(
            "Condition on Data ID to raise an exception. String expression which includes attributes of "
            "quantum data ID using a syntax of daf_butler user expressions (e.g. 'visit = 123')."
        ),
    )

    fail_exception = Field[str](
        dtype=str,
        default="builtins.ValueError",
        doc=(
            "Class name of the exception to raise when fail condition is triggered. Can be "
            "'lsst.pipe.base.NoWorkFound' to specify non-failure exception."
        ),
    )

    memory_required = Field[str](
        dtype=str,
        default=None,
        optional=True,
        doc=(
            "If not None, simulate an out-of-memory failure by raising only if ExecutionResource.max_mem "
            "exceeds this value.  This string should include units as parsed by astropy.units.Quantity "
            "(e.g. '4GB')."
        ),
    )

    def data_id_match(self) -> DataIdMatch | None:
        if not self.fail_condition:
            return None
        return DataIdMatch(self.fail_condition)


class BaseTestPipelineTask(PipelineTask):
    """A base class for test-utility `PipelineTask` classes that read and write
    mock datasets `runQuantum`.

    Parameters
    ----------
    config : `PipelineTaskConfig`
        The pipeline task config.
    initInputs : `~collections.abc.Mapping`
        The init inputs datasets.
    **kwargs : `~typing.Any`
        Keyword parameters passed to base class constructor.

    Notes
    -----
    This class overrides `runQuantum` to read inputs and write a bit of
    provenance into all of its outputs (always `MockDataset` instances).  It
    can also be configured to raise exceptions on certain data IDs.  It reads
    `MockDataset` inputs and simulates reading inputs of other types by
    creating `MockDataset` inputs from their DatasetRefs.

    Subclasses are responsible for defining connections, but init-input and
    init-output connections are not supported at runtime (they may be present
    as long as the task is never constructed).  All output connections must
    use mock storage classes.  `..Input` and `..PrerequisiteInput` connections
    that do not use mock storage classes will be handled by constructing a
    `MockDataset` from the `~lsst.daf.butler.DatasetRef` rather than actually
    reading them.
    """

    ConfigClass: ClassVar[type[PipelineTaskConfig]] = BaseTestPipelineTaskConfig

    def __init__(
        self,
        *,
        config: BaseTestPipelineTaskConfig,
        initInputs: Mapping[str, Any],
        **kwargs: Any,
    ):
        super().__init__(config=config, **kwargs)
        self.fail_exception: type | None = None
        self.data_id_match = self.config.data_id_match()
        if self.data_id_match:
            self.fail_exception = doImportType(self.config.fail_exception)
            self.memory_required = (
                Quantity(self.config.memory_required) if self.config.memory_required is not None else None
            )
        # Look for, check, and record init-inputs.
        task_connections = self.ConfigClass.ConnectionsClass(config=config)
        mock_dataset_quantum = MockDatasetQuantum(task_label=self.getName(), data_id={}, inputs={})
        for connection_name in task_connections.initInputs:
            input_dataset = initInputs[connection_name]
            if not isinstance(input_dataset, MockDataset):
                raise TypeError(
                    f"Expected MockDataset instance for init-input {self.getName()}.{connection_name}: "
                    f"got {input_dataset!r} of type {type(input_dataset)!r}."
                )
            connection = task_connections.allConnections[connection_name]
            if input_dataset.dataset_type.name != connection.name:
                raise RuntimeError(
                    f"Incorrect dataset type name for init-input {self.getName()}.{connection_name}: "
                    f"got {input_dataset.dataset_type.name!r}, expected {connection.name!r}."
                )
            if input_dataset.storage_class != connection.storageClass:
                raise RuntimeError(
                    f"Incorrect storage class for init-input {self.getName()}.{connection_name}: "
                    f"got {input_dataset.storage_class!r}, expected {connection.storageClass!r}."
                )
            # To avoid very deep provenance we trim inputs to a single
            # level.
            input_dataset.quantum = None
            mock_dataset_quantum.inputs[connection_name] = [input_dataset]
        # Add init-outputs as task instance attributes.
        for connection_name in task_connections.initOutputs:
            connection = task_connections.allConnections[connection_name]
            output_dataset = MockDataset(
                dataset_id=None,  # the task has no way to get this
                dataset_type=SerializedDatasetType(
                    name=connection.name,
                    storageClass=connection.storageClass,
                    dimensions=[],
                ),
                data_id={},
                run=None,  # task also has no way to get this
                quantum=mock_dataset_quantum,
                output_connection_name=connection_name,
            )
            setattr(self, connection_name, output_dataset)

    config: BaseTestPipelineTaskConfig

    def runQuantum(
        self,
        butlerQC: QuantumContext,
        inputRefs: InputQuantizedConnection,
        outputRefs: OutputQuantizedConnection,
    ) -> None:
        # docstring is inherited from the base class
        quantum = butlerQC.quantum

        _LOG.info("Mocking execution of task '%s' on quantum %s", self.getName(), quantum.dataId)

        assert quantum.dataId is not None, "Quantum DataId cannot be None"

        # Possibly raise an exception.
        if self.data_id_match is not None and self.data_id_match.match(quantum.dataId):
            assert self.fail_exception is not None, "Exception type must be defined"

            if self.memory_required is not None:
                if butlerQC.resources.max_mem < self.memory_required:
                    _LOG.info(
                        "Simulating out-of-memory failure for task '%s' on quantum %s",
                        self.getName(),
                        quantum.dataId,
                    )
                    self._fail(quantum)
            else:
                _LOG.info("Simulating failure of task '%s' on quantum %s", self.getName(), quantum.dataId)
                self._fail(quantum)

        # Populate the bit of provenance we store in all outputs.
        _LOG.info("Reading input data for task '%s' on quantum %s", self.getName(), quantum.dataId)
        mock_dataset_quantum = MockDatasetQuantum(
            task_label=self.getName(), data_id=dict(quantum.dataId.mapping), inputs={}
        )
        for name, refs in inputRefs:
            inputs_list = []
            ref: DatasetRef
            for ref in ensure_iterable(refs):
                if isinstance(ref.datasetType.storageClass, MockStorageClass):
                    input_dataset = butlerQC.get(ref)
                    if isinstance(input_dataset, DeferredDatasetHandle):
                        input_dataset = input_dataset.get()
                    if isinstance(input_dataset, MockDataset):
                        # To avoid very deep provenance we trim inputs to a
                        # single level.
                        input_dataset.quantum = None
                    elif not isinstance(input_dataset, ConvertedUnmockedDataset):
                        raise TypeError(
                            f"Expected MockDataset or ConvertedUnmockedDataset instance for {ref}; "
                            f"got {input_dataset!r} of type {type(input_dataset)!r}."
                        )
                else:
                    input_dataset = MockDataset(
                        dataset_id=ref.id,
                        dataset_type=ref.datasetType.to_simple(),
                        data_id=dict(ref.dataId.mapping),
                        run=ref.run,
                    )
                inputs_list.append(input_dataset)
            mock_dataset_quantum.inputs[name] = inputs_list

        # store mock outputs
        for name, refs in outputRefs:
            for ref in ensure_iterable(refs):
                output = MockDataset(
                    dataset_id=ref.id,
                    dataset_type=ref.datasetType.to_simple(),
                    data_id=dict(ref.dataId.mapping),
                    run=ref.run,
                    quantum=mock_dataset_quantum,
                    output_connection_name=name,
                )
                butlerQC.put(output, ref)

        _LOG.info("Finished mocking task '%s' on quantum %s", self.getName(), quantum.dataId)

    def _fail(self, quantum: Quantum) -> None:
        """Raise the configured exception.

        Parameters
        ----------
        quantum : `lsst.daf.butler.Quantum`
            Quantum producing the error.
        """
        message = f"Simulated failure: task={self.getName()} dataId={quantum.dataId}"
        if self.fail_exception is AnnotatedPartialOutputsError:
            # This exception is expected to always chain another.
            try:
                raise MockAlgorithmError(message)
            except AlgorithmError as original:
                error = AnnotatedPartialOutputsError.annotate(original, self, log=self.log)
                raise error from original
        else:
            assert self.fail_exception is not None, "Method should not be called."
            raise self.fail_exception(message)


class MockPipelineDefaultTargetConnections(PipelineTaskConnections, dimensions=()):
    pass


class MockPipelineDefaultTargetConfig(
    PipelineTaskConfig, pipelineConnections=MockPipelineDefaultTargetConnections
):
    pass


class MockPipelineDefaultTargetTask(PipelineTask):
    """A `~lsst.pipe.base.PipelineTask` class used as the default target for
    ``MockPipelineTaskConfig.original``.

    This is effectively a workaround for `lsst.pex.config.ConfigurableField`
    not supporting ``optional=True``, but that is generally a reasonable
    limitation for production code and it wouldn't make sense just to support
    test utilities.
    """

    ConfigClass = MockPipelineDefaultTargetConfig


class MockPipelineTaskConnections(BaseTestPipelineTaskConnections, dimensions=()):
    """A connections class that creates mock connections from the connections
    of a real PipelineTask.

    Parameters
    ----------
    config : `PipelineTaskConfig`
        The config to use for the connection.
    """

    def __init__(self, *, config: MockPipelineTaskConfig):
        self.original: PipelineTaskConnections = config.original.connections.ConnectionsClass(
            config=config.original.value
        )
        self.dimensions.update(self.original.dimensions)
        self.unmocked_dataset_types = frozenset(config.unmocked_dataset_types)
        for name, connection in self.original.allConnections.items():
            if connection.name not in self.unmocked_dataset_types:
                # We register the mock storage class with the global
                # singleton here, but can only put its name in the
                # connection. That means the same global singleton (or one
                # that also has these registrations) has to be available
                # whenever this dataset type is used.
                storage_class_name = MockStorageClass.get_or_register_mock(connection.storageClass).name
                kwargs: dict[str, Any] = {}
                if hasattr(connection, "dimensions"):
                    connection_dimensions = set(connection.dimensions)
                    # Replace the generic "skypix" placeholder with htm7, since
                    # that requires the dataset type to have already been
                    # registered.
                    if "skypix" in connection_dimensions:
                        connection_dimensions.remove("skypix")
                        connection_dimensions.add("htm7")
                    kwargs["dimensions"] = connection_dimensions
                connection = dataclasses.replace(
                    connection,
                    name=get_mock_name(connection.name),
                    storageClass=storage_class_name,
                    **kwargs,
                )
            elif name in self.original.outputs:
                raise ValueError(f"Unmocked dataset type {connection.name!r} cannot be used as an output.")
            elif name in self.original.initInputs:
                raise ValueError(
                    f"Unmocked dataset type {connection.name!r} cannot be used as an init-input."
                )
            elif name in self.original.initOutputs:
                raise ValueError(
                    f"Unmocked dataset type {connection.name!r} cannot be used as an init-output."
                )
            elif connection.name.endswith("_metadata") and connection.storageClass == "TaskMetadata":
                # Task metadata does not use a mock storage class, because it's
                # written by the system, but it does end up with the _mock_*
                # prefix because the task label does.
                connection = dataclasses.replace(connection, name=get_mock_name(connection.name))
            setattr(self, name, connection)

    def getSpatialBoundsConnections(self) -> Iterable[str]:
        return self.original.getSpatialBoundsConnections()

    def getTemporalBoundsConnections(self) -> Iterable[str]:
        return self.original.getTemporalBoundsConnections()

    def adjustQuantum(
        self,
        inputs: dict[str, tuple[BaseInput, Collection[DatasetRef]]],
        outputs: dict[str, tuple[Output, Collection[DatasetRef]]],
        label: str,
        data_id: DataCoordinate,
    ) -> tuple[
        Mapping[str, tuple[BaseInput, Collection[DatasetRef]]],
        Mapping[str, tuple[Output, Collection[DatasetRef]]],
    ]:
        # Convert the given mappings from the mock dataset types to the
        # original dataset types they were produced from.
        original_inputs = {}
        for connection_name, (_, mock_refs) in inputs.items():
            original_connection = getattr(self.original, connection_name)
            if original_connection.name in self.unmocked_dataset_types:
                refs = mock_refs
            else:
                refs = MockStorageClass.unmock_dataset_refs(mock_refs)
            original_inputs[connection_name] = (original_connection, refs)
        original_outputs = {}
        for connection_name, (_, mock_refs) in outputs.items():
            original_connection = getattr(self.original, connection_name)
            if original_connection.name in self.unmocked_dataset_types:
                refs = mock_refs
            else:
                refs = MockStorageClass.unmock_dataset_refs(mock_refs)
            original_outputs[connection_name] = (original_connection, refs)
        # Call adjustQuantum on the original connections class.
        adjusted_original_inputs, adjusted_original_outputs = self.original.adjustQuantum(
            original_inputs, original_outputs, label, data_id
        )
        # Convert the results back to the mock dataset type.s
        adjusted_inputs = {}
        for connection_name, (original_connection, original_refs) in adjusted_original_inputs.items():
            if original_connection.name in self.unmocked_dataset_types:
                refs = original_refs
            else:
                refs = MockStorageClass.mock_dataset_refs(original_refs)
            adjusted_inputs[connection_name] = (getattr(self, connection_name), refs)
        adjusted_outputs = {}
        for connection_name, (original_connection, original_refs) in adjusted_original_outputs.items():
            if original_connection.name in self.unmocked_dataset_types:
                refs = original_refs
            else:
                refs = MockStorageClass.mock_dataset_refs(original_refs)
            adjusted_outputs[connection_name] = (getattr(self, connection_name), refs)
        return adjusted_inputs, adjusted_outputs


class MockPipelineTaskConfig(BaseTestPipelineTaskConfig, pipelineConnections=MockPipelineTaskConnections):
    """Configuration class for `MockPipelineTask`."""

    original: ConfigurableField = ConfigurableField(
        doc="The original task being mocked by this one.", target=MockPipelineDefaultTargetTask
    )

    unmocked_dataset_types = ListField[str](
        doc=(
            "Names of input dataset types that should be used as-is instead "
            "of being mocked.  May include dataset types not relevant for "
            "this task, which will be ignored."
        ),
        default=(),
        optional=False,
    )


class MockPipelineTask(BaseTestPipelineTask):
    """A test-utility implementation of `PipelineTask` with connections
    generated by mocking those of a real task.

    Notes
    -----
    At present `MockPipelineTask` simply drops any ``initInput`` and
    ``initOutput`` connections present on the original, since `MockDataset`
    creation for those would have to happen in the code that executes the task,
    not in the task itself.  Because `MockPipelineTask` never instantiates the
    mock task (just its connections class), this is a limitation on what the
    mocks can be used to test, not anything deeper.
    """

    ConfigClass: ClassVar[type[PipelineTaskConfig]] = MockPipelineTaskConfig


class DynamicConnectionConfig(Config):
    """A config class that defines a completely dynamic connection."""

    dataset_type_name = Field[str](doc="Name for the dataset type as seen by the butler.", dtype=str)
    dimensions = ListField[str](doc="Dimensions for the dataset type.", dtype=str, default=[])
    storage_class = Field[str](
        doc="Name of the butler storage class for the dataset type.", dtype=str, default="StructuredDataDict"
    )
    is_calibration = Field[bool](doc="Whether this dataset type is a calibration.", dtype=bool, default=False)
    multiple = Field[bool](
        doc="Whether this connection gets or puts multiple datasets for each quantum.",
        dtype=bool,
        default=False,
    )
    mock_storage_class = Field[bool](
        doc="Whether the storage class should actually be a mock of the storage class given.",
        dtype=bool,
        default=True,
    )
    minimum = Field[int](
        doc="Minimum number of datasets per quantum required for this connection.  Ignored for non-inputs.",
        dtype=int,
        default=1,
    )

    def make_connection(self, cls: type[_T]) -> _T:
        storage_class = self.storage_class
        if self.mock_storage_class:
            storage_class = MockStorageClass.get_or_register_mock(storage_class).name
        if issubclass(cls, cT.BaseInput):
            return cls(  # type: ignore
                name=self.dataset_type_name,
                storageClass=storage_class,
                isCalibration=self.is_calibration,
                multiple=self.multiple,
                dimensions=frozenset(self.dimensions),
                minimum=self.minimum,
            )
        elif issubclass(cls, cT.DimensionedConnection):
            return cls(  # type: ignore
                name=self.dataset_type_name,
                storageClass=storage_class,
                isCalibration=self.is_calibration,
                multiple=self.multiple,
                dimensions=frozenset(self.dimensions),
            )
        else:
            return cls(
                name=self.dataset_type_name,
                storageClass=storage_class,
                multiple=self.multiple,
            )


class DynamicTestPipelineTaskConnections(PipelineTaskConnections, dimensions=()):
    """A connections class whose dimensions and connections are wholly
    determined via configuration.

    Parameters
    ----------
    config : `PipelineTaskConfig`
        Config to use for this connections object.
    """

    def __init__(self, *, config: DynamicTestPipelineTaskConfig):
        self.dimensions.update(config.dimensions)
        connection_config: DynamicConnectionConfig
        for connection_name, connection_config in config.init_inputs.items():
            setattr(self, connection_name, connection_config.make_connection(cT.InitInput))
        for connection_name, connection_config in config.init_outputs.items():
            setattr(self, connection_name, connection_config.make_connection(cT.InitOutput))
        for connection_name, connection_config in config.prerequisite_inputs.items():
            setattr(self, connection_name, connection_config.make_connection(cT.PrerequisiteInput))
        for connection_name, connection_config in config.inputs.items():
            setattr(self, connection_name, connection_config.make_connection(cT.Input))
        for connection_name, connection_config in config.outputs.items():
            setattr(self, connection_name, connection_config.make_connection(cT.Output))


class DynamicTestPipelineTaskConfig(
    BaseTestPipelineTaskConfig, pipelineConnections=DynamicTestPipelineTaskConnections
):
    """Configuration for DynamicTestPipelineTask."""

    dimensions = ListField[str](doc="Dimensions for the task's quanta.", dtype=str, default=[])
    init_inputs = ConfigDictField(
        doc=(
            "Init-input connections, keyed by the connection name as seen by the task. "
            "Must be empty if the task will be constructed."
        ),
        keytype=str,
        itemtype=DynamicConnectionConfig,
        default={},
    )
    init_outputs = ConfigDictField(
        doc=(
            "Init-output connections, keyed by the connection name as seen by the task. "
            "Must be empty if the task will be constructed."
        ),
        keytype=str,
        itemtype=DynamicConnectionConfig,
        default={},
    )
    prerequisite_inputs = ConfigDictField(
        doc="Prerequisite input connections, keyed by the connection name as seen by the task.",
        keytype=str,
        itemtype=DynamicConnectionConfig,
        default={},
    )
    inputs = ConfigDictField(
        doc="Regular input connections, keyed by the connection name as seen by the task.",
        keytype=str,
        itemtype=DynamicConnectionConfig,
        default={},
    )
    outputs = ConfigDictField(
        doc="Regular output connections, keyed by the connection name as seen by the task.",
        keytype=str,
        itemtype=DynamicConnectionConfig,
        default={},
    )


class DynamicTestPipelineTask(BaseTestPipelineTask):
    """A test-utility implementation of `PipelineTask` with dimensions and
    connections determined wholly from configuration.
    """

    ConfigClass: ClassVar[type[PipelineTaskConfig]] = DynamicTestPipelineTaskConfig
