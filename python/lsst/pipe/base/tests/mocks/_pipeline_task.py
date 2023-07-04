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

__all__ = ("MockPipelineTask", "MockPipelineTaskConfig", "mock_task_defs")

import dataclasses
import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from lsst.daf.butler import DeferredDatasetHandle
from lsst.pex.config import ConfigurableField, Field, ListField
from lsst.utils.doImport import doImportType
from lsst.utils.introspection import get_full_type_name
from lsst.utils.iteration import ensure_iterable

from ...config import PipelineTaskConfig
from ...connections import InputQuantizedConnection, OutputQuantizedConnection, PipelineTaskConnections
from ...pipeline import TaskDef
from ...pipelineTask import PipelineTask
from ._data_id_match import DataIdMatch
from ._storage_class import MockDataset, MockDatasetQuantum, MockStorageClass, get_mock_name

_LOG = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..._quantumContext import QuantumContext


def mock_task_defs(
    originals: Iterable[TaskDef],
    unmocked_dataset_types: Iterable[str] = (),
    force_failures: Mapping[str, tuple[str, type[Exception] | None]] | None = None,
) -> list[TaskDef]:
    """Create mocks for an iterable of TaskDefs.

    Parameters
    ----------
    originals : `~collections.abc.Iterable` [ `TaskDef` ]
        Original tasks and configuration to mock.
    unmocked_dataset_types : `~collections.abc.Iterable` [ `str` ], optional
        Names of overall-input dataset types that should not be replaced with
        mocks.
    force_failures : `~collections.abc.Mapping` [ `str`, `tuple` [ `str`, \
            `type` [ `Exception` ] or `None` ] ]
        Mapping from original task label to a 2-tuple indicating that some
        quanta should raise an exception when executed.  The first entry is a
        data ID match using the butler expression language (i.e. a string of
        the sort passed ass the ``where`` argument to butler query methods),
        while the second is the type of exception to raise when the quantum
        data ID matches the expression.

    Returns
    -------
    mocked : `list` [ `TaskDef` ]
        List of `TaskDef` objects using `MockPipelineTask` configurations that
        target the original tasks, in the same order.
    """
    unmocked_dataset_types = tuple(unmocked_dataset_types)
    if force_failures is None:
        force_failures = {}
    results: list[TaskDef] = []
    for original_task_def in originals:
        config = MockPipelineTaskConfig()
        config.original.retarget(original_task_def.taskClass)
        config.original = original_task_def.config
        config.unmocked_dataset_types.extend(unmocked_dataset_types)
        if original_task_def.label in force_failures:
            condition, exception_type = force_failures[original_task_def.label]
            config.fail_condition = condition
            if exception_type is not None:
                config.fail_exception = get_full_type_name(exception_type)
        mock_task_def = TaskDef(
            config=config, taskClass=MockPipelineTask, label=get_mock_name(original_task_def.label)
        )
        results.append(mock_task_def)
    return results


class MockPipelineDefaultTargetConnections(PipelineTaskConnections, dimensions=()):
    pass


class MockPipelineDefaultTargetConfig(
    PipelineTaskConfig, pipelineConnections=MockPipelineDefaultTargetConnections
):
    pass


class MockPipelineDefaultTargetTask(PipelineTask):
    """A `PipelineTask` class used as the default target for
    ``MockPipelineTaskConfig.original``.

    This is effectively a workaround for `lsst.pex.config.ConfigurableField`
    not supporting ``optional=True``, but that is generally a reasonable
    limitation for production code and it wouldn't make sense just to support
    test utilities.
    """

    ConfigClass = MockPipelineDefaultTargetConfig


class MockPipelineTaskConnections(PipelineTaskConnections, dimensions=()):
    def __init__(self, *, config: MockPipelineTaskConfig):
        original: PipelineTaskConnections = config.original.connections.ConnectionsClass(
            config=config.original.value
        )
        self.dimensions.update(original.dimensions)
        unmocked_dataset_types = frozenset(config.unmocked_dataset_types)
        for name, connection in original.allConnections.items():
            if name in original.initInputs or name in original.initOutputs:
                # We just ignore initInputs and initOutputs, because the task
                # is never given DatasetRefs for those and hence can't create
                # mocks.
                continue
            if connection.name not in unmocked_dataset_types:
                # We register the mock storage class with the global singleton
                # here, but can only put its name in the connection. That means
                # the same global singleton (or one that also has these
                # registrations) has to be available whenever this dataset type
                # is used.
                storage_class = MockStorageClass.get_or_register_mock(connection.storageClass)
                kwargs = {}
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
                    storageClass=storage_class.name,
                    **kwargs,
                )
            elif name in original.outputs:
                raise ValueError(f"Unmocked dataset type {connection.name!r} cannot be used as an output.")
            setattr(self, name, connection)


class MockPipelineTaskConfig(PipelineTaskConfig, pipelineConnections=MockPipelineTaskConnections):
    """Configuration class for `MockPipelineTask`."""

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

    def data_id_match(self) -> DataIdMatch | None:
        if not self.fail_condition:
            return None
        return DataIdMatch(self.fail_condition)


class MockPipelineTask(PipelineTask):
    """Implementation of `PipelineTask` used for running a mock pipeline.

    Notes
    -----
    This class overrides `runQuantum` to read inputs and write a bit of
    provenance into all of its outputs (always `MockDataset` instances).  It
    can also be configured to raise exceptions on certain data IDs.  It reads
    `MockDataset` inputs and simulates reading inputs of other types by
    creating `MockDataset` inputs from their DatasetRefs.

    At present `MockPipelineTask` simply drops any ``initInput`` and
    ``initOutput`` connections present on the original, since `MockDataset`
    creation for those would have to happen in the code that executes the task,
    not in the task itself.  Because `MockPipelineTask` never instantiates the
    mock task (just its connections class), this is a limitation on what the
    mocks can be used to test, not anything deeper.
    """

    ConfigClass: ClassVar[type[PipelineTaskConfig]] = MockPipelineTaskConfig

    def __init__(
        self,
        *,
        config: MockPipelineTaskConfig,
        **kwargs: Any,
    ):
        super().__init__(config=config, **kwargs)
        self.fail_exception: type | None = None
        self.data_id_match = self.config.data_id_match()
        if self.data_id_match:
            self.fail_exception = doImportType(self.config.fail_exception)

    config: MockPipelineTaskConfig

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
            _LOG.info("Simulating failure of task '%s' on quantum %s", self.getName(), quantum.dataId)
            message = f"Simulated failure: task={self.getName()} dataId={quantum.dataId}"
            assert self.fail_exception is not None, "Exception type must be defined"
            raise self.fail_exception(message)

        # Populate the bit of provenance we store in all outputs.
        _LOG.info("Reading input data for task '%s' on quantum %s", self.getName(), quantum.dataId)
        mock_dataset_quantum = MockDatasetQuantum(
            task_label=self.getName(), data_id=quantum.dataId.to_simple(), inputs={}
        )
        for name, refs in inputRefs:
            inputs_list = []
            for ref in ensure_iterable(refs):
                if isinstance(ref.datasetType.storageClass, MockStorageClass):
                    input_dataset = butlerQC.get(ref)
                    if isinstance(input_dataset, DeferredDatasetHandle):
                        input_dataset = input_dataset.get()
                    if not isinstance(input_dataset, MockDataset):
                        raise TypeError(
                            f"Expected MockDataset instance for {ref}; "
                            f"got {input_dataset!r} of type {type(input_dataset)!r}."
                        )
                    # To avoid very deep provenance we trim inputs to a single
                    # level.
                    input_dataset.quantum = None
                else:
                    input_dataset = MockDataset(ref=ref.to_simple())
                inputs_list.append(input_dataset)
            mock_dataset_quantum.inputs[name] = inputs_list

        # store mock outputs
        for name, refs in outputRefs:
            for ref in ensure_iterable(refs):
                output = MockDataset(
                    ref=ref.to_simple(), quantum=mock_dataset_quantum, output_connection_name=name
                )
                butlerQC.put(output, ref)

        _LOG.info("Finished mocking task '%s' on quantum %s", self.getName(), quantum.dataId)
