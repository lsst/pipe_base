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

__all__ = ["SingleQuantumExecutor"]

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Callable, Mapping
from itertools import chain
from typing import Any, cast

from lsst.daf.butler import (
    Butler,
    ButlerMetrics,
    CollectionType,
    DatasetRef,
    DatasetType,
    LimitedButler,
    NamedKeyDict,
    Quantum,
)
from lsst.daf.butler.registry.wildcards import CollectionWildcard
from lsst.pipe.base import (
    AdjustQuantumHelper,
    AnnotatedPartialOutputsError,
    ExecutionResources,
    Instrument,
    InvalidQuantumError,
    NoWorkFound,
    PipelineTask,
    QuantumContext,
    QuantumSuccessCaveats,
    TaskFactory,
)
from lsst.pipe.base.pipeline_graph import TaskNode

# During metadata transition phase, determine metadata class by
# asking pipe_base
from lsst.pipe.base.task import _TASK_FULL_METADATA_TYPE, _TASK_METADATA_TYPE
from lsst.utils.timer import logInfo

# -----------------------------
#  Imports for other modules --
# -----------------------------
from .log_capture import LogCapture
from .quantumGraphExecutor import QuantumExecutor
from .reports import QuantumReport

# ----------------------------------
#  Local non-exported definitions --
# ----------------------------------

_LOG = logging.getLogger(__name__)


class SingleQuantumExecutor(QuantumExecutor):
    """Executor class which runs one Quantum at a time.

    Parameters
    ----------
    butler : `~lsst.daf.butler.Butler` or `None`
        Data butler, `None` means that Quantum-backed butler should be used
        instead.
    taskFactory : `~lsst.pipe.base.TaskFactory`
        Instance of a task factory.
    skipExistingIn : `~typing.Any`
        Expressions representing the collections to search for existing output
        datasets. See :ref:`daf_butler_ordered_collection_searches` for allowed
        types. This class only checks for the presence of butler output run in
        the list of collections. If the output run is present in the list then
        the quanta whose complete outputs exist in the output run will be
        skipped. `None` or empty string/sequence disables skipping.
    clobberOutputs : `bool`, optional
        If `True`, then outputs from a quantum that exist in output run
        collection will be removed prior to executing a quantum. If
        ``skipExistingIn`` contains output run, then only partial outputs from
        a quantum will be removed. Only used when ``butler`` is not `None`.
    enableLsstDebug : `bool`, optional
        Enable debugging with ``lsstDebug`` facility for a task.
    limited_butler_factory : `Callable`, optional
        A method that creates a `~lsst.daf.butler.LimitedButler` instance for a
        given Quantum. This parameter must be defined if ``butler`` is `None`.
        If ``butler`` is not `None` then this parameter is ignored.
    resources : `~lsst.pipe.base.ExecutionResources`, optional
        The resources available to this quantum when executing.
    skipExisting : `bool`, optional
        If `True`, skip quanta whose metadata datasets are already stored.
        Unlike ``skipExistingIn``, this works with limited butlers as well as
        full butlers.  Always set to `True` if ``skipExistingIn`` matches
        ``butler.run``.
    assumeNoExistingOutputs : `bool`, optional
        If `True`, assume preexisting outputs are impossible (e.g. because this
        is known by higher-level code to be a new ``RUN`` collection), and do
        not look for them.  This causes the ``skipExisting`` and
        ``clobberOutputs`` options to be ignored, but unlike just setting both
        of those to `False`, it also avoids all dataset existence checks.
    raise_on_partial_outputs : `bool`, optional
        If `True` raise exceptions chained by
        `lsst.pipe.base.AnnotatedPartialOutputError` immediately, instead of
        considering the partial result a success and continuing to run
        downstream tasks.
    job_metadata : `~collections.abc.Mapping`
        Mapping with extra metadata to embed within the quantum metadata under
        the "job" key.  This is intended to correspond to information common
        to all quanta being executed in a single process, such as the time
        taken to load the quantum graph in a BPS job.
    """

    def __init__(
        self,
        butler: Butler | None,
        taskFactory: TaskFactory,
        skipExistingIn: Any = None,
        clobberOutputs: bool = False,
        enableLsstDebug: bool = False,
        limited_butler_factory: Callable[[Quantum], LimitedButler] | None = None,
        resources: ExecutionResources | None = None,
        skipExisting: bool = False,
        assumeNoExistingOutputs: bool = False,
        raise_on_partial_outputs: bool = True,
        job_metadata: Mapping[str, int | str | float] | None = None,
    ):
        self.butler = butler
        self.taskFactory = taskFactory
        self.enableLsstDebug = enableLsstDebug
        self.clobberOutputs = clobberOutputs
        self.limited_butler_factory = limited_butler_factory
        self.resources = resources
        self.assumeNoExistingOutputs = assumeNoExistingOutputs
        self.raise_on_partial_outputs = raise_on_partial_outputs
        self.job_metadata = job_metadata

        if self.butler is None:
            assert limited_butler_factory is not None, "limited_butler_factory is needed when butler is None"

        # Find whether output run is in skipExistingIn.
        # TODO: This duplicates logic in GraphBuilder, would be nice to have
        # better abstraction for this some day.
        self.skipExisting = skipExisting
        if self.butler is not None and skipExistingIn:
            skip_collections_wildcard = CollectionWildcard.from_expression(skipExistingIn)
            # As optimization check in the explicit list of names first
            self.skipExisting = self.butler.run in skip_collections_wildcard.strings
            if not self.skipExisting:
                # need to flatten it and check again
                self.skipExisting = self.butler.run in self.butler.registry.queryCollections(
                    skipExistingIn,
                    collectionTypes=CollectionType.RUN,
                )

    def execute(
        self, task_node: TaskNode, /, quantum: Quantum, quantum_id: uuid.UUID | None = None
    ) -> tuple[Quantum, QuantumReport | None]:
        # Docstring inherited from QuantumExecutor.execute
        assert quantum.dataId is not None, "Quantum DataId cannot be None"

        if self.butler is not None:
            self.butler.registry.refresh()

        result = self._execute(task_node, quantum, quantum_id=quantum_id)
        report = QuantumReport(dataId=quantum.dataId, taskLabel=task_node.label)
        return result, report

    def _execute(
        self, task_node: TaskNode, /, quantum: Quantum, quantum_id: uuid.UUID | None = None
    ) -> Quantum:
        """Execute the quantum.

        Internal implementation of `execute()`.
        """
        startTime = time.time()

        # Make a limited butler instance if needed (which should be QBB if full
        # butler is not defined).
        limited_butler: LimitedButler
        if self.butler is not None:
            limited_butler = self.butler
        else:
            # We check this in constructor, but mypy needs this check here.
            assert self.limited_butler_factory is not None
            limited_butler = self.limited_butler_factory(quantum)

        if self.butler is not None:
            log_capture = LogCapture.from_full(self.butler)
        else:
            log_capture = LogCapture.from_limited(limited_butler)
        with log_capture.capture_logging(task_node, quantum) as captureLog:
            # Save detailed resource usage before task start to metadata.
            quantumMetadata = _TASK_METADATA_TYPE()
            logInfo(None, "prep", metadata=quantumMetadata)  # type: ignore[arg-type]

            _LOG.info(
                "Preparing execution of quantum for label=%s dataId=%s.", task_node.label, quantum.dataId
            )

            # check whether to skip or delete old outputs, if it returns True
            # or raises an exception do not try to store logs, as they may be
            # already in butler.
            captureLog.store = False
            if self.checkExistingOutputs(quantum, task_node, limited_butler):
                _LOG.info(
                    "Skipping already-successful quantum for label=%s dataId=%s.",
                    task_node.label,
                    quantum.dataId,
                )
                return quantum
            captureLog.store = True

            try:
                quantum = self.updatedQuantumInputs(quantum, task_node, limited_butler)
            except NoWorkFound as exc:
                _LOG.info(
                    "Nothing to do for task '%s' on quantum %s; saving metadata and skipping: %s",
                    task_node.label,
                    quantum.dataId,
                    str(exc),
                )
                quantumMetadata["caveats"] = QuantumSuccessCaveats.from_adjust_quantum_no_work().value
                # Make empty metadata that looks something like what a
                # do-nothing task would write (but we don't bother with empty
                # nested PropertySets for subtasks).  This is slightly
                # duplicative with logic in pipe_base that we can't easily call
                # from here; we'll fix this on DM-29761.
                logInfo(None, "end", metadata=quantumMetadata)  # type: ignore[arg-type]
                fullMetadata = _TASK_FULL_METADATA_TYPE()
                fullMetadata[task_node.label] = _TASK_METADATA_TYPE()
                fullMetadata["quantum"] = quantumMetadata
                if self.job_metadata is not None:
                    fullMetadata["job"] = self.job_metadata
                self.writeMetadata(quantum, fullMetadata, task_node, limited_butler)
                return quantum

            # enable lsstDebug debugging
            if self.enableLsstDebug:
                try:
                    _LOG.debug("Will try to import debug.py")
                    import debug  # type: ignore # noqa:F401
                except ImportError:
                    _LOG.warning("No 'debug' module found.")

            # initialize global state
            self.initGlobals(quantum)

            # Ensure that we are executing a frozen config
            task_node.config.freeze()
            logInfo(None, "init", metadata=quantumMetadata)  # type: ignore[arg-type]
            init_input_refs = list(quantum.initInputs.values())

            _LOG.info(
                "Constructing task and executing quantum for label=%s dataId=%s.",
                task_node.label,
                quantum.dataId,
            )
            task = self.taskFactory.makeTask(task_node, limited_butler, init_input_refs)
            logInfo(None, "start", metadata=quantumMetadata)  # type: ignore[arg-type]
            try:
                caveats, outputsPut, butler_metrics = self.runQuantum(
                    task, quantum, task_node, limited_butler, quantum_id=quantum_id
                )
            except Exception as e:
                _LOG.error(
                    "Execution of task '%s' on quantum %s failed. Exception %s: %s",
                    task_node.label,
                    quantum.dataId,
                    e.__class__.__name__,
                    str(e),
                )
                raise
            else:
                quantumMetadata["butler_metrics"] = butler_metrics.model_dump()
                quantumMetadata["caveats"] = caveats.value
                # Stringify the UUID for easier compatibility with
                # PropertyList.
                quantumMetadata["outputs"] = [str(output) for output in outputsPut]
            logInfo(None, "end", metadata=quantumMetadata)  # type: ignore[arg-type]
            fullMetadata = task.getFullMetadata()
            fullMetadata["quantum"] = quantumMetadata
            if self.job_metadata is not None:
                fullMetadata["job"] = self.job_metadata
            self.writeMetadata(quantum, fullMetadata, task_node, limited_butler)
            stopTime = time.time()
            _LOG.info(
                "Execution of task '%s' on quantum %s took %.3f seconds",
                task_node.label,
                quantum.dataId,
                stopTime - startTime,
            )
        return quantum

    def checkExistingOutputs(
        self, quantum: Quantum, task_node: TaskNode, /, limited_butler: LimitedButler
    ) -> bool:
        """Decide whether this quantum needs to be executed.

        If only partial outputs exist then they are removed if
        ``clobberOutputs`` is True, otherwise an exception is raised.

        The ``LimitedButler`` is used for everything, and should be set to
        ``self.butler`` if no separate ``LimitedButler`` is available.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Quantum to check for existing outputs.
        task_node : `~lsst.pipe.base.pipeline_graph.TaskNode`
            Task definition structure.
        limited_butler : `~lsst.daf.butler.LimitedButler`
            Butler to use for querying and clobbering.

        Returns
        -------
        exist : `bool`
            `True` if ``self.skipExisting`` is defined, and a previous
            execution of this quanta appears to have completed successfully
            (either because metadata was written or all datasets were written).
            `False` otherwise.

        Raises
        ------
        RuntimeError
            Raised if some outputs exist and some not.
        """
        if self.assumeNoExistingOutputs:
            return False

        if self.skipExisting:
            _LOG.debug(
                "Checking existence of metadata from previous execution of label=%s dataId=%s.",
                task_node.label,
                quantum.dataId,
            )
            # Metadata output exists; this is sufficient to assume the previous
            # run was successful and should be skipped.
            [metadata_ref] = quantum.outputs[task_node.metadata_output.dataset_type_name]
            if metadata_ref is not None:
                if limited_butler.stored(metadata_ref):
                    return True

        # Find and prune (partial) outputs if `self.clobberOutputs` is set.
        _LOG.debug(
            "Looking for existing outputs in the way for label=%s dataId=%s.", task_node.label, quantum.dataId
        )
        ref_dict = limited_butler.stored_many(chain.from_iterable(quantum.outputs.values()))
        existingRefs = [ref for ref, exists in ref_dict.items() if exists]
        missingRefs = [ref for ref, exists in ref_dict.items() if not exists]
        if existingRefs:
            if not missingRefs:
                # Full outputs exist.
                if self.skipExisting:
                    return True
                elif self.clobberOutputs:
                    _LOG.info("Removing complete outputs for quantum %s: %s", quantum, existingRefs)
                    limited_butler.pruneDatasets(existingRefs, disassociate=True, unstore=True, purge=True)
                else:
                    raise RuntimeError(
                        f"Complete outputs exists for a quantum {quantum} "
                        "and neither clobberOutputs nor skipExisting is set: "
                        f"existingRefs={existingRefs}"
                    )
            else:
                # Partial outputs from a failed quantum.
                _LOG.debug(
                    "Partial outputs exist for quantum %s existingRefs=%s missingRefs=%s",
                    quantum,
                    existingRefs,
                    missingRefs,
                )
                if self.clobberOutputs:
                    # only prune
                    _LOG.info("Removing partial outputs for task %s: %s", task_node.label, existingRefs)
                    limited_butler.pruneDatasets(existingRefs, disassociate=True, unstore=True, purge=True)
                    return False
                else:
                    raise RuntimeError(
                        "Registry inconsistency while checking for existing quantum outputs:"
                        f" quantum={quantum} existingRefs={existingRefs}"
                        f" missingRefs={missingRefs}"
                    )

        # By default always execute.
        return False

    def updatedQuantumInputs(
        self, quantum: Quantum, task_node: TaskNode, /, limited_butler: LimitedButler
    ) -> Quantum:
        """Update quantum with extra information, returns a new updated
        Quantum.

        Some methods may require input DatasetRefs to have non-None
        ``dataset_id``, but in case of intermediate dataset it may not be
        filled during QuantumGraph construction. This method will retrieve
        missing info from registry.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        task_node : `~lsst.pipe.base.pipeline_graph.TaskNode`
            Task definition structure.
        limited_butler : `~lsst.daf.butler.LimitedButler`
            Butler to use for querying.

        Returns
        -------
        update : `~lsst.daf.butler.Quantum`
            Updated Quantum instance.
        """
        anyChanges = False
        updatedInputs: defaultdict[DatasetType, list] = defaultdict(list)
        for key, refsForDatasetType in quantum.inputs.items():
            _LOG.debug(
                "Checking existence of input '%s' for label=%s dataId=%s.",
                key.name,
                task_node.label,
                quantum.dataId,
            )
            toCheck = []
            newRefsForDatasetType = updatedInputs[key]
            for ref in refsForDatasetType:
                if self._should_assume_exists(quantum, ref):
                    newRefsForDatasetType.append(ref)
                else:
                    toCheck.append(ref)
            if not toCheck:
                _LOG.debug(
                    "Assuming overall input '%s' is present without checks for label=%s dataId=%s.",
                    key.name,
                    task_node.label,
                    quantum.dataId,
                )
                continue
            stored = limited_butler.stored_many(toCheck)
            for ref in toCheck:
                if stored[ref]:
                    newRefsForDatasetType.append(ref)
                else:
                    # This should only happen if a predicted intermediate was
                    # not actually produced upstream, but
                    # datastore misconfigurations can unfortunately also land
                    # us here.
                    _LOG.info("No dataset artifact found for %s", ref)
                    continue
            if len(newRefsForDatasetType) != len(refsForDatasetType):
                anyChanges = True
        # If we removed any input datasets, let the task check if it has enough
        # to proceed and/or prune related datasets that it also doesn't
        # need/produce anymore.  It will raise NoWorkFound if it can't run,
        # which we'll let propagate up.  This is exactly what we run during QG
        # generation, because a task shouldn't care whether an input is missing
        # because some previous task didn't produce it, or because it just
        # wasn't there during QG generation.
        namedUpdatedInputs = NamedKeyDict[DatasetType, list[DatasetRef]](updatedInputs.items())
        helper = AdjustQuantumHelper(namedUpdatedInputs, quantum.outputs)
        if anyChanges:
            _LOG.debug("Running adjustQuantum for label=%s dataId=%s.", task_node.label, quantum.dataId)
            assert quantum.dataId is not None, "Quantum DataId cannot be None"
            helper.adjust_in_place(task_node.get_connections(), label=task_node.label, data_id=quantum.dataId)
        return Quantum(
            taskName=quantum.taskName,
            taskClass=quantum.taskClass,
            dataId=quantum.dataId,
            initInputs=quantum.initInputs,
            inputs=helper.inputs,
            outputs=helper.outputs,
        )

    def runQuantum(
        self,
        task: PipelineTask,
        quantum: Quantum,
        task_node: TaskNode,
        /,
        limited_butler: LimitedButler,
        quantum_id: uuid.UUID | None = None,
    ) -> tuple[QuantumSuccessCaveats, list[uuid.UUID], ButlerMetrics]:
        """Execute task on a single quantum.

        Parameters
        ----------
        task : `~lsst.pipe.base.PipelineTask`
            Task object.
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.
        task_node : `~lsst.pipe.base.pipeline_graph.TaskNode`
            Task definition structure.
        limited_butler : `~lsst.daf.butler.LimitedButler`
            Butler to use for dataset I/O.
        quantum_id : `uuid.UUID` or `None`, optional
            ID of the quantum being executed.

        Returns
        -------
        flags : `QuantumSuccessCaveats`
            Flags that describe qualified successes.
        ids_put : list[ `uuid.UUID` ]
            Record of all the dataset IDs that were written by this quantum
            being executed.
        metrics : `lsst.daf.butler.ButlerMetrics`
            Butler metrics recorded for this quantum.
        """
        flags = QuantumSuccessCaveats.NO_CAVEATS

        # Create a butler that operates in the context of a quantum
        butlerQC = QuantumContext(limited_butler, quantum, resources=self.resources, quantum_id=quantum_id)

        # Get the input and output references for the task
        inputRefs, outputRefs = task_node.get_connections().buildDatasetRefs(quantum)

        # Call task runQuantum() method.
        try:
            with limited_butler.record_metrics() as butler_metrics:
                task.runQuantum(butlerQC, inputRefs, outputRefs)
        except NoWorkFound as err:
            # Not an error, just an early exit.
            _LOG.info(
                "Task '%s' on quantum %s exited early with no work found: %s.",
                task_node.label,
                quantum.dataId,
                str(err),
            )
            flags |= err.FLAGS
        except AnnotatedPartialOutputsError as caught:
            error: BaseException
            if caught.__cause__ is None:
                _LOG.error(
                    "Incorrect use of AnnotatedPartialOutputsError: no chained exception found.",
                    task_node.label,
                    quantum.dataId,
                )
                error = caught
            else:
                error = caught.__cause__
            if self.raise_on_partial_outputs:
                # Note: this is a real edge case that required some
                # experimentation: without 'from None' below, this raise would
                # produce a "while one exception was being handled, another was
                # raised" traceback involving AnnotatedPartialOutputsError.
                # With the 'from None', we get just the error chained to it, as
                # desired.
                raise error from None
            else:
                _LOG.error(
                    "Task '%s' on quantum %s exited with partial outputs; "
                    "considering this a qualified success and proceeding.",
                    task_node.label,
                    quantum.dataId,
                )
                _LOG.error(error, exc_info=error)
                flags |= caught.FLAGS
        if not butlerQC.outputsPut:
            flags |= QuantumSuccessCaveats.ALL_OUTPUTS_MISSING
        if not butlerQC.outputsPut == butlerQC.allOutputs:
            flags |= QuantumSuccessCaveats.ANY_OUTPUTS_MISSING
        ids_put = [output[2] for output in butlerQC.outputsPut]
        return flags, ids_put, butler_metrics

    def writeMetadata(
        self, quantum: Quantum, metadata: Any, task_node: TaskNode, /, limited_butler: LimitedButler
    ) -> None:
        # DatasetRef has to be in the Quantum outputs, can lookup by name
        try:
            [ref] = quantum.outputs[task_node.metadata_output.dataset_type_name]
        except LookupError as exc:
            raise InvalidQuantumError(
                "Quantum outputs is missing metadata dataset type "
                f"{task_node.metadata_output.dataset_type_name};"
                " this could happen due to inconsistent options between QuantumGraph generation"
                " and execution"
            ) from exc
        limited_butler.put(metadata, ref)

    def initGlobals(self, quantum: Quantum) -> None:
        """Initialize global state needed for task execution.

        Parameters
        ----------
        quantum : `~lsst.daf.butler.Quantum`
            Single Quantum instance.

        Notes
        -----
        There is an issue with initializing filters singleton which is done
        by instrument, to avoid requiring tasks to do it in runQuantum()
        we do it here when any dataId has an instrument dimension. Also for
        now we only allow single instrument, verify that all instrument
        names in all dataIds are identical.

        This will need revision when filter singleton disappears.
        """
        # can only work for full butler
        if self.butler is None:
            return
        oneInstrument = None
        for datasetRefs in chain(quantum.inputs.values(), quantum.outputs.values()):
            for datasetRef in datasetRefs:
                dataId = datasetRef.dataId
                instrument = cast(str, dataId.get("instrument"))
                if instrument is not None:
                    if oneInstrument is not None:
                        assert (  # type: ignore
                            instrument == oneInstrument
                        ), "Currently require that only one instrument is used per graph"
                    else:
                        oneInstrument = instrument
                        Instrument.fromName(instrument, self.butler.registry)

    def _should_assume_exists(self, quantum: Quantum, ref: DatasetRef) -> bool | None:
        """Report whether the given dataset can be assumed to exist because
        some previous check reported that it did.

        If this is `True` for a dataset does not in fact exist anymore, that's
        an unexpected problem that we want to raise as an exception, and
        definitely not a case where some predicted output just wasn't produced.
        We can't always tell the difference, but in this case we can.

        Parameters
        ----------
        quantum : `Quantum`
            Quantum being processed.
        ref : `lsst.daf.butler.DatasetRef`
            Reference to the input dataset.

        Returns
        -------
        exists : `bool` or `None`
            `True` if this dataset is definitely an overall input, `False` if
            some other quantum in the graph is expected to produce it, and
            `None` if the answer could not be determined.
        """
        if quantum.datastore_records:
            for datastore_record_data in quantum.datastore_records.values():
                if ref.id in datastore_record_data.records:
                    return True
            return False
        return None
