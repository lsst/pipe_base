__all__ = ("OutputExistsError", "AsyncBuilder")

from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from dataclasses import dataclass
from typing import (MutableMapping, Set, Coroutine, List, Optional, Union, Mapping, Dict, Iterable, Generator, Any, cast, 
                    Callable, Awaitable)
from queue import Queue, Empty

import inspect
import itertools
import types

from ... import connectionTypes, TaskDef
from .._customTypes import TaskName, DatasetTypeName, DatasetCoordinate, QuantumCoordinate
from ._asyncTask import AsyncTask, BuildDirection
from ._tableCacher import CachedTempTables, CacheQueueArg

from lsst.daf.butler import Registry, DimensionGraph

DatasetTypeCoordMap = MutableMapping[DatasetTypeName, Set[DatasetCoordinate]]
Quantum = MutableMapping[QuantumCoordinate, DatasetTypeCoordMap]


class OutputExistsError(Exception):
    pass


@dataclass(frozen=True, eq=True)
class AsyncJob:
    __slots__ = ("requires", "coro")
    requires: Set[connectionTypes.BaseConnection]
    coro: Coroutine


@dataclass(frozen=True, eq=True)
class EndSignal:
    __slots__ = ("task_name")
    task_name: TaskName


class AsyncBuilder:
    def __init__(self, *, registry: Registry, run: str, collections: List[str],
                 where: Optional[Union[str, Mapping[str, str]]] = None,
                 max_threads: int = 5, skip_existing: bool = True):
        self._tasks: Dict[str, AsyncTask] = {}
        self._registry = registry
        self._queue: Queue = Queue()
        self._cacheQueue: Queue = Queue()
        self._where = where
        self._datasetQuanta: Dict[DatasetTypeName, Set[DatasetCoordinate]] = {}
        self._threadExecutor = ThreadPoolExecutor(max_workers=max_threads+1)
        self._run = run
        self._collections = collections
        self._sip_existing = skip_existing

    def addTask(self, task: TaskDef, start: Set[DatasetTypeName], end: Set[DatasetTypeName],
                direction: BuildDirection = BuildDirection.FORWARD):
        self._tasks[task.taskName] = AsyncTask(task, start, end, direction)

    def build(self):
        """This is the method responsible for building the quantum graph given a set
        of input tasks.

        This functions as an event loop, listening for work to be completeted, and
        dispatches based on what queued jobs require.
        """
        jobs = set()
        eventBuffer = {}
        tableCache = CachedTempTables(self._cacheQueue, self._queue, self._registry)
        self._threadExecutor.submit(tableCache)
        forwardComplete = set()
        import ipdb;ipdb.set_trace()
        for task in self._tasks.values():
            coro = self._buildQuanta(task)
            requires = coro.send(None)
            jobs.add(AsyncJob(requires, coro))
        while jobs:
            try:
                eventBuffer.update(self._queue.get(), timeout=1)
            except Empty:
                # This is not a problem, the queue might be empty as everything
                # needed is already in the event buffer, continue processing
                pass
            toRemove = []
            for job in jobs:
                if job.requires.issubset(eventBuffer.keys()):
                    toRemove.append(job)
                    requires = job.coro.send({k: eventBuffer[k] for k in job.requires})
                    if not isinstance(requires, EndSignal):
                        jobs.append(AsyncJob(requires, job.coro))
                    else:
                        forwardComplete.add(requires.task_name)
            for job in toRemove:
                jobs.remove(job)
            if not forwardComplete.difference(self._tasks.keys()):
                break
        # Still need to think about propagating the other way to re trim
        self._cacheQueue.put(CacheQueueArg.build_empty())

        return self._taskQuanta

    @types.coroutine
    def _getJointTable(self, allDimensions, where, name):
        self._cacheQueue.put(CacheQueueArg(allDimensions, where, name))
        table = yield set(name)
        return table

    @types.coroutine
    def _verifyQuanta(self, resultingQuanta):
        def worker(datasetType, dataids):
            results = {}
            for dataId in dataids:
                result = self._registry.findDataset(datasetType, dataId, collections=self._run)
                results[dataId] = result
                self._queue.put({"f{datasetType}_verification": results})
        depends = set()
        for datasetType, dataIds in resultingQuanta:
            self._threadExecutor.submit(worker, datasetType, dataIds)
            depends.add(f"{datasetType}_verification")
        results = yield depends
        quanta = {}
        exceptionMsg = "The following datasets already exist in the output collection,"\
                       " set skip_existing leave existing datasets in place {}"
        exceptionList = []
        for dataset_verification, results in results.items():
            dataset = dataset_verification.rstrip("_verification")
            if any(results) and not self._sip_existing:
                exceptionMsg.append(f"{dataset} : { {k for k,v in results.items() if v} }")
            else:
                quanta[dataset] = [k for k, v in results.items() if v is None]
        if exceptionList:
            raise OutputExistsError(exceptionMsg.format(exceptionList))
        return quanta

    @types.coroutine
    def _getDataForNames(self, starting_names: Iterable[DatasetTypeName]) ->\
            Generator[Set[DatasetTypeName],
                      Dict[DatasetTypeName, Any],
                      Dict[DatasetTypeName, Any]]:
        """This function declares what input dataset type quanta are required to start processing, and
        waits via the event loop until they are available. If an asyncTask is defined to be a starting node
        then it will fire off the lookup of the input dataset types in a thread
        """
        # wait until the desired datasets have been looked up
        startingQuanta = yield set(starting_names)
        return startingQuanta

    async def _getResultingDatasetRefs(self, asyncTask: AsyncTask,
                                       startingQuanta: Mapping[DatasetTypeName,
                                                               Iterable[DatasetCoordinate]]) ->\
            DatasetTypeCoordMap:
        """This function takes in initial quanta and calls the appropriate propigate method and waits
        for the results to become available
        """
        if not inspect.isawaitable(asyncTask.build_func):
            # This function will be executed inside a thread
            def wrapper(registry: Registry, taskDimTable: Any, quantum: Quantum, where: str, queue: Queue):
                # This wraps a regular function call to put results into the queue
                queue.put(function(registry, taskDimTable, quantum, where))

            function = self._makeCoroutine(wrapper)
        else:
            function = cast(Callable[[Registry, Quantum, str, Queue, Set[DatasetTypeName],
                                      ThreadPoolExecutor],
                                     Awaitable[DatasetTypeCoordMap]],
                            asyncTask.build_func)
        return await function(self._registry, startingQuanta, self._where, self._queue,
                              set(asyncTask.resulting), self._threadExecutor)

    def _makeCoroutine(self, builder: Callable) ->\
            Callable[[Registry, Quantum, str, Queue, Set[DatasetTypeName], ThreadPoolExecutor],
                     Awaitable[DatasetTypeCoordMap]]:
        """This wraps a regular function call inside a coroutine that executes
        the function inside a thread, and then waits for whatever dependencies
        are input to be available
        """
        @types.coroutine
        def wrapper(registry: Registry, taskDimTable: Any, quantum: Quantum, where: str, queue: Queue,
                    depends: Set[DatasetTypeName], executor: ThreadPoolExecutor) ->\
            Generator[Set[DatasetTypeName],
                      DatasetTypeCoordMap,
                      DatasetTypeCoordMap]:
            # This wraps a regular function call to be used as a coroutine
            executor.submit(builder, registry, taskDimTable, quantum, where, queue)
            result = yield depends
            return result
        return wrapper

    async def _edgeDataProductBuilder(self, asyncTask: AsyncTask):
        def worker(jointTable):
            quantaDimensionGraph = DimensionGraph(self._registry.universe,
                                                  asyncTask.connections.dimensions)
            datasetTypeGraphs = {conn.name: DimensionGraph(self._registry.universe, conn.dimensions)
                                 for conn in asyncTask.starting}
            quanta = {}
            dataset_coordinates = defaultdict(set)
            for row in jointTable:
                task_dim_coord = row.subset(quantaDimensionGraph)
                per_quanta_ds_coordinates = quanta.setdefault(task_dim_coord, defaultdict(set))
                for name, graph in datasetTypeGraphs:
                    ds_type_subset = row.subset(graph)
                    per_quanta_ds_coordinates[name].add(ds_type_subset)
                    dataset_coordinates[name].add(ds_type_subset)
            asyncTask.quanta = quanta
            self._queue.put(dataset_coordinates)

        allDimensions = set()
        allDimensions &= asyncTask.task.connections.dimensions
        for connection in asyncTask.starting:
            allDimensions &= connection.dimensions
        identifier = f"{asyncTask.task.name}_jointTable"
        jointTable = await self._getJointTable(allDimensions, self._where, identifier)
        self._threadExecutor.submit(worker, jointTable)

    async def _buildQuantaWithRefs(self,
                                   asyncTask: AsyncTask,
                                   startingRefs: DatasetTypeCoordMap,
                                   incomplete: Set[DatasetTypeName]):
        def worker(jointTable, incomplete, queueTaskName):
            quantaDimensionGraph = DimensionGraph(self._registry.universe,
                                                  asyncTask.connections.dimensions)
            datasetTypeGraphs = {conn.name: DimensionGraph(self._registry.universe, conn.dimensions)
                                 for conn in asyncTask.starting}
            quanta = {}
            dataset_coordinates = defaultdict(set)
            for row in jointTable:
                task_dim_coord = row.subset(quantaDimensionGraph)
                per_quanta_ds_coordinates = quanta.setdefault(task_dim_coord, defaultdict(set))
                for name, graph in datasetTypeGraphs:
                    ds_type_subset = row.subset(graph)
                    per_quanta_ds_coordinates[name].add(ds_type_subset)
                    # only add dataset types that have not been yet been added to the event loop
                    if name in incomplete:
                        dataset_coordinates[name].add(ds_type_subset)
            asyncTask.quanta = quanta
            # add an event corresponding to the quanta being completed to the queue
            dataset_coordinates[queueTaskName] = True
            self._queue.put(dataset_coordinates)

            pass

        allDimensions = set()
        allDimensions = asyncTask.task.connections.dimensions
        for name in incomplete:
            connection = getattr(asyncTask.connections, name)
            allDimensions &= connection.dimensions
        identifier = f"{asyncTask.task.name}_jointTable"
        # build the where clause
        dims = defaultdict(set)
        for coord in startingRefs.values():
            for dim, value in coord.items():
                dims[dim].add(value)
        where = ("{} IN {} AND "*len(dims)).format(x for x in itertools.chain.from_iterable(dims.items()))
        # slice on where to remove the last AND statement
        jointTable = await self._getJointTable(allDimensions, where[:-4], identifier)

        queueTaskName = f"{asyncTask.task.name}_quantaDone"
        self._threadExecutor.submit(worker, jointTable, incomplete, queueTaskName)
        await self._getDataForNames(queueTaskName)

    async def _buildQuanta(self, asyncTask: AsyncTask):
        starting_names = (x.name for x in asyncTask.starting)
        incomplete = set()
        if asyncTask.graph_input:
            if not asyncTask.graph_input.difference(x.name for x in asyncTask.starting):
                # The set of products to fetch at the start is the same as all of the task's
                # inputs, this means that the task quanta should be built first
                await self._edgeDataProductBuilder(asyncTask)
            else:
                # need to remove ones that need looked because they are pure inputs
                starting_names = {x.name for x in asyncTask.starting if x not in asyncTask.graph_input}
                incomplete = asyncTask.graph_input
        startingRefs = await self._getDataForNames(starting_names)
        if not asyncTask.quanta:
            await self._buildQuantaWithRefs(asyncTask, startingRefs, incomplete)
        await self._getResultingDatasetRefs(asyncTask)
        # probably wait to verify until the end
        # quanta = await self._verifyQuanta(resultingQuanta)

        # Sometimes the combination of these two will cause the same datasettype to be added
        # to the dict more than once with the same values, but there is not an obvious way
        # to avoid that, and it should not be expensive so let it happen for now.
        # self._datasetQuanta.update(startingQuanta)
        # self._datasetQuanta.update(resultingQuanta)
        return EndSignal(asyncTask.task.taskName)
