__all__ = ("QuantumGraphBuilder",)
import itertools
from typing import (List, Optional, Mapping, Generator, Tuple, Union,)
import os
import networkx as nx
from dataclasses import dataclass, field
from collections import defaultdict

from lsst.daf.butler import Butler
from .. import (iterConnections, Pipeline)
from .asyncModules import AsyncBuilder, BuildDirection


class CounterList(list):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counts = {}

    def append(self, value):
        count = self.counts.setdefault(value, 0)
        self.counts[value] = count + 1
        super().append(value)


class OutputSet(set):
    @property
    def first(self):
        if len(self) == 0:
            return None
        return next(iter(self))


@dataclass
class DatasetTracker:
    inputs: set = field(default_factory=set)
    output: OutputSet = field(default_factory=OutputSet)


def datasetDictToEdgeIterator(datasetDict: Mapping[str, DatasetTracker]) ->\
        Generator[Tuple[str, str], None, None]:
    for datasetType, entry in datasetDict.items():
        if len(entry.output) > 1:
            raise ValueError(f"Datasets can only be produced by one task: "
                             "{entry.output} each claim to produce {datasetType}")
        output = entry.output.first
        if output is None:
            continue
        for inpt in entry.inputs:
            yield (output, inpt)


class QuantumGraphBuilder:
    def __init__(self, pipeline: Pipeline):
        self.graph = nx.DiGraph()
        self.pipeline = pipeline
        self.datasetDict = defaultdict(DatasetTracker)
        self._tasks = list(self.pipeline.toExpandedPipeline())
        for tsk in self._tasks:
            connections = tsk.connections
            # self.graph.add_node(tsk.taskName, taskdef=tsk)
            for inpt in itertools.chain(iterConnections(connections, "inputs"),
                                        iterConnections(connections, "prerequisiteInputs")):
                self.datasetDict[inpt.name].inputs.add(tsk.taskName)
            for output in iterConnections(connections, "outputs"):
                self.datasetDict[output.name].output.add(tsk.taskName)
        self.graph.add_edges_from(datasetDictToEdgeIterator(self.datasetDict))
        self.startingNodes = {x for x, n in self.graph.in_degree if n == 0}
        self.endingNodes = {x for x, n in self.graph.out_degree if n == 0}
        self.datasetQuanta = {}

    @classmethod
    def fromFile(cls, location: str):
        pipeline = Pipeline.fromFile(os.path.expandvars(location))
        return cls(pipeline)

    def traceProductToStartTask(self, product: str) -> List[str]:
        return list(nx.bfs_tree(self.graph, self.datasetDict[product].output.first, reverse=True))

    def findBuildOriginTasks(self, products: Optional[List[str]] = None) -> List[str]:
        if products is None:
            return list(self.startingNodes)
        counter = CounterList()
        for p in products:
            for value in self.traceProductToStartTask(p):
                counter.append(value)
        return [k for k, v in counter.counts.items() if v == 1]

    def build(self, registry: dafButler.Registry, products: Optional[Union[str, List[str]]] = None,
              where: Optional[Union[str, Mapping[str, str]]] = None):

        asyncBuilder = AsyncBuilder(registry=registry, run="test_1", collections=["shared/ci_hsc_output"],)
        for task in self._tasks:
            graph_input = {name
                           for name in task.connections.allConnections.keys()
                           if name in self.startingNodes}
            graph_output = {name
                            for name in task.connections.allConnections.keys()
                            if name in self.endingNodes}
            print(task)
            asyncBuilder.addTask(task, graph_input, graph_output, BuildDirection.BACKWARD)
        asyncBuilder.build()



    
    #def populateQuanta(self, node: str, registry: dafButler.registry, direction: BuildDirection):
    #    task = self.inputToTask[node]
    #    startIds = self._buildDataIds(pipeBase.iterConnections(task.connections, "inputs"))
    #    self.datasetQuanta.update(startIds)
    #    if direction is BuildDirection.FORWARD: 
    #        endIds = task.connections.defineQuantaForward(startIds)
    #    else:
    #        endIds = task.connections.defineQuantaBackwards(startIds)
    #    self.datasetQuanta.update(outputIds)

    # def build(self, collections: List[str], registry: dafButler.registry,
              # products: Optional[List[str]] = None,
              # where: Optional[str] = None):
        # boundrayNodes = self.findOutputTypesList(products=products)
        # for node in boundrayNodes:
            # if node in self.startingNodes:
                # subTree = nx.bfs_tree(self.graph, node)
                # walkDirection = BuildDirection.fORWARD
            # else:
                # subTree = nx.bfs_tree(self.graph, node, reverse=True)
                # walkDirection = BuildDirection.BACKWARDS
            # test = 1
            # for subNode in subTree:
                # pass

qBuilder = QuantumGraphBuilder.fromFile("$CI_HSC_GEN3_DIR/pipelines/CiHsc.yaml")
registry_path = os.path.expandvars("$CI_HSC_GEN3_DIR/DATA/butler.yaml")
registry = Butler(registry_path).registry
#qBuilder.findOutputTypesList(["deepCoadd_meas", "forced_src", "deepCoadd_measMatch"])
qBuilder.build(registry)
test = 1




    # def _edgeDataProductBuilder(self, asyncTask: AsyncTask):
        # """This function fires off all the initial lookups
        # """
        # def unified_query_worker(dataset):
        # def noQuantumWorker(dataset):
            # datasetQuanta: Set[DatasetCoordinate] =\
                # set(self._registry.queryDimensions(dataset.dimensions))
            # self._queue.put({DatasetTypeName(dataset.name): datasetQuanta})

        # def workerWithQuanta(datasets):
            # # Join the dimensions of all the input datasets, along with the task dimensions
            # allDimensions = asyncTask.quantum_dimensions.union(d.dimensions for d in datasets)
            # # Construct a where statement out of the concrete values of the task quanta
            # dimensionMapping: Dict[dafButler.Dimension, Set[Any]] = defaultdict(set)
            # for coord in asyncTask.quanta.keys():
                # for dimension in coord:
                    # dimensionMapping[dimension].add(coord[dimension])

            # # Query for everything all at once
            # registryResults = self._registry.queryDimensions(allDimensions)
            # # quanta being None means this is not one of the first tasks to be run, but that it
            # # has some datasets that will be loaded that are not produced by any prior task,
            # # which means that this task should get all of them as inputs, and they will be
            # # filtered down by this task later in propagateQuantaForward
            # #
            # # Otherwise, the task is one of the starting tasks and it's inputs should be
            # # constrained by whatever the possible task quanta are
            # taskQuanta: Mapping[QuantumCoordinate, Mapping[DatasetTypeName, Set[DatasetCoordinate]]] = {}
            # for dataset in datasets:
                # allDatasetCoordinates: Set[DatasetCoordinate] = set()
                # for taskQuanta in asyncTask.quanta:
                    # registryResults = set(self._registry.queryDimensions(dataset.dimensions,
                                                                         # dataId=taskQuanta))
                    # taskQuanta[taskQuanta][dataset.name] = registryResults
                    # allDatasetCoordinates.update(registryResults)
                # self._queue.put({dataset.name: allDatasetCoordinates})

        # if asyncTask.quanta:
            # # probably need to include where in here after I figure out when it is appropriate
            # self._threadExecutor.submit(workerWithQuanta, asyncTask.starting)
        # else:
            # # probably need to include where in here after I figure out when it is appropriate
            # for dataset in asyncTask.starting:
                # self._threadExecutor.submit(noQuantumWorker, dataset)