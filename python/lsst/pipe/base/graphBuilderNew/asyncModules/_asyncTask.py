__all__ = ("BuildDirection", "AsyncTask")
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum, auto
import itertools
from typing import Set, Dict, Tuple, Mapping, Union, Callable, Optional, Awaitable, Sequence
from queue import Queue

from lsst.daf.butler import Registry, DataCoordinate

from ... import (TaskDef, iterConnections, connectionTypes)
from .._customTypes import DatasetTypeName, DatasetCoordinate, QuantumCoordinate


class BuildDirection(Enum):
    FORWARD = auto()
    BACKWARD = auto()

    def reverse(self):
        if self is BuildDirection.FORWARD:
            return BuildDirection.BACKWARD
        else:
            return BuildDirection.FORWARD


@dataclass
class AsyncTask:
    __slots__ = ("task", "_graph_input", "_graph_output", "direction", "_starting", "_resulting", "quanta",)
    task: TaskDef
    _graph_input: Set[DatasetTypeName]
    _graph_output: Set[DatasetTypeName]
    direction: BuildDirection
    _starting: Tuple[connectionTypes.BaseConnection, ...]
    _resulting: Tuple[connectionTypes.BaseConnection, ...]
    quanta: Dict[QuantumCoordinate, Mapping[DatasetTypeName, DatasetCoordinate]]

    def __init__(self, task: TaskDef, graph_input: Set[DatasetTypeName],
                 graph_output: Set[DatasetTypeName], direction: BuildDirection):
        self.task = task
        self._graph_input = graph_input
        self._graph_output = graph_output
        self.direction = direction
        self.quanta = {}
        startingIterable = itertools.chain(iterConnections(task.connections, "inputs"),
                                           iterConnections(task.connections, "prerequisiteInputs"))
        self._starting = tuple((x for x in startingIterable))
        resultingIterable = iterConnections(task.connections, "outputs")
        self._resulting = tuple(x for x in resultingIterable)
        self.allDimensions = set(self.task.connections.dimensions)
        for connection in itertools.chain(self._starting, self._resulting):
            self.allDimensions.union(connection.dimensions)

    @property
    def graph_input(self) -> Set[DatasetTypeName]:
        if self.direction is BuildDirection.FORWARD:
            return self._graph_input
        else:
            return self._graph_output

    @property
    def graph_output(self) -> Set[DatasetTypeName]:
        if self.direction is BuildDirection.FORWARD:
            return self._graph_output
        else:
            return self._graph_input

    @property
    def starting(self) -> Tuple[connectionTypes.BaseConnection, ...]:
        if self.direction is BuildDirection.FORWARD:
            return self._starting
        else:
            return self._resulting

    @property
    def resulting(self) -> Tuple[connectionTypes.BaseConnection, ...]:
        if self.direction is BuildDirection.FORWARD:
            return self._resulting
        else:
            return self._starting

    @property
    def build_func(self) -> Union[Callable[[Registry,
                                            DataCoordinate,
                                            Optional[Union[str, Mapping[str, str]]]],
                                  Mapping[str, Sequence[DataCoordinate]]],
                                  Callable[[Registry,
                                            DataCoordinate,
                                            Optional[Union[str, Mapping[str, str]]],
                                            Queue,
                                            Set[DatasetTypeName],
                                            ThreadPoolExecutor],
                                           Awaitable[Dict[DatasetTypeName, Set[DatasetCoordinate]]]]]:
        if self.direction is BuildDirection.FORWARD:
            return self.task.connections.defineQuantaForward
        else:
            return self.task.connections.defineQuantaBackwards
