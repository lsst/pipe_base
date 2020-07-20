__all__ = ("CacheQueueArg", "CachedTempTables")
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Optional
from queue import Queue

import lsst.daf.butler as dafButler


@dataclass(frozen=True)
class CacheQueueArg:
    dimensions: Optional[set]
    where: Optional[str]
    identifier: Optional[str]

    def __bool__(self):
        if self.dimensions is None and self.where is None and self.identifier is None:
            return False
        else:
            return True

    @classmethod
    def build_empty(cls):
        return cls(None, None, None)

    def __hash__(self) -> int:
        return hash(self.dimensions, self.where)

    def __eq__(self, o: "CacheQueueArg") -> bool:
        return self.dimensions == o.dimensions and\
            self.where == o.where


@dataclass
class CachedTempTables:
    _inputQueue: Queue
    _outputQueue: Queue
    _registry: dafButler.Registry

    def __call__(self):
        cache = {}
        with ExitStack() as stack:
            request: CacheQueueArg
            for request in self._inputQueue.get():
                if not request:
                    break
                (request, name) = request
                result = cache.get(request, None)
                if result is None:
                    materialized = self._registry.queryDataIds(request.dimensions,
                                                               where=request.where).materialize()
                    entered = stack.enter_context(materialized)
                    result = entered.expanded()
                    cache[request] = result
                self._outputQueue.put({name: result})
