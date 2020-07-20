__all__ = ("DatasetTypeName", "TaskName", "DatasetCoordinate", "QuantumCoordinate")
from typing import NewType

from lsst.daf.butler import DataCoordinate

DatasetTypeName = NewType("DatasetTypeName", str)
TaskName = NewType("TaskName", str)
DatasetCoordinate = NewType("DatasetCoordinate", DataCoordinate)
QuantumCoordinate = NewType("QuantumCoordinate", DataCoordinate)
