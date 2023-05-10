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

import dataclasses
import itertools
from collections.abc import Iterable, Iterator

from lsst.daf.butler import Butler, DataCoordinate, DatasetRef
from lsst.resources import ResourcePath, ResourcePathExpression

from .graph import QuantumGraph
from .pipeline import PipelineDatasetTypes


@dataclasses.dataclass
class TaskExecutionReport:
    failed: set[DataCoordinate] = dataclasses.field(default_factory=set)
    missing_datasets_failed: set[DatasetRef] = dataclasses.field(default_factory=set)
    missing_datasets_not_produced: set[DatasetRef] = dataclasses.field(default_factory=set)
    datasets_produced: dict[str, int] = dataclasses.field(default_factory=dict)

    def inspect_quantum(self, quantum, refs, metadata_name):
        (ref,) = quantum.outputs[metadata_name]
        if ref.id not in refs[metadata_name]:
            self.failed.add(quantum.dataId)
            failed = True
        else:
            failed = False
        for ref in itertools.chain.from_iterable(quantum.outputs.values()):
            if ref.id not in refs[ref.datasetType.name]:
                if failed:
                    self.missing_datasets_failed.add(ref)
                else:
                    self.missing_datasets_not_produced.add(ref)
            else:
                self.datasets_produced[ref.datasetType.name] = (
                    self.datasets_produced.setdefault(ref.datasetType.name, 0) + 1
                )


@dataclasses.dataclass
class QuantumGraphExecutionReport:
    uri: ResourcePath
    tasks: dict[str, TaskExecutionReport] = dataclasses.field(default_factory=dict)

    @classmethod
    def make_reports(
        cls,
        butler: Butler,
        graph_uris: Iterable[ResourcePathExpression],
        collections: Iterable[str] | None = None,
    ) -> Iterator[QuantumGraphExecutionReport]:
        pipeline_dataset_types = None
        refs = {}
        for graph_uri in graph_uris:
            graph_uri = ResourcePath(graph_uri)
            qg = QuantumGraph.loadUri(graph_uri)
            report = cls(graph_uri)
            if pipeline_dataset_types is None:
                task_defs = list(qg.iterTaskGraph())
                pipeline_dataset_types = PipelineDatasetTypes.fromPipeline(task_defs, butler.registry)
                for dataset_type in itertools.chain(
                    pipeline_dataset_types.initIntermediates,
                    pipeline_dataset_types.initOutputs,
                    pipeline_dataset_types.intermediates,
                    pipeline_dataset_types.outputs,
                ):
                    refs[dataset_type.name] = {
                        ref.id: ref
                        for ref in butler.registry.queryDatasets(
                            dataset_type.name, collections=collections, findFirst=False
                        )
                    }
            for taskDef in qg.iterTaskGraph():
                task_report = TaskExecutionReport()
                for node in qg.getQuantaForTask(taskDef):
                    task_report.inspect_quantum(node.quantum, refs, metadata_name=taskDef.metadataDatasetName)
                report.tasks[taskDef.label] = task_report
            yield report
