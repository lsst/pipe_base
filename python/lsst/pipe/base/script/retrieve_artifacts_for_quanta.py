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

__all__ = ["retrieve_artifacts_for_quanta"]

import logging

from lsst.daf.butler import DatasetRef, QuantumBackedButler
from lsst.daf.butler.datastore.record_data import DatastoreRecordData
from lsst.pipe.base import QuantumGraph
from lsst.resources import ResourcePath

_LOG = logging.getLogger(__name__)


def retrieve_artifacts_for_quanta(
    graph: str,
    repo: str,
    dest: str,
    transfer: str,
    preserve_path: bool,
    clobber: bool,
    qgraph_node_id: list[str],
    include_inputs: bool,
    include_outputs: bool,
) -> list[ResourcePath]:
    """Retrieve artifacts from a graph and store locally.

    Parameters
    ----------
    graph : `str`
        URI string of the quantum graph.
    repo : `str`
        URI string of the Butler repo to use.
    dest : `str`
        URI string of the directory to write the artifacts.
    transfer : `str`
        Transfer mode to use when placing artifacts in the destination.
    preserve_path : `bool`
        If `True` the full datastore path will be retained within the
        destination directory, else only the filename will be used.
    clobber : `bool`
        If `True` allow transfers to overwrite files at the destination.
    qgraph_node_id : `tuple` [ `str` ]
        Quanta to extract.
    include_inputs : `bool`
        Whether to include input datasets in retrieval.
    include_outputs : `bool`
        Whether to include output datasets in retrieval.

    Returns
    -------
    paths : `list` [ `lsst.resources.ResourcePath` ]
        The paths to the artifacts that were written.
    """
    # Read graph into memory.
    nodes = qgraph_node_id or None
    qgraph = QuantumGraph.loadUri(graph, nodes=nodes)

    # Get data repository definitions from the QuantumGraph; these can have
    # different storage classes than those in the quanta.
    dataset_types = {dstype.name: dstype for dstype in qgraph.registryDatasetTypes()}

    datastore_records: dict[str, DatastoreRecordData] = {}
    refs: set[DatasetRef] = set()
    if include_inputs:
        # Collect input refs used by this graph.
        for task_def in qgraph.iterTaskGraph():
            if in_refs := qgraph.initInputRefs(task_def):
                refs.update(in_refs)
        for qnode in qgraph:
            for otherRefs in qnode.quantum.inputs.values():
                refs.update(otherRefs)
            for store_name, records in qnode.quantum.datastore_records.items():
                datastore_records.setdefault(store_name, DatastoreRecordData()).update(records)
    n_inputs = len(refs)
    if n_inputs:
        _LOG.info("Found %d input dataset%s.", n_inputs, "" if n_inputs == 1 else "s")

    if include_outputs:
        # Collect output refs that could be created by this graph.
        original_output_refs: set[DatasetRef] = set(qgraph.globalInitOutputRefs())
        for task_def in qgraph.iterTaskGraph():
            if out_refs := qgraph.initOutputRefs(task_def):
                original_output_refs.update(out_refs)
        for qnode in qgraph:
            for otherRefs in qnode.quantum.outputs.values():
                original_output_refs.update(otherRefs)

        # Convert output_refs to the data repository storage classes, too.
        for ref in original_output_refs:
            internal_dataset_type = dataset_types.get(ref.datasetType.name, ref.datasetType)
            if internal_dataset_type.storageClass_name != ref.datasetType.storageClass_name:
                refs.add(ref.overrideStorageClass(internal_dataset_type.storageClass_name))
            else:
                refs.add(ref)

    n_outputs = len(refs) - n_inputs
    if n_outputs:
        _LOG.info("Found %d output dataset%s.", n_outputs, "" if n_outputs == 1 else "s")

    # Make QBB, its config is the same as output Butler.
    qbb = QuantumBackedButler.from_predicted(
        config=repo,
        predicted_inputs=[ref.id for ref in refs],
        predicted_outputs=[],
        dimensions=qgraph.universe,
        datastore_records=datastore_records,
        dataset_types=dataset_types,
    )

    paths = qbb.retrieve_artifacts(
        refs, dest, transfer=transfer, overwrite=clobber, preserve_path=preserve_path
    )
    return paths
