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

__all__ = ["transfer_from_graph"]

from lsst.daf.butler import Butler, CollectionType, DatasetRef, QuantumBackedButler, Registry
from lsst.daf.butler.registry import MissingCollectionError
from lsst.pipe.base import QuantumGraph


def transfer_from_graph(
    graph: str,
    dest: str,
    register_dataset_types: bool,
    transfer_dimensions: bool,
    update_output_chain: bool,
) -> int:
    """Transfer output datasets from quantum graph to dest.

    Parameters
    ----------
    graph : `str`
        URI string of the quantum graph.
    dest : `str`
        URI string of the destination Butler repo.
    register_dataset_types : `bool`
        Indicate whether missing dataset types should be registered.
    transfer_dimensions : `bool`
        Indicate whether dimensions should be transferred along with datasets.
        It can be more efficient to disable this if it is known that all
        dimensions exist.
    update_output_chain : `bool`
        If quantum graph metadata includes output run name and output
        collection which is a chain, update the chain definition to include run
        name as a the first collection in the chain.

    Returns
    -------
    count : `int`
        Actual count of transferred datasets.
    """

    # Read whole graph into memory
    qgraph = QuantumGraph.loadUri(graph)

    # Collect output refs that could be created by this graph.
    output_refs: set[DatasetRef] = set(qgraph.globalInitOutputRefs())
    for task_def in qgraph.iterTaskGraph():
        if refs := qgraph.initOutputRefs(task_def):
            output_refs.update(refs)
    for qnode in qgraph:
        for refs in qnode.quantum.outputs.values():
            output_refs.update(refs)

    # Make QBB, its config is the same as output Butler.
    qbb = QuantumBackedButler.from_predicted(
        config=dest,
        predicted_inputs=[ref.getCheckedId() for ref in output_refs],
        predicted_outputs=[],
        dimensions=qgraph.universe,
        datastore_records={},
    )

    dest_butler = Butler(dest, writeable=True)

    transferred = dest_butler.transfer_from(
        qbb,
        output_refs,
        transfer="auto",
        register_dataset_types=register_dataset_types,
        transfer_dimensions=transfer_dimensions,
    )
    count = len(transferred)

    # If anything was transferred then update output chain definition if asked.
    if count > 0 and update_output_chain and (metadata := qgraph.metadata) is not None:
        # These are defined in CmdLineFwk.
        output_run = metadata.get("output_run")
        output = metadata.get("output")
        input = metadata.get("input")
        if output_run is not None and output is not None:
            _update_chain(dest_butler.registry, output, output_run, input)

    return count


def _update_chain(registry: Registry, output_chain: str, output_run: str, inputs: list[str] | None) -> None:
    """Update chain definition if it exists to include run as the first item
    in a chain. If it does not exist then create it to include all inputs and
    output.
    """
    try:
        # If output_chain is not a chain the exception will be raised.
        chain_definition = list(registry.getCollectionChain(output_chain))
    except MissingCollectionError:
        # We have to create chained collection to include inputs and output run
        # (this reproduces logic in CmdLineFwk).
        registry.registerCollection(output_chain, type=CollectionType.CHAINED)
        chain_definition = list(registry.queryCollections(inputs, flattenChains=True)) if inputs else []
        chain_definition = [output_run] + [run for run in chain_definition if run != output_run]
        registry.setCollectionChain(output_chain, chain_definition)
    else:
        # If run is in the chain but not the first item then remove it, will
        # re-insert at front below.
        try:
            index = chain_definition.index(output_run)
            if index == 0:
                # It is already at the top.
                return
            else:
                del chain_definition[index]
        except ValueError:
            pass

        chain_definition.insert(0, output_run)
        registry.setCollectionChain(output_chain, chain_definition)
