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

__all__ = ["transfer_from_graph"]

import math

from lsst.daf.butler import Butler, CollectionType, MissingCollectionError, QuantumBackedButler
from lsst.pipe.base import QuantumGraph
from lsst.utils.iteration import chunk_iterable
from lsst.utils.logging import getLogger

from .utils import filter_by_dataset_type_glob, filter_by_existence

_LOG = getLogger(__name__)


def transfer_from_graph(
    graph: str,
    dest: str,
    register_dataset_types: bool,
    transfer_dimensions: bool,
    update_output_chain: bool,
    dry_run: bool,
    dataset_type: tuple[str, ...],
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
    dry_run : `bool`
        Run the transfer without updating the destination butler.
    dataset_type : `tuple` of `str`
        Dataset type names. An empty tuple implies all dataset types.
        Can include globs.

    Returns
    -------
    count : `int`
        Actual count of transferred datasets.
    """
    # Read whole graph into memory
    qgraph = QuantumGraph.loadUri(graph)

    output_refs, _ = qgraph.get_refs(include_outputs=True, include_init_outputs=True, conform_outputs=True)

    # Get data repository dataset type definitions from the QuantumGraph.
    dataset_types = {dstype.name: dstype for dstype in qgraph.registryDatasetTypes()}

    # Make QBB, its config is the same as output Butler.
    qbb = QuantumBackedButler.from_predicted(
        config=dest,
        predicted_inputs=[ref.id for ref in output_refs],
        predicted_outputs=[],
        dimensions=qgraph.universe,
        datastore_records={},
        dataset_types=dataset_types,
    )

    # Filter the refs based on requested dataset types.
    filtered_refs = filter_by_dataset_type_glob(output_refs, dataset_type)
    _LOG.verbose("After filtering by dataset_type, number of datasets to transfer: %d", len(filtered_refs))

    dest_butler = Butler.from_config(dest, writeable=True)

    # For faster restarts, filter out those the destination already knows.
    filtered_refs = filter_by_existence(dest_butler, filtered_refs)

    # Transfer in chunks
    chunk_size = 50_000
    n_chunks = math.ceil(len(filtered_refs) / chunk_size)
    chunk_num = 0
    count = 0
    for chunk in chunk_iterable(filtered_refs, chunk_size=chunk_size):
        chunk_num += 1
        if n_chunks > 1:
            _LOG.verbose("Transferring %d datasets in chunk %d/%d", len(chunk), chunk_num, n_chunks)
        transferred = dest_butler.transfer_from(
            qbb,
            chunk,
            transfer="auto",
            register_dataset_types=register_dataset_types,
            transfer_dimensions=transfer_dimensions,
            dry_run=dry_run,
        )
        count += len(transferred)

    # If asked to do so, update output chain definition.
    if update_output_chain and (metadata := qgraph.metadata) is not None:
        # These are defined in CmdLineFwk.
        output_run = metadata.get("output_run")
        output = metadata.get("output")
        input = metadata.get("input")
        if output_run is not None and output is not None:
            _update_chain(dest_butler, output, output_run, input)

    return count


def _update_chain(butler: Butler, output_chain: str, output_run: str, inputs: list[str] | None) -> None:
    """Update chain definition if it exists to include run as the first item
    in a chain. If it does not exist then create it to include all inputs and
    output.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler where to update the collection chain.
    output_chain : `str`
        Name of the output CHAINED collection.
    output_run : `str`
        Name of the output RUN collection.
    inputs : `list` [`str`] | None
        All the input collections to be included in the chain if the
        chain is created.
    """
    # Do not need to update chain if output_run does not already exist.
    try:
        _ = butler.collections.get_info(output_run)
    except MissingCollectionError:
        _LOG.verbose(
            "Output RUN collection (%s) does not exist.  Skipping updating the output chain.",
            output_run,
        )
        return

    # Make chain collection if doesn't exist before calling prepend_chain.
    created_now = butler.collections.register(output_chain, CollectionType.CHAINED)
    if created_now:
        _LOG.verbose("Registered chain collection: %s", output_chain)
        if inputs:
            # First must flatten any input chains
            flattened = butler.collections.query(inputs, flatten_chains=True)

            # Add input collections to chain collection just made.  Using
            # extend instead of prepend in case of race condition where another
            # execution adds a run before this adds the inputs to the chain.
            butler.collections.extend_chain(output_chain, flattened)
    _LOG.verbose(
        "Prepending output chain collection (%s) with output RUN collection (%s)", output_chain, output_run
    )
    butler.collections.prepend_chain(output_chain, output_run)
