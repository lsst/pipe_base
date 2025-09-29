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

from typing import Any

import click

from lsst.daf.butler.cli.opt import (
    dataset_type_option,
    options_file_option,
    register_dataset_types_option,
    repo_argument,
    transfer_dimensions_option,
    transfer_option,
)
from lsst.daf.butler.cli.utils import ButlerCommand, split_commas, unwrap

from ... import script
from ..opt import instrument_argument, update_output_chain_option


@click.command(short_help="Add an instrument definition to the repository", cls=ButlerCommand)
@repo_argument(required=True)
@instrument_argument(required=True, nargs=-1, help="The fully-qualified name of an Instrument subclass.")
@click.option("--update", is_flag=True)
def register_instrument(*args: Any, **kwargs: Any) -> None:
    """Add an instrument to the data repository."""
    script.register_instrument(*args, **kwargs)


@click.command(short_help="Transfer datasets from a graph to a butler.", cls=ButlerCommand)
@click.argument("graph", required=True)
@click.argument("dest", required=True)
@register_dataset_types_option()
@transfer_dimensions_option(default=False)
@update_output_chain_option()
@click.option(
    "--dry-run", is_flag=True, default=False, help="Run the transfer but do not update the destination butler"
)
@dataset_type_option(help="Subset of dataset types to transfer from graph.")
@options_file_option()
def transfer_from_graph(**kwargs: Any) -> None:
    """Transfer datasets from a quantum graph to a destination butler.

    GRAPH is a URI to the source quantum graph file.

    DEST is a URI to the Butler repository that will receive copies of the
    datasets.
    """
    number = script.transfer_from_graph(**kwargs)
    print(f"Number of datasets transferred: {number}")


@click.command(short_help="Make Zip archive from output files using graph.", cls=ButlerCommand)
@click.argument("graph", required=True)
@repo_argument(
    required=True,
    help="REPO is a URI to a butler configuration that is used to configure "
    "the datastore of the quantum-backed butler.",
)
@click.argument("dest", required=True)
@dataset_type_option(help="Dataset types to include in Zip archive.")
@options_file_option()
def zip_from_graph(**kwargs: Any) -> None:
    """Transfer datasets from a quantum graph to a Zip archive.

    GRAPH is a URI to the source quantum graph file to use when building the
    Zip archive.

    DEST is a directory to write the Zip archive.
    """
    zip = script.zip_from_graph(**kwargs)
    print(f"Zip archive written to {zip}")


@click.command(short_help="Retrieve artifacts from subset of graph.", cls=ButlerCommand)
@click.argument("graph", required=True)
@repo_argument(
    required=True,
    help="REPO is a URI to a butler configuration that is used to configure "
    "the datastore of the quantum-backed butler.",
)
@click.argument("dest", required=True)
@transfer_option()
@click.option(
    "--preserve-path/--no-preserve-path",
    is_flag=True,
    default=True,
    help="Preserve the datastore path to the artifact at the destination.",
)
@click.option(
    "--clobber/--no-clobber",
    is_flag=True,
    default=False,
    help="If clobber, overwrite files if they exist locally.",
)
@click.option(
    "--qgraph-node-id",
    callback=split_commas,
    multiple=True,
    help=unwrap(
        """Only load a specified set of nodes when graph is
        loaded from a file, nodes are identified by UUID
        values. One or more comma-separated strings are
        accepted. By default all nodes are loaded. Ignored if
        graph is not loaded from a file."""
    ),
)
@click.option(
    "--include-inputs/--no-include-inputs",
    is_flag=True,
    default=True,
    help="Whether to include input datasets in retrieval.",
)
@click.option(
    "--include-outputs/--no-include-outputs",
    is_flag=True,
    default=True,
    help="Whether to include output datasets in retrieval.",
)
@options_file_option()
def retrieve_artifacts_for_quanta(**kwargs: Any) -> None:
    """Retrieve artifacts from given quanta defined in quantum graph.

    GRAPH is a URI to the source quantum graph file to use when building the
    Zip archive.

    DEST is a directory to write the Zip archive.
    """
    artifacts = script.retrieve_artifacts_for_quanta(**kwargs)
    print(f"Written {len(artifacts)} artifacts to {kwargs['dest']}.")


@click.command(short_help="Scan for the outputs of an active or completed quantum graph.", cls=ButlerCommand)
@repo_argument(required=False, help="Path to the central butler repository.")
@click.option("-g", "--predicted-graph", help="Path to the predicted quantum graph file.")
@click.option("-d", "--db-dir", help="Directory for the scanner's SQLite database files (POSIX only).")
@click.option("-o", "--output", help="Path to the output provenance quantum graph.")
@click.option(
    "--checkpoint-dir",
    help=(
        "Path to a persistent storage location to copy the scanner databases "
        "to periodically, if --db-dir is not persistent."
    ),
)
@click.option("-c", "--config", help="Path to a scanner configuration JSON file.")
@click.option(
    "-j",
    "--processes",
    type=click.IntRange(min=1),
    help="Number of processes to use.",
)
@click.option(
    "--assume-complete",
    "-a",
    "assume_complete",
    flag_value=True,
    default=None,
    help="Assume the quantum graph has been executed to completion.",
)
@click.option(
    "--assume-incomplete",
    "assume_complete",
    default=None,
    flag_value=False,
    help="Do not assume the quantum graph has been executed to completion (failures may be retried).",
)
@click.option(
    "--dry-run",
    flag_value=True,
    default=None,
    help="Do not actually perform any central database ingests or dataset artifact deletions.",
)
@click.option(
    "--interactive-status",
    "interactive_status",
    flag_value=True,
    default=None,
    help="Use progress bars for status reporting.",
)
@click.option(
    "--no-interactive-status",
    "interactive_status",
    flag_value=False,
    default=None,
    help="Use periodic logging for status reporting.",
)
@click.option(
    "--mock-storage-classes",
    "mock_storage_classes",
    flag_value=True,
    default=None,
    help="Enable support for storage classes by created by the lsst.pipe.base.tests.mocks package.",
)
@click.option(
    "--no-mock-storage-classes",
    "mock_storage_classes",
    flag_value=False,
    default=None,
    help="Do not support for storage classes by created by the lsst.pipe.base.tests.mocks package.",
)
@click.option(
    "--register-dataset-types",
    "register_dataset_types",
    flag_value=True,
    default=None,
    help="Register output dataset types before starting ingest.",
)
@click.option(
    "--no-register-dataset-types",
    "register_dataset_types",
    flag_value=False,
    default=None,
    help="Do not register output dataset types before starting ingest.",
)
@click.option(
    "--update-output-chain",
    "update_output_chain",
    flag_value=True,
    default=None,
    help="Prepend the output RUN collection to the output CHAINED collection after finishing ingest",
)
@click.option(
    "--no-update-output-chain",
    "update_output_chain",
    flag_value=False,
    default=None,
    help="Do not prepend the output RUN collection to the output CHAINED collection after finishing ingest",
)
def aggregate_graph(**kwargs: Any) -> None:
    """Scan for quantum graph's outputs to gather provenance, ingest datasets
    into the central butler repository, and delete datasets that are no
    longer needed.

    REPO is the path to the central data repository.

    If a configuration file path is not provided via -c/--config, the
    --predicted-graph, --db-dir, and --output options are required.
    """
    from ...script.aggregate_graph import aggregate_graph

    aggregate_graph(**kwargs)
