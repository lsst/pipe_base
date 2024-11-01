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

__all__ = ["zip_from_graph"]

import logging
import re

from lsst.daf.butler import QuantumBackedButler
from lsst.daf.butler.utils import globToRegex
from lsst.pipe.base import QuantumGraph
from lsst.resources import ResourcePath

_LOG = logging.getLogger(__name__)


def zip_from_graph(
    graph: str,
    repo: str,
    dest: str,
    dataset_type: tuple[str, ...],
) -> ResourcePath:
    """Create Zip export file from graph outputs.

    Parameters
    ----------
    graph : `str`
        URI string of the quantum graph.
    repo : `str`
        URI to a butler configuration used to define the datastore associated
        with the graph.
    dest : `str`
        Path to the destination directory for the Zip file.
    dataset_type : `tuple` of `str`
        Dataset type names. An empty tuple implies all dataset types.
        Can include globs.

    Returns
    -------
    zip_path : `lsst.resources.ResourcePath`
        Path to the Zip file.
    """
    # Read whole graph into memory
    qgraph = QuantumGraph.loadUri(graph)

    output_refs, _ = qgraph.get_refs(include_outputs=True, include_init_outputs=True, conform_outputs=True)

    # Get data repository dataset type definitions from the QuantumGraph.
    dataset_types = {dstype.name: dstype for dstype in qgraph.registryDatasetTypes()}

    # Make QBB, its config is the same as output Butler.
    qbb = QuantumBackedButler.from_predicted(
        config=repo,
        predicted_inputs=[ref.id for ref in output_refs],
        predicted_outputs=[],
        dimensions=qgraph.universe,
        datastore_records={},
        dataset_types=dataset_types,
    )

    # Filter the refs based on requested dataset types.
    regexes = globToRegex(dataset_type)
    if regexes is ...:
        filtered_refs = output_refs
    else:

        def _matches(dataset_type_name: str, regexes: list[str | re.Pattern]) -> bool:
            for regex in regexes:
                if isinstance(regex, str):
                    if dataset_type_name == regex:
                        return True
                elif regex.search(dataset_type_name):
                    return True
            return False

        filtered_refs = {ref for ref in output_refs if _matches(ref.datasetType.name, regexes)}

    _LOG.info("Retrieving artifacts for %d datasets and storing in Zip file.", len(output_refs))
    zip = qbb.retrieve_artifacts_zip(filtered_refs, dest)
    return zip
