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

"""Collection of common methods for use in unit tests."""

from __future__ import annotations

from lsst.daf.butler import DatasetRef

from ..graph import QuantumGraph


def check_output_run(graph: QuantumGraph, run: str) -> list[DatasetRef]:
    """Check that all output and intermediate datasets belong to a
    specified run.

    Parameters
    ----------
    graph : `QuantumGraph`
        Quantum graph.
    run : `str`
        Output run name.

    Returns
    -------
    refs : `list` [ `~lsst.daf.butler.DatasetRef` ]
        List of output/intermediate dataset references that do NOT belong to
        the specified run.
    """
    # Collect all inputs/outputs, so that we can build intermediate refs.
    output_refs: list[DatasetRef] = []
    input_refs: list[DatasetRef] = []
    for node in graph:
        for refs in node.quantum.outputs.values():
            output_refs += refs
        for refs in node.quantum.inputs.values():
            input_refs += refs
    for task_def in graph.iterTaskGraph():
        init_refs = graph.initOutputRefs(task_def)
        if init_refs:
            output_refs += init_refs
        init_refs = graph.initInputRefs(task_def)
        if init_refs:
            input_refs += init_refs
    output_refs += graph.globalInitOutputRefs()
    newRefs = [ref for ref in output_refs if ref.run != run]

    output_ids = {ref.id for ref in output_refs}
    intermediates = [ref for ref in input_refs if ref.id in output_ids]
    newRefs += [ref for ref in intermediates if ref.run != run]

    return newRefs


def get_output_refs(graph: QuantumGraph) -> list[DatasetRef]:
    """Return all output and intermediate references in a graph.

    Parameters
    ----------
    graph : `QuantumGraph`
        Quantum graph.

    Returns
    -------
    refs : `list` [ `~lsst.daf.butler.DatasetRef` ]
        List of all output/intermediate dataset references, intermediates
        will appear more than once in this list.
    """
    output_refs: set[DatasetRef] = set()
    for node in graph:
        for refs in node.quantum.outputs.values():
            output_refs.update(refs)
    for task_def in graph.iterTaskGraph():
        init_refs = graph.initOutputRefs(task_def)
        if init_refs:
            output_refs.update(init_refs)
    output_refs.update(graph.globalInitOutputRefs())

    result = list(output_refs)

    for node in graph:
        for refs in node.quantum.inputs.values():
            result += [ref for ref in refs if ref in output_refs]
    for task_def in graph.iterTaskGraph():
        init_refs = graph.initInputRefs(task_def)
        if init_refs:
            result += [ref for ref in init_refs if ref in output_refs]

    return result
