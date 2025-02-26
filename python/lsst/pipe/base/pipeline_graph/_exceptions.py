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
from __future__ import annotations

__all__ = (
    "ConnectionTypeConsistencyError",
    "DuplicateOutputError",
    "EdgesChangedError",
    "IncompatibleDatasetTypeError",
    "InvalidStepsError",
    "PipelineDataCycleError",
    "PipelineGraphError",
    "PipelineGraphExceptionSafetyError",
    "PipelineGraphReadError",
    "TaskNotImportedError",
    "UnresolvedGraphError",
)


class PipelineGraphError(RuntimeError):
    """Base exception raised when there is a problem constructing or resolving
    a pipeline graph.
    """


class DuplicateOutputError(PipelineGraphError):
    """Exception raised when multiple tasks in one pipeline produce the same
    output dataset type.
    """


class PipelineDataCycleError(PipelineGraphError):
    """Exception raised when a pipeline graph contains a cycle."""


class ConnectionTypeConsistencyError(PipelineGraphError):
    """Exception raised when the tasks in a pipeline graph use different (and
    incompatible) connection types for the same dataset type.
    """


class IncompatibleDatasetTypeError(PipelineGraphError):
    """Exception raised when the tasks in a pipeline graph define dataset types
    with the same name in incompatible ways, or when these are incompatible
    with the data repository definition.
    """


class UnresolvedGraphError(PipelineGraphError):
    """Exception raised when an operation requires dimensions or dataset types
    to have been resolved, but they have not been.
    """


class PipelineGraphReadError(PipelineGraphError, IOError):
    """Exception raised when a serialized PipelineGraph cannot be read."""


class TaskNotImportedError(PipelineGraphError):
    """Exception raised when accessing an attribute of a graph or graph node
    that is not available unless the task class has been imported and
    configured.
    """


class EdgesChangedError(PipelineGraphError):
    """Exception raised when the edges in one version of a pipeline graph
    are not consistent with those in another, but they were expected to be.
    """


class PipelineGraphExceptionSafetyError(PipelineGraphError):
    """Exception raised when a PipelineGraph method could not provide strong
    exception safety, and the graph may have been left in an inconsistent
    state.

    The originating exception is always chained when this exception is raised.
    """


class InvalidStepsError(PipelineGraphError):
    """Exception raised when the step definitions are invalid."""
