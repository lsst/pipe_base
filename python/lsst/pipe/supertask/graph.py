#
# LSST Data Management System
# Copyright 2017 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

"""Module defining execution graph classes and related methods.

There could be different representations of the pipeline execution
graph depending on the client needs. Presently this module contains
graph implementation which is based on requirements of command-line
environment. In the future we could add other implementations and
methods to convert between those representations.
"""

from __future__ import absolute_import, division, print_function

# "exported" names
__all__ = ["GraphOfTasks", "GraphTaskNodes"]

#--------------------------------
#  Imports of standard modules --
#--------------------------------

#-----------------------------
# Imports for other modules --
#-----------------------------

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

class GraphTaskNodes(object):
    """GraphTaskNodes represents a bunch of nodes in an execution graph.

    The node in SuperTask execution graph is represented by the SuperTask
    and a single Quantum instance. One possible representation of the graph
    is just a list of nodes without edges (edges can be deduced from nodes'
    quantum inputs and outputs if needed). That representation can be
    reduced to the list of SuperTasks and the corresponding list of Quanta.
    This class defines this reduced representation.

    Different frameworks may use different graph representation, this
    representation was based mostly on requirements of command-line
    "activator" which does not need explicit edges information.

    Attributes
    ----------
    taskDef : `TaskDef`
        Task defintion for this set of nodes.
    quanta : `list` of `Quanta`
        List of quanta corresponding to the task.
    """
    def __init__(self, taskDef, quanta):
        self.taskDef = taskDef
        self.quanta = quanta


class GraphOfTasks(list):
    """Graph is a sequence of GraphTaskNodes objects.

    Typically the order of the tasks in the list will be the same as the
    order of tasks in a pipeline (obviously depends on the code which
    constructs graph).

    Parameters
    ----------
    iterable : iterable of `GraphTaskNodes` instances, optional
        Initial sequence of per-task nodes.
    """
    def __init__(self, iterable=None):
        list.__init__(self, iterable or [])
