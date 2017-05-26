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

"""
Module defining Pipeline class and related methods.
"""

from __future__ import absolute_import, division, print_function

# "exported" names
__all__ = ["Pipeline"]

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

class Pipeline(list):
    """Pipeline is a sequence of SuperTasks and their corresponding
    configuration objects.

    Pipeline is given as one of the inputs to a supervising framework
    which builds execution graph out of it. Pipeline contains a sequence
    of SuperTasks and their configuration objects. SuperTask in a
    pipeline is just a name of a SuperTask class. Configuration is
    usually a `pex_config.Config` instance with all necessary overrides
    applied.

    Main purpose of this class is to provide a mechanism to pass pipeline
    definition from users to supervising framework. That mechanism is
    implemented using simple serialization and de-serialization via
    pickle. Note that pipeline serialization is not guaranteed to be
    compatible between different versions or releases.

    In current implementation Pipeline is a list (it inherits from list)
    and one can use all list methods on pipeline. Content of the pipeline
    can be modified, it is up to the client to verify that modifications
    leave pipeline in a consistent state. One could modify container
    directly by adding or removing its elements which are tuples of
    (task_name, config_instance).

    Parameters
    ----------
    pipeline : iterable of (taskName, config) tuples, optional
        Initial pipeline.
    """
    def __init__(self, iterable=None):
        list.__init__(self, iterable or [])
