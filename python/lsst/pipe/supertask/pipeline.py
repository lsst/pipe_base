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

"""Module defining Pipeline class and related methods.
"""

from __future__ import absolute_import, division, print_function

# "exported" names
__all__ = ["Pipeline", "TaskDef"]

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

class TaskDef(object):
    """TaskDef is a collection of information about task needed by Pipeline.

    The information includes task name, configuration object and optional
    task class. This class is just a collection of attributes and it exposes
    all of them so that attributes could potentially be modified in place
    (e.g. if configuration needs extra overrides).

    Attributes
    ----------
    taskName : str
        SuperTask class name, currently it is not specified whether this is a
        fully-qualified name or partial name (e.g. ``module.TaskClass``).
        Framework should be prepared to handle all cases.
    config : `pex_config.Config`
        Instance of the configuration class corresponding to this task class,
        usually with all overrides applied.
    taskClass : type or None
        SuperTask class object, can be None. If None then framework will have
        to locate and load class.
    label : str, optional
        SuperTask class object, can be None. If None then framework will have
        to locate and load class.
    """
    def __init__(self, taskName, config, taskClass=None, label=""):
        self.taskName = taskName
        self.config = config
        self.taskClass = taskClass
        self.label = label

    def __str__(self):
        rep = "TaskDef(" + self.taskName
        if self.label:
            rep += ", label=" + self.label
        rep += ")"
        return rep

class Pipeline(list):
    """Pipeline is a sequence of TaskDef objects.

    Pipeline is given as one of the inputs to a supervising framework
    which builds execution graph out of it. Pipeline contains a sequence
    of `TaskDef` instances.

    Main purpose of this class is to provide a mechanism to pass pipeline
    definition from users to supervising framework. That mechanism is
    implemented using simple serialization and de-serialization via
    pickle. Note that pipeline serialization is not guaranteed to be
    compatible between different versions or releases.

    In current implementation Pipeline is a list (it inherits from `list`)
    and one can use all list methods on pipeline. Content of the pipeline
    can be modified, it is up to the client to verify that modifications
    leave pipeline in a consistent state. One could modify container
    directly by adding or removing its elements.

    Parameters
    ----------
    pipeline : iterable of `TaskDef` instances, optional
        Initial sequence of tasks.
    """
    def __init__(self, iterable=None):
        list.__init__(self, iterable or [])

    def labelIndex(self, label):
        """Return task index given its label, returns -1 if label is not found
        """
        for idx, taskDef in enumerate(self):
            if taskDef.label == label:
                return idx
        return -1

    def __str__(self):
        infos = [str(tdef) for tdef in self]
        return "Pipeline({})".format(", ".join(infos))
