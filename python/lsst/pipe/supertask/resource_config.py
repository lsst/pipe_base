"""
Module defining ResourceConfig class and related methods.
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------

#-----------------------------
# Imports for other modules --
#-----------------------------
import lsst.pex.config as pexConfig

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

class ResourceConfig(pexConfig.Config):
    """Configuration for resource requirements.

    This configuration class will be used by some activators to estimate
    resource use by pipeline. Additionally some tasks could use it to adjust
    their resource use (e.g. reduce the number of threads).

    For some resources their limit can be estimated by corresponding task,
    in that case task could set the field value. For many fields defined in
    this class their associated resource used by a task will depend on the
    size of the data and is not known in advance. For these resources their
    value will be configured through overrides based on some external
    estimates.
    """
    min_memory_MB = pexConfig.Field(dtype=int, default=None, optional=True,
                                    doc="Minimal memory needed by task, can be None if estimate is unknown.")
    min_num_cores = pexConfig.Field(dtype=int, default=1,
                                    doc="Minimal number of cores needed by task.")

class ConfigWithResource(pexConfig.Config):
    """Configuration class which includes resource requirements.

    This configuration class can be used as a base class (instead of regular
    `lsst.pex.config.Config`) for configurations of the tasks that need
    to declare their resource use. Alternatively any task configuration
    class can include "resources" field just like this class does. The name
    "resources" is pre-defined field known to activators and used to access
    per-task resource configuration.
    """
    resources = pexConfig.ConfigField(dtype=ResourceConfig, doc=ResourceConfig.__doc__)
