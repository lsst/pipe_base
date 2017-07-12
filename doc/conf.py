#!/usr/bin/env python

import lsst.pipe.base
from documenteer.sphinxconfig.stackconf import build_package_configs


_g = globals()
_g.update(build_package_configs(
    project_name='pipe_base',
    version=lsst.pipe.base.version.__version__))
