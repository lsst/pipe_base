#!/usr/bin/env python
#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase

class ExampleConfig(pexConfig.Config):
    """Config for argument parser example
    """
    int = pexConfig.Field(
        dtype = int,
        doc = "Example integer value",
        default = 1,
        optional = False,
    )
    float = pexConfig.Field(
        dtype = float,
        doc = "Example float value",
        default = 3.14159265358979,
        optional = False,
    )
    intList = pexConfig.ListField(
        dtype = int,
        doc = "example list of integers",
        default = [-1, 0, 1],
        optional = False,
    )


parser = pipeBase.ArgumentParser()
namespace = parser.parse_args(config=ExampleConfig())
print "namespace=", namespace
