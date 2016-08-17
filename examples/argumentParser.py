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
"""Example showing use of the argument parser

Here are some examples that use the repository in obs_test (which is automatically setup
when pipe_base is setup):

./argumentParser.py $OBS_TEST_DIR/data/input --help
./argumentParser.py $OBS_TEST_DIR/data/input --id --show config data
./argumentParser.py $OBS_TEST_DIR/data/input --id filter=g --show data
./argumentParser.py $OBS_TEST_DIR/data/input --id filter=g --config oneFloat=1.5 --show config
"""
import lsst.pex.config as pexConfig
import lsst.pipe.base as pipeBase


class ExampleConfig(pexConfig.Config):
    """Config for argument parser example
    """
    oneInt = pexConfig.Field(
        dtype=int,
        doc="Example integer value",
        default=1,
    )
    oneFloat = pexConfig.Field(
        dtype=float,
        doc="Example float value",
        default=3.14159265358979,
    )
    oneStr = pexConfig.Field(
        dtype=str,
        doc="Example string value",
        default="default value",
    )
    intList = pexConfig.ListField(
        dtype=int,
        doc="example list of integers",
        default=[-1, 0, 1],
    )
    floatList = pexConfig.ListField(
        dtype=float,
        doc="example list of floats",
        default=[-2.7, 0, 3.7e42],
    )
    strList = pexConfig.ListField(
        dtype=str,
        doc="example list of strings",
        default=["a", "bb", "ccc"],
    )

parser = pipeBase.ArgumentParser(name="argumentParser")
parser.add_id_argument("--id", "raw", "data identifier", level="sensor")
config = ExampleConfig()
parsedCmd = parser.parse_args(config=config)
pcDict = parsedCmd.__dict__
for key in sorted(pcDict):
    print "parsedCmd.%s=%r" % (key, pcDict[key])
