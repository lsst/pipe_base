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

"""Define `PipelineTask` class and related methods."""

from __future__ import annotations

__all__ = ["PipelineTask"]  # Classes in this module

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar

from .connections import InputQuantizedConnection, OutputQuantizedConnection
from .task import Task

if TYPE_CHECKING:
    import logging

    from lsst.utils.logging import LsstLogAdapter

    from ._quantumContext import QuantumContext
    from .config import PipelineTaskConfig
    from .struct import Struct


class PipelineTask(Task):
    """Base class for all pipeline tasks.

    This is an abstract base class for PipelineTasks which represents an
    algorithm executed by framework(s) on data which comes from data butler,
    resulting data is also stored in a data butler.

    PipelineTask inherits from a `~lsst.pipe.base.Task` and uses the same
    configuration mechanism based on :ref:`lsst.pex.config`. `PipelineTask`
    classes also have a `PipelineTaskConnections` class associated with their
    config which defines all of the IO a `PipelineTask` will need to do.
    PipelineTask sub-class typically implements `run()` method which receives
    Python-domain data objects and returns `lsst.pipe.base.Struct` object with
    resulting data. `run()` method is not supposed to perform any I/O, it
    operates entirely on in-memory objects. `runQuantum()` is the method (can
    be re-implemented in sub-class) where all necessary I/O is performed, it
    reads all input data from data butler into memory, calls `run()` method
    with that data, examines returned `Struct` object and saves some or all of
    that data back to data butler. `runQuantum()` method receives a
    `QuantumContext` instance to facilitate I/O, a `InputQuantizedConnection`
    instance which defines all input `lsst.daf.butler.DatasetRef`, and a
    `OutputQuantizedConnection` instance which defines all the output
    `lsst.daf.butler.DatasetRef` for a single invocation of PipelineTask.

    Subclasses must be constructable with exactly the arguments taken by the
    PipelineTask base class constructor, but may support other signatures as
    well.

    Attributes
    ----------
    canMultiprocess : bool, True by default (class attribute)
        This class attribute is checked by execution framework, sub-classes
        can set it to ``False`` in case task does not support multiprocessing.

    Parameters
    ----------
    config : `~lsst.pex.config.Config`, optional
        Configuration for this task (an instance of ``self.ConfigClass``,
        which is a task-specific subclass of `PipelineTaskConfig`).
        If not specified then it defaults to ``self.ConfigClass()``.
    log : `logging.Logger`, optional
        Logger instance whose name is used as a log name prefix, or ``None``
        for no prefix.
    initInputs : `dict`, optional
        A dictionary of objects needed to construct this PipelineTask, with
        keys matching the keys of the dictionary returned by
        `getInitInputDatasetTypes` and values equivalent to what would be
        obtained by calling `~lsst.daf.butler.Butler.get` with those
        DatasetTypes and no data IDs.  While it is optional for the base class,
        subclasses are permitted to require this argument.
    **kwargs : `~typing.Any`
        Arbitrary parameters, passed to base class constructor.
    """

    ConfigClass: ClassVar[type[PipelineTaskConfig]]
    canMultiprocess: ClassVar[bool] = True

    def __init__(
        self,
        *,
        config: PipelineTaskConfig | None = None,
        log: logging.Logger | LsstLogAdapter | None = None,
        initInputs: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(config=config, log=log, **kwargs)

    run: Callable[..., Struct]  # The 'run' method for subclasses will have a different signature

    def run(self, **kwargs: Any) -> Struct:  # type: ignore
        """Run task algorithm on in-memory data.

        This method should be implemented in a subclass. This method will
        receive keyword-only arguments whose names will be the same as names of
        connection fields describing input dataset types. Argument values will
        be data objects retrieved from data butler. If a dataset type is
        configured with ``multiple`` field set to ``True`` then the argument
        value will be a list of objects, otherwise it will be a single object.

        If the task needs to know its input or output DataIds then it also has
        to override the `runQuantum` method.

        This method should return a `Struct` whose attributes share the same
        name as the connection fields describing output dataset types.

        Parameters
        ----------
        **kwargs : `~typing.Any`
            Arbitrary parameters accepted by subclasses.

        Returns
        -------
        struct : `Struct`
            Struct with attribute names corresponding to output connection
            fields.

        Examples
        --------
        Typical implementation of this method may look like:

        .. code-block:: python

            def run(self, *, input, calib):
                # "input", "calib", and "output" are the names of the
                # connection fields.

                # Assuming that input/calib datasets are `scalar` they are
                # simple objects, do something with inputs and calibs, produce
                # output image.
                image = self.makeImage(input, calib)

                # If output dataset is `scalar` then return object, not list
                return Struct(output=image)
        """
        raise NotImplementedError("run() is not implemented")

    def runQuantum(
        self,
        butlerQC: QuantumContext,
        inputRefs: InputQuantizedConnection,
        outputRefs: OutputQuantizedConnection,
    ) -> None:
        """Do butler IO and transform to provide in memory
        objects for tasks `~Task.run` method.

        Parameters
        ----------
        butlerQC : `QuantumContext`
            A butler which is specialized to operate in the context of a
            `lsst.daf.butler.Quantum`.
        inputRefs : `InputQuantizedConnection`
            Datastructure whose attribute names are the names that identify
            connections defined in corresponding `PipelineTaskConnections`
            class. The values of these attributes are the
            `lsst.daf.butler.DatasetRef` objects associated with the defined
            input/prerequisite connections.
        outputRefs : `OutputQuantizedConnection`
            Datastructure whose attribute names are the names that identify
            connections defined in corresponding `PipelineTaskConnections`
            class. The values of these attributes are the
            `lsst.daf.butler.DatasetRef` objects associated with the defined
            output connections.
        """
        inputs = butlerQC.get(inputRefs)
        outputs = self.run(**inputs)
        butlerQC.put(outputs, outputRefs)
