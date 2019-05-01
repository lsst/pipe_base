# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

"""Helper classes for specifying the multiplicity of datasets used by
PipelineTasks.

The symbols defined in this module are not automatically lifted into the main
lsst.pipe.base namespace; it is expected that instead the whole module will
be imported and used:

    from lsst.pipe.base import multiplicity

    m = multiplicity.Scalar()

"""

__all__ = ["Scalar", "Multiple", "Multiplicity", "MultiplicityError"]

from abc import ABC, abstractmethod


class MultiplicityError(TypeError):
    """Exception raised when the number of instances of a dataset type
    in a quantum do not match its declared multiplicity.
    """
    pass


class Multiplicity(ABC):

    @abstractmethod
    def isRequired(self):
        raise NotImplementedError()

    @abstractmethod
    def check(self, count, key, name):
        raise NotImplementedError()

    @abstractmethod
    def adaptSequenceToArg(self, sequence):
        raise NotImplementedError()

    @abstractmethod
    def adaptResultToSequence(self, arg):
        raise NotImplementedError()


class Scalar(Multiplicity):

    def __init__(self, optional=False):
        self.optional = optional

    def isRequired(self):
        return not self.optional

    def check(self, count, key, name):
        assert count >= 0
        if count == 0:
            if not self.optional:
                raise MultiplicityError(f"No instances found for dataset {name} ({key}).")
        elif count > 1:
            expected = "at most" if self.optional else "exactly"
            raise MultiplicityError(f"Got {count} instances of dataset {name} ({key}); "
                                    f"expected {expected} 1.")

    def adaptSequenceToArg(self, sequence):
        if len(sequence) == 1:
            return sequence[0]
        else:
            assert self.optional and len(sequence) == 0
            return None

    def adaptResultToSequence(self, arg):
        if arg is None:
            return []
        else:
            return [arg]


class Multiple(Multiplicity):

    def __init__(self, minimum=1, maximum=None):
        self.minimum = minimum
        self.maximum = maximum

    def isRequired(self):
        return self.minimum >= 1

    def check(self, count, key, name):
        if count < self.minimum:
            raise MultiplicityError(f"Got {count} instances for dataset {name} ({key}); "
                                    f"expected at least {self.minimum}.")
        if self.maximum is not None and count > self.maximum:
            raise MultiplicityError(f"Got {count} instances for dataset {name} ({key}); "
                                    f"expected at most {self.maximum}.")

    def adaptSequenceToArg(self, sequence):
        return sequence

    def adaptResultToSequence(self, result):
        return result
