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

"""A tool for aggregating provenance information, logs, and metadata from a
processing run into a `ProvenanceQuantumGraph`.

The aggregator uses multiple processes (typically) or threads to gather
information about a processing run that has already completed, ingest the
output datasets into a butler repository, and write provenance information to
disk.

The only public entry point is the `aggregate_graph` function, with all options
controlled by an `AggregatorConfig` instance.
Usually the tool is invoked from the command line as
``butler aggregate-graph``.

Most log messages are sent to the ``aggregate-graph`` log, which users may want
to set to ``VERBOSE`` to get additional detail, including periodic logging.
See `AggregatorConfig` for how to enable additional logging by each process.
"""

from __future__ import annotations

__all__ = ("AggregatorConfig", "FatalWorkerError", "aggregate_graph")

from ._config import AggregatorConfig
from ._supervisor import aggregate_graph
from ._communicators import FatalWorkerError

#
# Aggregator Design Notes and Package Layout
# ==========================================
#
# The aggregator tool is designed to run in multiple processes.  Using threads
# instead is supported, but mostly as a way to leverage the GIL to (mostly)
# only occupy a single core without having to maintain two largely distinct
# implementations.  Throughout the rest of these notes I'll just assume they're
# processes.
#
# There are four types of processes, each represented in code as a class in its
# own module:
#
# - The Supervisor runs in the main process and is responsible for interacting
#   with the user (reporting progress, handling KeyboardInterrupt) and
#   traversing the predicted quantum graph.
#
# - Scanner workers are responsible for reading log and metadata files using a
#   QBB, occasionally checking for the existence of other outputs (we can
#   usually get the information about which outputs exist from the metadata)
#   and sending the information we extract from them to other workers. Scanners
#   are the only kind of worker we can add more of when we are given more
#   cores, so we try to do as much pre-processing as possible on them (e.g.
#   compression, datastore record extraction).
#
# - The ingester worker is responsible for ingesting output datasets into the
#   central butler repository.  It batches up ingest requests from the scanners
#   until the batch size exceeds a configurable limit (e.g. 10k datasets), and
#   then ingests those pending requests in a single transaction.
#
# - The writer worker is responsible for actually writing the
#   `ProvenanceQuantumGraph` file.  Unlike the other workers, it needs to read
#   almost the entire predicted quantum graph file before it starts, so it can
#   take a few minutes before it starts its main loop of accepting write
#   requests from the scanners (most of the time) and the supervisor (only for
#   blocked quanta, which we don't bother to scan).  For each of the big
#   multi-block files (datasets, quanta, logs, metadata) the writes opens and
#   writes to a temporary file, which is moved into the provenance QG zip
#   archive at the end.  In addition, to try to keep expensive operations on
#   the scanners, the writer builds a ZStandard compression dictionary from the
#   first write requests it sees, and then sends that to the scanners so they
#   can take over doing the compression after that is complete. Since writing
#   the provenance graph is (at present) optional, sometimes there is no writer
#   worker.
#
# Each process is also associated with its own "communicator" class, which are
# all defined together in the _communicators.py file.  The communicators manage
# the queues that are used to send information between processes, as well as
# providing loggers and some other common utility functionality. Communicators
# are context managers that take care of cleanly shutting down each process
# when it completes or is interrupted; `multiprocessing.Queue` objects need to
# be cleared out carefully or you get deadlocks that prevent processes from
# actually exiting, and so a lot of the work of the communicators involves
# passing sentinal objects over those queues so we can identify when they
# really are empty.
#
# The other modules in the packages include:
#
# - _config.py: the `AggregatorConfig` model, which is essentially the public
#   interface for controlling the aggregator.
#
# - _progress.py: helper objects for [periodic] logging and tqdm progress bars.
#   It's not clear the tqdm stuff should continue to exist once development of
#   the tool is complete and we stop wanting to run it from an interactive
#   terminal, but it's certainly been useful for development so far.
#
# - _structs.py: simple dataclasses used by multiple workers (most of what we
#   put on the queues to request scans/ingests/writes);
#
# The main logger used by the aggregator is called
# ``lsst.pipe.base.quantum_graph.aggregator``, and it emits at ``VERBOSE`` and
# above.  At ``VERBOSE``, it's also used for periodic reporting of status
# (unless the tqdm progress bars are enabled instead). Each worker also has its
# own log (including the supervisor) with names like
# ``lsst.pipe.base.quantum_graph.aggregator.scanner-012`` that include
# additional information at ``DEBUG`` and above, but these are *not* propagated
# up; instead there's a config option to write per-worker logs to separate
# files in a directory.  It would probably be more idiomatic to set up a
# QueueHandler to feed all the LogRecords the supervisor and use Python logging
# configuration to send them wherever desired, but::
#
# - that seemed to require more wading into `lsst.daf.butler.cli.cliLog` a lot
#   more than I cared to (especially considering that we want this to work in
#   `threading` and `multiprocessing`, and logging configuration is very
#   different in those two contexts);
#
# - having the worker logs go to separate files is actually very nice, and it's
#   more efficient if they just do that themselves, and that's not something
#   our logging CLI can actually do, AFAICT.

