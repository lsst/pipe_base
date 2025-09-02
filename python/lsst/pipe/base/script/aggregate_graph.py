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

__all__ = ("aggregate_graph",)

import sys

from lsst.resources import ResourcePath, ResourcePathExpression

from ..quantum_graph.aggregator import AggregatorConfig, Supervisor


def aggregate_graph(
    *,
    repo: str | None = None,
    predicted_graph: str | None = None,
    db_dir: str | None = None,
    output: str | None = None,
    checkpoint_dir: str | None = None,
    config: str | None = None,
    processes: int | None = None,
    assume_complete: bool | None = None,
    dry_run: bool | None = None,
    interactive_status: bool | None = None,
    mock_storage_classes: bool = False,
) -> None:
    """Run the provenance scanner with common configuration options split out
    into keyword arguments.
    """
    scanner_config = _assemble_config(
        repo=repo,
        predicted_graph=predicted_graph,
        db_dir=db_dir,
        output=output,
        checkpoint_dir=checkpoint_dir,
        config=config,
        processes=processes,
        assume_complete=assume_complete,
        dry_run=dry_run,
        interactive_status=interactive_status,
        mock_storage_classes=mock_storage_classes,
    )
    try:
        Supervisor.run(scanner_config)
    except TimeoutError as err:
        # Timeouts are not necessaryly considered errors for the CLI, since we
        # want an aggregation job that has done all it can with
        # assume_complete=False to be a normal exit condition.
        if not scanner_config.assume_complete:
            print(err, file=sys.stdout)
        else:
            raise


def _assemble_config(
    *,
    repo: str | None,
    predicted_graph: str | None,
    db_dir: str | None,
    output: str | None,
    checkpoint_dir: str | None,
    config: ResourcePathExpression | None,
    processes: int | None,
    assume_complete: bool | None,
    dry_run: bool | None,
    interactive_status: bool | None,
    mock_storage_classes: bool | None,
) -> AggregatorConfig:
    if config is not None:
        config = ResourcePath(config)
        aggregator_config = AggregatorConfig.model_validate_json(config.read())
        if repo is not None:
            aggregator_config.butler_path = repo
        if predicted_graph is not None:
            aggregator_config.predicted_path = predicted_graph
        if db_dir is not None:
            aggregator_config.db_dir = db_dir
        if output is not None:
            aggregator_config.output_path = output
    else:
        if repo is None:
            raise ValueError("No config path and no butler path provided.")
        if predicted_graph is None:
            raise ValueError("No config path and no predicted quantum graph path provided.")
        aggregator_config = AggregatorConfig(
            predicted_path=predicted_graph,
            butler_path=repo,
            db_dir=db_dir,
        )
    if db_dir is None:
        aggregator_config.db_dir = db_dir
    if output is not None:
        aggregator_config.output_path = output
    if checkpoint_dir is not None:
        aggregator_config.checkpoint_dir = checkpoint_dir
    if processes is not None:
        aggregator_config.n_processes = processes
    if assume_complete is not None:
        aggregator_config.assume_complete = assume_complete
    if dry_run is not None:
        aggregator_config.dry_run = dry_run
    if interactive_status is not None:
        aggregator_config.interactive_status = interactive_status
    if mock_storage_classes is not None:
        aggregator_config.enable_mocks = mock_storage_classes
    return aggregator_config
