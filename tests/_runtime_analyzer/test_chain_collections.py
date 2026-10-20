"""Chained Butler collection handling in the runtime-table loader.

``extract_merged_runtime_table`` must expand a CHAINED collection into
its leaf members (head-first, newest-first) and merge them so the
newest member wins overlapping quanta, matching Butler chain search
semantics; the merged table's ``sources`` stay parallel to the input
entries and snapshot the resolved chain members.  These tests exercise
the loader with ``ProvenanceQuantumGraph.from_args`` patched to yield
chain-aware Butler stand-ins, and the CLI save/record paths for the
resulting descriptors.
"""

from __future__ import annotations

import json
import logging
import uuid
from unittest import mock

import pytest

pytest.importorskip("pyarrow")  # provided by the [runtime] extra

from lsst.daf.butler import (  # noqa: E402
    CollectionType,
    DatasetNotFoundError,
    MissingCollectionError,
)
from lsst.pipe.base._runtime_analyzer.runtime_table import (  # noqa: E402
    QuantumRuntimeTable,
    _flatten_run_chain,
    extract_merged_runtime_table,
)
from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph  # noqa: E402

from .support import (  # noqa: E402
    dep_present,
    fake_chain_graphs,
    fake_from_args,
    graph_node,
    invoke_cli,
    make_fake_butler,
)

_HAS_CLICK = dep_present("click")

# Sentinel quantum carried by the find-first graph when a test defines no
# provenance-bearing leaf at all (in-function guard cases); otherwise the
# find-first graph mirrors the head-most contributing leaf's nodes, as
# Butler chain search guarantees in reality.
_FIND_FIRST_QID = 999
_CHAIN_KEY = "R:chain"


def _chain_setup(
    leaves: dict,
    chains: dict[str, tuple[str, ...]],
    *,
    chain_name: str | None = None,
    non_run: dict | None = None,
    find_first_nodes: list[tuple] | None = None,
    calls: list[dict] | None = None,
) -> tuple[dict, mock.Mock, list]:
    """Build a chained-source ``from_args`` patch.

    ``leaves``/``chains``/``non_run`` feed :func:`make_fake_butler`;
    ``chain_name`` names the chained collection the source loads
    (default: the sole root of ``chains``, erroring if ambiguous);
    the yielded find-first graph mirrors the head-most contributing
    leaf's nodes (Butler find-first semantics), falling back to one
    sentinel quantum when no leaf has provenance; override with
    ``find_first_nodes``.  Returns ``(leaf_graphs, butler, from_args)``.
    """
    if chain_name is None:
        roots = [name for name in chains
                 if not any(name in members
                            for members in chains.values())]
        assert len(roots) == 1, "chain_name required for multiple roots"
        chain_name = roots[0]
    graphs = fake_chain_graphs(leaves)
    butler = make_fake_butler(graphs, chains, non_run)
    key = f"R:{chain_name}"
    if find_first_nodes is None:
        # Butler's find-first get returns the head-most RUN member's
        # graph; mirror exactly that member's nodes.
        head_nodes = next(
            (
                leaves[name]
                for name, leaf_type in _flatten_run_chain(butler, chain_name)
                if leaf_type is CollectionType.RUN
                and name in leaves
                and not isinstance(leaves[name], Exception)
            ),
            None,
        )
        find_first_nodes = (
            list(head_nodes)
            if head_nodes is not None
            else [
                graph_node(_FIND_FIRST_QID, "TaskT", 12345.0,
                           "{visit=99}"),
            ]
        )
    from_args = fake_from_args(
        {key: find_first_nodes},
        header_task="TaskT",
        calls=calls,
        butlers={key: butler},
    )
    return graphs, butler, from_args


class TestChainedCollectionExpansion:
    """Leaf expansion order, precedence, policies, and descriptors."""

    def test_head_first_order_and_newest_member_wins(self) -> None:
        """A chain loads its leaves head-first (newest first); the
        find-first graph serves as the head member's table (no second
        load) and overlapping quanta keep the newest member's row.
        """
        leaves = {
            "new": [graph_node(1, "TaskT", 10.0, "{visit=10}")],
            "old": [
                # Shared key with "new" -> shadowed by the newest leaf.
                graph_node(2, "TaskT", 999.0, "{visit=10}"),
                graph_node(3, "TaskT", 30.0, "{visit=20}"),
            ],
        }
        calls: list[dict] = []
        _graphs, butler, from_args = _chain_setup(
            leaves, {"chain": ("new", "old")}, calls=calls,
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            table = extract_merged_runtime_table([("R", "chain")])

        # One from_args call for the source, datasets=() as before.
        assert [(c["path"], c["collection"], c["datasets"])
                for c in calls] == [("R", "chain", ())]
        # One chain-wide metadata query finds both contributing members,
        # then every contributing leaf is loaded head-first with the same
        # run_provenance parameters from_args would use -- except the
        # head-most provider, whose graph is the find-first one already
        # yielded by from_args (reused, never re-serialized).
        assert butler.queries == ["chain"]  # one chain-wide query
        assert butler.loaded == ["old"]
        assert [(leaf, dtype, params) for leaf, dtype, params
                in butler.gets] == [
            ("old", "run_provenance",
             {"quanta": None, "datasets": ()}),
        ]

        by_qid = {uuid.UUID(bytes=q).int: i
                  for i, q in enumerate(table.quantum_id_list())}
        assert set(by_qid) == {1, 3}
        # The newest leaf wins the shared (task_label, data_id) key,
        # served from the reused find-first graph.
        assert float(table.run_time[by_qid[1]]) == pytest.approx(10.0)
        assert float(table.run_time[by_qid[3]]) == pytest.approx(30.0)

        # One descriptor per user source, snapshotting the members.
        assert table.sources == ("R:chain[new,old]",)
        assert table.n_sources == 1
        # Multiple leaf tables merged: no header count (the sum would
        # double-count shared quanta).
        assert table.n_expected == 0

    def test_single_leaf_chain_keeps_header_count(self) -> None:
        """A one-member chain goes through the single-table fast path:
        the leaf header count survives and the descriptor still lists
        the member.
        """
        leaves = {
            "only": [
                graph_node(1, "TaskT", 10.0, "{visit=10}"),
                graph_node(2, "TaskT", 20.0, "{visit=20}"),
            ],
        }
        _graphs, butler, from_args = _chain_setup(
            leaves, {"chain": ("only",)},
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            table = extract_merged_runtime_table([("R", "chain")])

        assert butler.loaded == []  # sole member is the find-first graph
        assert butler.queries == ["chain"]
        assert table.n_rows == 2
        assert table.n_expected == 2
        assert table.n_sources == 1
        assert table.sources == ("R:chain[only]",)

    def test_nested_chain_flattened_in_order(self) -> None:
        """A chain naming another chain flattens depth-first, keeping
        the head-first order across nesting levels.
        """
        leaves = {
            "runA": [graph_node(1, "TaskT", 10.0, "{visit=1}")],
            "runB": [graph_node(2, "TaskT", 20.0, "{visit=2}")],
            "runC": [graph_node(3, "TaskT", 30.0, "{visit=3}")],
        }
        _graphs, butler, from_args = _chain_setup(
            leaves, {"outer": ("inner", "runC"), "inner": ("runA", "runB")},
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            table = extract_merged_runtime_table([("R", "outer")])

        assert butler.loaded == ["runB", "runC"]  # runA served by reuse
        assert table.sources == ("R:outer[runA,runB,runC]",)
        assert table.n_rows == 3

    def test_non_run_member_skipped_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A TAGGED chain member cannot hold ``run_provenance``: it is
        skipped with a warning naming it; RUN members still load and the
        descriptor snapshots every member.
        """
        leaves = {"runA": [graph_node(1, "TaskT", 10.0, "{visit=1}")]}
        _graphs, butler, from_args = _chain_setup(
            leaves,
            {"chain": ("runA", "taggedB")},
            non_run={"taggedB": CollectionType.TAGGED},
        )

        with caplog.at_level(logging.WARNING), \
                mock.patch.object(ProvenanceQuantumGraph, "from_args",
                                  side_effect=from_args):
            table = extract_merged_runtime_table([("R", "chain")])

        assert butler.loaded == []  # runA served by find-first reuse
        assert table.n_rows == 1
        assert table.sources == ("R:chain[runA,taggedB]",)
        assert any(
            "taggedB" in record.getMessage()
            and "non-RUN" in record.getMessage()
            for record in caplog.records
        )

    def test_missing_provenance_member_warned_and_skipped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A RUN leaf without ``run_provenance`` warns by name and the
        remaining leaves still load.
        """
        missing = DatasetNotFoundError(
            "No run_provenance datasets in collection 'old'"
        )
        _graphs, butler, from_args = _chain_setup(
            {
                "new": [graph_node(1, "TaskT", 10.0, "{visit=1}")],
                "old": missing,
            },
            {"chain": ("new", "old")},
        )

        with caplog.at_level(logging.WARNING), \
                mock.patch.object(ProvenanceQuantumGraph, "from_args",
                                  side_effect=from_args):
            table = extract_merged_runtime_table([("R", "chain")])

        assert butler.queries == ["chain"]  # one chain-wide query
        assert butler.loaded == []  # 'new' served by find-first reuse
        assert table.n_rows == 1
        assert table.sources == ("R:chain[new,old]",)
        assert any(
            "old" in record.getMessage()
            and "no run provenance" in record.getMessage()
            for record in caplog.records
        )

    def test_all_members_missing_raises_naming_chain_and_members(
        self,
    ) -> None:
        """A chain whose every member lacks ``run_provenance`` raises
        with the chain name and every member tried.
        """
        _graphs, _butler, from_args = _chain_setup(
            {
                "new": DatasetNotFoundError("no run_provenance in 'new'"),
                "old": DatasetNotFoundError("no run_provenance in 'old'"),
            },
            {"chain": ("new", "old")},
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            with pytest.raises(ValueError) as excinfo:
                extract_merged_runtime_table([("R", "chain")])

        message = str(excinfo.value)
        assert "'chain'" in message
        assert "'new'" in message
        assert "'old'" in message

    def test_all_non_run_members_raises(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A chain of only non-RUN members warns per offender (once
        each) then raises naming the chain and every member.
        """
        _graphs, butler, from_args = _chain_setup(
            {},
            {"chain": ("tagA", "calB")},
            non_run={"tagA": CollectionType.TAGGED,
                     "calB": CollectionType.CALIBRATION},
        )

        with caplog.at_level(logging.WARNING), \
                mock.patch.object(ProvenanceQuantumGraph, "from_args",
                                  side_effect=from_args):
            with pytest.raises(ValueError) as excinfo:
                extract_merged_runtime_table([("R", "chain")])

        assert butler.loaded == []
        warnings = [
            record.getMessage()
            for record in caplog.records
            if "non-RUN member" in record.getMessage()
        ]
        assert len(warnings) == 2
        message = str(excinfo.value)
        assert "'chain'" in message
        assert "'tagA'" in message
        assert "'calB'" in message

    def test_find_first_miss_names_requested_source(self) -> None:
        """When the find-first ``get`` inside ``from_args`` misses (the
        real-butler path for a collection, chained or not, with no
        ``run_provenance`` anywhere), the raised ``DatasetNotFoundError``
        names the repo and the collection as the user requested them --
        the raw butler error names neither.
        """
        from_args = fake_from_args({
            _CHAIN_KEY: DatasetNotFoundError(
                "no run_provenance datasets in collection 'chain'"
            )
        })

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            with pytest.raises(DatasetNotFoundError) as excinfo:
                extract_merged_runtime_table([("R", "chain")])

        message = str(excinfo.value)
        assert "'R'" in message
        assert "'chain'" in message
        # The original error is kept as the cause.
        assert isinstance(excinfo.value.__cause__, DatasetNotFoundError)

    def test_find_first_miss_on_plain_run_names_source(self) -> None:
        """A plain (non-chained) collection missing its provenance gets
        the same clear message (it was never butler-explicit either).
        """
        from_args = fake_from_args(
            {"R:coll": DatasetNotFoundError("no run_provenance")}
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            with pytest.raises(DatasetNotFoundError) as excinfo:
                extract_merged_runtime_table([("R", "coll")])

        message = str(excinfo.value)
        assert "'R'" in message
        assert "'coll'" in message

    def test_nested_missing_member_propagates(self) -> None:
        """A chain naming a collection that does not exist (anywhere)
        propagates ``MissingCollectionError`` unchanged while being
        flattened (no chain-naming error wrap).
        """
        # The fake knows only "chain"; the named child "sub" resolves to
        # nothing at all, like a deleted run still referenced by a chain.
        butler = make_fake_butler({}, {"chain": ("sub", "runA")})
        from_args = fake_from_args(
            {_CHAIN_KEY: [
                graph_node(_FIND_FIRST_QID, "TaskT", 12345.0, "{visit=99}"),
            ]},
            butlers={_CHAIN_KEY: butler},
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            with pytest.raises(MissingCollectionError):
                extract_merged_runtime_table([("R", "chain")])

    def test_duplicate_member_loaded_once(self) -> None:
        """A run named twice in one chain loads once (cycle/repeat
        guard) and is snapshotted once in the descriptor.
        """
        leaves = {
            "runA": [graph_node(1, "TaskT", 10.0, "{visit=1}")],
            "runB": [graph_node(2, "TaskT", 20.0, "{visit=2}")],
        }
        _graphs, butler, from_args = _chain_setup(
            leaves, {"chain": ("runB", "runA", "runB")},
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            table = extract_merged_runtime_table([("R", "chain")])

        assert butler.loaded == ["runA"]  # runB served by find-first reuse
        assert table.sources == ("R:chain[runB,runA]",)
        assert table.n_rows == 2

    def test_missing_collection_propagates(self) -> None:
        """``MissingCollectionError`` from the collection lookup
        propagates unchanged (no silent empty-table fallback).
        """
        butler = make_fake_butler({}, chains={})
        from_args = fake_from_args(
            {_CHAIN_KEY: []}, butlers={_CHAIN_KEY: butler},
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            with pytest.raises(MissingCollectionError):
                extract_merged_runtime_table([("R", "chain")])

    def test_expansion_logged_at_info(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Expansion emits one info line naming the chain and members
        in head-first order.
        """
        leaves = {"new": [graph_node(1, "TaskT", 10.0, "{visit=1}")],
                  "old": [graph_node(2, "TaskT", 20.0, "{visit=2}")]}
        _graphs, _butler, from_args = _chain_setup(
            leaves, {"chain": ("new", "old")},
        )

        with caplog.at_level(logging.INFO), \
                mock.patch.object(ProvenanceQuantumGraph, "from_args",
                                  side_effect=from_args):
            extract_merged_runtime_table([("R", "chain")])

        assert any(
            "is chained" in record.getMessage()
            and "new, old" in record.getMessage()
            for record in caplog.records
        )


class TestNonChainedSourceDescriptors:
    """Plain (non-chain) sources keep the cheap path; only the
    descriptor gains the collection.
    """

    def test_plain_mock_butler_not_expanded(self) -> None:
        """Mock-safety: the bare ``mock.Mock()`` butler yielded by the
        shared fake loader must not trigger any chain machinery (its
        ``get_info(...).type`` is a plain Mock, never ``CHAINED``).
        """
        nodes = [graph_node(1, "TaskT", 10.0, "{visit=1}")]
        from_args = fake_from_args({"R:coll": nodes})

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            table = extract_merged_runtime_table([("R", "coll")])

        # Rows come from the yielded graph; nothing consulted the butler.
        assert table.n_rows == 1
        assert table.sources == ("R:coll",)

    def test_plain_run_butler_descriptor(self) -> None:
        """A RUN collection yields the ``"repo:collection"`` descriptor
        and loads the yielded graph directly (no per-leaf ``get``).
        """
        nodes = [graph_node(1, "TaskT", 10.0, "{visit=1}")]
        butler = make_fake_butler(
            {"coll": fake_chain_graphs({"coll": nodes})["coll"]},
        )
        from_args = fake_from_args(
            {"R:coll": nodes}, butlers={"R:coll": butler},
        )

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            table = extract_merged_runtime_table([("R", "coll")])

        assert butler.loaded == []
        assert butler.gets == []
        assert table.n_rows == 1
        assert table.sources == ("R:coll",)

    def test_file_source_descriptor_unchanged(self) -> None:
        """File (``collection=None``) entries keep the bare path as
        their descriptor and never query a butler.
        """
        nodes = [graph_node(1, "TaskT", 10.0, "{visit=1}")]
        from_args = fake_from_args({"a.qg": nodes, "b.qg": nodes})

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            table = extract_merged_runtime_table([("a.qg", None),
                                                  ("b.qg", None)])

        assert table.sources == ("a.qg", "b.qg")
        assert table.n_sources == 2


class TestChainSnapshotMetadata:
    """Cache metadata and tracked-run records snapshot chain members."""

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    def test_preprocess_chain_source_metadata_snapshots_members(
        self, tmp_path,
    ) -> None:
        """``preprocess`` of a chained source writes the resolved
        descriptor (chain name plus every member) into the Parquet
        ``sources`` metadata.
        """
        leaves = {
            "new": [graph_node(1, "TaskT", 10.0, "{visit=1}")],
            "old": [graph_node(2, "TaskT", 20.0, "{visit=2}")],
        }
        _graphs, _butler, from_args = _chain_setup(
            leaves, {"chain": ("new", "old")},
        )
        out = str(tmp_path / "out.parquet")

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args):
            result = invoke_cli(
                ['--repo', 'R', '-c', 'chain', 'preprocess', '-o', out])

        assert result.exit_code == 0, f"preprocess crashed: {result.exception}"
        loaded = QuantumRuntimeTable.from_parquet(out)
        assert json.loads(
            loaded.to_arrow().schema.metadata[b'sources']
        ) == ["R:chain[new,old]"]

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    def test_tracked_run_record_stores_snapshot_descriptor(
        self,
    ) -> None:
        """``tracked-run record`` of a chained source stores the
        resolved snapshot (chain name + members) in the ``collection``
        column and the repo as-is.
        """
        from lsst.pipe.base._runtime_analyzer import cli

        leaves = {"new": [graph_node(1, "TaskT", 10.0, "{visit=1}")],
                  "old": [graph_node(2, "TaskT", 20.0, "{visit=2}")]}
        _graphs, _butler, from_args = _chain_setup(
            leaves, {"chain": ("new", "old")},
        )

        recorded: dict = {}

        def fake_record_run(**kwargs):
            recorded.update(kwargs)
            return {'quanta': 2, 'tasks': 1}

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args), \
                mock.patch.object(cli, 'record_run',
                                  side_effect=fake_record_run), \
                mock.patch(
                    'lsst.pipe.base._runtime_analyzer.tracker.history.'
                    '_auto_compare'):
            result = invoke_cli(
                ['-r', 'R', '-c', 'chain',
                 'tracked-run', 'record', '-l', 'L'])

        assert result.exit_code == 0, f"record crashed: {result.exception}"
        assert recorded['repo'] == 'R'
        assert recorded['collection'] == 'chain[new,old]'

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    def test_tracked_run_record_plain_run_keeps_collection_name(
        self,
    ) -> None:
        """A plain RUN source records the collection name exactly as
        before (no brackets, no snapshot machinery).
        """
        from lsst.pipe.base._runtime_analyzer import cli

        nodes = [graph_node(1, "TaskT", 10.0, "{visit=1}")]
        butler = make_fake_butler(
            {"coll": fake_chain_graphs({"coll": nodes})["coll"]},
        )
        from_args = fake_from_args(
            {"R:coll": nodes}, butlers={"R:coll": butler},
        )

        recorded: dict = {}

        def fake_record_run(**kwargs):
            recorded.update(kwargs)
            return {'quanta': 1, 'tasks': 1}

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args), \
                mock.patch.object(cli, 'record_run',
                                  side_effect=fake_record_run), \
                mock.patch(
                    'lsst.pipe.base._runtime_analyzer.tracker.history.'
                    '_auto_compare'):
            result = invoke_cli(
                ['-r', 'R', '-c', 'coll',
                 'tracked-run', 'record', '-l', 'L'])

        assert result.exit_code == 0, f"record crashed: {result.exception}"
        assert recorded['repo'] == 'R'
        assert recorded['collection'] == 'coll'

    @pytest.mark.skipif(not _HAS_CLICK, reason="click not installed")
    def test_tracked_run_record_butler_after_graph_aligns_index(
        self,
    ) -> None:
        """With mixed sources, the recorded snapshot comes from the
        Butler entry's own descriptor position (index 1 here), not from
        the first source.
        """
        from lsst.pipe.base._runtime_analyzer import cli

        leaves = {"new": [graph_node(1, "TaskT", 10.0, "{visit=1}")],
                  "old": [graph_node(2, "TaskT", 20.0, "{visit=2}")]}
        graphs = fake_chain_graphs(leaves)
        butler = make_fake_butler(graphs, {"chain": ("new", "old")})
        graph_nodes = [graph_node(7, "TaskT", 7.0, "{visit=7}")]
        from_args = fake_from_args(
            {"g.qg": graph_nodes,
             _CHAIN_KEY: [
                 graph_node(_FIND_FIRST_QID, "TaskT", 12345.0,
                            "{visit=99}"),
             ]},
            butlers={_CHAIN_KEY: butler},
        )

        recorded: dict = {}

        def fake_record_run(**kwargs):
            recorded.update(kwargs)
            return {'quanta': 3, 'tasks': 1}

        with mock.patch.object(ProvenanceQuantumGraph, "from_args",
                               side_effect=from_args), \
                mock.patch.object(cli, 'record_run',
                                  side_effect=fake_record_run), \
                mock.patch(
                    'lsst.pipe.base._runtime_analyzer.tracker.history.'
                    '_auto_compare'):
            result = invoke_cli(
                ['-g', 'g.qg', '-r', 'R', '-c', 'chain',
                 'tracked-run', 'record', '-l', 'L'])

        assert result.exit_code == 0, f"record crashed: {result.exception}"
        assert recorded['repo'] == 'R'
        assert recorded['collection'] == 'chain[new,old]'
