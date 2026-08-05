"""Tests for `dagron.trace` — persistent execution traces with replay.

The headline claim of Phase 7: every node execution is appended to a
JSONL log; payloads live in the Phase 6 ContentCache; `replay(at=t)`
reconstructs the exact per-node state at any past wall-clock instant.
Pure nodes replay byte-identically; impure nodes are flagged
non-replayable but still expose their logged value.
"""

from __future__ import annotations

import time

import pytest

from dagron import Effect
from dagron.contentcache import ContentCache
from dagron.trace import (
    ReplayedNode,
    TraceReader,
    TraceRecord,
    TraceWriter,
    list_runs,
    new_run_id,
    replay,
)

# ---------------------------------------------------------------------------
# TraceRecord — round-trip
# ---------------------------------------------------------------------------


class TestTraceRecord:
    def test_round_trip(self):
        r = TraceRecord(
            timestamp=1234567890.5,
            name="my_node",
            output_fp="abcd1234",
            duration_ns=42,
            effect="pure",
            replayable=True,
            metadata={"k": "v"},
        )
        line = r.to_json()
        back = TraceRecord.from_json(line)
        assert back == r

    def test_failed_record_round_trip(self):
        r = TraceRecord(
            timestamp=1.0,
            name="bad",
            output_fp="",  # no output for failures
            error="boom",
            replayable=False,
        )
        back = TraceRecord.from_json(r.to_json())
        assert back == r


# ---------------------------------------------------------------------------
# TraceWriter
# ---------------------------------------------------------------------------


class TestTraceWriter:
    def test_writes_jsonl(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        with TraceWriter(log, cas=cas) as w:
            w.record("a", value=1, effect=Effect.PURE)
            w.record("b", value=[1, 2], effect=Effect.READ)

        # Each line is a valid JSON object terminated by \n.
        lines = log.read_text().strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            import json

            json.loads(line)

    def test_payload_stored_in_cas(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        payload = {"complex": [1, 2, {"nested": True}]}
        with TraceWriter(log, cas=cas) as w:
            rec = w.record("x", value=payload, effect=Effect.PURE)

        # Read the payload back via the CAS using the recorded fingerprint.
        fp_bytes = bytes.fromhex(rec.output_fp)
        val, hit = cas.get(fp_bytes)
        assert hit is True
        assert val == payload

    def test_failed_record_has_no_payload(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        with TraceWriter(log, cas=cas) as w:
            rec = w.record("oops", error="something went wrong")
        assert rec.output_fp == ""
        assert rec.error == "something went wrong"

    def test_effect_drives_replayable_flag(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        with TraceWriter(log, cas=cas) as w:
            r_pure = w.record("p", value=1, effect=Effect.PURE)
            r_read = w.record("r", value=2, effect=Effect.READ)
            r_write = w.record("w", value=3, effect=Effect.WRITE)
            r_net = w.record("n", value=4, effect=Effect.NETWORK)
            r_nd = w.record("nd", value=5, effect=Effect.NONDETERMINISTIC)

        assert r_pure.replayable is True
        assert r_read.replayable is True
        assert r_write.replayable is False
        assert r_net.replayable is False
        assert r_nd.replayable is False


# ---------------------------------------------------------------------------
# TraceReader
# ---------------------------------------------------------------------------


class TestTraceReader:
    def test_reads_records_back(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        t0 = time.time()
        with TraceWriter(log, cas=cas) as w:
            w.record("a", value=1, effect=Effect.PURE, timestamp=t0)
            w.record("b", value=2, effect=Effect.PURE, timestamp=t0 + 1)

        reader = TraceReader(log, cas=cas)
        recs = list(reader.records())
        assert [r.name for r in recs] == ["a", "b"]

    def test_records_until(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        t0 = 1000.0
        with TraceWriter(log, cas=cas) as w:
            for i in range(5):
                w.record(f"n{i}", value=i, effect=Effect.PURE, timestamp=t0 + i)

        reader = TraceReader(log, cas=cas)
        names = [r.name for r in reader.records_until(t0 + 2.5)]
        assert names == ["n0", "n1", "n2"]

    def test_timeline(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        with TraceWriter(log, cas=cas) as w:
            w.record("first", value=1, effect=Effect.PURE, timestamp=10.0)
            w.record("second", value=2, effect=Effect.PURE, timestamp=20.0)

        reader = TraceReader(log, cas=cas)
        assert reader.timeline() == [(10.0, "first"), (20.0, "second")]

    def test_empty_or_missing_log(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        reader = TraceReader(tmp_path / "no-such.jsonl", cas=cas)
        assert list(reader.records()) == []
        assert reader.timeline() == []

    def test_skips_malformed_lines(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        # Manually write some good and some bad lines.
        with TraceWriter(log, cas=cas) as w:
            w.record("ok", value=1, effect=Effect.PURE, timestamp=1.0)
        # Corrupt: append garbage in the middle
        with log.open("ab") as fh:
            fh.write(b"this is not json\n")
            fh.write(b'{"missing": "fields"}\n')

        # Append another good line.
        with TraceWriter(log, cas=cas) as w:
            w.record("ok2", value=2, effect=Effect.PURE, timestamp=2.0)

        reader = TraceReader(log, cas=cas)
        names = [r.name for r in reader.records()]
        assert names == ["ok", "ok2"]


# ---------------------------------------------------------------------------
# replay() — the headline feature
# ---------------------------------------------------------------------------


class TestReplay:
    def test_replay_at_end(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        t0 = 100.0
        with TraceWriter(log, cas=cas) as w:
            w.record("a", value=[1, 2, 3], effect=Effect.PURE, timestamp=t0)
            w.record("b", value="hello", effect=Effect.PURE, timestamp=t0 + 1)

        state = replay(log, cas=cas)
        assert set(state.keys()) == {"a", "b"}
        assert state["a"].value == [1, 2, 3]
        assert state["b"].value == "hello"
        assert all(s.replayable for s in state.values())

    def test_replay_at_intermediate_time(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        t0 = 100.0
        with TraceWriter(log, cas=cas) as w:
            w.record("a", value=1, effect=Effect.PURE, timestamp=t0)
            w.record("b", value=2, effect=Effect.PURE, timestamp=t0 + 10)
            w.record("c", value=3, effect=Effect.PURE, timestamp=t0 + 20)

        # Cutoff between b and c.
        state = replay(log, at=t0 + 15, cas=cas)
        assert set(state.keys()) == {"a", "b"}

    def test_replay_returns_byte_identical_payload(self, tmp_path):
        # The headline claim: pure nodes' replayed values are == their original.
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        complex_payload = {
            "rows": [{"id": i, "name": f"r{i}"} for i in range(50)],
            "meta": {"version": 7, "tags": ["a", "b", "c"]},
        }

        with TraceWriter(log, cas=cas) as w:
            w.record("snapshot", value=complex_payload, effect=Effect.PURE)

        state = replay(log, cas=cas)
        assert state["snapshot"].value == complex_payload

    def test_impure_node_marked_non_replayable_but_value_present(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        with TraceWriter(log, cas=cas) as w:
            w.record("send", value="ok", effect=Effect.NETWORK)

        state = replay(log, cas=cas)
        node = state["send"]
        assert node.value == "ok"
        assert node.replayable is False
        assert node.has_value is True

    def test_failed_node_has_error_no_value(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        with TraceWriter(log, cas=cas) as w:
            w.record("breaks", error="boom")

        state = replay(log, cas=cas)
        node = state["breaks"]
        assert node.value is None
        assert node.error == "boom"
        assert node.has_value is False

    def test_re_recorded_node_takes_latest_value(self, tmp_path):
        # If the same node was recorded multiple times (e.g., a retry),
        # the latest record up to `at` wins.
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        with TraceWriter(log, cas=cas) as w:
            w.record("retry", value=1, effect=Effect.PURE, timestamp=1.0)
            w.record("retry", value=2, effect=Effect.PURE, timestamp=2.0)
            w.record("retry", value=3, effect=Effect.PURE, timestamp=3.0)

        # Cutoff at t=2 → second value wins.
        state = replay(log, at=2.0, cas=cas)
        assert state["retry"].value == 2
        # Cutoff at end → third value wins.
        state = replay(log, cas=cas)
        assert state["retry"].value == 3


# ---------------------------------------------------------------------------
# Process restart resilience
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_log_survives_writer_close_and_reopen(self, tmp_path):
        # Simulating "process restart" — write, close, then read from a fresh reader.
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        # Process A: write
        w = TraceWriter(log, cas=cas)
        w.record("a", value="from process A", effect=Effect.PURE)
        w.close()

        # Process B (fresh reader, fresh CAS handle): read
        cas2 = ContentCache(cache_dir=tmp_path / "cas")
        reader = TraceReader(log, cas=cas2)
        state = replay(reader)
        assert state["a"].value == "from process A"

    def test_append_across_writer_lifetimes(self, tmp_path):
        cas = ContentCache(cache_dir=tmp_path / "cas")
        log = tmp_path / "run.jsonl"

        for i in range(3):
            with TraceWriter(log, cas=cas) as w:
                w.record(f"n{i}", value=i, effect=Effect.PURE, timestamp=float(i))

        reader = TraceReader(log, cas=cas)
        names = [r.name for r in reader.records()]
        assert names == ["n0", "n1", "n2"]


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


class TestConvenience:
    def test_new_run_id_is_unique(self):
        assert new_run_id() != new_run_id()

    def test_list_runs(self, tmp_path):
        # No runs yet
        assert list_runs(tmp_path) == []

        # Create a couple of trace files.
        (tmp_path / "alpha.jsonl").write_text("")
        (tmp_path / "beta.jsonl").write_text("")
        (tmp_path / "ignored.txt").write_text("")

        runs = list_runs(tmp_path)
        names = sorted(p.name for p in runs)
        assert names == ["alpha.jsonl", "beta.jsonl"]


# ---------------------------------------------------------------------------
# ReplayedNode dataclass
# ---------------------------------------------------------------------------


def test_replayed_node_has_value_property():
    n = ReplayedNode(name="ok", timestamp=1.0, value=42)
    assert n.has_value is True
    bad = ReplayedNode(name="x", timestamp=1.0, value=None, error="boom")
    assert bad.has_value is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
