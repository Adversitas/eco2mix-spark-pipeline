"""Structured Streaming over replayed real events, checked against the batch result."""

import json
import os
import sys
from datetime import datetime, timezone

import pytest

from eco2mix.replay import ReplayConfig, by_arrival_day, to_event, with_arrival, write_files
from eco2mix.streaming import (
    EVENT_FIELDS,
    batch_hourly_reference,
    compare,
    hourly_aggregates,
    parse_kafka_value,
    read_events,
)

# Streaming sources and checkpoints go through Hadoop's local file system, which on
# Windows needs winutils.exe (HADOOP_HOME). CI runs these tests on Linux.
needs_hadoop_fs = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="Spark streaming on Windows needs HADOOP_HOME/winutils",
)


T0 = int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp())


def _records(silver):
    cols = ["region_code", "unix_timestamp(ts) as epoch", *EVENT_FIELDS]
    return [r.asDict() for r in silver.selectExpr(*cols).collect()]


def _run_stream(spark, events_dir, checkpoint, name, watermark="2 hours"):
    ev = read_events(spark, "files", path=str(events_dir), max_files_per_trigger=1)
    q = (
        hourly_aggregates(ev, watermark)
        .writeStream.outputMode("append")
        .format("memory")
        .queryName(name)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    return spark.table(name)


def test_replay_orders_by_arrival_and_marks_late():
    recs = [
        {"region_code": "84", "epoch": T0 + h * 3600, **dict.fromkeys(EVENT_FIELDS, 1.0)}
        for h in range(24)
    ]
    events = with_arrival(recs, ReplayConfig(late_fraction=0.25, seed=1))
    arrivals = [a for a, *_ in events]
    assert arrivals == sorted(arrivals)
    late = [e for _, e, is_late in events if is_late]
    assert 0 < len(late) < 24
    days = [d for d, _ in by_arrival_day(events)]
    assert days == sorted(set(days))


def test_event_json_roundtrip():
    ev = to_event(
        {
            "region_code": "93",
            "epoch": int(datetime(2023, 6, 20, 12, 30, tzinfo=timezone.utc).timestamp()),
            **dict.fromkeys(EVENT_FIELDS, 2.5),
        }
    )
    assert ev["ts"] == "2023-06-20T12:30:00Z"
    assert json.loads(json.dumps(ev)) == ev


def test_kafka_value_parsing(spark):
    ev = {"region_code": "84", "ts": "2023-01-10T00:00:00Z", **dict.fromkeys(EVENT_FIELDS, 3.0)}
    # built in SQL (like Kafka's binary `value` column) rather than from Python objects
    payload = json.dumps(ev).replace("'", "\\'")
    df = spark.sql(f"select cast('{payload}' as binary) as value")
    row = parse_kafka_value(df).first()
    assert row.region_code == "84" and row.consommation == 3.0


@needs_hadoop_fs
def test_stream_matches_batch_when_nothing_is_late(spark, silver, tmp_path):
    write_files(with_arrival(_records(silver), ReplayConfig(seed=0)), tmp_path / "events")
    out = _run_stream(spark, tmp_path / "events", tmp_path / "ckpt", "hourly_ontime")
    stats = compare(out, batch_hourly_reference(silver))
    # every emitted window is exact; the only windows not emitted are the ones the
    # watermark has not closed yet at the end of the replay
    assert stats["value_mismatch"] == stats["missing_in_stream"]
    assert stats["extra_in_stream"] == 0
    assert stats["count_mismatch"] == 0
    assert 0 < stats["missing_in_stream"] <= 2 * 3  # last <= 3 hours, 2 regions


@needs_hadoop_fs
def test_late_events_are_dropped_not_miscounted(spark, silver, tmp_path):
    cfg = ReplayConfig(late_fraction=0.05, seed=3)
    events = with_arrival(_records(silver), cfg)
    write_files(events, tmp_path / "events")
    out = _run_stream(spark, tmp_path / "events", tmp_path / "ckpt", "hourly_late")
    ref = batch_hourly_reference(silver)
    n_ref = ref.agg({"n_events": "sum"}).first()[0]
    n_out = out.agg({"n_events": "sum"}).first()[0]
    assert n_out < n_ref
    # no window ever counts more events than really happened in it
    joined = out.alias("s").join(ref.alias("r"), ["hour", "region_code"])
    assert joined.where("s.n_events > r.n_events").count() == 0
