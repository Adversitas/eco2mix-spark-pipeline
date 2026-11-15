"""Replay real half-hourly records as a stream of JSON events.

Each record gets an *arrival time*: its timestamp plus a small random delay (out of
order, but within the watermark), or, for a chosen fraction, a large delay that puts it
behind the watermark. Events are then emitted in arrival order:
- to a directory of JSON-lines files, one file per arrival day (the Spark file source
  reads one file per micro-batch, so the watermark advances day by day); or
- to a Kafka topic, keyed by region so each region stays ordered within a partition.

Times are handled as UTC epoch seconds. Spark timestamps collected to Python become
naive *local* datetimes, which would silently shift every event by the machine's UTC
offset (and break around daylight-saving changes).
"""

from __future__ import annotations

import json
import random
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from eco2mix.streaming import EVENT_FIELDS

Event = tuple[int, dict, bool]  # (arrival epoch seconds, payload, is_late)


@dataclass(frozen=True)
class ReplayConfig:
    max_jitter_minutes: int = 60  # below the 2-hour watermark: reordered but counted
    late_fraction: float = 0.0
    late_delay_hours: int = 30  # far behind the watermark
    seed: int = 0


def iso_utc(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_event(rec: dict) -> dict:
    """``rec`` has ``region_code``, ``epoch`` (UTC seconds) and the event fields."""
    return {
        "region_code": rec["region_code"],
        "ts": iso_utc(rec["epoch"]),
        **{f: rec[f] for f in EVENT_FIELDS},
    }


def with_arrival(records: Iterable[dict], cfg: ReplayConfig) -> list[Event]:
    rng = random.Random(cfg.seed)
    out = []
    for rec in records:
        late = rng.random() < cfg.late_fraction
        delay = (
            cfg.late_delay_hours * 3600
            if late
            else int(rng.uniform(0, cfg.max_jitter_minutes * 60))
        )
        out.append((rec["epoch"] + delay, to_event(rec), late))
    out.sort(key=lambda t: t[0])
    return out


def by_arrival_day(events: list[Event]) -> Iterator[tuple[str, list[dict]]]:
    day, batch = None, []
    for arrival, ev, _ in events:
        d = iso_utc(arrival)[:10]
        if day is not None and d != day:
            yield day, batch
            batch = []
        day = d
        batch.append(ev)
    if batch:
        yield day, batch


def expected_late_drops(
    events: list[Event], watermark_s: int = 7200, window_s: int = 3600, lag_batches: int = 2
) -> int:
    """Estimate how many events Spark will drop as late, file by file.

    With one file per micro-batch, the watermark Spark uses to drop late rows in batch N
    is computed from the event times of batches up to N - ``lag_batches``. Measured on
    the 2023 replay, ``lag_batches=1`` (the intuitive "previous batch" rule) predicts
    2,086 drops, while ``lag_batches=2`` predicts 253 and Spark 3.5 reports 256: in
    practice the watermark tolerates one extra batch interval of lateness.
    """
    batches = [
        [
            int(
                datetime.strptime(e["ts"], "%Y-%m-%dT%H:%M:%SZ")
                .replace(tzinfo=timezone.utc)
                .timestamp()
            )
            for e in batch
        ]
        for _, batch in by_arrival_day(events)
    ]
    dropped = 0
    running_max: list[int] = []
    for ts in batches:
        running_max.append(max(ts) if not running_max else max(running_max[-1], max(ts)))
    for i, ts in enumerate(batches):
        j = i - lag_batches
        if j < 0:
            continue
        wm = running_max[j] - watermark_s
        dropped += sum(1 for t in ts if (t - t % window_s) + window_s <= wm)
    return dropped


def write_files(events: list[Event], out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    n_files = 0
    for day, batch in by_arrival_day(events):
        path = out_dir / f"arrival={day}.json"
        path.write_text("\n".join(json.dumps(e) for e in batch) + "\n", encoding="utf-8")
        n_files += 1
    return n_files


def send_kafka(events: list[Event], bootstrap: str, topic: str) -> int:
    from kafka import KafkaProducer  # optional dependency

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=20,
        acks="all",
    )
    for _, ev, _ in events:
        producer.send(topic, key=ev["region_code"], value=ev)
    producer.flush()
    return len(events)
