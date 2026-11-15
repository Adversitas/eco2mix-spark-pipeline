"""``eco2mix`` command line.

eco2mix download                       ODRE export -> data/raw/eco2mix_regional_<year>.csv
eco2mix batch   [--out DIR]            quality report, gold tables, annual summary
eco2mix replay  [--late-fraction F]    real records -> JSON event files (or Kafka)
eco2mix stream  [--verify]             Structured Streaming over the events,
                                       checked against the batch reference
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

RAW = "data/raw/eco2mix_regional_2023.csv"
EXPORT = (
    "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "eco2mix-regional-cons-def/exports/csv"
)


def _download(year: int) -> Path:
    import httpx

    dest = Path(f"data/raw/eco2mix_regional_{year}.csv")
    dest.parent.mkdir(parents=True, exist_ok=True)
    where = (
        f"date_heure >= '{year}-01-01T00:00:00+01:00' "
        f"and date_heure < '{year + 1}-01-01T00:00:00+01:00'"
    )
    params = {"where": where, "delimiter": ";"}
    with httpx.stream("GET", EXPORT, params=params, timeout=600) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_bytes():
                fh.write(chunk)
    return dest


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="eco2mix")
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("download")
    d.add_argument("--year", type=int, default=2023)
    b = sub.add_parser("batch")
    b.add_argument("--input", default=RAW)
    b.add_argument("--out", default=None, help="write gold parquet here")
    r = sub.add_parser("replay")
    r.add_argument("--input", default=RAW)
    r.add_argument("--events-dir", default="data/events")
    r.add_argument("--late-fraction", type=float, default=0.0)
    r.add_argument("--seed", type=int, default=0)
    r.add_argument("--kafka", default=None, help="bootstrap servers; default: write files")
    r.add_argument("--topic", default="eco2mix")
    s = sub.add_parser("stream")
    s.add_argument("--source", choices=["files", "kafka"], default="files")
    s.add_argument("--events-dir", default="data/events")
    s.add_argument("--kafka", default=None)
    s.add_argument("--topic", default="eco2mix")
    s.add_argument("--watermark", default="2 hours")
    s.add_argument("--checkpoint", default="data/checkpoints/hourly")
    s.add_argument("--verify", action="store_true", help="compare with the batch reference")
    s.add_argument("--input", default=RAW)
    args = ap.parse_args(argv)

    if args.cmd == "download":
        print(_download(args.year))
        return 0

    from eco2mix.batch import read_raw, spark_session, to_silver

    spark = spark_session(kafka=args.cmd == "stream" and args.source == "kafka")
    spark.sparkContext.setLogLevel("ERROR")

    if args.cmd == "batch":
        from eco2mix.batch import (
            annual_region_summary,
            daily_region_mix,
            peak_hours,
            quality_report,
        )

        t0 = time.perf_counter()
        raw = read_raw(spark, args.input)
        silver = to_silver(raw).cache()
        report = quality_report(raw, silver)
        daily = daily_region_mix(silver).cache()
        summary = annual_region_summary(daily)
        rows = summary.collect()
        peaks = peak_hours(silver).orderBy("region_code").collect()
        if args.out:
            silver.write.mode("overwrite").partitionBy("region_code").parquet(f"{args.out}/silver")
            daily.write.mode("overwrite").parquet(f"{args.out}/gold/daily_region_mix")
            summary.write.mode("overwrite").parquet(f"{args.out}/gold/annual_region_summary")
        report["seconds"] = round(time.perf_counter() - t0, 1)
        print(json.dumps(report, indent=1, default=str))
        print(
            f"{'region':28} {'cons TWh':>9} {'prod TWh':>9} {'renew':>6} {'self-suff':>9} peak MW"
        )
        for x in rows:
            print(
                f"{x.region:28} {x.consumption_twh:9.2f} {x.production_twh:9.2f} "
                f"{x.renewable_share:6.1%} {x.self_sufficiency:9.2f} {x.peak_mw:7.0f}"
            )
        for p in peaks:
            print(f"peak {p.region:28} {p.ts_utc} UTC {p.consommation:.0f} MW")
        return 0

    if args.cmd == "replay":
        from pyspark.sql import functions as F

        from eco2mix.replay import (
            ReplayConfig,
            expected_late_drops,
            send_kafka,
            with_arrival,
            write_files,
        )
        from eco2mix.streaming import EVENT_FIELDS

        silver = to_silver(read_raw(spark, args.input)).select(
            "region_code", F.unix_timestamp("ts").alias("epoch"), *EVENT_FIELDS
        )
        records = [row.asDict() for row in silver.toLocalIterator()]
        cfg = ReplayConfig(late_fraction=args.late_fraction, seed=args.seed)
        events = with_arrival(records, cfg)
        n_late = sum(late for *_, late in events)
        if args.kafka:
            n = send_kafka(events, args.kafka, args.topic)
            print(f"sent {n} events ({n_late} late) to {args.kafka}/{args.topic}")
        else:
            n_files = write_files(events, Path(args.events_dir))
            print(
                f"wrote {len(events)} events ({n_late} late) in {n_files} files; "
                f"behind the watermark at arrival (simulated): {expected_late_drops(events)}"
            )
        return 0

    if args.cmd == "stream":
        from eco2mix.streaming import (
            batch_hourly_reference,
            compare,
            hourly_aggregates,
            read_events,
        )

        events = read_events(
            spark,
            args.source,
            path=args.events_dir,
            bootstrap=args.kafka,
            topic=args.topic,
            max_files_per_trigger=1,
        )
        hourly = hourly_aggregates(events, args.watermark)
        t0 = time.perf_counter()
        query = (
            hourly.writeStream.outputMode("append")
            .format("memory")
            .queryName("hourly")
            .option("checkpointLocation", args.checkpoint)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        secs = time.perf_counter() - t0
        progress = query.recentProgress  # retention raised in spark_session()
        n_in = sum(p["numInputRows"] for p in progress)
        dropped = sum(
            op.get("numRowsDroppedByWatermark", 0)
            for p in progress
            for op in p.get("stateOperators", [])
        )
        out = spark.table("hourly")
        print(
            f"{len(progress)} micro-batches, {n_in} events, {out.count()} region-hours "
            f"in {secs:.1f}s ({n_in / secs:.0f} events/s); "
            f"spark numRowsDroppedByWatermark={dropped}"
        )
        if args.verify:
            ref = batch_hourly_reference(to_silver(read_raw(spark, args.input)))
            stats = compare(out, ref)
            stats["events_read"] = n_in
            stats["events_counted"] = out.agg({"n_events": "sum"}).first()[0]
            stats["rows_dropped_by_watermark"] = dropped
            print(json.dumps(stats, indent=1))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
