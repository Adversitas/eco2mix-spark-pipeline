"""Streaming: hourly regional aggregates from half-hourly events, with a watermark.

Events are JSON messages, one per (region, half-hour), as a grid operator's metering
feed would emit them. They arrive from Kafka in production (``source="kafka"``) or from
a directory of JSON-lines files (``source="files"``), which is what the tests and the
CI replay use. Both go through the same parsing and windowing code.

Each output row is one region-hour. The watermark bounds how late an event may arrive
and still be counted; later events are dropped, and the replay tool measures how many.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

EVENT_FIELDS = ("consommation", "nucleaire", "eolien", "solaire", "hydraulique")
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("region_code", T.StringType()),
        T.StructField("ts", T.TimestampType()),
        *[T.StructField(f, T.DoubleType()) for f in EVENT_FIELDS],
    ]
)


def read_events(
    spark: SparkSession,
    source: str,
    path: str | None = None,
    bootstrap: str | None = None,
    topic: str = "eco2mix",
    max_files_per_trigger: int | None = None,
) -> DataFrame:
    if source == "files":
        reader = spark.readStream.schema(EVENT_SCHEMA)
        if max_files_per_trigger:
            reader = reader.option("maxFilesPerTrigger", max_files_per_trigger)
        return reader.json(path)
    if source == "kafka":
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        return parse_kafka_value(raw)
    raise ValueError(f"unknown source {source!r}")


def parse_kafka_value(raw: DataFrame) -> DataFrame:
    return raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")).select(
        "e.*"
    )


def _hourly(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.window("ts", "1 hour").alias("w"), "region_code")
        .agg(
            F.count("*").alias("n_events"),
            *[F.sum(F.col(f) * 0.5).alias(f"{f}_mwh") for f in EVENT_FIELDS],
            F.max("consommation").alias("peak_consumption_mw"),
        )
        .select(
            F.col("w.start").alias("hour"),
            "region_code",
            "n_events",
            *[f"{f}_mwh" for f in EVENT_FIELDS],
            "peak_consumption_mw",
        )
    )


def hourly_aggregates(events: DataFrame, watermark: str = "2 hours") -> DataFrame:
    """One row per region and hour: energy (MWh) and peak power (MW)."""
    return _hourly(events.withWatermark("ts", watermark))


def batch_hourly_reference(silver: DataFrame) -> DataFrame:
    """The same aggregation computed in batch, used to verify the streaming output."""
    return _hourly(silver.select("region_code", "ts", *EVENT_FIELDS))


def compare(stream_out: DataFrame, reference: DataFrame, tol: float = 1e-6) -> dict:
    """Join streaming output with the batch reference and count disagreements."""
    s = stream_out.alias("s")
    r = reference.alias("r")
    joined = r.join(s, ["hour", "region_code"], "full_outer")
    diff = sum(
        (
            F.when(
                F.abs(F.coalesce(F.col(f"s.{f}_mwh"), F.lit(-1e18)) - F.col(f"r.{f}_mwh")) > tol,
                1,
            ).otherwise(0)
            for f in EVENT_FIELDS
        ),
        F.lit(0),
    )
    stats = joined.select(
        F.count("*").alias("windows"),
        F.sum(F.col("s.n_events").isNull().cast("int")).alias("missing_in_stream"),
        F.sum(F.col("r.n_events").isNull().cast("int")).alias("extra_in_stream"),
        F.sum((F.col("s.n_events") != F.col("r.n_events")).cast("int")).alias("count_mismatch"),
        F.sum((diff > 0).cast("int")).alias("value_mismatch"),
    ).first()
    return stats.asDict()
