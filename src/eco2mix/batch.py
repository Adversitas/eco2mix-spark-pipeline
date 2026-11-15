"""Batch pipeline: raw eco2mix CSV -> silver (typed, de-duplicated) -> gold aggregates.

eco2mix publishes, for each French region and each half-hour, the average power (MW) of
consumption, of each production source and of physical exchanges with neighbours. One
half-hour at P MW is P x 0.5 MWh of energy, which is how every energy figure below is
computed.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCES = ("thermique", "nucleaire", "eolien", "solaire", "hydraulique", "bioenergies")
RENEWABLE = ("eolien", "solaire", "hydraulique", "bioenergies")
MEASURES = ("consommation", *SOURCES, "pompage", "ech_physiques")
HALF_HOUR_H = 0.5

RAW_SCHEMA = T.StructType(
    [
        T.StructField("code_insee_region", T.StringType()),
        T.StructField("libelle_region", T.StringType()),
        T.StructField("nature", T.StringType()),
        T.StructField("date", T.StringType()),
        T.StructField("heure", T.StringType()),
        T.StructField("date_heure", T.StringType()),
        *[T.StructField(m, T.StringType()) for m in MEASURES],
    ]
)


KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def spark_session(
    app: str = "eco2mix", master: str = "local[*]", kafka: bool = False
) -> SparkSession:
    builder = SparkSession.builder
    if kafka:  # pulls the Kafka connector from Maven Central on first use
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    return (
        builder.appName(app)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        # keep every micro-batch's progress (default 100) so totals are exact
        .config("spark.sql.streaming.numRecentProgressUpdates", "10000")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read the ODRE CSV export. Columns beyond the ones we use are ignored by name."""
    df = spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8").csv(path)
    first = df.columns[0]
    if first != "code_insee_region":  # strip a UTF-8 BOM from the first header
        df = df.withColumnRenamed(first, "code_insee_region")
    return df.select(*[c.name for c in RAW_SCHEMA.fields])


def to_silver(raw: DataFrame) -> DataFrame:
    """Typed, one row per (region, half-hour), latest duplicate kept."""
    typed = raw.select(
        F.col("code_insee_region").alias("region_code"),
        F.col("libelle_region").alias("region"),
        F.to_timestamp("date_heure").alias("ts"),
        *[F.col(m).cast("double").alias(m) for m in MEASURES],
    ).where(F.col("ts").isNotNull() & F.col("region_code").isNotNull())
    w = Window.partitionBy("region_code", "ts").orderBy(F.monotonically_increasing_id().desc())
    return typed.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def duplicate_report(raw: DataFrame) -> dict:
    """Rows sharing (region, UTC timestamp): identical copies or conflicting values?"""
    key = F.concat_ws("|", *[F.coalesce(F.col(m), F.lit("")) for m in MEASURES])
    groups = (
        raw.groupBy("code_insee_region", "date_heure")
        .agg(F.count("*").alias("n"), F.countDistinct(key).alias("variants"))
        .where("n > 1")
    )
    stats = groups.agg(
        F.coalesce(F.sum(F.when(F.col("variants") == 1, F.col("n") - 1)), F.lit(0)).alias("same"),
        F.coalesce(F.sum(F.when(F.col("variants") > 1, F.col("n") - 1)), F.lit(0)).alias("diff"),
    ).first()
    return {"duplicate_rows_identical": stats.same, "duplicate_rows_conflicting": stats.diff}


def quality_report(raw: DataFrame, silver: DataFrame) -> dict:
    """Counts a data engineer would look at before trusting the gold tables."""
    n_raw = raw.count()
    n_silver = silver.count()
    nulls = silver.select(
        *[F.sum(F.col(m).isNull().cast("int")).alias(m) for m in MEASURES]
    ).first()
    negatives = silver.select(
        *[F.sum((F.col(s) < 0).cast("int")).alias(s) for s in SOURCES]
    ).first()

    # Timestamps are formatted inside Spark (session time zone UTC): collecting them to
    # Python would convert them to the machine's local time.
    iso = "yyyy-MM-dd'T'HH:mm'Z'"
    span = silver.agg(
        F.date_format(F.min("ts"), iso).alias("lo"),
        F.date_format(F.max("ts"), iso).alias("hi"),
        ((F.unix_timestamp(F.max("ts")) - F.unix_timestamp(F.min("ts"))) / 1800 + 1)
        .cast("long")
        .alias("expected"),
    ).first()
    expected = span.expected
    per_region = silver.groupBy("region_code").count().collect()
    missing = {r.region_code: expected - r["count"] for r in per_region if r["count"] != expected}

    # Regional balance: consumption = production + pumping + physical exchanges
    # (pumping and exports are negative). The residual measures how consistent the
    # published figures are with each other.
    prod = sum((F.coalesce(F.col(s), F.lit(0.0)) for s in SOURCES), F.lit(0.0))
    resid = silver.select(
        (
            F.col("consommation")
            - prod
            - F.coalesce("pompage", F.lit(0.0))
            - F.coalesce("ech_physiques", F.lit(0.0))
        ).alias("r"),
        F.col("consommation").alias("c"),
    ).where(F.col("c").isNotNull() & (F.col("c") > 0))
    q = resid.select(
        F.percentile_approx(F.abs("r") / F.col("c"), [0.5, 0.99, 1.0], 10000).alias("q")
    ).first()["q"]

    return {
        "raw_rows": n_raw,
        "silver_rows": n_silver,
        "duplicates_removed": n_raw - n_silver,
        "regions": len(per_region),
        **duplicate_report(raw),
        "first_ts": span.lo,
        "last_ts": span.hi,
        "expected_half_hours_per_region": expected,
        "missing_half_hours": missing,
        "null_values": {k: v for k, v in nulls.asDict().items() if v},
        "negative_production_values": {k: v for k, v in negatives.asDict().items() if v},
        "balance_residual_share": {"p50": q[0], "p99": q[1], "max": q[2]},
    }


def daily_region_mix(silver: DataFrame) -> DataFrame:
    """Energy per day and region (GWh), renewable share, peak and net exchanges."""
    day = silver.withColumn("day", F.to_date(F.from_utc_timestamp("ts", "Europe/Paris")))
    gwh = {m: F.sum(F.coalesce(F.col(m), F.lit(0.0)) * HALF_HOUR_H / 1000) for m in MEASURES}
    agg = day.groupBy("region_code", "region", "day").agg(
        F.count("*").alias("half_hours"),
        *[expr.alias(f"{m}_gwh") for m, expr in gwh.items()],
        F.max("consommation").alias("peak_consumption_mw"),
    )
    production = sum((F.col(f"{s}_gwh") for s in SOURCES), F.lit(0.0))
    renewable = sum((F.col(f"{s}_gwh") for s in RENEWABLE), F.lit(0.0))
    return agg.withColumn("production_gwh", production).withColumn(
        "renewable_share", F.when(production > 0, renewable / production)
    )


def peak_hours(silver: DataFrame, n: int = 1) -> DataFrame:
    """Top-n consumption half-hours per region (window function)."""
    w = Window.partitionBy("region_code").orderBy(F.col("consommation").desc_nulls_last())
    return (
        silver.withColumn("rank", F.row_number().over(w))
        .where(F.col("rank") <= n)
        .select(
            "region_code",
            "region",
            "ts",
            F.date_format("ts", "yyyy-MM-dd HH:mm").alias("ts_utc"),
            "consommation",
            "rank",
        )
    )


def annual_region_summary(daily: DataFrame) -> DataFrame:
    sources = [f"{s}_gwh" for s in SOURCES]
    out = daily.groupBy("region_code", "region").agg(
        F.round(F.sum("consommation_gwh") / 1000, 2).alias("consumption_twh"),
        F.round(F.sum("production_gwh") / 1000, 2).alias("production_twh"),
        *[F.sum(c).alias(c) for c in sources],
        F.max("peak_consumption_mw").alias("peak_mw"),
    )
    production = sum((F.col(c) for c in sources), F.lit(0.0))
    renewable = sum((F.col(f"{s}_gwh") for s in RENEWABLE), F.lit(0.0))
    return (
        out.withColumn("renewable_share", F.round(renewable / production, 3))
        .withColumn(
            "self_sufficiency", F.round(F.col("production_twh") / F.col("consumption_twh"), 2)
        )
        .drop(*sources)
        .orderBy(F.col("consumption_twh").desc())
    )
