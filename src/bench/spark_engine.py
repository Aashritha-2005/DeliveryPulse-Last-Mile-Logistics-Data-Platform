"""The benchmark workload (src/bench/spec.py) in PySpark, local mode.

Spark's scratch space (``spark.local.dir``) and the JVM's temporary directory are passed in, so
that the benchmark's disk budget covers them (src/bench/disk.py).
"""

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as sf

from src.bench.spec import (
    CANCELLED,
    DEGREES_TO_RADIANS,
    DELIVERED,
    EARTH_RADIUS_KM,
    EVENT_REQUIRED,
    EVENT_TYPES,
    EVENTS_FILE,
    GPS_PING,
    MAX_SPEED_KMH,
    OFFSET_SECONDS,
    ORDER_REQUIRED,
    ORDERS_FILE,
    ROW_ID,
    SPARK_TS_FORMAT,
    STATUSES,
    WorkloadResult,
)

DRIVER_MEMORY = "2g"
SHUFFLE_PARTITIONS = 16


def session(local_dir: Path, tmp_dir: Path, app: str = "deliverypulse-bench") -> SparkSession:
    """A local SparkSession using all cores, with its scratch space under ``local_dir``."""
    local_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)
    java_tmp = f"-Djava.io.tmpdir={tmp_dir.resolve()}"
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app)
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
        .config("spark.local.dir", str(local_dir.resolve()))
        .config("spark.driver.extraJavaOptions", java_tmp)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def epoch_seconds(column: str) -> Column:
    """A contract timestamp as epoch seconds (double; null if empty or invalid)."""
    parsed = sf.try_to_timestamp(sf.col(column), sf.lit(SPARK_TS_FORMAT))
    return sf.unix_timestamp(parsed).cast("double")


def reporting_date(seconds: Column) -> Column:
    """The Asia/Kolkata calendar date (``yyyy-MM-dd``) of epoch seconds (session time zone UTC)."""
    shifted = sf.timestamp_seconds((seconds + OFFSET_SECONDS).cast("long"))
    return sf.date_format(shifted, "yyyy-MM-dd")


def given(column: str) -> Column:
    """True where ``column`` is non-empty."""
    return sf.coalesce(sf.col(column), sf.lit("")) != ""


def present(columns: Sequence[str]) -> Column:
    """True where every column is non-empty."""
    condition = sf.lit(True)
    for column in columns:
        condition = condition & given(column)
    return condition


def clean_orders(orders: DataFrame) -> DataFrame:
    """Typed orders with a ``valid`` flag (rules O1-O4)."""
    typed = (
        orders.withColumn("created", epoch_seconds("order_created_ts"))
        .withColumn("promised", epoch_seconds("promised_ts"))
        .withColumn("updated", epoch_seconds("source_updated_at"))
        .withColumn("pickup", epoch_seconds("pickup_ts"))
        .withColumn("delivery", epoch_seconds("delivery_ts"))
    )
    delivered = sf.col("status") == DELIVERED
    delivery_after_pickup = (
        sf.col("pickup").isNotNull()
        & sf.col("delivery").isNotNull()
        & (sf.col("delivery") >= sf.col("pickup"))
    )
    valid = (
        present(ORDER_REQUIRED)
        & sf.col("created").isNotNull()
        & sf.col("promised").isNotNull()
        & sf.col("updated").isNotNull()
        & sf.col("status").isin(*STATUSES)
        & (~given("pickup_ts") | sf.col("pickup").isNotNull())
        & (~given("delivery_ts") | sf.col("delivery").isNotNull())
        & (~delivered | delivery_after_pickup)
    )
    return typed.withColumn("valid", sf.coalesce(valid, sf.lit(False)))


def clean_events(events: DataFrame) -> DataFrame:
    """Typed events with a ``valid`` flag (rules E1-E2)."""
    typed = (
        events.withColumn("ts", epoch_seconds("event_ts"))
        .withColumn("lat_d", sf.expr("try_cast(lat AS DOUBLE)"))
        .withColumn("lon_d", sf.expr("try_cast(lon AS DOUBLE)"))
        .withColumn("ping", sf.col("event_type") == GPS_PING)
    )
    coordinates_ok = sf.col("lat_d").between(-90, 90) & sf.col("lon_d").between(-180, 180)
    valid = (
        present(EVENT_REQUIRED)
        & sf.col("ts").isNotNull()
        & sf.col("event_type").isin(*EVENT_TYPES)
        & (~sf.col("ping") | sf.coalesce(coordinates_ok, sf.lit(False)))
    )
    return typed.withColumn("valid", sf.coalesce(valid, sf.lit(False)))


def flag_gps(clean: DataFrame) -> DataFrame:
    """Clean events with a ``flagged`` column (impossible GPS pings, per driver in time order)."""
    order = Window.partitionBy("driver_id").orderBy("ts", "event_id")
    previous_lat = sf.lag("lat_d").over(order)
    previous_lon = sf.lag("lon_d").over(order)
    previous_ts = sf.lag("ts").over(order)
    phi1 = previous_lat * DEGREES_TO_RADIANS
    phi2 = sf.col("lat_d") * DEGREES_TO_RADIANS
    dphi = (sf.col("lat_d") - previous_lat) * DEGREES_TO_RADIANS
    dlambda = (sf.col("lon_d") - previous_lon) * DEGREES_TO_RADIANS
    h = sf.pow(sf.sin(dphi / 2), 2) + sf.cos(phi1) * sf.cos(phi2) * sf.pow(sf.sin(dlambda / 2), 2)
    distance = 2 * EARTH_RADIUS_KM * sf.asin(sf.sqrt(h))
    seconds = sf.col("ts") - previous_ts
    impossible = (seconds <= 0) | ((distance / seconds) * 3600 > MAX_SPEED_KMH)
    flagged = previous_ts.isNotNull() & impossible
    pings = clean.where(sf.col("ping")).withColumn("flagged", sf.coalesce(flagged, sf.lit(False)))
    others = clean.where(~sf.col("ping")).withColumn("flagged", sf.lit(False))
    return pings.unionByName(others)


def week_counts(spark: SparkSession, path: Path, week: Sequence[str]) -> list[tuple[str, int, int]]:
    """(date, events, gps_pings) for ``week`` from a typed events copy (src/bench/reads.py)."""
    day = sf.col("date").cast("string")
    rows = (
        spark.read.parquet(str(path))
        .where(day.isin(*week))
        .groupBy(day.alias("day"))
        .agg(
            sf.count(sf.lit(1)).alias("events"),
            sf.sum((sf.col("event_type") == GPS_PING).cast("long")).alias("pings"),
        )
        .orderBy("day")
        .collect()
    )
    return [(str(row["day"]), int(row["events"]), int(row["pings"])) for row in rows]


def _count_input_and_rejected(frame: DataFrame) -> tuple[int, int]:
    row = frame.agg(
        sf.count(sf.lit(1)).alias("input"),
        sf.sum((~sf.col("valid")).cast("long")).alias("rejected"),
    ).first()
    if row is None:
        raise RuntimeError("Spark returned no counts.")
    return int(row["input"]), int(row["rejected"] or 0)


def run_workload(spark: SparkSession, shards: Sequence[Path]) -> WorkloadResult:
    """The whole workload over ``shards``."""
    orders = clean_orders(spark.read.parquet(*[str(shard / ORDERS_FILE) for shard in shards]))
    orders_input, orders_rejected = _count_input_and_rejected(orders)
    newest = Window.partitionBy("order_id").orderBy(sf.desc("updated"), sf.desc(ROW_ID))
    valid_orders = orders.where(sf.col("valid"))
    clean = valid_orders.withColumn("rank", sf.row_number().over(newest)).where("rank = 1")
    delivered = sf.col("status") == DELIVERED
    late = delivered & (sf.col("delivery") > sf.col("promised"))
    delivery_seconds = sf.when(delivered, sf.col("delivery") - sf.col("pickup")).otherwise(0)
    days = (
        clean.withColumn("date", reporting_date(sf.col("created")))
        .groupBy("date", "city")
        .agg(
            sf.count(sf.lit(1)).alias("orders"),
            sf.sum(delivered.cast("long")).alias("delivered"),
            sf.sum(late.cast("long")).alias("late"),
            sf.sum((sf.col("status") == CANCELLED).cast("long")).alias("cancelled"),
            sf.sum(delivery_seconds.cast("long")).alias("seconds"),
        )
        .orderBy("date", "city")
        .collect()
    )
    clean_count = sum(int(row["orders"]) for row in days)
    result = WorkloadResult(
        orders_input=orders_input,
        orders_rejected=orders_rejected,
        orders_duplicate=valid_orders.count() - clean_count,
        orders_clean=clean_count,
        order_days=[
            (
                str(row["date"]),
                str(row["city"]),
                int(row["orders"]),
                int(row["delivered"]),
                int(row["late"]),
                int(row["cancelled"]),
                int(row["seconds"]),
            )
            for row in days
        ],
    )

    events = clean_events(spark.read.parquet(*[str(shard / EVENTS_FILE) for shard in shards]))
    events_input, events_rejected = _count_input_and_rejected(events)
    first = Window.partitionBy("event_id").orderBy(ROW_ID)
    valid_events = events.where(sf.col("valid"))
    deduped = valid_events.withColumn("rank", sf.row_number().over(first)).where("rank = 1")
    event_rows = (
        flag_gps(deduped)
        .withColumn("date", reporting_date(sf.col("ts")))
        .groupBy("date")
        .agg(
            sf.count(sf.lit(1)).alias("events"),
            sf.sum(sf.col("ping").cast("long")).alias("pings"),
            sf.sum(sf.col("flagged").cast("long")).alias("flagged"),
        )
        .orderBy("date")
        .collect()
    )
    clean_events_count = sum(int(row["events"]) for row in event_rows)
    result.events_input = events_input
    result.events_rejected = events_rejected
    result.events_duplicate = valid_events.count() - clean_events_count
    result.events_clean = clean_events_count
    result.gps_flagged = sum(int(row["flagged"]) for row in event_rows)
    result.event_days = [
        (str(row["date"]), int(row["events"]), int(row["pings"]), int(row["flagged"]))
        for row in event_rows
    ]
    return result
