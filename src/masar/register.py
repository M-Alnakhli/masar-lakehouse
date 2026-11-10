"""Register path-based bronze Delta tables in the local metastore so dbt can use them as sources.

Run after landing bronze:  python -m masar.register
"""
from pyspark.errors import AnalysisException

from masar.config import BRONZE
from masar.spark import get_spark

TABLES = ["trips", "drivers", "gps_pings"]


def register_bronze() -> None:
    spark = get_spark("masar-register")
    spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
    from pathlib import Path
    for t in TABLES:
        if (Path(BRONZE) / t / "_delta_log").exists():
            want = f"{BRONZE}/{t}"
            if spark.catalog.tableExists(f"bronze.{t}"):
                try:
                    have = (spark.sql(f"DESCRIBE TABLE EXTENDED bronze.{t}")
                            .where("col_name = 'Location'").first()[1])
                except AnalysisException:  # entry points at a path that no longer exists
                    have = "<missing path>"
                if have.removeprefix("file:").rstrip("/") != want.rstrip("/"):
                    # stale entry (e.g. registered from another checkout path).
                    # EXTERNAL table: DROP only removes the catalog entry, not the data.
                    print(f"re-pointing bronze.{t}: {have} -> {want}")
                    spark.sql(f"DROP TABLE bronze.{t}")
            spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{t} USING DELTA LOCATION '{want}'")
            print(f"registered bronze.{t}")
        else:
            print(f"skipped bronze.{t} (not landed yet)")


if __name__ == "__main__":
    register_bronze()
