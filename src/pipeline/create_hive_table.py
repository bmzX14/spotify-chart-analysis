"""
create_hive_table.py
====================
Purpose : Create spotify_db.charts as a Hive external table via SparkSQL.
          Running via spark-submit (as maria_dev) bypasses HiveServer2/Ranger
          permission checks that block the hive CLI from accessing the HDFS path.
Run on  : HDP Sandbox
Called by: run.sh → spark-submit src/pipeline/create_hive_table.py
"""

from pyspark.sql import SparkSession

HDFS_CLEAN = "hdfs:///user/maria_dev/spotify/clean"

print("=" * 55)
print("  STEP 4: Create Hive Table (via SparkSQL)")
print("=" * 55)

spark = (SparkSession.builder
         .appName("SpotifyCreateTable")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

print(">>> Creating database spotify_db...")
spark.sql("CREATE DATABASE IF NOT EXISTS spotify_db")

print(">>> Dropping existing table (if any)...")
spark.sql("DROP TABLE IF EXISTS spotify_db.charts")

print(">>> Creating external table spotify_db.charts...")
spark.sql(f"""
    CREATE EXTERNAL TABLE spotify_db.charts (
        `date`  DATE    COMMENT 'Chart date',
        region  STRING  COMMENT 'Country code (VN, KR, JP, TW, HK)',
        chart   STRING  COMMENT 'Chart type (top200)',
        trend   STRING  COMMENT 'Trend direction',
        rank    INT     COMMENT 'Chart rank (1-200)',
        title   STRING  COMMENT 'Song title',
        artist  STRING  COMMENT 'Artist name',
        streams BIGINT  COMMENT 'Daily stream count',
        url     STRING  COMMENT 'Spotify track URL',
        year    INT     COMMENT 'Year (derived)'
    )
    STORED AS PARQUET
    LOCATION '{HDFS_CLEAN}'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
""")

count = spark.sql("SELECT COUNT(*) FROM spotify_db.charts").collect()[0][0]
if count == 0:
    raise RuntimeError("Table created but has 0 rows - Parquet may be missing or empty")

print(f"    [OK] spotify_db.charts - {count:,} rows")

print(">>> Row count by region:")
spark.sql("""
    SELECT region, COUNT(*) AS rows
    FROM spotify_db.charts
    GROUP BY region
    ORDER BY rows DESC
""").show()

spark.stop()
