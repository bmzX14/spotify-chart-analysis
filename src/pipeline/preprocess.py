"""
preprocess.py
=============
Purpose : Read CSV from HDFS, clean data, save as Parquet.
Run on  : HDP Sandbox
Called by: run.sh  →  spark-submit src/pipeline/preprocess.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

HDFS_RAW   = "hdfs:///user/maria_dev/spotify/raw"
HDFS_CLEAN = "hdfs:///user/maria_dev/spotify/clean"

# ── Main ──────────────────────────────────────────────────────────
print("=" * 55)
print("  STEP 3: PySpark Preprocessing")
print("=" * 55)

spark = (SparkSession.builder
         .appName("SpotifyPreprocess")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# 1. Read all CSV files from HDFS
print(">>> Reading data from HDFS...")
df = spark.read.csv(HDFS_RAW, header="true", sep=",", inferSchema="true")
print(f"    Total rows: {df.count():,}")
df.printSchema()

# 2. Normalize column names
df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

# 3. Normalize region names to project country codes.
REGION_MAP = {
    "vietnam":     "VN",
    "south korea": "KR",
    "japan":       "JP",
    "taiwan":      "TW",
    "hong kong":   "HK",
}
region_expr = F.col("region")
for full_name, code in REGION_MAP.items():
    region_expr = F.when(
        F.lower(F.col("region")) == full_name, code
    ).otherwise(region_expr)
df = df.withColumn("region", region_expr)

# 4. Cast to correct types
print(">>> Casting column types...")
df = (df
      .withColumn("rank",    F.col("rank").cast(IntegerType()))
      .withColumn("streams", F.col("streams").cast(LongType()))
      .withColumn("date",    F.to_date(F.col("date"), "yyyy-MM-dd"))
      .withColumn("year",    F.year("date")))

# 5. Remove nulls and invalid rows
before = df.count()
df = df.dropna(subset=["title", "artist", "streams", "region", "date", "rank"])
df = df.filter(F.col("streams") > 0).filter(F.col("rank").between(1, 200))
after = df.count()
print(f">>> Cleaned: {before:,} -> {after:,} rows (removed {before - after:,} invalid rows)")

# 6. Preview safely on HDP's ASCII locale.
print(">>> Preview (5 rows):")
try:
    df.select("date", "region", "rank", "title", "artist", "streams") \
      .show(5, truncate=False)
except Exception:
    df.select("region", "rank", "streams").show(5)

print(">>> Row count by country:")
try:
    df.groupBy("region").count().orderBy(F.col("count").desc()).show()
except Exception:
    pass

# 7. Save as Parquet
print(f">>> Saving Parquet -> {HDFS_CLEAN}")
df.write.mode("overwrite").parquet(HDFS_CLEAN)
print("    [OK] Saved successfully.")
print(">>> Done! Next: spark-submit src/pipeline/create_hive_table.py")

spark.stop()
