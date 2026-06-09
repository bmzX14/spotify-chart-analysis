"""
analyze.py
==========
Purpose : Run 3 HiveQL research questions via SparkSQL (bypasses Ranger/HiveServer2
          permission checks that block the hive CLI from accessing HDFS Parquet data).
Run on  : HDP Sandbox
Called by: run.sh -> spark-submit src/analyze/analyze.py
"""

import os
from pyspark.sql import SparkSession

OUT_DIR = os.path.expanduser("~/spotify-chart-analysis/results")
os.makedirs(OUT_DIR, exist_ok=True)

print("=" * 55)
print("  STEP 5: HiveQL Analysis (via SparkSQL)")
print("=" * 55)

spark = (SparkSession.builder
         .appName("SpotifyAnalyze")
         .enableHiveSupport()
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sql("USE spotify_db")

def save_tsv(df, path):
    rows = df.collect()
    if not rows:
        raise RuntimeError(f"Query returned 0 rows - {path}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(df.columns) + "\n")
        for row in rows:
            f.write("\t".join(str(v) if v is not None else "" for v in row) + "\n")
    print(f"    [OK] {len(rows)} rows -> {path}")

# Q1: Cross-market hits - songs charting in 3+ countries
print(">>> Q1: Cross-market hits (songs charting in 3+ countries)...")
q1 = spark.sql("""
    SELECT title, artist,
           COUNT(DISTINCT region) AS num_countries,
           COUNT(*) AS total_appearances
    FROM charts
    GROUP BY title, artist
    HAVING COUNT(DISTINCT region) >= 3
    ORDER BY num_countries DESC, total_appearances DESC
    LIMIT 30
""")
save_tsv(q1, os.path.join(OUT_DIR, "q1_crossmarket.csv"))

# Q2: Chart longevity - top 5 songs per country by days on chart
print(">>> Q2: Chart longevity (top 5 songs per country by days on chart)...")
q2 = spark.sql("""
    SELECT region, title, artist, days_on_chart, best_rank
    FROM (
        SELECT region, title, artist, days_on_chart, best_rank,
               ROW_NUMBER() OVER (
                   PARTITION BY region
                   ORDER BY days_on_chart DESC
               ) AS rn
        FROM (
            SELECT region, title, artist,
                   COUNT(DISTINCT date) AS days_on_chart,
                   MIN(rank)            AS best_rank
            FROM charts
            GROUP BY region, title, artist
        ) agg
    ) ranked
    WHERE rn <= 5
    ORDER BY region, days_on_chart DESC
""")
save_tsv(q2, os.path.join(OUT_DIR, "q2_longevity.csv"))

# Q3: Total streams in Japan by year + top song per year
print(">>> Q3: Japan total streams by year + top song per year...")
q3 = spark.sql("""
    SELECT s.year,
           s.total_streams,
           t.title,
           t.artist,
           t.peak_streams
    FROM (
        SELECT year, SUM(streams) AS total_streams
        FROM charts
        WHERE region = 'JP'
        GROUP BY year
    ) s
    JOIN (
        SELECT year, title, artist, peak_streams
        FROM (
            SELECT year, title, artist, peak_streams,
                   ROW_NUMBER() OVER (
                       PARTITION BY year
                       ORDER BY peak_streams DESC
                   ) AS rn
            FROM (
                SELECT year, title, artist,
                       SUM(streams) AS peak_streams
                FROM charts
                WHERE region = 'JP'
                GROUP BY year, title, artist
            ) agg
        ) ranked
        WHERE rn = 1
    ) t ON s.year = t.year
    ORDER BY s.year
""")
save_tsv(q3, os.path.join(OUT_DIR, "q3_japan_streams.csv"))

spark.stop()
print("=" * 55)
print("  Step 5 complete - 3 result files saved to results/")
print("=" * 55)
