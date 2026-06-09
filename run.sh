#!/bin/bash
# =============================================================
# run.sh — Full pipeline automation (bonus points criterion)
#
# Run on  : HDP Sandbox  [maria_dev@sandbox-hdp ~]$
# Usage   : bash run.sh [KAGGLE_USERNAME] [KAGGLE_KEY]
# Example : bash run.sh myuser abc123xyz
#
# Pipeline (each step must complete before the next runs):
#   1 → Download Spotify Charts from Kaggle (3.8GB, 2017-2021)
#   2 → Upload CSV files to HDFS
#   3 → spark-submit preprocess.py → clean data → Parquet
#   4 → spark-submit create_hive_table.py → create spotify_db.charts
#   5 → HiveQL analysis → results/q1,q2,q3.csv
#   6 → python visualize.py → results/*.png
#   7 → hdfs dfs -put results/*.png → HDFS /user/maria_dev/spotify/result/
# =============================================================

# ── Log helpers ──────────────────────────────────────────────────
B='\033[0;34m'; G='\033[0;32m'; Y='\033[1;33m'; R='\033[0;31m'; N='\033[0m'
log()  { echo -e "${B}[INFO]${N} $1"; }
ok()   { echo -e "${G}[ OK ]${N} $1"; }
warn() { echo -e "${Y}[WARN]${N} $1"; }
err()  { echo -e "${R}[ERR ]${N} $1"; exit 1; }
skip() { echo -e "${Y}[SKIP]${N} $1 — already done"; }

# ── Kaggle credentials ────────────────────────────────────────────
KAGGLE_USERNAME="${1:-$KAGGLE_USERNAME}"
KAGGLE_KEY="${2:-$KAGGLE_KEY}"

setup_kaggle() {
    mkdir -p ~/.kaggle
    if [ -n "$KAGGLE_USERNAME" ] && [ -n "$KAGGLE_KEY" ]; then
        printf '{"username":"%s","key":"%s"}' \
            "$KAGGLE_USERNAME" "$KAGGLE_KEY" > ~/.kaggle/kaggle.json
        chmod 600 ~/.kaggle/kaggle.json
        ok "Kaggle credentials configured"
    elif [ -f ~/.kaggle/kaggle.json ]; then
        ok "Using existing ~/.kaggle/kaggle.json"
    else
        err "Missing Kaggle credentials.\nUsage: bash run.sh USERNAME API_KEY"
    fi
}

# ── Environment check ─────────────────────────────────────────────
check_env() {
    log "Checking HDP environment..."
    command -v hdfs         >/dev/null 2>&1 || err "hdfs not found!"
    command -v spark-submit >/dev/null 2>&1 || err "spark-submit not found!"
    command -v hive         >/dev/null 2>&1 || err "hive not found!"
    [ -x /usr/bin/python3.6 ]              || err "/usr/bin/python3.6 not found!"
    AVAIL=$(hdfs dfs -df -h / 2>/dev/null | tail -1 | awk '{print $3}')
    ok "HDFS, Spark, Hive, Python ready — HDFS free: ${AVAIL}"
}

# ── Step 1: Download & split data ────────────────────────────────
step1_collect() {
    echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "STEP 1/7: Download & split data from Kaggle"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd ~/spotify-chart-analysis
    N=$(ls data/raw/*.csv 2>/dev/null | wc -l)
    if [ "$N" -gt 50 ]; then
        skip "Step 1 (found ${N} CSV files in data/raw/)"
        return 0
    fi
    /usr/bin/python3.6 src/ingest/collect.py || err "Data collection failed — check Kaggle credentials"
    ok "Step 1 complete"
}

# ── Step 2: Upload to HDFS ────────────────────────────────────────
step2_hdfs() {
    echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "STEP 2/7: Upload to HDFS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd ~/spotify-chart-analysis
    bash src/ingest/upload_to_hdfs.sh || err "HDFS upload failed — check HDFS connectivity"
    ok "Step 2 complete"
}

# ── Step 3: Spark preprocessing ──────────────────────────────────
step3_spark() {
    echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "STEP 3/7: PySpark preprocessing (~5-10 min)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd ~/spotify-chart-analysis
    T0=$(date +%s)
    export PYSPARK_PYTHON=/usr/bin/python3.6
    export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
    export HDP_VERSION=3.0.1
    export PYTHONIOENCODING=utf-8
    spark-submit \
        --master local[*] \
        --driver-memory 2g \
        src/pipeline/preprocess.py || err "Spark preprocessing failed — check Spark logs"
    ok "Step 3 complete ($(( $(date +%s) - T0 ))s)"
}

# ── Step 4: Create Hive table ─────────────────────────────────────
step4_hive_table() {
    echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "STEP 4/7: Create Hive table via SparkSQL"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd ~/spotify-chart-analysis

    export PYSPARK_PYTHON=/usr/bin/python3.6
    export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
    export HDP_VERSION=3.0.1
    export PYTHONIOENCODING=utf-8
    spark-submit \
        --master local[*] \
        --driver-memory 1g \
        src/pipeline/create_hive_table.py || err "Hive table creation failed — check Spark log"
    ok "Step 4 complete"
}

# ── Step 5: HiveQL analysis ───────────────────────────────────────
step5_analyze() {
    echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "STEP 5/7: HiveQL analysis → 3 research questions"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd ~/spotify-chart-analysis
    mkdir -p results

    # Use spark-submit (not hive CLI) — Ranger blocks hive CLI from HDFS Parquet path
    export PYSPARK_PYTHON=/usr/bin/python3.6
    export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
    export HDP_VERSION=3.0.1
    export PYTHONIOENCODING=utf-8
    spark-submit \
        --master local[*] \
        --driver-memory 2g \
        src/analyze/analyze.py || err "HiveQL analysis failed — check Spark logs"

    csv_ok() { awk -F'\t' 'NF>1{found=1;exit} END{exit !found}' "$1" 2>/dev/null; }
    for f in results/q1_crossmarket.csv results/q2_longevity.csv results/q3_japan_streams.csv; do
        csv_ok "$f" || err "Step 5: $f missing or empty after spark-submit"
    done

    echo; ls -lh results/*.csv
    ok "Step 5 complete"
}

# ── Step 6: Visualization ─────────────────────────────────────────
step6_visualize() {
    echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "STEP 6/7: Generate visualization charts"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd ~/spotify-chart-analysis

    csv_ok() { awk -F'\t' 'NF>1{found=1;exit} END{exit !found}' "$1" 2>/dev/null; }
    for f in results/q1_crossmarket.csv results/q2_longevity.csv results/q3_japan_streams.csv; do
        { [ -s "$f" ] && csv_ok "$f"; } || err "Step 6: $f missing or empty — step 5 did not complete successfully"
    done

    # Always remove old PNGs so visualization re-runs with fresh data
    rm -f results/*.png

    # Install matplotlib and add a CJK font for Japanese/Korean/Vietnamese titles
    log "  Installing matplotlib..."
    /usr/bin/python3.6 -m pip install matplotlib -q --user 2>/dev/null || true

    log "  Installing CJK font for Vietnamese/Japanese/Chinese labels..."
    BUNDLED_FONT="src/analyze/fonts/wqy-microhei.ttc"
    mkdir -p src/analyze/fonts

    # A genuine wqy-microhei.ttc is ~5MB. Reject anything much smaller --
    # a bad URL/redirect/network error can make wget save an HTML error
    # page AT the destination path, which would otherwise look "present"
    # on every future run and silently break CJK rendering again.
    font_is_valid() {
        [ -s "$1" ] && [ "$(wc -c < "$1" 2>/dev/null)" -gt 1000000 ]
    }

    # Method 1 (preferred): bundle font in project directory — visualize.py loads it
    # directly by path, so no yum/fc-list needed at all.
    if ! font_is_valid "$BUNDLED_FONT"; then
        log "  Downloading WenQuanYi font into project (one-time)..."
        rm -f "$BUNDLED_FONT"
        wget -q --timeout=60 \
            -O "$BUNDLED_FONT" \
            "https://raw.githubusercontent.com/anthonyfok/fonts-wqy-microhei/master/wqy-microhei.ttc" \
            2>/dev/null || true
        font_is_valid "$BUNDLED_FONT" || rm -f "$BUNDLED_FONT"
    fi

    # Method 2: system install (fallback if download fails / no internet)
    if ! font_is_valid "$BUNDLED_FONT" && ! fc-list :lang=ja 2>/dev/null | grep -q .; then
        sudo yum install -y wqy-microhei-fonts 2>/dev/null || true
    fi

    # Method 3: EPEL + Noto (second fallback)
    if ! font_is_valid "$BUNDLED_FONT" && ! fc-list :lang=ja 2>/dev/null | grep -q .; then
        sudo yum install -y epel-release 2>/dev/null || true
        sudo yum install -y google-noto-sans-cjk-ttc-fonts 2>/dev/null || true
    fi

    /usr/bin/python3.6 -c "import matplotlib.font_manager; matplotlib.font_manager._rebuild()" 2>/dev/null || true

    # Run visualization (exits 1 if CSV files are missing)
    /usr/bin/python3.6 src/analyze/visualize.py || err "Visualization failed — check CSV results in results/"

    PNG_N=$(ls results/*.png 2>/dev/null | wc -l)
    ok "Step 6 complete — ${PNG_N} charts saved to results/"
    ls -lh results/*.png 2>/dev/null
}

# ── Step 7: Upload result images to HDFS ──────────────────────────
HDFS_RESULT="/user/maria_dev/spotify/result"

step7_upload_results() {
    echo; echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log "STEP 7/7: Upload result images to HDFS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    cd ~/spotify-chart-analysis

    # Check PNG files exist locally
    PNG_N=$(ls results/*.png 2>/dev/null | wc -l)
    if [ "$PNG_N" -eq 0 ]; then
        err "No PNG files found in results/ — step 6 did not complete successfully"
    fi

    # Always upload — use -f to overwrite any old PNGs already on HDFS

    # Create HDFS result directory
    log "  Creating HDFS directory: ${HDFS_RESULT}/"
    hdfs dfs -mkdir -p "$HDFS_RESULT" || err "Cannot create HDFS directory: ${HDFS_RESULT}"

    # Upload each PNG (force overwrite)
    DONE=0
    for f in results/*.png; do
        fname=$(basename "$f")
        if hdfs dfs -put -f "$f" "${HDFS_RESULT}/${fname}" 2>/dev/null; then
            DONE=$((DONE + 1))
            ok "  Uploaded: ${fname}"
        else
            warn "  Failed: ${fname} — retrying..."
            sleep 2
            if hdfs dfs -put -f "$f" "${HDFS_RESULT}/${fname}" 2>/dev/null; then
                DONE=$((DONE + 1))
                ok "  Uploaded (retry): ${fname}"
            else
                err "  Cannot upload: ${fname}"
            fi
        fi
    done

    echo
    log "  HDFS result directory contents:"
    hdfs dfs -ls "$HDFS_RESULT/" 2>/dev/null
    ok "Step 7 complete — ${DONE} PNG files uploaded to hdfs://${HDFS_RESULT}/"
}

# ── Run pipeline ──────────────────────────────────────────────────
T_TOTAL=$(date +%s)
echo
echo "╔══════════════════════════════════════════════════╗"
echo "║  Spotify Chart Analysis — Full Pipeline          ║"
echo "║  HDP Sandbox  •  bash run.sh                     ║"
echo "╚══════════════════════════════════════════════════╝"

check_env
setup_kaggle
step1_collect
step2_hdfs
step3_spark
step4_hive_table
step5_analyze
step6_visualize
step7_upload_results

ELAPSED=$(( $(date +%s) - T_TOTAL ))
echo
echo "╔══════════════════════════════════════════════════╗"
printf "║  Pipeline complete!  Total time: %ds\n" "$ELAPSED"
echo "║"
echo "║  Local  : ~/spotify-chart-analysis/results/"
echo "║  HDFS   : hdfs://${HDFS_RESULT}/"
echo "║  Charts : 01_crossmarket_hits.png"
echo "║           02_chart_longevity.png"
echo "║           03_japan_streams.png"
echo "║"
echo "╚══════════════════════════════════════════════════╝"
