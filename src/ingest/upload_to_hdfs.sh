#!/bin/bash
# upload_to_hdfs.sh
# Purpose : Upload all Asian CSV files from HDP local disk to HDFS.
# Run on  : HDP Sandbox
# Called by: run.sh (automatic)

LOCAL_DIR="$HOME/spotify-chart-analysis/data/raw"
HDFS_DIR="/user/maria_dev/spotify/raw"

echo ">>> Creating HDFS directory..."
hdfs dfs -mkdir -p "$HDFS_DIR"

FILE_COUNT=$(ls "$LOCAL_DIR"/*.csv 2>/dev/null | wc -l)
[ "$FILE_COUNT" -eq 0 ] && echo "[ERROR] No CSV files found in $LOCAL_DIR" && exit 1

echo ">>> Uploading $FILE_COUNT files to HDFS (force overwrite)..."
DONE=0
FAILED=0

for f in "$LOCAL_DIR"/*.csv; do
    fname=$(basename "$f")
    if hdfs dfs -put -f "$f" "$HDFS_DIR/$fname" 2>/dev/null; then
        DONE=$((DONE+1))
        if [ $((DONE % 20)) -eq 0 ]; then
            echo "    Uploaded $DONE/$FILE_COUNT files..."
        fi
    else
        echo "    [WARN] Failed: $fname — retrying..."
        sleep 2
        hdfs dfs -put -f "$f" "$HDFS_DIR/$fname" 2>/dev/null && \
            DONE=$((DONE+1)) || \
            FAILED=$((FAILED+1))
    fi
done

echo ""
echo ">>> Uploaded: $DONE files"
hdfs dfs -du -s -h "$HDFS_DIR"
if [ "$FAILED" -gt 0 ]; then
    echo ">>> [ERR] $FAILED files failed to upload — check HDFS connectivity"
    exit 1
fi
echo ">>> [OK] Upload complete"
