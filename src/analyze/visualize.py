import os
import sys
from io import StringIO
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend: no display needed on HDP
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# Resolve results/ relative to the project root.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))      # src/analyze/
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SCRIPT_DIR))  # project root
RESULTS_DIR = os.path.join(_PROJECT_ROOT, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

COUNTRY_NAMES = {
    "VN": "Vietnam",
    "KR": "Korea",
    "JP": "Japan",
    "TW": "Taiwan",
    "HK": "Hong Kong",
}
COUNTRY_COLORS = {
    "Vietnam":   "#E8534A",
    "Korea":     "#5B8FF9",
    "Japan":     "#F6BD16",
    "Taiwan":    "#5AD8A6",
    "Hong Kong": "#FF99C3",
}

# Load one known CJK-capable font file and use its exact family name.
# This avoids matplotlib's fuzzy fallback behavior for Vietnamese/CJK text.
def _load_cjk_font():
    _FONTS_DIR = os.path.join(_SCRIPT_DIR, "fonts")
    candidates = [
        # Bundled fonts downloaded by run.sh.
        os.path.join(_FONTS_DIR, "wqy-microhei.ttc"),
        os.path.join(_FONTS_DIR, "NotoSansCJK-Regular.ttc"),
        # Common system locations as fallback.
        "/usr/share/fonts/wqy-microhei/wqy-microhei.ttc",
        "/usr/share/fonts/wqy-zenhei/wqy-zenhei.ttc",
        "/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc",
        os.path.expanduser("~/.fonts/wqy-microhei.ttc"),
        os.path.expanduser("~/.fonts/NotoSansCJK-Regular.ttc"),
    ]
    for path in candidates:
        if not os.path.isfile(path):
            continue
        try:
            fm.fontManager.addfont(path)
            # Use the exact registered family name from the font file.
            name = fm.FontProperties(fname=path).get_name()
            return name, path
        except Exception:
            continue
    return None, None

_CJK_FONT_NAME, _CJK_FONT_PATH = _load_cjk_font()
_HAS_CJK = _CJK_FONT_NAME is not None

plt.rcParams.update({
    "figure.dpi":           130,
    "font.size":            11,
    "axes.spines.top":      False,
    "axes.spines.right":    False,
    "axes.unicode_minus":   False,
})
if _CJK_FONT_NAME:
    # Force one exact font family for all text artists.
    plt.rcParams["font.family"] = _CJK_FONT_NAME


# ── Helpers ───────────────────────────────────────────────────────

def load_tsv(filename, col_names):
    """Load a tab-separated result file. Returns None if missing or empty."""
    path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        print("  [ERR]  %s not found or empty" % filename)
        return None

    HIVE_NOISE = ("Note ", "WARN ", "INFO ", "SLF4J", "OK", "Time taken",
                  "Logging", "deprecated", "hive>", "WARNING")
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        clean_lines = [
            l for l in fh
            if "\t" in l and not any(l.startswith(p) for p in HIVE_NOISE)
        ]

    if not clean_lines:
        print("  [ERR]  %s has no valid tab-separated data rows" % filename)
        return None

    try:
        df = pd.read_csv(StringIO("".join(clean_lines)), sep="\t",
                         header=None, names=col_names, on_bad_lines="skip")
    except TypeError:
        df = pd.read_csv(StringIO("".join(clean_lines)), sep="\t",
                         header=None, names=col_names, error_bad_lines=False)

    df = df[df[col_names[0]] != col_names[0]]  # drop repeated header rows

    NUMERIC_COLS = {
        "num_countries", "total_appearances",
        "days_on_chart", "best_rank",
        "total_streams", "peak_streams", "year",
    }
    for c in col_names:
        if c in NUMERIC_COLS:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    numeric_present = [c for c in col_names if c in NUMERIC_COLS]
    if numeric_present:
        df = df.dropna(subset=numeric_present, how="all")

    print("  [OK]   %s - %d rows" % (filename, len(df)))
    return df


def save(fig, name):
    path = os.path.join(RESULTS_DIR, name)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print("  [OK]   Saved -> %s" % path)


def _label(text, maxlen=28):
    """Truncate label. Fall back to ASCII replacement if no CJK font found."""
    s = str(text)[:maxlen]
    if not _HAS_CJK:
        s = s.encode("ascii", errors="replace").decode()
    return s


# ── Chart 1 - Q1: Cross-market hits ──────────────────────────────

def plot_crossmarket(df):
    """Horizontal bar: songs charting in 3+ countries, colored by market count."""
    df = df.copy()
    df = df.dropna(subset=["num_countries", "total_appearances"])
    df["num_countries"] = df["num_countries"].astype(int)
    df["total_appearances"] = df["total_appearances"].astype(int)
    df["label"] = (df["artist"].astype(str).str[:20]
                   + "  -  " + df["title"].astype(str).str[:28])

    color_map = {3: "#A5D8FF", 4: "#339AF0", 5: "#1864AB",
                 6: "#003A8C", 7: "#001D4A"}
    colors = [color_map.get(int(n), "#A5D8FF") for n in df["num_countries"]]

    fig, ax = plt.subplots(figsize=(13, max(8, len(df) * 0.42)))
    ax.barh(df["label"], df["total_appearances"], color=colors)
    ax.invert_yaxis()

    from matplotlib.patches import Patch
    seen_counts = sorted(df["num_countries"].unique())
    legend_els = [
        Patch(color=color_map.get(int(n), "#A5D8FF"),
              label="Charted in %d markets" % int(n))
        for n in seen_counts
    ]
    ax.legend(handles=legend_els, loc="lower right", fontsize=9)
    ax.set_xlabel("Total Chart Appearances")
    ax.set_title(
        "Q1: Cross-Market Hits - Songs Charting in 3+ Countries (2017-2021)",
        fontsize=13, fontweight="bold", pad=12)
    plt.tight_layout()
    save(fig, "01_crossmarket_hits.png")


# ── Chart 2 - Q2: Chart longevity ────────────────────────────────

def plot_longevity(df):
    """Small-multiples horizontal bar: top 5 songs per country by days on chart."""
    df = df.copy()
    df = df.dropna(subset=["days_on_chart"])
    df["days_on_chart"] = df["days_on_chart"].astype(int)
    df["best_rank"] = pd.to_numeric(df["best_rank"], errors="coerce").fillna(0).astype(int)
    df["country"] = df["region"].map(COUNTRY_NAMES).fillna(df["region"])

    countries = [v for v in COUNTRY_NAMES.values() if v in df["country"].values]
    n = len(countries)
    if n == 0:
        print("  [WARN] No data to plot for Q2")
        return

    fig, axes = plt.subplots(1, n, figsize=(4 * n, 7), sharey=False)
    if n == 1:
        axes = [axes]

    for ax, country in zip(axes, countries):
        sub = (df[df["country"] == country]
               .nlargest(5, "days_on_chart")
               .reset_index(drop=True))
        if sub.empty:
            ax.set_visible(False)
            continue

        labels = [_label(t, 18) + "\n" + _label(a, 16)
                  for t, a in zip(sub["title"], sub["artist"])]
        color = COUNTRY_COLORS.get(country, "#aaa")
        ax.barh(labels[::-1], sub["days_on_chart"].values[::-1], color=color)

        for i, (_, row) in enumerate(sub[::-1].iterrows()):
            best = int(row["best_rank"])
            if best > 0:
                ax.text(2, i, "#%d" % best,
                        va="center", fontsize=8, color="white", fontweight="bold")

        ax.set_title(country, fontsize=11, fontweight="bold", color=color, pad=6)
        ax.set_xlabel("Days on Chart", fontsize=9)
        ax.tick_params(axis="y", labelsize=8)

    fig.suptitle(
        "Q2: Chart Longevity - Top 5 Songs per Country by Days on Chart (2017-2021)",
        fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    save(fig, "02_chart_longevity.png")


# ── Chart 3 - Q3: Japan total streams + top song per year ────────

def plot_japan_streams(df):
    """Bar chart: total streams in Japan by year, each bar annotated with top song."""
    df = df.copy()
    df = df.dropna(subset=["year", "total_streams"])
    df["year"] = df["year"].astype(int)
    df["total_streams"] = df["total_streams"].astype(float)
    df["streams_B"] = df["total_streams"] / 1e9
    df = df.sort_values("year")

    fig, ax = plt.subplots(figsize=(11, 7))

    bars = ax.bar(df["year"].astype(str), df["streams_B"],
                  color="#F6BD16", alpha=0.88, edgecolor="#c9960c", linewidth=0.8)

    # Annotate total streams value above each bar
    for bar, val in zip(bars, df["streams_B"]):
        ax.text(bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.03,
                "%.2fB" % val,
                ha="center", va="bottom", fontsize=10, color="#7d5a00", fontweight="bold")

    # Annotate top song inside each bar (title + artist)
    for bar, (_, row) in zip(bars, df.iterrows()):
        title_str = _label(row.get("title", ""), 22)
        artist_str = _label(row.get("artist", ""), 18)
        label_text = title_str + "\n" + artist_str
        bar_mid = bar.get_height() / 2
        if bar.get_height() > 0.3:
            ax.text(bar.get_x() + bar.get_width() / 2,
                    bar_mid, label_text,
                    ha="center", va="center",
                    fontsize=8.5, color="#3d2c00",
                    linespacing=1.4,
                    bbox=dict(boxstyle="round,pad=0.25", fc="white",
                              alpha=0.55, ec="none"))

    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Total Streams (Billions)", fontsize=11)
    ax.set_title(
        "Q3: Japan Spotify - Total Streams by Year & #1 Song (2017-2021)",
        fontsize=13, fontweight="bold", pad=12)
    ax.grid(axis="y", alpha=0.2)
    ax.set_axisbelow(True)
    plt.tight_layout()
    save(fig, "03_japan_streams.png")


# ── Main ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("  Spotify Chart - Visualization")
    print("=" * 55)
    if _CJK_FONT_NAME:
        print("  CJK font: %s  (%s)" % (_CJK_FONT_NAME, _CJK_FONT_PATH))
    else:
        print("  Font: default  (CJK/Vietnamese titles -> ASCII fallback)")
        print("  -> Run 'bash run.sh' once to auto-download the bundled CJK font, or")
        print("     manually place a font file at src/analyze/fonts/wqy-microhei.ttc")

    df_q1 = load_tsv("q1_crossmarket.csv",
                     ["title", "artist", "num_countries", "total_appearances"])
    df_q2 = load_tsv("q2_longevity.csv",
                     ["region", "title", "artist", "days_on_chart", "best_rank"])
    df_q3 = load_tsv("q3_japan_streams.csv",
                     ["year", "total_streams", "title", "artist", "peak_streams"])

    if df_q1 is None or df_q2 is None or df_q3 is None:
        print("\n[ERR] One or more result CSV files are missing.")
        print("      Run the full pipeline first: bash run.sh")
        sys.exit(1)

    print("\n>>> Drawing charts...")
    plot_crossmarket(df_q1)
    plot_longevity(df_q2)
    plot_japan_streams(df_q3)

    print("\n>>> Done! Charts saved to '%s/':" % RESULTS_DIR)
    for f in sorted(os.listdir(RESULTS_DIR)):
        if f.endswith(".png"):
            print("    %s" % f)
