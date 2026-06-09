# Spotify Chart Trend Analysis — Asian Countries

Apache Spark와 Hive를 기반으로 HDP Sandbox 환경에서 구축한 빅데이터 파이프라인으로, 2017–2021년 아시아 5개국(베트남·한국·일본·대만·홍콩)의 Spotify Top 200 차트 트렌드를 분석한다.

---

## 프로젝트 개요

Spotify Top 200 차트 데이터(2017–2021년, 아시아 5개국: 베트남·한국·일본·대만·홍콩)를 분석하는 빅데이터 파이프라인 프로젝트이다. HDP Sandbox 환경에서 **Apache Spark + Hive**를 핵심 기술 스택으로 사용하며, 데이터 수집 → HDFS 적재 → 전처리(Parquet 변환) → Hive 테이블 등록 → SparkSQL 분석 → Matplotlib 시각화 → 결과 업로드까지 **7단계가 `run.sh` 한 번 실행으로 자동화**되어 있다.

원본 데이터는 약 3.8GB(2,600만 행 이상)로, 단일 CSV 파일 기반으로 반복 처리하기에는 부담이 큰 규모이다. 따라서 HDFS 분산 저장과 Spark 병렬 처리를 활용하는 빅데이터 파이프라인으로 구성하였다. 또한 HDP Sandbox의 Apache Ranger가 `hive` CLI의 HDFS Parquet 경로 접근을 차단하므로, 모든 Hive 쿼리는 `spark-submit`을 통해 `enableHiveSupport()`로 우회 실행한다 (자세한 내용은 아래 "시스템 아키텍처" 참고).

---

## 연구 질문 (Research Questions)

다음 3가지 질문에 답한다:

**Q1 — 크로스마켓 히트곡 (Cross-Market Hits)**
2017–2021년 사이 아시아 5개 시장 중 어느 곡이 가장 많은 국가에서 동시에 차트인했는가? 3개국 이상에서 차트인한 곡만 "크로스마켓 히트곡"으로 간주하며, 차트인한 시장 수 → 총 차트인 횟수 순으로 정렬한다.

**Q2 — 차트 체류 기간 (Chart Longevity)**
각 국가에서 가장 오래 차트에 머문 곡은 무엇인가? Top 200에 등장한 일수(distinct days)로 측정하며, 국가별 상위 5곡과 그 곡의 최고 순위(best rank)를 함께 보여준다.

**Q3 — 일본 스트리밍 추이 (Japan Streaming Trends)**
2017–2021년 일본 Spotify 차트의 연도별 총 스트리밍 수는 어떻게 변화했으며, 그해 가장 많이 스트리밍된 곡은 무엇인가? 일본 시장의 일별 스트리밍 수를 연도별로 집계해 성장 추이를 보여주고, 그해를 대표하는 1위 곡을 함께 식별한다.

---

## 기술 스택 (Tech Stack)

| 역할 | 기술 |
|------|------|
| 데이터 수집 | Python, Kaggle API |
| 데이터 저장 | HDFS (Parquet, SNAPPY) |
| 데이터 전처리 | Apache Spark (PySpark) |
| 데이터 분석 | SparkSQL / HiveQL |
| 시각화 | Python, Matplotlib |
| 실행 환경 | HDP Sandbox 3.0 (Google Cloud VM) |

---

## 시스템 아키텍처 (System Architecture)

```
[Kaggle API]
     |  src/ingest/collect.py
     |  Download + split by country & month
     v
[data/raw/]  VN_2021-01.csv, KR_2021-01.csv, ... (100+ files)
     |  src/ingest/upload_to_hdfs.sh
     v
[HDFS: /user/maria_dev/spotify/raw/]
     |  spark-submit src/pipeline/preprocess.py
     v
[HDFS: /user/maria_dev/spotify/clean/]  <- Parquet (SNAPPY)
     |  spark-submit src/pipeline/create_hive_table.py
     v
[Hive: spotify_db.charts]
     |  spark-submit src/analyze/analyze.py
     v
[results/q1_crossmarket.csv, q2_longevity.csv, q3_japan_streams.csv]
     |  python src/analyze/visualize.py
     v
[results/01_crossmarket_hits.png, 02_chart_longevity.png, 03_japan_streams.png]
     |  hdfs dfs -put
     v
[HDFS: /user/maria_dev/spotify/result/]
```

> **참고:** 3~5단계는 `hive` CLI 대신 `spark-submit`을 사용한다. HDP Sandbox의 Apache Ranger가
> `hive` CLI의 HDFS Parquet 경로 직접 접근을 차단하기 때문에, `enableHiveSupport()`를 활성화한
> Spark 세션으로 우회 실행한다.

---

## 사전 준비 (Prerequisites)

**1. Kaggle API 토큰 발급:**
```
kaggle.com -> Account -> Create New API Token -> username과 key 확인
```

**2. HDP Sandbox 안에서 저장소 클론:**
---

## 실행 방법 (Run the Full Pipeline)

```bash
# 맥북 -> GCP VM으로 SSH 접속
gcphdp

# GCP VM -> HDP Sandbox로 SSH 접속
ssh maria_dev@localhost -p 2222
# 비밀번호: maria_dev

#HDP Sandbox
git clone https://github.com/bmzX14/spotify-chart-analysis
cd ~/spotify-chart-analysis
bash run.sh [KAGGLE_USERNAME] [KAGGLE_API_KEY]
# 예: bash run.sh myuser abc123xyz
```

`run.sh`는 아래 7단계를 순서대로 자동 실행하며, 한 단계라도 실패하면 즉시 중단된다 (예상 소요 시간: **20–35분**):

| 단계 | 설명 | 파일 |
|------|------|------|
| 1 | Kaggle에서 데이터 다운로드(~3.8GB) 후 국가·월별로 분할 | `src/ingest/collect.py` |
| 2 | 분할된 CSV 파일을 HDFS에 업로드 | `src/ingest/upload_to_hdfs.sh` |
| 3 | PySpark로 데이터 정제·타입 변환 후 Parquet(SNAPPY 압축)으로 저장 | `src/pipeline/preprocess.py` |
| 4 | SparkSQL로 외부 테이블 `spotify_db.charts` 생성 | `src/pipeline/create_hive_table.py` |
| 5 | SparkSQL로 Q1·Q2·Q3 쿼리 실행 → `results/q*.csv` 생성 | `src/analyze/analyze.py` |
| 6 | Matplotlib으로 3개의 차트 생성 → `results/*.png` 저장 | `src/analyze/visualize.py` |
| 7 | 결과 PNG 파일을 HDFS `/user/maria_dev/spotify/result/`에 업로드 | (run.sh 내부) |

실행 결과는 로컬 `results/` 폴더와 HDFS 양쪽에 저장된다.

---

## 결과 (Results)

| 분석 | 핵심 결과                                                                                                                                                                                                                                                                                                                                         | 결과 파일 |
|------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| **Q1** 크로스마켓 히트곡 | 3개국 이상에서 차트인한 상위 30곡 중 **30곡 전부가 5개 시장 모두**에서 차트인 — 단순한 "3개국 이상" 기준을 가뿐히 넘는 진정한 범아시아 히트곡들로 확인됨. 1위는 Ed Sheeran "Shape of You"(총 6,000회 이상 차트인); Ed Sheeran(4곡), BTS(3곡), BLACKPINK(2곡)가 반복 등장하며 글로벌 팝과 K-pop이 아시아 전역에서 동시에 통했음을 보여줌                                                                                                           | `01_crossmarket_hits.png` |
| **Q2** 차트 체류 기간 | 홍콩·일본·대만은 상위곡들이 **1,600일 이상**(5년 연구 기간 거의 전체) 동안 차트에 머무르며 자국 가수(Cantopop·J-pop·Mandopop) 중심으로 장기 인기를 유지. 베트남(2018년 서비스 시작)은 약 1,210~1,330일 수준. 한국(2021년 서비스 시작)은 2021년 1년분 데이터만 존재해 **약 302일에서 상한선**이 형성됨 — VN·KR의 Days on Chart 수치는 서비스 시작 시점 차이로 인해 HK·JP·TW와 단순 비교가 적절하지 않다                                                                | `02_chart_longevity.png` |
| **Q3** 일본 스트리밍 추이 | 2017→2021년 일본의 연간 총 스트리밍 수가 **약 3억 → 약 42억(약 13.8배)** 으로 폭발적으로 성장. "Pretender"(Official HIGE DANdism)가 2019·2020년 **2년 연속 1위**를 차지했고, 그 사이 peak_streams가 2,800만→6,260만으로 **2배 이상 증가**해 발매 이듬해에 오히려 더 큰 인기를 끈 이례적 사례를 보여줌. 2017년(Shape of You)을 제외하면 2018~2021년 1위곡은 모두 일본 자국 아티스트(打上花火, Pretender, ドライフラワー)로, 글로벌 히트에서 자국 음악 강세로 전환되는 흐름이 관찰됨 | `03_japan_streams.png` |

위 결과는 파이프라인 실행 후 생성되는 CSV 및 PNG 산출물을 기준으로 요약한 것이다.

---

## 프로젝트 구조 (Project Structure)

```
spotify-chart-analysis/
├── README.md
├── run.sh                              <- 전체 파이프라인 자동화 스크립트 (7단계)
├── data/
│   ├── README.md                       <- 데이터 출처 및 스키마 설명
│   └── sample/
│       └── JP_sample.csv              <- 일본(JP) 샘플 1,000행
├── src/
│   ├── ingest/
│   │   ├── collect.py                  <- Kaggle 다운로드 + 국가/월별 분할
│   │   └── upload_to_hdfs.sh          <- CSV를 HDFS에 업로드
│   ├── pipeline/
│   │   ├── preprocess.py              <- PySpark: 정제 -> Parquet 변환
│   │   └── create_hive_table.py       <- SparkSQL: spotify_db.charts 테이블 생성
│   └── analyze/
│       ├── analyze.py                 <- SparkSQL: 3개 연구 질문 -> CSV
│       └── visualize.py               <- Matplotlib: CSV -> PNG 차트
└── results/
    ├── 01_crossmarket_hits.png
    ├── 02_chart_longevity.png
    └── 03_japan_streams.png
```

> 참고: `results/q1_crossmarket.csv`, `q2_longevity.csv`, `q3_japan_streams.csv`는
> 실행 시 자동 생성되지만 `.gitignore`로 제외되어 저장소에는 포함되지 않는다.

---

## 데이터 출처 (Data Source)

- **데이터셋:** [Spotify Charts — Kaggle](https://www.kaggle.com/datasets/dhruvildave/spotify-charts)
- **분석 범위:** Top 200, 아시아 5개국(VN, KR, JP, TW, HK), 2017–2021
- **스키마:** `date, region, chart, trend, rank, title, artist, streams, url`
- **크기:** 원본 CSV 약 3.8GB

### 알려진 데이터 한계 (Known Data Limitations)

| 국가 | 이슈 |
|------|------|
| 한국 | Spotify 서비스가 2021년 정식 출시 → 2021년 데이터만 존재 |
| 베트남 | Spotify 서비스가 2018년 정식 출시 → 2017년 데이터가 존재하지 않음 |
| 일본 | 5개년 데이터가 모두 완전함 — Q3 스트리밍 추이 분석의 주요 시장 |

---

## AI Tool Usage
- Claude: 디버깅 보조, 대용량 데이터셋 HDP 업로드 방법 질의, 
  Matplotlib 그래프 표현 관련 아이디어 정리
- ChatGPT: 한국어 문장 교정
