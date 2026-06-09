# 데이터 (Data)

## 출처 (Source)
- **데이터셋:** [Spotify Charts — Kaggle](https://www.kaggle.com/datasets/dhruvildave/spotify-charts)
- **작성자:** Dhruvil Dave
- **수집 기간:** 2017–2021
- **크기:** ~3.8 GB (원본 기준)

## 데이터 다운로드 방법
```bash
python src/ingest/collect.py
# 또는 전체 파이프라인 실행:
bash run.sh [KAGGLE_USERNAME] [KAGGLE_API_KEY]
```

## 스키마 (Schema)

| 컬럼 (Column) | 타입 (Type) | 설명 (Description)                                            |
|---------------|-------------|---------------------------------------------------------------|
| date          | DATE        | 차트 날짜                                                     |
| region        | STRING      | 국가 코드 (VN, KR, JP, TW, HK)                               |
| chart         | STRING      | 차트 종류 — top200 전용                                       |
| trend         | STRING      | 순위 변동 방향 (NEW_ENTRY, MOVE_UP, MOVE_DOWN, SAME_POSITION) |
| rank          | INT         | 차트 순위 (1–200)                                             |
| title         | STRING      | 곡 제목                                                       |
| artist        | STRING      | 아티스트 이름                                                 |
| streams       | BIGINT      | 일일 스트리밍 횟수                                            |
| url           | STRING      | Spotify 트랙 URL                                              |
| year          | INT         | 연도 (preprocess.py에서 추가된 파생 컬럼)                     |

## 데이터 규모 (Scale)
- Kaggle 원본: ~3.8 GB (전체 데이터셋)
- 필터링된 원본 파일: ~5개국 × 5년 × ~12개월 × 200곡/일
- 전처리 후 (Parquet, SNAPPY 압축): ~180 MB

## 샘플 데이터 (Sample Data)

`data/sample/JP_sample.csv`는 2017년 1월 기준 일본(JP) 데이터 1,000행으로 구성된 샘플 파일이다.
전체 데이터셋을 다운로드하지 않고도 스키마 및 데이터 구조를 확인할 수 있도록 GitHub에 직접 커밋하였다.

전체 데이터셋은 5개국(VN, KR, JP, TW, HK)의 2017–2021년 데이터를 포함한다.
전체 데이터를 다운로드하려면 아래 명령어를 실행한다.
```bash
python src/ingest/collect.py
```

## 참고 사항 (Notes)
- 원본 및 중간 데이터(`data/raw/`, `data/raw_download/`)는 `.gitignore`를 통해 GitHub 커밋에서 제외하였다.
- 전체 데이터셋(3.8 GB)을 받으려면 `python src/ingest/collect.py`를 실행하거나 전체 파이프라인(`bash run.sh`)을 사용한다.