# HDB Resale Prices ETL (PySpark)

Notebook-driven ETL pipeline for HDB resale transactions, built with modular Python components and PySpark.

## What this project contains

- `HDB_ETL_Pipeline.ipynb`: primary orchestrator (recommended entry point)
- `config/etl_config.py`: centralized pipeline configuration
- `extractors/data_extractor.py`: source discovery, CSV extraction, and dataset union
- `transformers/data_transformer.py`: type casting, standardization, filtering, outlier removal, deduplication, ID generation, hashing
- `validators/data_validator.py`: profiling + validation checks
- `loaders/data_loader.py`: deterministic CSV/Parquet output writing
- `utils/logger.py`, `utils/helpers.py`: logging and helper utilities
- `output/raw`: extracted raw files
- `output/processed_data`: transformed/cleaned/hashed/failed outputs

## Current folder structure

```
etl_project/
├── HDB_ETL_Pipeline.ipynb
├── README.md
├── requirements.txt
├── config/
│   └── etl_config.py
├── extractors/
│   └── data_extractor.py
├── transformers/
│   └── data_transformer.py
├── validators/
│   └── data_validator.py
├── loaders/
│   └── data_loader.py
├── utils/
│   ├── logger.py
│   └── helpers.py
└── output/
    ├── raw/
    └── processed_data/
```

## Prerequisites

- Python 3.10+
- Java 8+ or 11+ (required by Spark)
- pip

## Installation

```bash
pip install -r requirements.txt
```

## Input data location

By default, the extractor reads from:

```python
source_path = "../ResaleFlatPrices/"
```

in `config/etl_config.py`.

Update this path if your raw CSV files are in a different location.

## Run the pipeline

Primary run mode:

```bash
jupyter notebook HDB_ETL_Pipeline.ipynb
```

Then run cells from top to bottom.

## Pipeline stages

1. **Initialize** Spark, config, logger, and ETL components
2. **Extract** all CSVs from source path and combine into one DataFrame
3. **Filter** by date range (`start_month` to `end_month`)
4. **Profile** input data (shape, nulls, categorical distribution, numeric stats)
5. **Transform**
   - cast datatypes
   - standardize string values
   - compute remaining lease fields
   - remove resale price outliers (grouped IQR)
   - deduplicate rows
   - create and hash `Resale Identifier`
6. **Validate** row count, critical null checks, and master-field quality rules
7. **Load** outputs to `output/processed_data`

## Key configuration

Edit `config/etl_config.py`:

- `source_path`: where raw CSV files are read from
- `destination_path`: processed output directory
- `source_format`, `target_format`
- `start_month`, `end_month` (date filter)
- `min_rows` (validation threshold)
- `critical_columns` (required fields)

## Outputs

Typical outputs produced by this project:

- `output/raw/*_raw.csv`
- `output/processed_data/Cleaned/cleaned_df.csv`
- `output/processed_data/Transformed/transformed_df.csv`
- `output/processed_data/Hashed/hashed_df.csv`
- `output/processed_data/Failed/failed_df.csv`

## Notes

- This repository is currently notebook-first for readability (no `main.py` CLI entrypoint). For actual production usecase, move contents of HDB_ETL_Pipeline.ipynb to HDB_ETL_Pipeline.py.
- Modules are designed for reuse if you later add a script-based orchestrator.
