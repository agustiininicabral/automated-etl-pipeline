# Automated ETL Pipeline

A reproducible, end-to-end ETL portfolio project using Python's standard library and SQLite. It extracts customer JSON and order CSV files, validates and transforms records, loads them transactionally, and produces audit outputs.

## Capabilities

- Extracts multiple structured source formats (JSON and CSV).
- Normalizes customer and order fields with deterministic validation rules.
- Rejects malformed, duplicate, orphaned and invalid records with explicit reasons.
- Loads accepted records into SQLite using idempotent upserts and a transaction.
- Produces a rejects CSV and a JSON run report.
- Includes a CLI, synthetic source data and automated tests.
- Requires no API credentials, database server, Docker or third-party runtime dependency.

## Run locally

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -e ".[dev]"
python -m etl_pipeline --input data/raw --output output
pytest
```

The command creates `output/warehouse.sqlite`, `output/rejected_orders.csv` and `output/run_report.json`. Re-running the same input is safe: existing records are updated by business key rather than duplicated.

## Pipeline stages

1. **Extract** — read `customers.json` and `orders.csv`.
2. **Transform** — trim and normalize fields, parse ISO dates and decimal amounts, enforce allowed statuses, check customer references and deduplicate order IDs.
3. **Load** — create the SQLite schema and upsert accepted customers and orders in one transaction.
4. **Audit** — report input, accepted and rejected counts, loaded row counts and rejection reasons.

## Data contract

- Customer: positive integer `customer_id`, non-empty `name`, valid-looking unique `email`.
- Order: positive integer `order_id`, known `customer_id`, ISO `order_date`, positive `amount`, and status in `completed`, `pending`, `cancelled`.
- Amounts are stored as decimal strings in SQLite to avoid binary floating-point rounding.

## Project layout

- `src/etl_pipeline/pipeline.py`: extraction, transformation, loading and orchestration.
- `sql/schema.sql`: target warehouse schema.
- `data/raw/`: deterministic synthetic source files.
- `tests/`: unit and integration tests.
- `.github/workflows/ci.yml`: Python 3.11–3.13 CI.

## Limitations

This is a local batch ETL demonstration, not a distributed scheduler or production secrets-management system. The sample inputs are synthetic. API extraction is intentionally not simulated: the pipeline demonstrates real file extraction and a clean boundary where a real API adapter could be added.

## License

MIT. See [LICENSE](LICENSE).
