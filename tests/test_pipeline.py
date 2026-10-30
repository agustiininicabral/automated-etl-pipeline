import csv
import json
import sqlite3
import sys
from pathlib import Path

import pytest

from etl_pipeline.pipeline import (
    extract_sources,
    main,
    run_pipeline,
    transform_customers,
    transform_orders,
)


ROOT = Path(__file__).resolve().parents[1]


def test_sample_pipeline_loads_and_reports_rejections(tmp_path):
    report = run_pipeline(ROOT / "data" / "raw", tmp_path / "out")
    assert report["customers_input"] == 5
    assert report["customers_accepted"] == 5
    assert report["orders_input"] == 12
    assert report["orders_accepted"] == 8
    assert report["rejected_records"] == 4
    with sqlite3.connect(tmp_path / "out" / "warehouse.sqlite") as con:
        assert con.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 5
        assert con.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 8
        assert con.execute("PRAGMA foreign_key_check").fetchall() == []
    assert (tmp_path / "out" / "run_report.json").exists()
    assert (tmp_path / "out" / "rejected_records.csv").exists()


def test_customer_normalization_and_duplicate_rejection():
    rows = [
        {"customer_id": 1, "name": " Ana ", "email": " ANA@EXAMPLE.COM "},
        {"customer_id": 2, "name": "Bob", "email": "ana@example.com"},
        {"customer_id": 3, "name": "", "email": "bad@example.com"},
    ]
    accepted, rejected = transform_customers(rows)
    assert accepted == [{"customer_id": 1, "name": "Ana", "email": "ana@example.com"}]
    assert len(rejected) == 2


def test_customer_invalid_id_and_email_are_rejected():
    rows = [
        {"customer_id": 0, "name": "Zero", "email": "zero@example.com"},
        {"customer_id": 2, "name": "Bad email", "email": "not-an-email"},
    ]
    accepted, rejected = transform_customers(rows)
    assert accepted == []
    assert len(rejected) == 2


def test_order_validation_rejects_orphans_and_invalid_amounts():
    rows = [
        {"order_id": "1", "customer_id": "1", "order_date": "2026-01-01", "amount": "10.00", "status": "completed"},
        {"order_id": "2", "customer_id": "99", "order_date": "2026-01-01", "amount": "10.00", "status": "completed"},
        {"order_id": "3", "customer_id": "1", "order_date": "2026-01-01", "amount": "-1", "status": "completed"},
    ]
    accepted, rejected = transform_orders(rows, {1})
    assert len(accepted) == 1
    assert len(rejected) == 2


def test_order_duplicate_business_key_is_rejected():
    row = {"order_id": "7", "customer_id": "1", "order_date": "2026-02-01", "amount": "5", "status": "pending"}
    accepted, rejected = transform_orders([row, row], {1})
    assert len(accepted) == 1
    assert rejected[0]["reason"] == "duplicate order_id"


def test_order_invalid_date_and_status_are_rejected():
    rows = [
        {"order_id": "8", "customer_id": "1", "order_date": "not-a-date", "amount": "5", "status": "pending"},
        {"order_id": "9", "customer_id": "1", "order_date": "2026-02-01", "amount": "5", "status": "unknown"},
    ]
    accepted, rejected = transform_orders(rows, {1})
    assert accepted == []
    assert len(rejected) == 2


def test_pipeline_is_idempotent(tmp_path):
    output = tmp_path / "warehouse"
    run_pipeline(ROOT / "data" / "raw", output)
    with sqlite3.connect(output / "warehouse.sqlite") as con:
        first = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    run_pipeline(ROOT / "data" / "raw", output)
    with sqlite3.connect(output / "warehouse.sqlite") as con:
        second = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert con.execute("SELECT COUNT(DISTINCT order_id) FROM orders").fetchone()[0] == second
    assert first == second == 8


def test_extract_fails_for_invalid_customer_json(tmp_path):
    (tmp_path / "customers.json").write_text("{}", encoding="utf-8")
    (tmp_path / "orders.csv").write_text("order_id,customer_id\n", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON array"):
        extract_sources(tmp_path)


def test_extract_fails_for_empty_orders_file(tmp_path):
    (tmp_path / "customers.json").write_text("[]", encoding="utf-8")
    (tmp_path / "orders.csv").write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="orders.csv is empty"):
        extract_sources(tmp_path)


def test_report_rejection_counts_are_consistent(tmp_path):
    report = run_pipeline(ROOT / "data" / "raw", tmp_path / "out")
    assert report["customers_accepted"] + report["rejected_records"] >= report["customers_input"]
    assert sum(report["rejection_reasons"].values()) == report["rejected_records"]


def test_main_entrypoint_runs_with_explicit_paths(tmp_path, monkeypatch):
    output = tmp_path / "cli-output"
    monkeypatch.setattr(sys, "argv", ["etl-pipeline", "--input", str(ROOT / "data" / "raw"), "--output", str(output)])
    assert main() == 0
    assert (output / "warehouse.sqlite").exists()
