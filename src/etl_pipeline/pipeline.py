from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


ALLOWED_STATUSES = {"completed", "pending", "cancelled"}
ROOT = Path(__file__).resolve().parents[2]


def extract_sources(input_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Read customer JSON and order CSV without mutating the source files."""
    customers_path = input_dir / "customers.json"
    orders_path = input_dir / "orders.csv"
    with customers_path.open(encoding="utf-8") as stream:
        customers = json.load(stream)
    with orders_path.open(encoding="utf-8", newline="") as stream:
        orders = list(csv.DictReader(stream))
    if not isinstance(customers, list):
        raise ValueError("customers.json must contain a JSON array")
    if not orders and not orders_path.read_text(encoding="utf-8").strip():
        raise ValueError("orders.csv is empty")
    return customers, orders


def _positive_int(value: Any, field: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{field} must be positive")
    return parsed


def transform_customers(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    seen_ids: set[int] = set()
    seen_emails: set[str] = set()
    for index, row in enumerate(rows, start=1):
        try:
            customer_id = _positive_int(row.get("customer_id"), "customer_id")
            name = str(row.get("name", "")).strip()
            email = str(row.get("email", "")).strip().lower()
            if not name:
                raise ValueError("name is required")
            if "@" not in email or "." not in email.rsplit("@", 1)[-1]:
                raise ValueError("email is invalid")
            if customer_id in seen_ids:
                raise ValueError("duplicate customer_id")
            if email in seen_emails:
                raise ValueError("duplicate email")
            seen_ids.add(customer_id)
            seen_emails.add(email)
            accepted.append({"customer_id": customer_id, "name": name, "email": email})
        except (TypeError, ValueError) as exc:
            rejected.append({"source_row": str(index), "record_id": str(row.get("customer_id", "")), "reason": str(exc)})
    return accepted, rejected


def transform_orders(
    rows: list[dict[str, Any]], valid_customer_ids: set[int]
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    seen_ids: set[int] = set()
    for index, row in enumerate(rows, start=2):
        try:
            order_id = _positive_int(row.get("order_id"), "order_id")
            customer_id = _positive_int(row.get("customer_id"), "customer_id")
            if order_id in seen_ids:
                raise ValueError("duplicate order_id")
            if customer_id not in valid_customer_ids:
                raise ValueError("customer_id does not exist in customer source")
            order_date = date.fromisoformat(str(row.get("order_date", "")).strip()).isoformat()
            amount = Decimal(str(row.get("amount", "")).strip()).quantize(Decimal("0.01"))
            if not amount.is_finite() or amount <= 0:
                raise ValueError("amount must be a positive finite number")
            status = str(row.get("status", "")).strip().lower()
            if status not in ALLOWED_STATUSES:
                raise ValueError("status is not allowed")
            seen_ids.add(order_id)
            accepted.append({
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date,
                "amount": format(amount, ".2f"),
                "status": status,
            })
        except (TypeError, ValueError, InvalidOperation) as exc:
            rejected.append({"source_row": str(index), "record_id": str(row.get("order_id", "")), "reason": str(exc)})
    return accepted, rejected


def load_warehouse(
    database_path: Path, customers: list[dict[str, Any]], orders: list[dict[str, Any]]
) -> dict[str, int]:
    """Atomically create schema and upsert transformed rows into SQLite."""
    schema = (ROOT / "sql" / "schema.sql").read_text(encoding="utf-8")
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(schema)
        now = datetime.now(timezone.utc).isoformat()
        with connection:
            connection.executemany(
                """INSERT INTO customers(customer_id, name, email, updated_at)
                   VALUES (:customer_id, :name, :email, :updated_at)
                   ON CONFLICT(customer_id) DO UPDATE SET
                     name=excluded.name, email=excluded.email, updated_at=excluded.updated_at""",
                [{**row, "updated_at": now} for row in customers],
            )
            connection.executemany(
                """INSERT INTO orders(order_id, customer_id, order_date, amount, status, updated_at)
                   VALUES (:order_id, :customer_id, :order_date, :amount, :status, :updated_at)
                   ON CONFLICT(order_id) DO UPDATE SET
                     customer_id=excluded.customer_id, order_date=excluded.order_date,
                     amount=excluded.amount, status=excluded.status, updated_at=excluded.updated_at""",
                [{**row, "updated_at": now} for row in orders],
            )
        counts = {
            "customers_loaded": connection.execute("SELECT COUNT(*) FROM customers").fetchone()[0],
            "orders_loaded": connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
        }
        return counts
    finally:
        connection.close()


def run_pipeline(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    customers_raw, orders_raw = extract_sources(input_dir)
    customers, rejected_customers = transform_customers(customers_raw)
    orders, rejected_orders = transform_orders(
        orders_raw, {row["customer_id"] for row in customers}
    )
    counts = load_warehouse(output_dir / "warehouse.sqlite", customers, orders)

    rejected = [
        {"entity": "customer", **row} for row in rejected_customers
    ] + [{"entity": "order", **row} for row in rejected_orders]
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "rejected_records.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["entity", "source_row", "record_id", "reason"])
        writer.writeheader()
        writer.writerows(rejected)

    report = {
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "customers_input": len(customers_raw),
        "customers_accepted": len(customers),
        "orders_input": len(orders_raw),
        "orders_accepted": len(orders),
        "rejected_records": len(rejected),
        "rejection_reasons": dict(Counter(row["reason"] for row in rejected)),
        **counts,
    }
    (output_dir / "run_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the local customer/order ETL pipeline.")
    parser.add_argument("--input", type=Path, default=ROOT / "data" / "raw")
    parser.add_argument("--output", type=Path, default=ROOT / "output")
    args = parser.parse_args()
    report = run_pipeline(args.input, args.output)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
