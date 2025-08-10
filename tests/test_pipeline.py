import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))
from pipeline import DataPipeline


def setup_db():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE subscriptions (
            user_id TEXT,
            prod_id TEXT,
            sbsc_end_dt DATE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE payments (
            user_id TEXT,
            prod_id TEXT,
            amount NUMERIC,
            status TEXT,
            coupon_amt NUMERIC,
            refund_amt NUMERIC,
            paid_at DATE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE product_changes (
            user_id TEXT,
            prod_id TEXT,
            downgraded BOOLEAN,
            changed_at DATE
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE behaviors (
            user_id TEXT,
            prod_id TEXT,
            event_type TEXT,
            event_time DATE
        );
        """
    )
    return conn


def seed_data(conn):
    conn.executemany(
        "INSERT INTO subscriptions VALUES (?, ?, ?)",
        [
            ("u1", "p1", "2024-01-20"),
            ("u2", "p1", "2024-02-15"),
        ],
    )
    conn.executemany(
        "INSERT INTO payments VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("u1", "p1", 100, "succ", 10, 0, "2024-01-10"),
            ("u1", "p1", 100, "fail", 0, 0, "2024-01-12"),
            ("u1", "p1", 200, "succ", 0, 0, "2023-12-20"),
        ],
    )
    conn.executemany(
        "INSERT INTO product_changes VALUES (?, ?, ?, ?)",
        [
            ("u1", "p1", 0, "2024-01-05"),
            ("u1", "p1", 1, "2023-12-20"),
        ],
    )
    conn.executemany(
        "INSERT INTO behaviors VALUES (?, ?, ?, ?)",
        [
            ("u1", "p1", "cancel_keyword_search", "2024-01-14"),
            ("u1", "p1", "faq_cancel_views", "2024-01-06"),
            ("u1", "p1", "cancel_page_visit", "2024-01-10"),
            ("u1", "p1", "cancel_page_visit", "2023-12-20"),
        ],
    )
    conn.commit()


def test_build_training_dataset():
    conn = setup_db()
    seed_data(conn)
    pipeline = DataPipeline(conn)
    rows = pipeline.build_training_dataset("2024-01-15", label_days=7)

    assert len(rows) == 2
    u1 = [r for r in rows if r["user_id"] == "u1"][0]
    u2 = [r for r in rows if r["user_id"] == "u2"][0]

    # User u1 expectations
    assert u1["label"] == 1
    assert u1["payment_fail_cnt_14d"] == 1
    assert u1["payment_succ_cnt_14d"] == 1
    assert u1["payment_succ_cnt_30d"] == 2
    assert u1["switch_cnt_14d"] == 1
    assert u1["switch_cnt_30d"] == 2
    assert u1["cancel_page_visit_14d"] == 1

    # User u2 has no activity and label 0
    assert u2["label"] == 0
    numeric_fields = [k for k in u2.keys() if k not in {"user_id", "prod_id", "label"}]
    assert all(u2[k] == 0 for k in numeric_fields)
