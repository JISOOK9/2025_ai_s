"""Data pipeline for churn prediction features and labels."""

from __future__ import annotations

import csv
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Dict, Any


@dataclass
class DataPipeline:
    """Pipeline to build feature set for churn prediction.

    The pipeline executes SQL queries against the provided database
    connection to compute aggregates for payments, product changes and
    behavioural events. A churn label is assigned using the SQL template
    ``churn_label_feature.sql`` as referenced in the PRD.
    """

    conn: sqlite3.Connection

    def build_training_dataset(
        self, as_of_date: str, label_days: int = 7
    ) -> Iterable[Dict[str, Any]]:
        """Build feature dataset for the given ``as_of_date``.

        Parameters
        ----------
        as_of_date:
            Reference date (``YYYY-MM-DD``) for feature aggregation.
        label_days:
            Number of days in the future in which a subscription end
            date results in a churn label of ``1``.

        Returns
        -------
        Iterable of dictionaries representing feature rows.
        """

        params = {
            "as_of_date": as_of_date,
            "n_days": f"+{label_days} day",
        }

        label_sql_path = Path(__file__).with_name("churn_label_feature.sql")
        label_sql = label_sql_path.read_text(encoding="utf-8").strip().rstrip(";")

        query = f"""
WITH
payment_14 AS (
    SELECT user_id, prod_id,
        SUM(CASE WHEN status='fail' THEN 1 ELSE 0 END) AS fail_cnt_14d,
        SUM(CASE WHEN status='succ' THEN 1 ELSE 0 END) AS succ_cnt_14d,
        SUM(coupon_amt) AS coupon_amt_14d,
        SUM(refund_amt) AS refund_amt_14d,
        AVG(amount) AS avg_amt_14d
    FROM payments
    WHERE paid_at >= DATE(:as_of_date, '-14 day')
      AND paid_at < DATE(:as_of_date, '+1 day')
    GROUP BY user_id, prod_id
),
payment_30 AS (
    SELECT user_id, prod_id,
        SUM(CASE WHEN status='fail' THEN 1 ELSE 0 END) AS fail_cnt_30d,
        SUM(CASE WHEN status='succ' THEN 1 ELSE 0 END) AS succ_cnt_30d,
        SUM(coupon_amt) AS coupon_amt_30d,
        SUM(refund_amt) AS refund_amt_30d,
        AVG(amount) AS avg_amt_30d
    FROM payments
    WHERE paid_at >= DATE(:as_of_date, '-30 day')
      AND paid_at < DATE(:as_of_date, '+1 day')
    GROUP BY user_id, prod_id
),
product_14 AS (
    SELECT user_id, prod_id,
        COUNT(*) AS switch_cnt_14d,
        SUM(CASE WHEN downgraded THEN 1 ELSE 0 END) AS downgraded_14d
    FROM product_changes
    WHERE changed_at >= DATE(:as_of_date, '-14 day')
      AND changed_at < DATE(:as_of_date, '+1 day')
    GROUP BY user_id, prod_id
),
product_30 AS (
    SELECT user_id, prod_id,
        COUNT(*) AS switch_cnt_30d,
        SUM(CASE WHEN downgraded THEN 1 ELSE 0 END) AS downgraded_30d
    FROM product_changes
    WHERE changed_at >= DATE(:as_of_date, '-30 day')
      AND changed_at < DATE(:as_of_date, '+1 day')
    GROUP BY user_id, prod_id
),
behavior_14 AS (
    SELECT user_id, prod_id,
        SUM(CASE WHEN event_type='cancel_keyword_search' THEN 1 ELSE 0 END) AS cancel_keyword_search_14d,
        SUM(CASE WHEN event_type='faq_cancel_views' THEN 1 ELSE 0 END) AS faq_cancel_views_14d,
        SUM(CASE WHEN event_type='cancel_page_visit' THEN 1 ELSE 0 END) AS cancel_page_visit_14d
    FROM behaviors
    WHERE event_time >= DATE(:as_of_date, '-14 day')
      AND event_time < DATE(:as_of_date, '+1 day')
    GROUP BY user_id, prod_id
),
label AS (
{label_sql}
)
SELECT
    s.user_id,
    s.prod_id,
    COALESCE(label.label, 0) AS label,
    COALESCE(payment_14.fail_cnt_14d, 0) AS payment_fail_cnt_14d,
    COALESCE(payment_14.succ_cnt_14d, 0) AS payment_succ_cnt_14d,
    COALESCE(payment_14.coupon_amt_14d, 0) AS coupon_amt_14d,
    COALESCE(payment_14.refund_amt_14d, 0) AS refund_amt_14d,
    COALESCE(payment_14.avg_amt_14d, 0) AS avg_amt_14d,
    COALESCE(payment_30.fail_cnt_30d, 0) AS payment_fail_cnt_30d,
    COALESCE(payment_30.succ_cnt_30d, 0) AS payment_succ_cnt_30d,
    COALESCE(payment_30.coupon_amt_30d, 0) AS coupon_amt_30d,
    COALESCE(payment_30.refund_amt_30d, 0) AS refund_amt_30d,
    COALESCE(payment_30.avg_amt_30d, 0) AS avg_amt_30d,
    COALESCE(product_14.switch_cnt_14d, 0) AS switch_cnt_14d,
    COALESCE(product_14.downgraded_14d, 0) AS downgraded_14d,
    COALESCE(product_30.switch_cnt_30d, 0) AS switch_cnt_30d,
    COALESCE(product_30.downgraded_30d, 0) AS downgraded_30d,
    COALESCE(behavior_14.cancel_keyword_search_14d, 0) AS cancel_keyword_search_14d,
    COALESCE(behavior_14.faq_cancel_views_14d, 0) AS faq_cancel_views_14d,
    COALESCE(behavior_14.cancel_page_visit_14d, 0) AS cancel_page_visit_14d
FROM subscriptions AS s
LEFT JOIN payment_14 ON payment_14.user_id = s.user_id AND payment_14.prod_id = s.prod_id
LEFT JOIN payment_30 ON payment_30.user_id = s.user_id AND payment_30.prod_id = s.prod_id
LEFT JOIN product_14 ON product_14.user_id = s.user_id AND product_14.prod_id = s.prod_id
LEFT JOIN product_30 ON product_30.user_id = s.user_id AND product_30.prod_id = s.prod_id
LEFT JOIN behavior_14 ON behavior_14.user_id = s.user_id AND behavior_14.prod_id = s.prod_id
LEFT JOIN label ON label.user_id = s.user_id AND label.prod_id = s.prod_id;
"""

        cur = self.conn.cursor()
        cur.execute(query, params)
        columns = [c[0] for c in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return rows

    def export_csv(
        self, data: Iterable[Dict[str, Any]], output_path: str | Path
    ) -> None:
        """Export dataset to CSV.

        In a production system this step would upload a parquet file to
        object storage such as S3. For the exercise we emit a local CSV
        for verification purposes.
        """

        rows = list(data)
        if not rows:
            return
        output_path = Path(output_path)
        with output_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
