import csv
from typing import List, Dict, Any, Sequence

import psycopg2
from psycopg2.extras import execute_batch


class PostgresPipeline:
    """A simple ETL pipeline that loads CSV rows into a PostgreSQL table.

    This replaces the previous SQLite-based pipeline with a PostgreSQL
    implementation. The pipeline is encapsulated in a class to make it
    easier to maintain and extend.
    """

    def __init__(self, dsn: str, table: str) -> None:
        """Initialize the pipeline.

        Args:
            dsn: Connection string for psycopg2 (e.g. 'dbname=ai user=ai').
            table: Destination table name.
        """
        self.dsn = dsn
        self.table = table
        self.conn = None

    def connect(self) -> None:
        """Open a database connection if not already connected."""
        if self.conn is None:
            self.conn = psycopg2.connect(self.dsn)

    def close(self) -> None:
        """Close the database connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def extract(self, path: str) -> List[Dict[str, Any]]:
        """Read records from a CSV file."""
        with open(path, newline="") as f:
            return list(csv.DictReader(f))

    def transform(self, rows: List[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
        """Transform records before loading.

        In this simple example the method returns rows unchanged, but
        additional transformation logic can be added here.
        """
        return rows

    def load(self, rows: Sequence[Dict[str, Any]]) -> None:
        """Insert records into PostgreSQL."""
        if not rows:
            return
        columns = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        column_list = ", ".join(columns)
        query = f"INSERT INTO {self.table} ({column_list}) VALUES ({placeholders})"
        data = [tuple(r[c] for c in columns) for r in rows]
        with self.conn.cursor() as cur:
            execute_batch(cur, query, data)
        self.conn.commit()

    def run(self, csv_path: str) -> None:
        """Execute the full ETL process."""
        self.connect()
        rows = self.extract(csv_path)
        transformed = self.transform(rows)
        self.load(transformed)
        self.close()


if __name__ == "__main__":
    # Example usage:
    # pipeline = PostgresPipeline("dbname=ai user=ai password=secret host=localhost", "ai_user_metrics")
    # pipeline.run("metrics.csv")
    pass
