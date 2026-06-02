import sqlite3
import json
import time
from typing import List, Dict, Any, Optional

class SQLiteExecutionStore:
    """Persists workflow run metadata, execution times, and outputs to an embedded SQLite database."""

    def __init__(self, db_path: str = ":memory:"):
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self.db_path == ":memory:":
            return self._conn
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    start_time REAL,
                    end_time REAL,
                    status TEXT,
                    task_count INTEGER
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS task_executions (
                    run_id TEXT,
                    task_name TEXT,
                    status TEXT,
                    attempts INTEGER,
                    result_json TEXT,
                    error_msg TEXT,
                    PRIMARY KEY (run_id, task_name)
                )
            """)
            conn.commit()

    def record_run(self, run_id: str, status: str, start_time: float, end_time: float, tasks: List[Any]) -> None:
        with self._get_connection() as conn:
            conn.execute(
                "INSERT INTO workflow_runs VALUES (?, ?, ?, ?, ?)",
                (run_id, start_time, end_time, status, len(tasks))
            )
            for t in tasks:
                try:
                    res_str = json.dumps(t.result)
                except Exception:
                    res_str = str(t.result)
                err_str = str(t.error) if t.error else None
                conn.execute(
                    "INSERT INTO task_executions VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, t.name, t.status.value, t.attempts, res_str, err_str)
                )
            conn.commit()

    def get_run_summary(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT run_id, start_time, end_time, status, task_count FROM workflow_runs WHERE run_id = ?", (run_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "run_id": row[0],
                "duration_seconds": round(row[2] - row[1], 4),
                "status": row[3],
                "task_count": row[4]
            }
