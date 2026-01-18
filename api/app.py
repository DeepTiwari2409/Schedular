import os
import uuid
from datetime import datetime, timezone

from flask import Flask, request, jsonify
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def get_db_conn():
    # Example: postgresql://taskengine:taskengine_pass@postgres:5432/taskengine_db
    dsn = os.environ["DATABASE_URL"]
    return psycopg.connect(dsn, row_factory=dict_row)

app = Flask(__name__)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/tasks")
def create_task():
    payload = request.get_json(force=True) or {}

    image = payload.get("image")
    if not image or not isinstance(image, str):
        return jsonify({"error": "Field 'image' is required and must be a string"}), 400

    cmd = payload.get("cmd", [])
    env = payload.get("env", {})
    max_attempts = payload.get("max_attempts", 3)

    # run_at is optional; default is now (UTC)
    run_at_str = payload.get("run_at")
    if run_at_str:
        try:
            run_at = datetime.fromisoformat(run_at_str)
            if run_at.tzinfo is None:
                return jsonify({"error": "run_at must include timezone offset (e.g. +05:30)"}), 400
        except Exception:
            return jsonify({"error": "run_at must be ISO8601, e.g. 2026-01-14T10:00:00+05:30"}), 400
    else:
        run_at = utc_now()

    task_id = str(uuid.uuid4())

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks (id, state, image, cmd, env, run_at, max_attempts)
                VALUES (%s, 'PENDING', %s, %s::jsonb, %s::jsonb, %s, %s)
                """,
                (task_id, image, psycopg.types.json.Jsonb(cmd), psycopg.types.json.Jsonb(env), run_at, max_attempts),
            )

    return jsonify({"task_id": task_id}), 201

@app.get("/tasks/<task_id>")
def get_task(task_id: str):
    try:
        uuid.UUID(task_id)
    except Exception:
        return jsonify({"error": "Invalid task_id"}), 400

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, state, image, cmd, env, attempt, max_attempts, run_at, next_run_at,
                       last_error, exit_code, created_at, updated_at
                FROM tasks
                WHERE id = %s
                """,
                (task_id,),
            )
            row = cur.fetchone()

    if not row:
        return jsonify({"error": "Task not found"}), 404

    # Convert datetimes to ISO strings for JSON
    for k in ["run_at", "next_run_at", "created_at", "updated_at"]:
        if row.get(k) is not None:
            row[k] = row[k].isoformat()

    return jsonify(row), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
