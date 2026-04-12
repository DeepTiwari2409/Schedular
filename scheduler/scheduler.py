import os
import asyncio
from datetime import datetime, timezone

import asyncpg
import redis

DB_DSN = os.environ["DATABASE_URL"]
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_QUEUE = os.environ.get("REDIS_QUEUE", "taskengine:queue")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "1.0"))

SCHEDULER_ID = os.environ.get("SCHEDULER_ID", "scheduler-1")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

async def reap_expired_leases(pool: asyncpg.Pool) -> int:
    """
    Reclaim tasks whose worker lease expired while task is still RUNNING.
    Returns number of tasks requeued to PENDING.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT t.id
                FROM tasks t
                JOIN task_ownership o ON o.task_id = t.id
                WHERE t.state = 'RUNNING'
                  AND o.lease_expires_at < now()
                FOR UPDATE OF t SKIP LOCKED
                LIMIT 50
                """
            )
            if not rows:
                return 0

            task_ids = [str(r["id"]) for r in rows]

            # Delete ownership so a new worker can claim later
            await conn.execute(
                "DELETE FROM task_ownership WHERE task_id = ANY($1::uuid[])",
                task_ids
            )

            # Move tasks back to PENDING so scheduler will enqueue again
            await conn.execute(
                """
                UPDATE tasks
                SET state = 'PENDING',
                    updated_at = now(),
                    last_error = COALESCE(last_error, '') || '\n[reaper] lease expired, requeued\n'
                WHERE id = ANY($1::uuid[])
                """,
                task_ids
            )

            return len(task_ids)


async def pick_and_enqueue(pool: asyncpg.Pool, r: redis.Redis) -> int:
    """
    Returns number of tasks enqueued in this iteration.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            now = utc_now()

            # 1) Pick due tasks, lock them so other schedulers skip them.
            rows = await conn.fetch(
                """
                SELECT id
                FROM tasks
                WHERE
                  (
                    state = 'PENDING' AND run_at <= $1
                  )
                  OR
                  (
                    state = 'RETRY_WAIT' AND next_run_at IS NOT NULL AND next_run_at <= $1
                  )
                ORDER BY COALESCE(next_run_at, run_at) ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $2
                """,
                now, BATCH_SIZE
            )

            if not rows:
                return 0

            task_ids = [str(row["id"]) for row in rows]

            # 2) Mark as ENQUEUED inside the same transaction
            await conn.execute(
                """
                UPDATE tasks
                SET state = 'ENQUEUED', updated_at = now()
                WHERE id = ANY($1::uuid[])
                """,
                task_ids
            )

    # 3) Push to Redis after commit.
    # If Redis is down, tasks remain ENQUEUED; we'll add reconciliation later.
    for tid in task_ids:
        r.lpush(REDIS_QUEUE, tid)

    return len(task_ids)

async def main():
    print(f"[{SCHEDULER_ID}] starting. DB={DB_DSN} redis={REDIS_HOST}:{REDIS_PORT} queue={REDIS_QUEUE}")

    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        while True:
            try:
                reaped = await reap_expired_leases(pool)
                if reaped > 0:
                    print(f"[{SCHEDULER_ID}] reaped {reaped} expired RUNNING tasks -> PENDING")

                n = await pick_and_enqueue(pool, r)
                if n > 0:
                    print(f"[{SCHEDULER_ID}] enqueued {n} tasks")

                await asyncio.sleep(POLL_INTERVAL_SEC)

            except Exception as e:
                print(f"[{SCHEDULER_ID}] ERROR: {e}")
                await asyncio.sleep(2.0)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
