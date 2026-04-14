import os
import asyncio
from datetime import datetime, timezone, timedelta
import uuid as uuidlib
import docker
from docker.errors import DockerException
import asyncpg
import redis
import json
import random
import math

DB_DSN = os.environ["DATABASE_URL"]

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_QUEUE = os.environ.get("REDIS_QUEUE", "taskengine:queue")

WORKER_ID = os.environ.get("WORKER_ID", f"worker-{uuidlib.uuid4()}")
LEASE_TTL_SEC = int(os.environ.get("LEASE_TTL_SEC", "15"))
HEARTBEAT_EVERY_SEC = float(os.environ.get("HEARTBEAT_EVERY_SEC", str(max(1, LEASE_TTL_SEC // 3))))
FAKE_WORK_SEC = int(os.environ.get("FAKE_WORK_SEC", "6"))

def compute_backoff_seconds(attempt: int) -> int:
    """
    attempt: 1,2,3...
    returns delay seconds with exponential backoff + jitter, capped.
    """
    base = 2.0
    cap = 60.0
    delay = min(cap, base * (2 ** (attempt - 1)))
    jitter = random.uniform(0, delay * 0.2)
    return int(math.ceil(delay + jitter))

def normalize_env(env_val) -> dict:
    if env_val is None:
        return {}
    if isinstance(env_val, dict):
        return env_val
    if isinstance(env_val, str):
        s = env_val.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            raise RuntimeError(f"Invalid env JSON string: {env_val!r}")
        if not isinstance(parsed, dict):
            raise RuntimeError(f"env must be a JSON object/dict, got: {type(parsed)}")
        return parsed
    raise RuntimeError(f"env must be dict or JSON string, got: {type(env_val)}")

def normalize_cmd(cmd_val) -> list:
    if cmd_val is None:
        return []
    if isinstance(cmd_val, list):
        return cmd_val
    if isinstance(cmd_val, str):
        s = cmd_val.strip()
        if not s:
            return []
        # allow JSON string list
        try:
            parsed = json.loads(s)
        except json.JSONDecodeError:
            # treat as a shell string if you want; otherwise error
            return [cmd_val]
        if isinstance(parsed, list):
            return parsed
        return [cmd_val]
    return list(cmd_val)

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

async def fetch_task_spec(pool: asyncpg.Pool, task_id: str) -> tuple[str, list, dict]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT image, cmd, env FROM tasks WHERE id = $1::uuid",
            task_id
        )
        if row is None:
            raise RuntimeError("Task not found in DB")
        image = row["image"]
        cmd = normalize_cmd(row["cmd"])
        env = normalize_env(row["env"])
        return image, cmd, env

async def run_container(image: str, cmd: list, env: dict, stop_evt: asyncio.Event) -> tuple[int, str]:
    """
    Runs a Docker container and returns (exit_code, logs_text).
    Stops container if stop_evt is set (ownership lost).
    """
    def _blocking_run() -> tuple[int, str]:
        client = docker.from_env()
        container = None
        try:
            # Pull image (safe even if already present)
            client.images.pull(image)

            container = client.containers.run(
                image=image,
                command=cmd if cmd else None,
                environment=env if env else None,
                detach=True,
                remove=False,  # remove after we capture logs
            )

            # Poll until completion or stop request
            while True:
                if stop_evt.is_set():
                    try:
                        container.kill()
                    except Exception:
                        pass
                    raise RuntimeError("Stopped due to ownership loss")

                container.reload()
                status = container.status  # created/running/exited
                if status == "exited":
                    break

                # sleep a bit in blocking thread
                import time
                time.sleep(0.5)

            # Capture exit code + logs
            inspect = client.api.inspect_container(container.id)
            exit_code = int(inspect["State"]["ExitCode"])
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")

            return exit_code, logs

        finally:
            if container is not None:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    try:
        return await asyncio.to_thread(_blocking_run)
    except DockerException as e:
        raise RuntimeError(f"Docker error: {e}") from e


async def claim_task(conn: asyncpg.Connection, task_id: str, owner_id: str) -> int | None:
    """
    Attempt to claim task_id.
    Returns fencing_token on success, or None if claim fails.
    """
    now = utc_now()
    lease_expires_at = now + timedelta(seconds=LEASE_TTL_SEC)

    # Lock task row first (prevents concurrent state transitions)
    row = await conn.fetchrow(
        "SELECT state FROM tasks WHERE id = $1::uuid FOR UPDATE",
        task_id
    )
    if row is None:
        return None
    if row["state"] != "ENQUEUED":
        return None

    # Lock ownership row if exists
    own = await conn.fetchrow(
        "SELECT owner_id, lease_expires_at, fencing_token FROM task_ownership WHERE task_id = $1::uuid FOR UPDATE",
        task_id
    )

    if own is None:
        token = 1
        await conn.execute(
            """
            INSERT INTO task_ownership(task_id, owner_id, lease_expires_at, last_heartbeat_at, fencing_token)
            VALUES ($1::uuid, $2, $3, now(), $4)
            """,
            task_id, owner_id, lease_expires_at, token
        )
    else:
        # If someone else owns it and lease still valid, we cannot steal it
        if own["lease_expires_at"] > now and own["owner_id"] != owner_id:
            return None

        # Lease expired OR same owner re-claiming: take ownership and bump token
        token = int(own["fencing_token"]) + 1
        await conn.execute(
            """
            UPDATE task_ownership
            SET owner_id = $2,
                lease_expires_at = $3,
                last_heartbeat_at = now(),
                fencing_token = $4
            WHERE task_id = $1::uuid
            """,
            task_id, owner_id, lease_expires_at, token
        )

    # Transition task -> RUNNING (guarded)
    updated = await conn.execute(
        """
        UPDATE tasks
        SET state = 'RUNNING', updated_at = now()
        WHERE id = $1::uuid AND state = 'ENQUEUED'
        """,
        task_id
    )
    # asyncpg returns "UPDATE <n>"
    if not updated.endswith(" 1"):
        return None

    return token

async def ownership_is_valid(conn: asyncpg.Connection, task_id: str, owner_id: str, token: int) -> bool:
    row = await conn.fetchrow(
        """
        SELECT owner_id, fencing_token, lease_expires_at
        FROM task_ownership
        WHERE task_id = $1::uuid
        """,
        task_id
    )
    if row is None:
        return False
    if row["owner_id"] != owner_id:
        return False
    if int(row["fencing_token"]) != int(token):
        return False
    if row["lease_expires_at"] <= utc_now():
        return False
    return True

async def heartbeat_loop(pool: asyncpg.Pool, task_id: str, owner_id: str, token: int, stop_evt: asyncio.Event):
    """
    Periodically renew lease. If renewal fails, we lost ownership -> stop.
    """
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(HEARTBEAT_EVERY_SEC)
            async with pool.acquire() as conn:
                # res = await conn.execute(
                #     """
                #     UPDATE task_ownership
                #     SET lease_expires_at = (now() + ($4 || ' seconds')::interval),
                #         last_heartbeat_at = now()
                #     WHERE task_id = $1::uuid
                #       AND owner_id = $2
                #       AND fencing_token = $3
                #     """,
                #     task_id, owner_id, token, LEASE_TTL_SEC
                # )
                res = await conn.execute(
                    """
                    UPDATE task_ownership
                    SET lease_expires_at = now() + ($4 * interval '1 second'),
                        last_heartbeat_at = now()
                    WHERE task_id = $1::uuid
                    AND owner_id = $2
                    AND fencing_token = $3
                    """,
                    task_id, owner_id, token, LEASE_TTL_SEC
                )

                if not res.endswith(" 1"):
                    print(f"[{WORKER_ID}] LOST OWNERSHIP (heartbeat failed) task={task_id} token={token}")
                    stop_evt.set()
                    return
    except Exception as e:
        print(f"[{WORKER_ID}] Heartbeat error task={task_id}: {e}")
        stop_evt.set()

async def finalize_success(pool: asyncpg.Pool, task_id: str, owner_id: str, token: int):
    async with pool.acquire() as conn:
        async with conn.transaction():
            # fenced completion: only current owner+token may finalize
            res = await conn.execute(
                """
                UPDATE tasks
                SET state = 'SUCCEEDED',
                    exit_code = 0,
                    logs = COALESCE(logs, '') || $4,
                    updated_at = now()
                WHERE id = $1::uuid
                  AND state = 'RUNNING'
                  AND EXISTS (
                    SELECT 1 FROM task_ownership
                    WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3
                  )
                """,
                task_id, owner_id, token, f"\n[worker={owner_id} token={token}] done\n"
            )
            if not res.endswith(" 1"):
                print(f"[{WORKER_ID}] Finalize skipped (not owner anymore) task={task_id} token={token}")
                return

            # release ownership record (optional but clean)
            await conn.execute(
                "DELETE FROM task_ownership WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3",
                task_id, owner_id, token
            )

async def finalize_success_with_logs(pool: asyncpg.Pool, task_id: str, owner_id: str, token: int, logs: str, exit_code: int):
    async with pool.acquire() as conn:
        async with conn.transaction():
            res = await conn.execute(
                """
                UPDATE tasks
                SET state = 'SUCCEEDED',
                    exit_code = $5,
                    logs = $4,
                    updated_at = now()
                WHERE id = $1::uuid
                  AND state = 'RUNNING'
                  AND EXISTS (
                    SELECT 1 FROM task_ownership
                    WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3
                  )
                """,
                task_id, owner_id, token, logs, exit_code
            )
            if not res.endswith(" 1"):
                print(f"[{WORKER_ID}] Finalize skipped (not owner anymore) task={task_id} token={token}")
                return

            await conn.execute(
                "DELETE FROM task_ownership WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3",
                task_id, owner_id, token
            )


# async def finalize_failed_with_logs(pool: asyncpg.Pool, task_id: str, owner_id: str, token: int, logs: str, exit_code: int):
#     async with pool.acquire() as conn:
#         async with conn.transaction():
#             res = await conn.execute(
#                 """
#                 UPDATE tasks
#                 SET state = 'FAILED',
#                     exit_code = $5,
#                     logs = $4,
#                     last_error = 'container exited non-zero',
#                     updated_at = now()
#                 WHERE id = $1::uuid
#                   AND state = 'RUNNING'
#                   AND EXISTS (
#                     SELECT 1 FROM task_ownership
#                     WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3
#                   )
#                 """,
#                 task_id, owner_id, token, logs, exit_code
#             )
#             if not res.endswith(" 1"):
#                 print(f"[{WORKER_ID}] Finalize skipped (not owner anymore) task={task_id} token={token}")
#                 return

#             await conn.execute(
#                 "DELETE FROM task_ownership WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3",
#                 task_id, owner_id, token
#             )

async def finalize_retry_or_fail(pool: asyncpg.Pool, task_id: str, owner_id: str, token: int, logs: str, exit_code: int):
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Get current attempt and max_attempts
            row = await conn.fetchrow(
                "SELECT attempt, max_attempts FROM tasks WHERE id = $1::uuid FOR UPDATE",
                task_id
            )
            if row is None:
                return

            attempt = int(row["attempt"])
            max_attempts = int(row["max_attempts"])

            next_attempt = attempt + 1

            # If we still have retries left, schedule retry
            if next_attempt < max_attempts:
                backoff = compute_backoff_seconds(next_attempt)
                res = await conn.execute(
                    """
                    UPDATE tasks
                    SET state = 'RETRY_WAIT',
                        attempt = $5,
                        next_run_at = now() + ($6 * interval '1 second'),
                        exit_code = $4,
                        logs = $3,
                        last_error = $7,
                        updated_at = now()
                    WHERE id = $1::uuid
                      AND state = 'RUNNING'
                      AND EXISTS (
                        SELECT 1 FROM task_ownership
                        WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $8
                      )
                    """,
                    task_id,
                    owner_id,
                    logs,
                    exit_code,
                    next_attempt,
                    backoff,
                    f"retry scheduled in {backoff}s after exit_code={exit_code}",
                    token
                )
                if res.endswith(" 1"):
                    # release ownership so another worker can run it later
                    await conn.execute(
                        "DELETE FROM task_ownership WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3",
                        task_id, owner_id, token
                    )
                    print(f"[{WORKER_ID}] Scheduled RETRY task={task_id} attempt={next_attempt}/{max_attempts} in {backoff}s")
                else:
                    print(f"[{WORKER_ID}] Retry finalize skipped (not owner) task={task_id} token={token}")
                return

            # No retries left => FAILED
            res = await conn.execute(
                """
                UPDATE tasks
                SET state = 'FAILED',
                    attempt = $5,
                    exit_code = $4,
                    logs = $3,
                    last_error = $6,
                    updated_at = now()
                WHERE id = $1::uuid
                  AND state = 'RUNNING'
                  AND EXISTS (
                    SELECT 1 FROM task_ownership
                    WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $7
                  )
                """,
                task_id,
                owner_id,
                logs,
                exit_code,
                next_attempt,
                f"final failure after {next_attempt}/{max_attempts} attempts, exit_code={exit_code}",
                token
            )
            if res.endswith(" 1"):
                await conn.execute(
                    "DELETE FROM task_ownership WHERE task_id = $1::uuid AND owner_id = $2 AND fencing_token = $3",
                    task_id, owner_id, token
                )
                print(f"[{WORKER_ID}] Marked FAILED task={task_id} attempts={next_attempt}/{max_attempts}")
            else:
                print(f"[{WORKER_ID}] Failed finalize skipped (not owner) task={task_id} token={token}")


async def process_one(pool: asyncpg.Pool, r: redis.Redis, task_id: str):
    try:
        uuidlib.UUID(task_id)
    except Exception:
        print(f"[{WORKER_ID}] Invalid task_id in queue: {task_id}")
        return

    # Claim in a transaction
    async with pool.acquire() as conn:
        async with conn.transaction():
            token = await claim_task(conn, task_id, WORKER_ID)

    if token is None:
        # Someone else owns it or state changed. Ignore safely.
        print(f"[{WORKER_ID}] Claim failed task={task_id}")
        return

    print(f"[{WORKER_ID}] Claimed task={task_id} token={token}")

    # Pre-flight check before executing
    async with pool.acquire() as conn:
        ok = await ownership_is_valid(conn, task_id, WORKER_ID, token)
    if not ok:
        print(f"[{WORKER_ID}] Pre-flight ownership check failed task={task_id} token={token}")
        return

    stop_evt = asyncio.Event()
    hb_task = asyncio.create_task(heartbeat_loop(pool, task_id, WORKER_ID, token, stop_evt))

    # Fake work loop (checks stop flag so we can stop if ownership is lost)
    try:
        # Fetch task spec from DB
        image, cmd, env = await fetch_task_spec(pool, task_id)

        print(f"[{WORKER_ID}] Starting container image={image} task={task_id} token={token}")
        exit_code, logs_text = await run_container(image, cmd, env, stop_evt)

        # If ownership lost mid-run, stop_evt would be set and run_container raises.
        if stop_evt.is_set():
            print(f"[{WORKER_ID}] Container stopped due to ownership loss task={task_id} token={token}")
            return

        # Ownership check right before finalize
        async with pool.acquire() as conn:
            ok = await ownership_is_valid(conn, task_id, WORKER_ID, token)
        if not ok:
            print(f"[{WORKER_ID}] Ownership lost before finalize task={task_id} token={token}")
            return

        # Finalize based on exit code (for now: SUCCEEDED if 0 else FAILED)
        if exit_code == 0:
            await finalize_success_with_logs(pool, task_id, WORKER_ID, token, logs_text, exit_code)
            print(f"[{WORKER_ID}] Completed task={task_id} token={token} exit_code={exit_code}")
        else:
            await finalize_retry_or_fail(pool, task_id, WORKER_ID, token, logs_text, exit_code)
            print(f"[{WORKER_ID}] Non-zero exit handled task={task_id} token={token} exit_code={exit_code}")


        print(f"[{WORKER_ID}] Completed task={task_id} token={token}")
    finally:
        stop_evt.set()
        try:
            await hb_task
        except Exception:
            pass

async def main():
    print(f"[{WORKER_ID}] starting. DB={DB_DSN} redis={REDIS_HOST}:{REDIS_PORT} queue={REDIS_QUEUE}")

    pool = await asyncpg.create_pool(DB_DSN, min_size=1, max_size=5)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        while True:
            # Blocking pop from Redis (done in thread to not block asyncio loop)
            task_id = await asyncio.to_thread(lambda: r.brpop(REDIS_QUEUE, timeout=5))
            if not task_id:
                continue
            # brpop returns (queue, value)
            _, tid = task_id
            await process_one(pool, r, tid)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
