import asyncio
import subprocess
import os
import json
import logging
from datetime import datetime
from .db import get_db_connection, return_db_connection

logger = logging.getLogger("JobRunner")

class JobRunner:
    @staticmethod
    async def run_job(job_id, script_path, config_override=None, proxy_manager=None):
        """
        Executes a script as a subprocess.
        Updates orchestrator_history and writes logs to orchestrator_logs.
        """
        # 1. Prepare Config & Env
        env = os.environ.copy()
        
        # Inject Proxies
        if proxy_manager:
            proxies = proxy_manager.get_proxies()
            if proxies:
                env["ORCHESTRATOR_PROXIES"] = ",".join(proxies  )
                env["ORCHESTRATOR_VALIDATED"] = "1"
        
        # Inject API base URL for dynamic refresh
        if "ORCHESTRATOR_API_URL" not in env:
            env["ORCHESTRATOR_API_URL"] = os.getenv("ORCHESTRATOR_API_URL", "http://localhost:8005")
        
        # Prepare Arguments
        args = ["python3", "-u", script_path] # -u for unbuffered stdout
        if config_override and config_override != "{}":
            args.extend(["--config", config_override])

        # 2. DB: Create History Record
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO orchestrator_history (job_id, start_time, status)
            VALUES (?, CURRENT_TIMESTAMP, 'RUNNING')
        """, (job_id,))
        run_id = cursor.lastrowid
        conn.commit()
        return_db_connection(conn)
        
        logger.info(f"Started Job {job_id} (Run {run_id}): {' '.join(args)}")

        # 3. Execute Subprocess
        try:
            # Create log queue and writer task
            log_queue = asyncio.Queue()
            log_writer_task = asyncio.create_task(JobRunner._batch_log_writer(run_id, log_queue))
            
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=os.path.dirname(script_path)
            )
            
            # 4. Stream Logs to Queue
            async def read_stream(stream, stream_name):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    text = line.decode().strip()
                    if text:
                        print(f"[Job {job_id}] {text}")
                        await log_queue.put(text)

            await asyncio.gather(
                read_stream(process.stdout, "stdout"),
                read_stream(process.stderr, "stderr")
            )
            
            # Signal log writer to finish
            await log_queue.put(None)
            await log_writer_task
            
            exit_code = await process.wait()
            status = 'SUCCESS' if exit_code == 0 else 'FAILURE'
            
        except Exception as e:
            logger.error(f"Job failed: {e}")
            exit_code = -1
            status = 'FAILURE'
            # Log the crash
            await log_queue.put(f"CRASH: {str(e)}")
            await log_queue.put(None)
            await log_writer_task

        # 5. DB: Update History Record
        end_conn = get_db_connection()
        end_conn.cursor().execute("""
            UPDATE orchestrator_history 
            SET end_time=CURRENT_TIMESTAMP, status=?, exit_code=?
            WHERE run_id=?
        """, (status, exit_code, run_id))
        end_conn.commit()
        
        # 6. Cleanup: Keep only last 5 runs
        try:
            end_conn.execute("""
                DELETE FROM orchestrator_history 
                WHERE job_id = ? 
                AND run_id NOT IN (
                    SELECT run_id 
                    FROM orchestrator_history 
                    WHERE job_id = ? 
                    ORDER BY start_time DESC 
                    LIMIT 5
                )
            """, (job_id, job_id))
            end_conn.commit()
        except Exception as e:
            logger.error(f"Failed to cleanup history for job {job_id}: {e}")

        return_db_connection(end_conn)
        
        return run_id

    @staticmethod
    async def _batch_log_writer(run_id, queue):
        """
        Batched log writer to reduce DB connections.
        Collects logs and writes in batches.
        """
        conn = get_db_connection()
        batch = []
        batch_size = 100
        timeout = 1.0  # seconds
        
        while True:
            try:
                # Try to collect up to batch_size logs
                while len(batch) < batch_size:
                    log_line = await asyncio.wait_for(queue.get(), timeout=timeout)
                    if log_line is None:
                        # Sentinel value - finish up
                        if batch:
                            cursor = conn.cursor()
                            cursor.executemany(
                                "INSERT INTO orchestrator_logs (run_id, log_text, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                                [(run_id, line) for line in batch]
                            )
                            conn.commit()
                        return_db_connection(conn)
                        return
                    batch.append(log_line)
            except asyncio.TimeoutError:
                # Timeout - write whatever we have
                pass
            
            # Write batch if we have anything
            if batch:
                cursor = conn.cursor()
                cursor.executemany(
                    "INSERT INTO orchestrator_logs (run_id, log_text, created_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    [(run_id, line) for line in batch]
                )
                conn.commit()
                batch = []
