from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import asyncio
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
from pydantic import BaseModel

from .db import get_db_connection, return_db_connection
from .proxy_manager import ProxyManager
from .job_runner import JobRunner
import sys
# Add parent dir to path to import setup script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from setup_orchestrator_db import create_tables

# --- Models ---
class RunJobRequest(BaseModel):
    config: Optional[Dict[str, Any]] = {}

class TaskCreate(BaseModel):
    job_id: int
    name: str
    interval_minutes: int
    config: Optional[Dict[str, Any]] = {}
    enabled: bool = True

class TaskUpdate(BaseModel):
    name: Optional[str] = None
    interval_minutes: Optional[int] = None
    config: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None

# --- Lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("Starting Orchestrator Services...")
    # Initialize DB and Scripts
    try:
        # Scan dir is parent of this file
        scan_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        create_tables(scan_dir)
    except Exception as e:
        print(f"DB Init Failed: {e}")
        
    ProxyManager().start_refresher()
    
    # Start Scheduler
    scheduler_task = asyncio.create_task(scheduler_loop())
    
    yield
    
    # Shutdown
    print("Shutting down services...")
    scheduler_task.cancel()

app = FastAPI(title="P2C Orchestrator", version="1.0.0", lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global task tracking for job cancellation
active_tasks = {}  # {run_id: asyncio.Task}

# --- Scheduler ---
async def scheduler_loop():
    """Background task to check for scheduled jobs to run."""
    print("Scheduler: Started.")
    while True:
        try:
            await asyncio.sleep(10) # Check every 10 seconds
            
            conn = get_db_connection()
            if not conn: continue
            
            cursor = conn.cursor()
            now = datetime.now()
            
            # Find tasks where enabled=1 AND (next_run <= now OR next_run IS NULL)
            # Use 'limit 10' to verify logic without flooding
            cursor.execute("""
                SELECT t.task_id, t.job_id, t.config_json, t.interval_minutes, j.script_path
                FROM orchestrator_tasks t
                JOIN orchestrator_jobs j ON t.job_id = j.job_id
                WHERE t.enabled = 1 
                AND (t.next_run IS NULL OR t.next_run <= ?)
            """, (now,))
            
            tasks_to_run = cursor.fetchall()
            
            for row in tasks_to_run:
                task_id, job_id, config_json, interval, script_path = row
                
                # Calculate next run time
                next_run = now + timedelta(minutes=interval)
                
                # Update DB immediately to prevent double-execution
                cursor.execute("""
                    UPDATE orchestrator_tasks 
                    SET last_run = ?, next_run = ? 
                    WHERE task_id = ?
                """, (now, next_run, task_id))
                conn.commit()
                
                print(f"Scheduler: Triggering Task {task_id} (Job {job_id})")
                
                # Execute Job
                full_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", script_path))
                # Fire and forget (JobRunner handles logging)
                asyncio.create_task(JobRunner.run_job(job_id, full_path, config_json, ProxyManager()))
                
            return_db_connection(conn)
            
        except asyncio.CancelledError:
            print("Scheduler: Stopped.")
            break
        except Exception as e:
            print(f"Scheduler Error: {e}")
            await asyncio.sleep(5) # Backoff on error

# --- Routes ---



@app.get("/health")
def health_check():
    conn = get_db_connection()
    valid = conn is not None
    if conn: return_db_connection(conn)
    return {"db": "connected" if valid else "error", "proxies": len(ProxyManager().get_proxies())}

@app.get("/api/jobs")
def get_jobs():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT job_id, name, script_path, default_config, enabled FROM orchestrator_jobs")
    jobs = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        jobs.append(dict(zip(columns, row)))
    return_db_connection(conn)
    return jobs

@app.post("/api/jobs/scan")
def scan_jobs():
    scan_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    create_tables(scan_dir)
    return {"status": "ok"}

@app.post("/api/jobs/{job_id}/run")
async def run_job(job_id: int, request: RunJobRequest, background_tasks: BackgroundTasks):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT script_path FROM orchestrator_jobs WHERE job_id=?", (job_id,))
    row = cursor.fetchone()
    return_db_connection(conn)
    
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    
    script_path = row[0]
    full_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", script_path))
    
    import json
    config_str = json.dumps(request.config) if request.config else "{}"
    
    # Create task and store for tracking/cancellation
    task = asyncio.create_task(JobRunner.run_job(job_id, full_path, config_str, ProxyManager()))
    
    # Task will update active_tasks dict with run_id when available
    # For now, return immediately
    return {"status": "Job started", "job_id": job_id}

@app.get("/api/history")
def get_history(limit: int = 50):
    conn = get_db_connection()
    cursor = conn.cursor()
    # FIX: Use parameterized query to prevent SQL injection
    cursor.execute("""
        SELECT h.run_id, j.name, h.start_time, h.end_time, h.status, h.exit_code 
        FROM orchestrator_history h
        JOIN orchestrator_jobs j ON h.job_id = j.job_id
        ORDER BY h.start_time DESC
        LIMIT ?
    """, (limit,))
    history = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        history.append(dict(zip(columns, row)))
    return_db_connection(conn)
    return history

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# ... (Existing Routes) ...

@app.get("/api/logs/{run_id}")
def get_logs(run_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT log_text, created_at FROM orchestrator_logs WHERE run_id=? ORDER BY log_id ASC", (run_id,))
    logs = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        logs.append(dict(zip(columns, row)))
    return_db_connection(conn)
    return logs

# --- Task Management API ---

@app.get("/api/tasks")
def get_tasks():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT t.*, j.name as job_name 
        FROM orchestrator_tasks t
        JOIN orchestrator_jobs j ON t.job_id = j.job_id
        ORDER BY t.task_id ASC
    """)
    tasks = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        tasks.append(dict(zip(columns, row)))
    return_db_connection(conn)
    return tasks

@app.post("/api/tasks")
def create_task(task: TaskCreate):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    import json
    config_str = json.dumps(task.config) if task.config else "{}"
    
    # Set next_run to NOW so it runs immediately (or should we wait for interval?)
    # Let's set it to NOW + interval so it doesn't run instantly upon creation?
    # Or maybe user EXPECTS it to run? Let's use NULL so it runs on next sweep.
    
    cursor.execute("""
        INSERT INTO orchestrator_tasks (job_id, name, config_json, interval_minutes, enabled)
        VALUES (?, ?, ?, ?, ?)
    """, (task.job_id, task.name, config_str, task.interval_minutes, 1 if task.enabled else 0))
    
    new_id = cursor.lastrowid
    conn.commit()
    return_db_connection(conn)
    return {"status": "Task created", "task_id": new_id}

@app.put("/api/tasks/{task_id}")
def update_task(task_id: int, task: TaskUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Build dynamic update query
    fields = []
    values = []
    
    if task.name is not None:
        fields.append("name = ?")
        values.append(task.name)
    
    if task.interval_minutes is not None:
        fields.append("interval_minutes = ?")
        values.append(task.interval_minutes)
        # If interval changes, should we reset next_run? Maybe not.
        
    if task.config is not None:
        import json
        fields.append("config_json = ?")
        values.append(json.dumps(task.config))
        
    if task.enabled is not None:
        fields.append("enabled = ?")
        values.append(1 if task.enabled else 0)
        
    if not fields:
        return {"status": "No changes"}
        
    values.append(task_id)
    sql = f"UPDATE orchestrator_tasks SET {', '.join(fields)} WHERE task_id = ?"
    
    cursor.execute(sql, tuple(values))
    conn.commit()
    return_db_connection(conn)
    
    return {"status": "Task updated"}

@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM orchestrator_tasks WHERE task_id = ?", (task_id,))
    conn.commit()
    return_db_connection(conn)
    return {"status": "Task deleted"}

@app.post("/api/tasks/{task_id}/run")
def run_task_now(task_id: int):
    """Force run a task immediately."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Reset next_run to NOW, scheduler will pick it up in <10s
    cursor.execute("UPDATE orchestrator_tasks SET next_run = CURRENT_TIMESTAMP WHERE task_id = ?", (task_id,))
    conn.commit()
    return_db_connection(conn)
    
    return {"status": "Task scheduled for immediate execution"}

# Mount UI (Place this last)
# Determine absolute path to UI dist folder
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UI_DIST_DIR = os.path.join(BASE_DIR, "ui", "dist")

@app.get("/api/status")
def read_status():
    return {"status": "P2C Orchestrator Running", "proxies": ProxyManager().get_status()}

# --- Proxy Management API ---
@app.get("/api/proxies/status")
def get_proxy_status():
    return ProxyManager().get_status()

@app.post("/api/proxies/refresh")
def refresh_proxies():
    triggered = ProxyManager().force_refresh()
    return {"status": "Refresh started" if triggered else "Already validating"}

@app.post("/api/proxies/config")
def update_proxy_config(config: Dict[str, Any]):
    ProxyManager().update_config(config)
    return {"status": "Config updated", "config": ProxyManager().get_status()["config"]}

@app.get("/api/proxies/list")
def get_proxy_list():
    """Returns the current list of valid proxies for scripts to refresh their pool."""
    return {"proxies": ProxyManager().get_proxies()}

# Mount UI (Place this last)
if os.path.exists(UI_DIST_DIR):
    # Mount assets
    assets_dir = os.path.join(UI_DIST_DIR, "assets")
    if os.path.exists(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    
    # Serve Index for Root
    @app.get("/")
    async def serve_root():
        return FileResponse(os.path.join(UI_DIST_DIR, "index.html"))

    # Catch-all for SPA routing
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        if full_path.startswith("api"):
            raise HTTPException(status_code=404, detail="Not Found")
        return FileResponse(os.path.join(UI_DIST_DIR, "index.html"))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8005)
