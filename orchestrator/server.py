from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import asyncio
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
    yield
    # Shutdown
    print("Shutting down services...")

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
