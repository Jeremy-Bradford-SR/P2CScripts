import pyodbc
import os
import glob
import json
from shared_utils import status
from orchestrator.db import get_db_connection

def scan_for_scripts(root_dir="."):
    """Scans the directory for .py files in scripts/ingestion."""
    scripts = []
    
    search_dirs = [
        os.path.join(root_dir, "scripts", "ingestion"),
        os.path.join(root_dir, "scripts", "ETL")
    ]
    
    files = []
    for d in search_dirs:
        if os.path.exists(d):
            files.extend(glob.glob(os.path.join(d, "*.py")))
            
    # Filter out __init__.py
    files = [f for f in files if not os.path.basename(f).startswith("__")]

    for file_path in files:
        filename = os.path.basename(file_path)
        # Calculate relative path from root
        rel_path = os.path.relpath(file_path, root_dir)
        
        # Default config based on filename or empty
        default_config = '{}'
        if "DailyBulletin" in filename and "P2C" in filename:
            default_config = '{"days": 3, "workers": 7, "chunk_size": 7, "log_level": "INFO"}'
        elif "JailInmates" in filename:
             default_config = '{"days": 3, "workers": 7}'
        elif "backfill_geocoding" in filename:
             default_config = '{}'
        elif "UpdateDAB" in filename:
             default_config = '{}'

        scripts.append((filename.replace('.py', ''), rel_path, default_config))
    return scripts

def create_tables(scan_dir="."):
    conn = get_db_connection()
    if not conn:
        status("DB Setup", "Failed to connect to DB.")
        return

    cursor = conn.cursor()
    
    tables_sql = [

        """
        CREATE TABLE IF NOT EXISTS orchestrator_jobs (
            job_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            script_path TEXT NOT NULL,
            default_config TEXT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orchestrator_tasks (
            task_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            config_json TEXT DEFAULT '{}',
            interval_minutes INTEGER NOT NULL,
            last_run DATETIME NULL,
            next_run DATETIME NULL,
            enabled INTEGER DEFAULT 1,
            FOREIGN KEY(job_id) REFERENCES orchestrator_jobs(job_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orchestrator_history (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            start_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            end_time DATETIME NULL,
            status TEXT NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILURE'
            exit_code INTEGER NULL,
            FOREIGN KEY(job_id) REFERENCES orchestrator_jobs(job_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orchestrator_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            log_text TEXT NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(run_id) REFERENCES orchestrator_history(run_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orchestrator_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT NOT NULL,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    ]

    try:
        for i, sql in enumerate(tables_sql):
            cursor.execute(sql)
        
        status("DB Setup", "Tables verified.")
        
        # Scan and Seed Jobs
        scripts = scan_for_scripts(scan_dir)
        new_count = 0
        for name, path, config in scripts:
            # Check if exists
            cursor.execute("SELECT job_id FROM orchestrator_jobs WHERE name = ?", (name,))
            row = cursor.fetchone()
            if not row:
                cursor.execute("INSERT INTO orchestrator_jobs (name, script_path, default_config) VALUES (?, ?, ?)", (name, path, config))
                new_count += 1
            else:
                # Update path if it changed (migration support)
                cursor.execute("UPDATE orchestrator_jobs SET script_path = ? WHERE name = ?", (path, name))
        
        if new_count > 0:
            status("DB Setup", f"Registered {new_count} new scripts.")

        # Seed initial Proxy Manager Config
        cursor.execute("SELECT 1 FROM orchestrator_config WHERE config_key = 'proxy_manager_config'")
        if not cursor.fetchone():
            default_config = {
                "concurrency": int(os.environ.get("PROXY_CONCURRENCY", 250)),
                "ttl": int(os.environ.get("PROXY_TTL", 600)),
                "test_url": os.environ.get("PROXY_TEST_URL", "http://p2c.cityofdubuque.org/main.aspx"),
                "target_pool_size": int(os.environ.get("PROXY_TARGET_POOL_SIZE", 100)),
                "sources": [s.strip() for s in os.environ.get("PROXY_SOURCES", "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt").split(",")]
            }
            cursor.execute("INSERT INTO orchestrator_config (config_key, config_value) VALUES (?, ?)", 
                           ("proxy_manager_config", json.dumps(default_config)))
            status("DB Setup", "Seeded default proxy_manager_config.")

        conn.commit()
        status("DB Setup", "Database setup complete.")
        
    except Exception as e:
        status("DB Setup", f"Error: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    create_tables()
