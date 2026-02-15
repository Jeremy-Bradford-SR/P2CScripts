import asyncio
import os
import sys

# Ensure we can import from P2CScripts package
# Assuming we run this from P2CScripts/orchestrator/ or P2CScripts/ root
# Depending on CWD, we might need adjustments. 
# Let's assume we run from devroot/P2C-Stack/P2CScripts

# Add devroot/P2C-Stack to path
current_dir = os.path.dirname(os.path.abspath(__file__))
p2c_stack_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(p2c_stack_root)

from P2CScripts.orchestrator.job_runner import JobRunner
from P2CScripts.orchestrator.db import get_db_connection, return_db_connection

async def main():
    job_id = 6 # test_doc_date_fix
    # Script is relative to P2CScripts
    script_path = "scripts/ingestion/test_doc_date_fix.py"
    full_path = os.path.join(p2c_stack_root, "P2CScripts", script_path)
    
    print(f"Running job {job_id} 7 times to overflow limit of 5...")
    for i in range(7):
        print(f"Run {i+1}...")
        # config "cat" needs to be string
        await JobRunner.run_job(job_id, full_path, config_override="{}")
        
    # Verify count
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM orchestrator_history WHERE job_id = ?", (job_id,))
    count = cursor.fetchone()[0]
    return_db_connection(conn)
    
    print(f"Total History Records for Job {job_id}: {count}")
    
    if count == 5:
        print("SUCCESS: History limited to 5 records.")
    else:
        print(f"FAILURE: Expected 5 records, found {count}.")

if __name__ == "__main__":
    asyncio.run(main())
