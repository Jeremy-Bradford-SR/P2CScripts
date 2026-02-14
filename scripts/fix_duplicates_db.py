import pyodbc
import sys
import os

# Add parent dir to path to import shared_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import shared_utils

def clean_duplicates():
    print("Connecting to Database...")
    conn = shared_utils.get_db_connection()
    cursor = conn.cursor()

    print("Analyzing DailyBulletinArrests for SEMANTIC duplicates (Same Name/Time/Key, Different ID)...")
    
    # Check for duplicates where Name/Time/Key are same (Case Insensitive) but ID is different
    check_sql = """
    SELECT [key], event_time, name, COUNT(*) as Cnt
    FROM DailyBulletinArrests
    GROUP BY [key], event_time, name
    HAVING COUNT(*) > 1
    """
    
    cursor.execute(check_sql)
    rows = cursor.fetchall()

    if not rows:
        print("No semantic duplicates found on (Key, Time, Name). Table is clean.")
        conn.close()
        return

    print(f"Found {len(rows)} groups of semantic duplicates.")
    
    deleted_total = 0
    
    for row in rows:
        r_key = row.key
        r_time = row.event_time
        r_name = row.name # This is one specific casing, but GROUP BY might define the group.
        
        # Fetch all matching records for this group (ignoring case on Name if simpler, or using the exact values from group if Collation is CI)
        # Assuming database collation is likely CI (Case Insensitive) for equality, but let's be safe.
        # We'll select all records that match Key and Time, then filter by Name in Python or SQL with LIKE
        
        # Fetch IDs and Names
        cursor.execute("SELECT id, name, location FROM DailyBulletinArrests WHERE [key]=? AND event_time=? AND name=?", r_key, r_time, r_name)
        variants = cursor.fetchall()
        
        if len(variants) < 2:
             continue # Should not happen given HAVING count > 1

        # Sort variants to decide which to keep.
        # Preference: 
        # 1. Keep the one with the "Shortest" ID (Base ID) to avoid the "-AR" suffix versions?
        #    - Yes, usually we want '12345' over '12345-AR' if both exist, to match the probable original.
        # 2. Or keep the one with the "Best" casing? (Mixed vs Upper).
        
        # Sort by ID Length (Shortest first)
        variants.sort(key=lambda x: len(x.id))
        
        keeper = variants[0]
        duplicates = variants[1:]
        
        print(f"Keeping: {keeper.id} ({keeper.name}). Deleting: {[d.id for d in duplicates]}")
        
        for dup in duplicates:
             cursor.execute("DELETE FROM DailyBulletinArrests WHERE id=?", dup.id)
             deleted_total += 1

    conn.commit()
    print(f"Cleanup Complete. Deleted {deleted_total} duplicate records.")
    
    conn.close()

if __name__ == "__main__":
    clean_duplicates()
