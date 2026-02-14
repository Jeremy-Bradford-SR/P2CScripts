import shared_utils
import sys

def check_ids():
    conn = shared_utils.get_db_connection()
    cursor = conn.cursor()
    
    ids = ['2026', '2026000361']
    
    print(f"Checking IDs: {ids}")
    
    for rid in ids:
        print(f"\n--- Checking ID: {rid} ---")
        cursor.execute("SELECT id, [key], name, [time], [case], description FROM dbo.DailyBulletinArrests WHERE id = ?", rid)
        rows = cursor.fetchall()
        
        if not rows:
            print("  [NOT FOUND] No record exists with this ID.")
        else:
            for row in rows:
                print(f"  [FOUND] Key: {row.key}")
                print(f"          Name: {row.name}")
                print(f"          Time: {row.time}")
                print(f"          Case: {row.case}")
                print(f"          Desc: {row.description}")

    conn.close()

if __name__ == "__main__":
    check_ids()
