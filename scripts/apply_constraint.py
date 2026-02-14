import pyodbc
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import shared_utils

def apply_constraint():
    print("Connecting to Database...")
    conn = shared_utils.get_db_connection()
    cursor = conn.cursor()

    print("applying UNIQUE constraint UQ_ID_Key to DailyBulletinArrests...")
    
    try:
        # Check if exists
        cursor.execute("SELECT 1 FROM sys.indexes WHERE name='UQ_ID_Key' AND object_id = OBJECT_ID('DailyBulletinArrests')")
        if cursor.fetchone():
             print("Constraint UQ_ID_Key already exists.")
        else:
             cursor.execute("ALTER TABLE DailyBulletinArrests ADD CONSTRAINT UQ_ID_Key UNIQUE (id, [key])")
             print("Constraint applied successfully.")
        conn.commit()
    except Exception as e:
        print(f"Error applying constraint: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    apply_constraint()
