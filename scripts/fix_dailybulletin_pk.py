import pyodbc
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import shared_utils

def fix_pk():
    print("Connecting to Database...")
    conn = shared_utils.get_db_connection()
    cursor = conn.cursor()

    try:
        # Check and Drop UQ_ID_Key first
        print("Checking for UQ_ID_Key...")
        cursor.execute("SELECT 1 FROM sys.indexes WHERE name='UQ_ID_Key' AND object_id = OBJECT_ID('DailyBulletinArrests')")
        if cursor.fetchone():
             print("Dropping constraint UQ_ID_Key...")
             cursor.execute("ALTER TABLE DailyBulletinArrests DROP CONSTRAINT UQ_ID_Key")
             conn.commit()

        # Check if PK exists
        print("Checking for existing PK_DailyBulletinArrests...")
        cursor.execute("SELECT name FROM sys.key_constraints WHERE name='PK_DailyBulletinArrests' AND type='PK'")
        if cursor.fetchone():
             print("Dropping existing PK_DailyBulletinArrests...")
             cursor.execute("ALTER TABLE DailyBulletinArrests DROP CONSTRAINT PK_DailyBulletinArrests")
             conn.commit()
             print("Dropped PK.")
        else:
             print("PK_DailyBulletinArrests does not exist.")

        # Create new composite PK
        print("Adding new Composite PK (id, [key])...")
        # Ensure columns are NOT NULL (PK requirement)
        # Assuming they are already or we might need to set them.
        # But 'id' is likely PK so it's NOT NULL. 'key' might be nullable?
        # Let's check/set NOT NULL just in case.
        cursor.execute("ALTER TABLE DailyBulletinArrests ALTER COLUMN [key] VARCHAR(50) NOT NULL")
        cursor.execute("ALTER TABLE DailyBulletinArrests ALTER COLUMN [id] VARCHAR(50) NOT NULL") # Ensure ID size matches schema
        conn.commit()
        
        cursor.execute("ALTER TABLE DailyBulletinArrests ADD CONSTRAINT PK_DailyBulletinArrests PRIMARY KEY (id, [key])")
        conn.commit()
        print("Composite PK applied successfully.")

    except Exception as e:
        print(f"Error fixing PK: {e}")
        # Retrieve full error if possible
    finally:
        conn.close()

if __name__ == "__main__":
    fix_pk()
