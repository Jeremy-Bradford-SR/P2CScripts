import pyodbc
import os

def get_db_connection():
    """Establishes a database connection using settings from .env-db."""
    # Path to .env-db (one directory up from scripts/)
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env-db')
    
    conn_str_raw = ""
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                if line.startswith("ConnectionStrings__Default="):
                    conn_str_raw = line.strip().split("=", 1)[1]
                    break
    
    if not conn_str_raw:
        raise Exception("Could not find ConnectionStrings__Default in .env-db")

    # Parse connection string
    parts = [p for p in conn_str_raw.split(';') if p]
    params = {}
    for p in parts:
        if '=' in p:
            k, v = p.split('=', 1)
            params[k.strip()] = v.strip()

    # Build ODBC connection string
    driver = "{ODBC Driver 18 for SQL Server}"
    server = params.get('Server')
    database = params.get('Database')
    uid = params.get('User Id')
    pwd = params.get('Password')
    trust_cert = params.get('TrustServerCertificate', 'no')
    if trust_cert.lower() == 'true':
        trust_cert = 'yes'
    
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};TrustServerCertificate={trust_cert};"
    return pyodbc.connect(conn_str)

def create_view():
    sql = """
    CREATE OR ALTER VIEW vw_ViolatorsWithJailInfo AS
    SELECT
        A.name AS ArrestRecordName,
        A.firstname AS FirstName,
        A.lastname AS LastName,
        A.charge AS ArrestCharge,
        A.event_time AS ArrestDate,
        A.location AS ArrestLocation,
        
        -- Offender Summary Info
        S.OffenderNumber AS DocOffenderNumber,
        S.Gender AS DocGender,
        S.Age AS DocAge,
        
        -- Aggregated Offenses (from Offender_Detail)
        (SELECT 
            STRING_AGG(T3.Offense, ', ') WITHIN GROUP (ORDER BY T3.Offense)
         FROM 
            dbo.Offender_Summary AS T1
         JOIN 
            (SELECT DISTINCT OffenderNumber, Offense FROM dbo.Offender_Detail WHERE Offense IS NOT NULL) AS T3 
            ON T1.OffenderNumber = T3.OffenderNumber
         WHERE 
            T1.Name = S.Name
        ) AS OriginalOffenses,

        -- Jail Info (Subquery to get latest booking)
        J.book_id AS JailBookId,
        J.arrest_date AS JailArrestDate,
        J.released_date AS JailReleasedDate,
        J.total_bond_amount AS JailBondAmount,
        (SELECT STRING_AGG(charge_description, ', ') FROM jail_charges WHERE book_id = J.book_id) AS JailCharges

    FROM
        dbo.DailyBulletinArrests AS A
    INNER JOIN
        dbo.Offender_Summary AS S ON S.Name = CONCAT_WS(' ', A.firstname, A.middlename, A.lastname)
    OUTER APPLY (
        SELECT TOP 1 *
        FROM dbo.jail_inmates AS Ji
        WHERE Ji.firstname = A.firstname AND Ji.lastname = A.lastname
        ORDER BY Ji.arrest_date DESC
    ) AS J;
    """
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        print("Executing CREATE OR ALTER VIEW...")
        cursor.execute(sql)
        conn.commit()
        print("View created successfully.")
        
        # Verification
        print("Verifying view...")
        cursor.execute("SELECT TOP 5 ArrestRecordName, JailBookId FROM vw_ViolatorsWithJailInfo")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_view()
