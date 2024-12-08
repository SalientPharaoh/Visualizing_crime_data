# check_db.py
import sqlite3
import pandas as pd

def check_database():
    print("Checking crime_data.db...")
    
    try:
        # Connect to database
        conn = sqlite3.connect('crime_data.db')
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("\nTables found:", [t[0] for t in tables])
        
        # Check incidents table
        if 'incidents' in [t[0] for t in tables]:
            # Get column info
            cursor.execute("PRAGMA table_info(incidents);")
            columns = cursor.fetchall()
            print("\nColumns in incidents table:")
            for col in columns:
                print(f"- {col[1]} ({col[2]})")
            
            # Get row count
            cursor.execute("SELECT COUNT(*) FROM incidents")
            count = cursor.fetchone()[0]
            print(f"\nTotal rows: {count}")
            
            # Get date range
            cursor.execute("SELECT MIN(date), MAX(date) FROM incidents")
            min_date, max_date = cursor.fetchone()
            print(f"Date range: {min_date} to {max_date}")
            
            # Get sample data
            print("\nSample data:")
            df = pd.read_sql_query("SELECT * FROM incidents LIMIT 5", conn)
            print(df)
            
            # Get source distribution
            print("\nIncidents by source:")
            df = pd.read_sql_query(
                "SELECT source, COUNT(*) as count FROM incidents GROUP BY source", 
                conn
            )
            print(df)
            
        else:
            print("No incidents table found!")
            
        conn.close()
        
    except Exception as e:
        print(f"Error checking database: {str(e)}")

if __name__ == "__main__":
    check_database()