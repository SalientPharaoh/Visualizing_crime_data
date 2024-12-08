# database.py
import sqlite3
from datetime import datetime
import pandas as pd
import logging

class CrimeDatabase:
    def __init__(self, db_path='crime_data.db'):
        self.db_path = db_path
        self.initialize_database()
    
    def initialize_database(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Main incidents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                incident_id TEXT UNIQUE,
                date TEXT,
                description TEXT,
                location TEXT,
                crime_type TEXT,
                source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Source tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sources (
                source TEXT PRIMARY KEY,
                last_fetch TEXT,
                status TEXT,
                records_count INTEGER
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_date ON incidents(date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON incidents(source)')
        
        conn.commit()
        conn.close()
    
    def save_incidents(self, df: pd.DataFrame) -> int:
        """Save incidents to database with proper date handling"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        saved_count = 0
        
        # Convert DataFrame dates to strings in ISO format
        if 'date' in df.columns:
            df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        for _, row in df.iterrows():
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO incidents 
                    (incident_id, date, description, location, crime_type, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    str(row['incident_id']),
                    str(row['date']),
                    str(row['description']),
                    str(row['location']),
                    str(row['crime_type']),
                    str(row['source'])
                ))
                
                if cursor.rowcount > 0:
                    saved_count += 1
                    
            except Exception as e:
                logging.error(f"Error saving incident {row['incident_id']}: {str(e)}")
                logging.error(f"Row data: {row.to_dict()}")
                
        conn.commit()
        conn.close()
        return saved_count

    def update_source(self, source: str, status: str, records_count: int):
        """Update source status in the tracking table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO sources 
                (source, last_fetch, status, records_count)
                VALUES (?, ?, ?, ?)
            ''', (
                source,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                status,
                records_count
            ))
            conn.commit()
            
        except Exception as e:
            logging.error(f"Error updating source {source}: {str(e)}")
            
        finally:
            conn.close()

    def get_latest_date(self, source: str) -> datetime:
        """Get the most recent incident date for a source"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT MAX(date) FROM incidents WHERE source = ?', (source,))
        result = cursor.fetchone()[0]
        
        conn.close()
        
        if result:
            return datetime.strptime(result, '%Y-%m-%d %H:%M:%S')
        return None

    def get_source_status(self):
        """Get status of all sources"""
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query('SELECT * FROM sources', conn)
        conn.close()
        return df