# database.py
import sqlite3
import pandas as pd
import logging
from datetime import datetime

class CrimeDatabase:
    def __init__(self, db_path='crime_data.db'):
        self.db_path = db_path
        self.initialize_database()
    
    def get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def initialize_database(self):
        """Initialize database with required tables"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Create incidents table
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
        
        # Create source tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sources (
                source TEXT PRIMARY KEY,
                last_fetch TEXT,
                status TEXT,
                records_count INTEGER
            )
        ''')
        
        conn.commit()
        conn.close()
        
        logging.info("Database initialized successfully")
    
    def save_incidents(self, df: pd.DataFrame) -> int:
        """Save incidents to database"""
        if df.empty:
            return 0
            
        conn = self.get_connection()
        saved_count = 0
        
        # Convert dates to string if they're datetime
        if 'date' in df.columns and pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        for _, row in df.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO incidents 
                    (incident_id, date, description, location, crime_type, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    str(row['incident_id']),
                    str(row['date']),
                    str(row.get('description', '')),
                    str(row.get('location', '')),
                    str(row.get('crime_type', '')),
                    str(row['source'])
                ))
                if cursor.rowcount > 0:
                    saved_count += 1
            except Exception as e:
                logging.error(f"Error saving incident: {str(e)}")
                logging.error(f"Problematic row: {row}")
                
        conn.commit()
        conn.close()
        return saved_count

    def update_source(self, source: str, status: str, records_count: int):
        """Update source tracking information"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
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

    def get_source_status(self) -> pd.DataFrame:
        """Get status of all sources"""
        conn = self.get_connection()
        df = pd.read_sql_query('SELECT * FROM sources', conn)
        conn.close()
        return df

    def get_statistics(self):
        """Get database statistics"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        stats = {}
        
        # Total incidents
        cursor.execute("SELECT COUNT(*) FROM incidents")
        stats['total_incidents'] = cursor.fetchone()[0]
        
        # Incidents by source
        cursor.execute("SELECT source, COUNT(*) FROM incidents GROUP BY source")
        stats['by_source'] = dict(cursor.fetchall())
        
        # Date range
        cursor.execute("SELECT MIN(date), MAX(date) FROM incidents")
        min_date, max_date = cursor.fetchone()
        stats['date_range'] = {'min': min_date, 'max': max_date}
        
        conn.close()
        return stats