# test_pipeline.py
import os
import sqlite3
import logging
from datetime import datetime
from database import CrimeDatabase
from scrapers import NewsAPIScraper, PoliceScraper
import pandas as pd
logging.basicConfig(level=logging.INFO)

def test_database():
    """Test database connection and tables"""
    print("\n=== Testing Database ===")
    db = CrimeDatabase('crime_data.db')
    
    # Check if database file exists
    if os.path.exists('crime_data.db'):
        print("Database file exists")
        
        # Check tables
        conn = sqlite3.connect('crime_data.db')
        cursor = conn.cursor()
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("\nTables in database:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"- {table[0]}: {count} rows")
        
        conn.close()
    else:
        print("Database file does not exist!")

def test_scrapers():
    """Test data scrapers"""
    print("\n=== Testing Scrapers ===")
    
    # Test NewsAPI
    api_key = 'e52b12c7e8624710acf7cb2caea28cd5'  # Your API key
    news_scraper = NewsAPIScraper(api_key)
    print("\nTesting NewsAPI scraper...")
    news_data = news_scraper.fetch_data()
    print(f"Retrieved {len(news_data)} news articles")
    if not news_data.empty:
        print("\nSample news data:")
        print(news_data[['date', 'location', 'crime_type']].head())
    
    # Test Police data
    print("\nTesting Police data scraper...")
    police_scraper = PoliceScraper()
    police_data = police_scraper.fetch_data()
    print(f"Retrieved {len(police_data)} police records")
    if not police_data.empty:
        print("\nSample police data:")
        print(police_data[['date', 'location', 'crime_type']].head())

def check_data_quality():
    """Check data quality in database"""
    print("\n=== Checking Data Quality ===")
    
    conn = sqlite3.connect('crime_data.db')
    df = pd.read_sql_query("SELECT * FROM incidents", conn)
    conn.close()
    
    if not df.empty:
        print(f"\nTotal records: {len(df)}")
        print("\nMissing values:")
        print(df.isnull().sum())
        
        print("\nDate range:")
        print(f"Earliest: {df['date'].min()}")
        print(f"Latest: {df['date'].max()}")
        
        print("\nSources distribution:")
        print(df['source'].value_counts())
    else:
        print("No data in database!")

def main():
    print("Starting pipeline test...")
    
    # Test components
    test_database()
    test_scrapers()
    check_data_quality()
    
    print("\nTest complete!")

if __name__ == "__main__":
    main()