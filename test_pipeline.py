# test_pipeline.py
import os
import logging
from datetime import datetime
from database import CrimeDatabase
from scrapers import NewsAPIScraper, PoliceScraper
import pandas as pd
from dotenv import load_dotenv
import asyncio

logging.basicConfig(level=logging.INFO)

async def test_database():
    """Test database connection and operations"""
    print("\n=== Testing Database ===")
    try:
        db = CrimeDatabase()
        print("Successfully connected to PostgreSQL")
        
        # Get database statistics
        stats = await db.get_statistics()
        print("\nDatabase Statistics:")
        print(f"Total incidents: {stats['total_incidents']}")
        print("\nIncidents by source:")
        for source, count in stats['by_source'].items():
            print(f"- {source}: {count}")
            
        if stats['date_range']['min'] and stats['date_range']['max']:
            print("\nDate range:")
            print(f"Earliest: {stats['date_range']['min']}")
            print(f"Latest: {stats['date_range']['max']}")
            
        # Check source status
        source_status = db.get_source_status()
        if not source_status.empty:
            print("\nSource Status:")
            print(source_status)
            
    except Exception as e:
        print(f"Database connection error: {str(e)}")

async def check_data_quality():
    """Check data quality in database"""
    print("\n=== Checking Data Quality ===")
    
    try:
        db = CrimeDatabase()
        stats = await db.get_statistics()
        
        if stats['total_incidents'] > 0:
            print(f"\nTotal records: {stats['total_incidents']}")
            
            print("\nDate range:")
            print(f"Earliest: {stats['date_range']['min']}")
            print(f"Latest: {stats['date_range']['max']}")
            
            print("\nSources distribution:")
            for source, count in stats['by_source'].items():
                print(f"{source}: {count}")
        else:
            print("No data in database!")
            
    except Exception as e:
        print(f"Error checking data quality: {str(e)}")

async def test_scrapers():
    """Test data scraping functionality"""
    print("\n=== Testing Scrapers ===")
    
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('NEWS_API_KEY')
    
    if not api_key:
        print("NEWS_API_KEY not found in environment variables")
        return
        
    try:
        # Test NewsAPI scraper
        print("\nTesting NewsAPI scraper...")
        news_scraper = NewsAPIScraper(api_key)
        news_data = news_scraper.fetch_data(days_back=7)
        if not news_data.empty:
            print(f"Successfully retrieved {len(news_data)} news articles")
            
            # Test saving to database
            db = CrimeDatabase()
            saved = db.save_incidents(news_data)
            print(f"Saved {saved} incidents to database")
        else:
            print("No news data retrieved")
            
        # Test Police scraper
        print("\nTesting Police data scraper...")
        police_scraper = PoliceScraper()
        police_data = police_scraper.fetch_data()
        if not police_data.empty:
            print(f"Successfully retrieved {len(police_data)} police records")
            
            # Test saving to database
            db = CrimeDatabase()
            saved = db.save_incidents(police_data)
            print(f"Saved {saved} incidents to database")
        else:
            print("No police data retrieved")
            
    except Exception as e:
        print(f"Error testing scrapers: {str(e)}")

async def main():
    """Run all tests"""
    print("=== Starting Pipeline Tests ===")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    await test_database()
    await check_data_quality()
    await test_scrapers()
    
    print("\n=== Tests Complete ===")

if __name__ == "__main__":
    asyncio.run(main())