# main.py
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from database import CrimeDatabase
from scrapers import NewsAPIScraper, PoliceScraper
import asyncio

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def fetch_all_data():
    """Fetch data from all sources"""
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('NEWS_API_KEY')
    
    if not api_key:
        api_key = 'e52b12c7e8624710acf7cb2caea28cd5'  # Fallback key
    
    # Initialize database
    db = CrimeDatabase()
    
    try:
        # Fetch news data
        logging.info("Fetching news data...")
        news_scraper = NewsAPIScraper(api_key)
        news_data = news_scraper.fetch_data()
        if not news_data.empty:
            saved = db.save_incidents(news_data)
            db.update_source('newsapi', 'success', saved)
            logging.info(f"Saved {saved} news incidents")
        
        # Fetch Chicago police data
        logging.info("Fetching Chicago police data...")
        police_scraper = PoliceScraper()
        # Get today's date and format it for the API
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Set parameters to get recent data
        police_scraper.sources['chicago']['params'] = {
            '$limit': 1000,  # Increase limit
            '$order': 'date DESC',
            '$where': f"date >= '{today}'"
        }
        
        police_data = police_scraper.fetch_data()
        if not police_data.empty:
            saved = db.save_incidents(police_data)
            db.update_source('chicago_police', 'success', saved)
            logging.info(f"Saved {saved} police incidents")
        
        # Get final statistics
        stats = await db.get_statistics()
        print("\nData Collection Summary:")
        print("------------------------")
        print(f"Total incidents in database: {stats['total_incidents']}")
        print("\nBy source:")
        for source, count in stats['by_source'].items():
            print(f"- {source}: {count}")
            
        if stats['date_range']['min'] and stats['date_range']['max']:
            print("\nDate range:")
            print(f"From: {stats['date_range']['min']}")
            print(f"To: {stats['date_range']['max']}")
        
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(fetch_all_data())