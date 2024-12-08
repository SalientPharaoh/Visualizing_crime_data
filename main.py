# main.py
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
from database import CrimeDatabase
from scrapers import NewsAPIScraper, PoliceScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('NEWS_API_KEY')
    
    if not api_key:
        logging.error("NEWS_API_KEY not found in environment variables")
        api_key = 'e52b12c7e8624710acf7cb2caea28cd5'  # Fallback to provided key
    
    # Initialize database
    db = CrimeDatabase('crime_data.db')
    
    # Initialize scrapers
    news_scraper = NewsAPIScraper(api_key)
    police_scraper = PoliceScraper()
    
    # Fetch news data
    logging.info("Fetching news data...")
    news_data = news_scraper.fetch_data()
    if not news_data.empty:
        saved = db.save_incidents(news_data)
        db.update_source('newsapi', 'success', saved)
        logging.info(f"Saved {saved} news incidents")
    
    # Fetch police data (Chicago only for now, removing SF)
    logging.info("Fetching police data...")
    police_scraper.sources.pop('sf', None)  # Remove SF source temporarily
    police_data = police_scraper.fetch_data()
    if not police_data.empty:
        saved = db.save_incidents(police_data)
        db.update_source('police', 'success', saved)
        logging.info(f"Saved {saved} police incidents")

if __name__ == "__main__":
    main()

