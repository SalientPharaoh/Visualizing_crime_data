# main.py
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from database import CrimeDatabase
from scrapers import NewsAPIScraper, PoliceScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def fetch_all_data():
    """Fetch data from all sources"""
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('NEWS_API_KEY')
    
    if not api_key:
        api_key = 'e52b12c7e8624710acf7cb2caea28cd5'  # Fallback key
    
    # Initialize database
    db = CrimeDatabase('crime_data.db')
    
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
            
        # Print summary
        print("\nData Collection Summary:")
        print("------------------------")
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get total count
        cursor.execute("SELECT COUNT(*) FROM incidents")
        total = cursor.fetchone()[0]
        print(f"Total incidents in database: {total}")
        
        # Get count by source
        cursor.execute("""
            SELECT source, COUNT(*) as count 
            FROM incidents 
            GROUP BY source
        """)
        for source, count in cursor.fetchall():
            print(f"{source}: {count} incidents")
            
        # Get date range
        cursor.execute("""
            SELECT MIN(date), MAX(date) 
            FROM incidents
        """)
        min_date, max_date = cursor.fetchone()
        print(f"\nDate range: {min_date} to {max_date}")
        
        conn.close()
        
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    fetch_all_data()