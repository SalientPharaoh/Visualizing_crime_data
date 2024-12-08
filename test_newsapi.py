#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from scrapers import NewsAPIScraper
import logging

logging.basicConfig(level=logging.INFO)

def test_newsapi():
    # Load environment variables
    load_dotenv()
    api_key = os.getenv('NEWS_API_KEY')
    
    if not api_key:
        api_key = 'e52b12c7e8624710acf7cb2caea28cd5'  # Fallback key
    
    # Initialize scraper
    scraper = NewsAPIScraper(api_key)
    
    # Fetch data for last 7 days to ensure we get some results
    news_data = scraper.fetch_data(days_back=7)
    
    if not news_data.empty:
        print(f"\nSuccessfully retrieved {len(news_data)} articles")
        print("\nSample articles:")
        for _, article in news_data.head().iterrows():
            print(f"\nDate: {article['date']}")
            print(f"Type: {article['crime_type']}")
            print(f"Location: {article['location']}")
            print(f"Description: {article['description'][:200]}...")
    else:
        print("No articles retrieved")

if __name__ == "__main__":
    test_newsapi()
