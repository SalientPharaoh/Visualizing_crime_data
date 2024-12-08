# scrapers.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import logging
from typing import Dict, Optional

class PoliceScraper:
    def __init__(self):
        self.sources = {
            'chicago': {
                'url': 'https://data.cityofchicago.org/resource/crimes.json',
                'params': {
                    '$limit': 100,
                    '$order': ':id',
                    '$where': 'date >= "2024-01-01"'  # Get recent data
                }
            }
        }
    
    def fetch_data(self, start_date: Optional[datetime] = None) -> pd.DataFrame:
        all_data = []
        
        for city, config in self.sources.items():
            try:
                response = requests.get(config['url'], params=config['params'])
                response.raise_for_status()
                data = response.json()
                
                # Process the data
                processed_data = []
                for record in data:
                    processed_data.append({
                        'incident_id': f"chicago_{record.get('id', '')}",
                        'date': record.get('date'),
                        'description': record.get('description', ''),
                        'location': record.get('block', 'Unknown'),
                        'crime_type': record.get('primary_type', 'Unknown'),
                        'source': 'chicago_pd',
                        'latitude': float(record.get('latitude', 0)) if record.get('latitude') else None,
                        'longitude': float(record.get('longitude', 0)) if record.get('longitude') else None
                    })
                
                if processed_data:
                    df = pd.DataFrame(processed_data)
                    df['date'] = pd.to_datetime(df['date'])
                    all_data.append(df)
                    logging.info(f"Retrieved {len(df)} records from {city}")
                    
            except Exception as e:
                logging.error(f"Error fetching {city} data: {str(e)}")
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

class NewsAPIScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
    
    def fetch_data(self, start_date: Optional[datetime] = None) -> pd.DataFrame:
        params = {
            'q': '(crime OR shooting OR murder OR theft) AND (police OR arrest)',
            'language': 'en',
            'pageSize': 100,
            'sortBy': 'publishedAt'
        }
        
        if start_date:
            params['from'] = start_date.isoformat()
        
        try:
            response = requests.get(
                self.base_url,
                headers={'X-Api-Key': self.api_key},
                params=params
            )
            response.raise_for_status()
            data = response.json()
            
            if data['status'] != 'ok':
                raise Exception(f"NewsAPI Error: {data.get('message')}")
            
            # Process the articles
            processed_data = []
            for article in data['articles']:
                incident_id = hashlib.sha256(
                    f"{article.get('url', '')}{article.get('publishedAt', '')}".encode()
                ).hexdigest()
                
                processed_data.append({
                    'incident_id': incident_id,
                    'date': article.get('publishedAt'),
                    'description': article.get('description', ''),
                    'location': article.get('source', {}).get('name', 'Unknown'),
                    'crime_type': 'news_report',
                    'source': 'newsapi',
                    'latitude': None,
                    'longitude': None
                })
            
            df = pd.DataFrame(processed_data)
            df['date'] = pd.to_datetime(df['date'])
            logging.info(f"Retrieved {len(df)} news articles")
            return df
            
        except Exception as e:
            logging.error(f"Error fetching news: {str(e)}")
            return pd.DataFrame()