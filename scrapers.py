# scrapers.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import logging
from typing import Dict, Optional

class NewsAPIScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2/everything"
    
    def fetch_data(self, days_back: int = 1) -> pd.DataFrame:
        """Fetch crime-related news"""
        params = {
            'q': '(crime OR shooting OR murder OR theft) AND (police OR arrest)',
            'language': 'en',
            'pageSize': 100,
            'sortBy': 'publishedAt',
            'from': (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        }
        
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
            
            articles = data.get('articles', [])
            logging.info(f"Retrieved {len(articles)} articles from NewsAPI")
            
            # Process articles
            processed_data = []
            for article in articles:
                if not article:
                    continue
                    
                # Generate unique ID
                url = article.get('url', '')
                published = article.get('publishedAt', '')
                if not url or not published:
                    continue
                    
                incident_id = hashlib.sha256(
                    f"{url}{published}".encode()
                ).hexdigest()
                
                # Process article
                processed_data.append({
                    'incident_id': incident_id,
                    'date': published,
                    'description': article.get('description', ''),
                    'location': article.get('source', {}).get('name', 'Unknown'),
                    'crime_type': self._classify_crime_type(
                        f"{article.get('title', '')} {article.get('description', '')}"
                    ),
                    'source': 'newsapi'
                })
            
            df = pd.DataFrame(processed_data)
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
            return df
            
        except Exception as e:
            logging.error(f"Error fetching news: {str(e)}")
            return pd.DataFrame()

    def _classify_crime_type(self, text: str) -> str:
        """Classify crime type based on text content"""
        if not text:
            return 'UNCLASSIFIED'
            
        text = text.lower()
        if any(word in text for word in ['murder', 'kill', 'dead', 'death', 'homicide']):
            return 'HOMICIDE'
        elif any(word in text for word in ['shoot', 'shot', 'gunfire', 'gun']):
            return 'SHOOTING'
        elif any(word in text for word in ['rob', 'theft', 'stole', 'burglary']):
            return 'ROBBERY/THEFT'
        elif any(word in text for word in ['assault', 'attack', 'fight']):
            return 'ASSAULT'
        elif any(word in text for word in ['drug', 'narcotic']):
            return 'DRUG RELATED'
        return 'OTHER'

class PoliceScraper:
    def __init__(self):
        self.sources = {
            'chicago': {
                'url': 'https://data.cityofchicago.org/resource/crimes.json',
                'params': {
                    '$limit': 100,
                    '$order': ':id',
                    '$where': f"date >= '{datetime.now().strftime('%Y-%m-%d')}'"
                }
            }
        }
    
    def fetch_data(self) -> pd.DataFrame:
        """Fetch police department data"""
        all_data = []
        
        for city, config in self.sources.items():
            try:
                response = requests.get(config['url'], params=config['params'])
                response.raise_for_status()
                data = response.json()
                
                processed_data = []
                for record in data:
                    incident_id = f"{city}_{record.get('id', '')}"
                    processed_data.append({
                        'incident_id': incident_id,
                        'date': record.get('date'),
                        'description': record.get('description', ''),
                        'location': record.get('block', 'Unknown'),
                        'crime_type': record.get('primary_type', 'UNKNOWN'),
                        'source': f'{city}_police'
                    })
                
                if processed_data:
                    df = pd.DataFrame(processed_data)
                    df['date'] = pd.to_datetime(df['date'])
                    all_data.append(df)
                    logging.info(f"Retrieved {len(df)} records from {city}")
                    
            except Exception as e:
                logging.error(f"Error fetching {city} data: {str(e)}")
        
        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()