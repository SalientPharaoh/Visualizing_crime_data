# analyze_crime_data.py
import sqlite3
import pandas as pd
import re
from datetime import datetime

class CrimeAnalyzer:
    def __init__(self, db_path='crime_data.db'):
        self.db_path = db_path
        
    def parse_date(self, date_str):
        """Parse dates with error handling"""
        try:
            # First try ISO format
            return pd.to_datetime(date_str, format='ISO8601', utc=True).tz_localize(None)
        except:
            try:
                # Then try general parsing
                return pd.to_datetime(date_str).tz_localize(None)
            except:
                print(f"Could not parse date: {date_str}")
                return None

    def load_data(self):
        """Load and clean data"""
        conn = sqlite3.connect(self.db_path)
        self.df = pd.read_sql_query('SELECT * FROM incidents', conn)
        conn.close()
        
        print("\nSample of raw dates:")
        print(self.df['date'].head())
        
        # Convert dates safely
        print("\nConverting dates...")
        self.df['date'] = self.df['date'].apply(self.parse_date)
        
        # Clean descriptions and standardize sources
        self.df['description'] = self.df['description'].fillna('')
        self.df['source'] = self.df['source'].replace({
            'chicago_pd': 'chicago_police',
            'police_chicago': 'chicago_police'
        })
        
        # Remove any rows where date parsing failed
        valid_dates = self.df['date'].notna()
        invalid_count = (~valid_dates).sum()
        if invalid_count > 0:
            print(f"\nRemoved {invalid_count} rows with invalid dates")
            self.df = self.df[valid_dates]
        
        print(f"\nProcessed {len(self.df)} valid incidents")
        print("\nSample of processed dates:")
        print(self.df['date'].head())
        
        return self.df

    def analyze_incidents(self):
        """Analyze recent incidents by source"""
        print("\n=== Recent Crime Analysis ===")
        
        for source in self.df['source'].unique():
            source_data = self.df[self.df['source'] == source]
            print(f"\n{source.upper()}")
            print(f"Total incidents: {len(source_data)}")
            
            # Show most recent incidents
            recent = source_data.sort_values('date', ascending=False).head(5)
            for _, incident in recent.iterrows():
                print("\n---")
                print(f"Date: {incident['date'].strftime('%Y-%m-%d %H:%M')}")
                print(f"Location: {incident['location']}")
                print(f"Type: {incident['crime_type']}")
                
                if len(str(incident['description'])) > 10:
                    desc = str(incident['description']).strip()
                    print("Description:", desc)
                    
                    # Extract interesting details
                    details = self.extract_details(desc)
                    if details:
                        for key, value in details.items():
                            print(f"{key.title()}: {', '.join(value)}")

    def extract_details(self, text):
        """Extract relevant details from description"""
        details = {}
        
        # Convert to lowercase for matching
        text = text.lower()
        
        # Weapons
        weapons = re.findall(r'\b(gun|knife|firearm|weapon|pistol|rifle)\b', text)
        if weapons:
            details['weapons'] = list(set(weapons))
        
        # Money amounts
        amounts = re.findall(r'\$\s*(\d+(?:,\d{3})*(?:\.\d{2})?)', text)
        if amounts:
            details['amounts'] = amounts
        
        # Violence keywords
        violence = re.findall(r'\b(assault|battery|fight|attack|threat|shot|stabbed|injured)\b', text)
        if violence:
            details['violence_indicators'] = list(set(violence))
        
        # Locations
        locations = re.findall(r'\b(\d+(?:st|nd|rd|th)?\s+(?:street|ave|avenue|road|rd|block|blvd|boulevard))\b', text)
        if locations:
            details['locations'] = list(set(locations))
        
        return details

    def print_statistics(self):
        """Print summary statistics"""
        print("\n=== Crime Statistics ===")
        
        # Source distribution
        print("\nIncidents by Source:")
        print(self.df['source'].value_counts())
        
        # Date range
        print("\nDate Range:")
        print(f"Earliest: {self.df['date'].min().strftime('%Y-%m-%d %H:%M')}")
        print(f"Latest: {self.df['date'].max().strftime('%Y-%m-%d %H:%M')}")
        
        # Time of day analysis
        self.df['hour'] = self.df['date'].dt.hour
        time_periods = pd.cut(self.df['hour'], 
                            bins=[0,6,12,18,24], 
                            labels=['Night (12AM-6AM)', 'Morning (6AM-12PM)',
                                   'Afternoon (12PM-6PM)', 'Evening (6PM-12AM)'])
        
        print("\nIncidents by Time of Day:")
        print(time_periods.value_counts())
        
        # Crime types
        if 'crime_type' in self.df.columns:
            print("\nMost Common Crime Types:")
            print(self.df['crime_type'].value_counts().head())

def main():
    try:
        analyzer = CrimeAnalyzer()
        
        print("Loading data...")
        analyzer.load_data()
        
        print("\nAnalyzing incidents...")
        analyzer.analyze_incidents()
        
        print("\nGenerating statistics...")
        analyzer.print_statistics()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()