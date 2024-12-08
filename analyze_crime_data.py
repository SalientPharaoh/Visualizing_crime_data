# analyze_crime_data.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import logging
from database import CrimeDatabase
import asyncio

class CrimeAnalyzer:
    def __init__(self):
        self.setup_style()
        self.db = CrimeDatabase()

    def setup_style(self):
        """Set up plotting style"""
        plt.style.use('seaborn')  # Use default seaborn style
        sns.set_theme()  # Use seaborn's default theme
        sns.set_palette("husl")
        
    async def load_data(self, days_back=None):
        """Load data with optional date filtering"""
        try:
            # Get data from PostgreSQL
            stats = await self.db.get_statistics()
            source_status = self.db.get_source_status()
            
            # Print database stats
            print(f"\nDatabase Statistics:")
            print(f"Total incidents: {stats['total_incidents']}")
            print("\nIncidents by source:")
            for source, count in stats['by_source'].items():
                print(f"{source}: {count}")
            
            if stats['date_range']['min'] and stats['date_range']['max']:
                print(f"\nDate range: {stats['date_range']['min']} to {stats['date_range']['max']}")
            
            print("\nSource Status:")
            if not source_status.empty:
                print(source_status.to_string())
            else:
                print("No source status information available")
                
        except Exception as e:
            logging.error(f"Error loading data: {str(e)}")

    def generate_report(self, output_file='reports/latest_analysis.md'):
        """Generate analysis report"""
        try:
            os.makedirs('reports', exist_ok=True)
            with open(output_file, 'w') as f:
                f.write("# Crime Data Analysis Report\n\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                # Add database statistics
                stats = asyncio.run(self.db.get_statistics())
                f.write("## Database Statistics\n\n")
                f.write(f"Total incidents: {stats['total_incidents']}\n\n")
                
                f.write("### Incidents by Source\n\n")
                for source, count in stats['by_source'].items():
                    f.write(f"- {source}: {count}\n")
                f.write("\n")
                
                if stats['date_range']['min'] and stats['date_range']['max']:
                    f.write("### Date Range\n\n")
                    f.write(f"From: {stats['date_range']['min']}\n")
                    f.write(f"To: {stats['date_range']['max']}\n\n")
                
                # Add source status
                source_status = self.db.get_source_status()
                if not source_status.empty:
                    f.write("### Source Status\n\n")
                    f.write("```\n")
                    f.write(source_status.to_string())
                    f.write("\n```\n")
                
        except Exception as e:
            logging.error(f"Error generating report: {str(e)}")

    def _plot_hourly_distribution(self):
        """Plot hourly distribution of incidents"""
        plt.figure(figsize=(12, 6))
        self.df['hour'] = self.df['date'].dt.hour
        sns.histplot(data=self.df, x='hour', bins=24)
        plt.title('Hourly Distribution of Incidents')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Incidents')
        plt.savefig('images/hourly_distribution.svg')
        plt.close()

    def _plot_source_distribution(self):
        """Plot distribution by source"""
        plt.figure(figsize=(10, 6))
        source_counts = self.df['source'].value_counts()
        source_counts.plot(kind='bar')
        plt.title('Incidents by Source')
        plt.xlabel('Source')
        plt.ylabel('Number of Incidents')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('images/source_distribution.svg')
        plt.close()

    def _plot_crime_types(self):
        """Plot distribution of crime types"""
        plt.figure(figsize=(12, 6))
        crime_counts = self.df['crime_type'].value_counts().head(10)
        crime_counts.plot(kind='bar')
        plt.title('Top 10 Crime Types')
        plt.xlabel('Crime Type')
        plt.ylabel('Number of Incidents')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('images/crime_types.svg')
        plt.close()

    def _print_recent_incidents(self):
        """Print details of recent incidents"""
        recent = self.df.sort_values('date', ascending=False).head(5)
        for _, incident in recent.iterrows():
            print(f"\n### {incident['crime_type']} on {incident['date'].strftime('%Y-%m-%d %H:%M')}")
            print(f"- Location: {incident['location']}")
            if pd.notna(incident['description']):
                print(f"- Description: {incident['description']}")

async def main():
    analyzer = CrimeAnalyzer()
    await analyzer.load_data()
    analyzer.generate_report()

if __name__ == "__main__":
    asyncio.run(main())