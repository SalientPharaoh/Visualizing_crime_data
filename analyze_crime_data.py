# analyze_crime_data.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import logging

class CrimeAnalyzer:
    def __init__(self, db_path='crime_data.db'):
        self.db_path = db_path
        self.setup_style()
        
    def setup_style(self):
        """Setup matplotlib style"""
        plt.style.use('seaborn-v0_8')  # Updated style name
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = [10, 6]
        plt.rcParams['svg.fonttype'] = 'none'

    def load_data(self, days_back=None):
        """Load data with optional date filtering"""
        conn = sqlite3.connect(self.db_path)
        
        if days_back:
            cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            query = f"SELECT * FROM incidents WHERE date >= '{cutoff_date}'"
        else:
            query = "SELECT * FROM incidents"
            
        self.df = pd.read_sql_query(query, conn)
        conn.close()

        if not self.df.empty:
            # Convert dates
            self.df['date'] = pd.to_datetime(self.df['date'])
            self.df = self.df.sort_values('date')
            
            print(f"\nLoaded {len(self.df)} incidents")
            print(f"Date range: {self.df['date'].min()} to {self.df['date'].max()}")
            print("\nIncidents by source:")
            print(self.df['source'].value_counts())
        else:
            print("No data found in database")
            
        return self.df

    def create_visualizations(self):
        """Create all visualizations"""
        if len(self.df) == 0:
            print("No data available for visualizations")
            return

        os.makedirs('reports/images', exist_ok=True)
        
        # Time distribution
        plt.figure(figsize=(12, 6))
        self.df['hour'] = self.df['date'].dt.hour
        hourly_counts = self.df['hour'].value_counts().sort_index()
        
        plt.bar(hourly_counts.index, hourly_counts.values, alpha=0.7)
        plt.title('Incidents by Hour of Day')
        plt.xlabel('Hour')
        plt.ylabel('Number of Incidents')
        plt.grid(True, alpha=0.3)
        plt.savefig('reports/images/hourly_distribution.svg')
        plt.close()
        
        # Source distribution
        plt.figure(figsize=(10, 6))
        source_counts = self.df['source'].value_counts()
        plt.pie(source_counts.values, labels=source_counts.index, autopct='%1.1f%%')
        plt.title('Incidents by Source')
        plt.savefig('reports/images/source_distribution.svg')
        plt.close()
        
        # Crime types
        plt.figure(figsize=(12, 6))
        crime_counts = self.df['crime_type'].value_counts().head(10)
        plt.barh(range(len(crime_counts)), crime_counts.values)
        plt.yticks(range(len(crime_counts)), crime_counts.index)
        plt.title('Top 10 Crime Types')
        plt.xlabel('Number of Incidents')
        plt.tight_layout()
        plt.savefig('reports/images/crime_types.svg')
        plt.close()

    def generate_report(self, output_file='reports/latest_analysis.md'):
        """Generate analysis report"""
        os.makedirs('reports', exist_ok=True)
        
        report = []
        report.append("# Crime Data Analysis Report")
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        
        if len(self.df) == 0:
            report.append("No data available for analysis")
            with open(output_file, 'w') as f:
                f.write('\n'.join(report))
            return
            
        # Create visualizations
        self.create_visualizations()
        
        # Add statistics
        report.append("## Summary Statistics")
        report.append(f"- Total Incidents: {len(self.df)}")
        report.append(f"- Date Range: {self.df['date'].min().strftime('%Y-%m-%d %H:%M')} to {self.df['date'].max().strftime('%Y-%m-%d %H:%M')}")
        report.append(f"- Number of Sources: {self.df['source'].nunique()}")
        report.append("")
        
        # Add visualizations
        report.append("## Time Analysis")
        report.append("![Hourly Distribution](images/hourly_distribution.svg)")
        report.append("")
        
        report.append("## Source Distribution")
        report.append("![Source Distribution](images/source_distribution.svg)")
        report.append("")
        
        report.append("## Crime Types")
        report.append("![Crime Types](images/crime_types.svg)")
        report.append("")
        
        # Recent incidents
        report.append("## Recent Incidents")
        recent = self.df.sort_values('date', ascending=False).head(5)
        for _, incident in recent.iterrows():
            report.append(f"\n### {incident['crime_type']} on {incident['date'].strftime('%Y-%m-%d %H:%M')}")
            report.append(f"- Location: {incident['location']}")
            if incident['description']:
                report.append(f"- Description: {incident['description']}")
            report.append("")
        
        # Write report
        with open(output_file, 'w') as f:
            f.write('\n'.join(report))
        print(f"\nReport saved to {output_file}")

def main():
    try:
        analyzer = CrimeAnalyzer()
        
        # Load all data (remove date filter)
        analyzer.load_data()
        
        # Generate report
        analyzer.generate_report()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()