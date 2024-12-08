# analyze_crime_data.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import re
import os

class CrimeAnalyzer:
    def __init__(self, db_path='crime_data.db'):
        self.db_path = db_path
        self.setup_style()
        
    def setup_style(self):
        """Set up plotting style"""
        plt.style.use('seaborn')
        sns.set_palette("husl")
        plt.rcParams['figure.figsize'] = [10, 6]
        plt.rcParams['svg.fonttype'] = 'none'  # Make text selectable in SVG

    def create_visualizations(self):
        """Create and save visualizations"""
        os.makedirs('reports/images', exist_ok=True)
        
        self.create_time_distribution()
        self.create_source_distribution()
        self.create_crime_type_distribution()
        self.create_hourly_heatmap()

    def create_time_distribution(self):
        """Create time distribution plot"""
        plt.figure(figsize=(12, 6))
        
        # Create hourly distribution
        hourly_counts = self.df['date'].dt.hour.value_counts().sort_index()
        plt.bar(hourly_counts.index, hourly_counts.values, alpha=0.7)
        
        plt.title('Incident Distribution by Hour of Day')
        plt.xlabel('Hour (24-hour format)')
        plt.ylabel('Number of Incidents')
        plt.grid(True, alpha=0.3)
        
        # Add average line
        plt.axhline(y=hourly_counts.mean(), color='r', linestyle='--', 
                   label=f'Average ({hourly_counts.mean():.1f} incidents)')
        plt.legend()
        
        plt.savefig('reports/images/time_distribution.svg', bbox_inches='tight')
        plt.close()

    def create_source_distribution(self):
        """Create source distribution plot"""
        plt.figure(figsize=(10, 6))
        
        source_counts = self.df['source'].value_counts()
        colors = sns.color_palette('husl', n_colors=len(source_counts))
        
        plt.pie(source_counts.values, labels=source_counts.index, colors=colors,
                autopct='%1.1f%%', startangle=90)
        plt.title('Incidents by Source')
        
        plt.savefig('reports/images/source_distribution.svg', bbox_inches='tight')
        plt.close()

    def create_crime_type_distribution(self):
        """Create crime type distribution plot"""
        plt.figure(figsize=(12, 6))
        
        crime_counts = self.df['crime_type'].value_counts().head(10)
        sns.barplot(x=crime_counts.values, y=crime_counts.index)
        
        plt.title('Top 10 Crime Types')
        plt.xlabel('Number of Incidents')
        
        plt.savefig('reports/images/crime_types.svg', bbox_inches='tight')
        plt.close()

    def create_hourly_heatmap(self):
        """Create hourly heatmap by source"""
        plt.figure(figsize=(15, 8))
        
        # Create pivot table
        hourly_by_source = pd.crosstab(self.df['date'].dt.hour, 
                                      self.df['source'])
        
        # Create heatmap
        sns.heatmap(hourly_by_source, cmap='YlOrRd', annot=True, fmt='d')
        
        plt.title('Hourly Incidents by Source')
        plt.xlabel('Source')
        plt.ylabel('Hour of Day')
        
        plt.savefig('reports/images/hourly_heatmap.svg', bbox_inches='tight')
        plt.close()

    def generate_report(self):
        """Generate a formatted analysis report with visualizations"""
        # Create visualizations first
        self.create_visualizations()
        
        report = []
        
        # Header
        report.append("# Crime Data Analysis Report")
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        
        # Summary Statistics
        report.append("## Summary Statistics (Last 24 Hours)")
        report.append(f"Total Incidents: {len(self.df)}")
        report.append(f"Date Range: {self.df['date'].min().strftime('%Y-%m-%d %H:%M')} to {self.df['date'].max().strftime('%Y-%m-%d %H:%M')}\n")
        
        # Add time distribution visualization
        report.append("## Temporal Distribution")
        report.append("![Time Distribution](images/time_distribution.svg)")
        report.append("*Figure 1: Distribution of incidents across hours of the day*\n")
        
        # Add source distribution
        report.append("## Source Distribution")
        report.append("![Source Distribution](images/source_distribution.svg)")
        report.append("*Figure 2: Breakdown of incidents by source*\n")
        
        # Add crime type distribution
        report.append("## Crime Types")
        report.append("![Crime Types](images/crime_types.svg)")
        report.append("*Figure 3: Most common types of incidents*\n")
        
        # Add heatmap
        report.append("## Hourly Patterns")
        report.append("![Hourly Heatmap](images/hourly_heatmap.svg)")
        report.append("*Figure 4: Heatmap showing incident patterns by hour and source*\n")
        
        # Recent Major Incidents
        report.append("## Recent Major Incidents")
        for idx, row in self.df.head(5).iterrows():
            report.append(f"\n### Incident at {row['date'].strftime('%Y-%m-%d %H:%M')}")
            report.append(f"Location: {row['location']}")
            report.append(f"Type: {row['crime_type']}")
            if row['description']:
                report.append(f"Description: {row['description']}")
            report.append("")
        
        return "\n".join(report)

def main():
    try:
        analyzer = CrimeAnalyzer()
        analyzer.load_data()
        report = analyzer.generate_report()
        print(report)
        
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
