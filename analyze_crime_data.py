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
        plt.rcParams['svg.fonttype'] = 'none'

    def load_data(self):
        """Load and clean data from SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT * FROM incidents 
                WHERE date >= datetime('now', '-1 day')
                ORDER BY date DESC
            """
            self.df = pd.read_sql_query(query, conn)
            conn.close()

            # Convert dates
            self.df['date'] = pd.to_datetime(self.df['date'])
            
            # Clean descriptions
            self.df['description'] = self.df['description'].fillna('')
            
            # Standardize sources
            self.df['source'] = self.df['source'].replace({
                'chicago_pd': 'chicago_police',
                'police_chicago': 'chicago_police'
            })
            
            print(f"Loaded {len(self.df)} incidents from the last 24 hours")
            return self.df
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            self.df = pd.DataFrame()
            return self.df

    def create_visualizations(self):
        """Create and save visualizations"""
        if len(self.df) == 0:
            print("No data available for visualizations")
            return

        os.makedirs('reports/images', exist_ok=True)
        
        try:
            self.create_time_distribution()
            self.create_source_distribution()
            self.create_crime_type_distribution()
            self.create_hourly_heatmap()
        except Exception as e:
            print(f"Error creating visualizations: {str(e)}")

    def create_time_distribution(self):
        """Create time distribution plot"""
        plt.figure(figsize=(12, 6))
        
        hourly_counts = self.df['date'].dt.hour.value_counts().sort_index()
        plt.bar(hourly_counts.index, hourly_counts.values, alpha=0.7)
        
        plt.title('Incident Distribution by Hour of Day')
        plt.xlabel('Hour (24-hour format)')
        plt.ylabel('Number of Incidents')
        plt.grid(True, alpha=0.3)
        
        plt.axhline(y=hourly_counts.mean(), color='r', linestyle='--', 
                   label=f'Average ({hourly_counts.mean():.1f} incidents)')
        plt.legend()
        
        plt.savefig('reports/images/time_distribution.svg', bbox_inches='tight')
        plt.close()

    def create_source_distribution(self):
        """Create source distribution plot"""
        plt.figure(figsize=(10, 6))
        
        source_counts = self.df['source'].value_counts()
        plt.pie(source_counts.values, labels=source_counts.index,
               autopct='%1.1f%%', startangle=90)
        plt.title('Incidents by Source')
        
        plt.savefig('reports/images/source_distribution.svg', bbox_inches='tight')
        plt.close()

    def create_crime_type_distribution(self):
        """Create crime type distribution plot"""
        plt.figure(figsize=(12, 6))
        
        type_counts = self.df['crime_type'].value_counts().head(10)
        bars = plt.bar(range(len(type_counts)), type_counts.values)
        plt.xticks(range(len(type_counts)), type_counts.index, rotation=45, ha='right')
        
        plt.title('Top Crime Types')
        plt.xlabel('Type')
        plt.ylabel('Number of Incidents')
        
        # Add value labels on top of bars
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    f'{int(height)}',
                    ha='center', va='bottom')
        
        plt.tight_layout()
        plt.savefig('reports/images/crime_types.svg', bbox_inches='tight')
        plt.close()

    def create_hourly_heatmap(self):
        """Create hourly heatmap by source"""
        plt.figure(figsize=(15, 8))
        
        pivot_table = pd.crosstab(
            self.df['date'].dt.hour,
            self.df['source']
        )
        
        sns.heatmap(pivot_table, cmap='YlOrRd', annot=True, fmt='d')
        plt.title('Hourly Incidents by Source')
        plt.xlabel('Source')
        plt.ylabel('Hour of Day')
        
        plt.tight_layout()
        plt.savefig('reports/images/hourly_heatmap.svg', bbox_inches='tight')
        plt.close()

    def analyze_text(self, text):
        """Analyze text for key information"""
        if not isinstance(text, str):
            return {}

        patterns = {
            'violence': r'\b(kill|shot|stab|assault|attack|fight|threat|robbery|crime|murder)\w*\b',
            'weapons': r'\b(gun|knife|firearm|weapon|pistol|rifle)\b',
            'money': r'\$\s*\d+(?:,\d{3})*(?:\.\d{2})?'
        }
        
        results = {}
        for key, pattern in patterns.items():
            matches = re.findall(pattern, text.lower())
            if matches:
                results[key] = list(set(matches))
        
        return results

    def generate_report(self):
        """Generate a formatted analysis report with visualizations"""
        if len(self.df) == 0:
            return "No data available for analysis"

        # Create visualizations
        self.create_visualizations()
        
        report = []
        
        # Header
        report.append("# Crime Data Analysis Report")
        report.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        
        # Summary
        report.append("## Summary Statistics (Last 24 Hours)")
        report.append(f"- Total Incidents: {len(self.df)}")
        report.append(f"- Time Range: {self.df['date'].min().strftime('%Y-%m-%d %H:%M')} to {self.df['date'].max().strftime('%Y-%m-%d %H:%M')}")
        report.append(f"- Number of Sources: {self.df['source'].nunique()}\n")
        
        # Visualizations
        report.append("## Temporal Analysis")
        report.append("![Time Distribution](images/time_distribution.svg)")
        report.append("*Figure 1: Distribution of incidents throughout the day*\n")
        
        report.append("## Source Analysis")
        report.append("![Source Distribution](images/source_distribution.svg)")
        report.append("*Figure 2: Distribution of incidents by source*\n")
        
        report.append("## Crime Type Analysis")
        report.append("![Crime Types](images/crime_types.svg)")
        report.append("*Figure 3: Most common types of incidents*\n")
        
        report.append("## Time x Source Analysis")
        report.append("![Hourly Heatmap](images/hourly_heatmap.svg)")
        report.append("*Figure 4: Incident patterns by hour and source*\n")
        
        # Recent Incidents
        report.append("## Recent Major Incidents")
        for _, incident in self.df.head(5).iterrows():
            report.append(f"\n### {incident['crime_type']} at {incident['date'].strftime('%Y-%m-%d %H:%M')}")
            report.append(f"- Location: {incident['location']}")
            
            if incident['description']:
                report.append(f"- Description: {incident['description']}")
                
                # Add any extracted information
                analysis = self.analyze_text(incident['description'])
                if analysis.get('violence'):
                    report.append(f"- Violence Indicators: {', '.join(analysis['violence'])}")
                if analysis.get('weapons'):
                    report.append(f"- Weapons Mentioned: {', '.join(analysis['weapons'])}")
                if analysis.get('money'):
                    report.append(f"- Monetary Values: {', '.join(analysis['money'])}")
            
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
