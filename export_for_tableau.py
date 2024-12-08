# export_for_tableau.py
import sqlite3
import pandas as pd
from datetime import datetime

def export_data():
    """Export data in formats suitable for Tableau"""
    conn = sqlite3.connect('crime_data.db')
    
    # Main incidents data
    incidents_df = pd.read_sql_query("""
        SELECT 
            incident_id,
            date,
            description,
            location,
            crime_type,
            source,
            strftime('%Y', date) as year,
            strftime('%m', date) as month,
            strftime('%d', date) as day,
            strftime('%H', date) as hour
        FROM incidents
    """, conn)
    
    # Create summary tables
    crime_by_type = pd.read_sql_query("""
        SELECT 
            crime_type,
            source,
            COUNT(*) as incident_count,
            strftime('%Y-%m', date) as month_year
        FROM incidents
        GROUP BY crime_type, source, month_year
    """, conn)
    
    hourly_patterns = pd.read_sql_query("""
        SELECT 
            strftime('%H', date) as hour,
            crime_type,
            COUNT(*) as incident_count
        FROM incidents
        GROUP BY hour, crime_type
    """, conn)
    
    # Export to CSV
    incidents_df.to_csv('tableau_data/incidents_full.csv', index=False)
    crime_by_type.to_csv('tableau_data/crime_by_type.csv', index=False)
    hourly_patterns.to_csv('tableau_data/hourly_patterns.csv', index=False)
    
    print("Data exported successfully!")
    print("\nFor Tableau, you can create visualizations like:")
    print("1. Crime Heatmap by Hour and Type")
    print("2. Crime Trends Over Time")
    print("3. Source Distribution")
    print("4. Crime Type Distribution")
    print("5. Temporal Patterns")

if __name__ == "__main__":
    # Create export directory
    import os
    os.makedirs('tableau_data', exist_ok=True)
    
    export_data()