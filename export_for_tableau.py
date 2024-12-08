# export_for_tableau.py
import pandas as pd
from datetime import datetime
from database import CrimeDatabase
import os
import logging
import asyncio

async def export_data():
    """Export data in formats suitable for Tableau"""
    try:
        db = CrimeDatabase()
        
        # Create export directory
        os.makedirs('tableau_data', exist_ok=True)
        
        logging.info("Getting database statistics...")
        stats = await db.get_statistics()
        logging.info(f"Found {stats['total_incidents']} incidents")
        
        # Get source status
        source_status = db.get_source_status()
        if not source_status.empty:
            source_status.to_csv('tableau_data/source_status.csv', index=False)
            logging.info("Exported source status data")
        
        # Create summary tables
        # Export source status
        source_status = db.get_source_status()
        if not source_status.empty:
            source_status.to_csv('tableau_data/source_status.csv', index=False)
            logging.info("Exported source status data")
        
        # Export statistics
        stats_df = pd.DataFrame([{
            'total_incidents': stats['total_incidents'],
            'min_date': stats['date_range']['min'],
            'max_date': stats['date_range']['max'],
            'export_time': datetime.now()
        }])
        stats_df.to_csv('tableau_data/statistics.csv', index=False)
        logging.info("Exported statistics data")
        
        # Export source counts
        source_counts = pd.DataFrame([
            {'source': source, 'count': count}
            for source, count in stats['by_source'].items()
        ])
        if not source_counts.empty:
            source_counts.to_csv('tableau_data/source_counts.csv', index=False)
            logging.info("Exported source counts data")
        
        logging.info("Data export completed successfully")
        
    except Exception as e:
        logging.error(f"Error exporting data: {str(e)}")
        raise

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(export_data())