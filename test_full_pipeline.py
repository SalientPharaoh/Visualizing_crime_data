#!/usr/bin/env python3
import asyncio
import pandas as pd
from datetime import datetime
from database import CrimeDatabase
import logging

logging.basicConfig(level=logging.INFO)

async def test_full_pipeline():
    """Test all database operations"""
    try:
        # Initialize database
        db = CrimeDatabase()
        logging.info("Database connection initialized")

        # 1. Test saving incidents
        test_data = pd.DataFrame([
            {
                'incident_id': 'TEST001',
                'date': datetime.now(),
                'description': 'Test incident 1',
                'location': 'Test Location 1',
                'crime_type': 'Test Crime',
                'source': 'test_source'
            },
            {
                'incident_id': 'TEST002',
                'date': datetime.now(),
                'description': 'Test incident 2',
                'location': 'Test Location 2',
                'crime_type': 'Test Crime',
                'source': 'test_source'
            }
        ])
        
        logging.info("Saving test incidents...")
        saved_count = db.save_incidents(test_data)
        logging.info(f"Saved {saved_count} incidents")

        # 2. Test updating source
        logging.info("Updating source information...")
        db.update_source('test_source', 'success', 2)
        
        # 3. Test getting source status
        logging.info("Getting source status...")
        source_status = db.get_source_status()
        logging.info(f"Source status: \n{source_status}")

        # 4. Test getting statistics
        logging.info("Getting database statistics...")
        stats = await db.get_statistics()
        logging.info(f"Database statistics: {stats}")

        # 5. Test updating existing incident
        update_data = pd.DataFrame([
            {
                'incident_id': 'TEST001',
                'date': datetime.now(),
                'description': 'Updated test incident 1',
                'location': 'Updated Location 1',
                'crime_type': 'Updated Crime',
                'source': 'test_source'
            }
        ])
        
        logging.info("Testing update of existing incident...")
        update_count = db.save_incidents(update_data)
        logging.info(f"Updated {update_count} incidents")

        # Final statistics
        final_stats = await db.get_statistics()
        logging.info(f"Final database statistics: {final_stats}")
        
        return True

    except Exception as e:
        logging.error(f"Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(test_full_pipeline())
