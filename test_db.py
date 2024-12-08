#!/usr/bin/env python3
import asyncio
from database import CrimeDatabase
import logging

logging.basicConfig(level=logging.INFO)

async def test_connection():
    """Test database connection and basic operations"""
    try:
        # Initialize database
        db = CrimeDatabase()
        logging.info("Database connection successful!")
        
        # Get statistics
        stats = await db.get_statistics()
        logging.info(f"Database statistics: {stats}")
        
        return True
        
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(test_connection())
