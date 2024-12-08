# check_db.py
import pandas as pd
import logging
from database import CrimeDatabase
from datetime import datetime

def check_database():
    print("Checking MongoDB database...")
    
    try:
        # Connect to database
        db = CrimeDatabase()
        
        # Check collections
        collections = db.db.list_collection_names()
        print("\nCollections found:", collections)
        
        # Check incidents collection
        if 'incidents' in collections:
            # Get document count
            count = db.incidents.count_documents({})
            print(f"\nTotal documents: {count}")
            
            # Get date range
            date_range = db.incidents.aggregate([
                {
                    '$group': {
                        '_id': None,
                        'min_date': {'$min': '$date'},
                        'max_date': {'$max': '$date'}
                    }
                }
            ])
            date_range = list(date_range)
            if date_range:
                print(f"Date range: {date_range[0]['min_date']} to {date_range[0]['max_date']}")
            
            # Get sample data
            print("\nSample data:")
            cursor = db.incidents.find().limit(5)
            df = pd.DataFrame(list(cursor))
            if not df.empty:
                print(df[['incident_id', 'date', 'crime_type', 'source']])
            
            # Get source distribution
            print("\nIncidents by source:")
            pipeline = [
                {'$group': {'_id': '$source', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}}
            ]
            source_dist = list(db.incidents.aggregate(pipeline))
            for doc in source_dist:
                print(f"{doc['_id']}: {doc['count']} incidents")
                
        else:
            print("No incidents collection found!")
            
    except Exception as e:
        print(f"Error checking database: {str(e)}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    check_database()