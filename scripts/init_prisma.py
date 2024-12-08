#!/usr/bin/env python3
import os
import sys
import subprocess
from dotenv import load_dotenv

def init_prisma():
    """Initialize Prisma client and database"""
    try:
        # Install prisma CLI
        subprocess.run(['pip', 'install', 'prisma'], check=True)
        
        # Set DATABASE_URL from DATABASE_URI for Prisma
        database_uri = os.getenv('DATABASE_URI')
        if not database_uri:
            print("ERROR: DATABASE_URI environment variable not set")
            sys.exit(1)
            
        os.environ['DATABASE_URL'] = database_uri
        
        # Generate Prisma client
        subprocess.run(['prisma', 'generate'], check=True)
        
        print("Prisma client generated successfully!")
        
    except subprocess.CalledProcessError as e:
        print(f"Error initializing Prisma: {str(e)}")
        sys.exit(1)

def push_schema():
    """Push schema to database"""
    try:
        subprocess.run(['prisma', 'db', 'push'], check=True)
        print("Schema pushed to database successfully!")
        
    except subprocess.CalledProcessError as e:
        print(f"Error pushing schema: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    load_dotenv()
    init_prisma()
    push_schema()
