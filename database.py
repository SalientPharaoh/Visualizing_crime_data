# database.py
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table, Index, func
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import and_

# Initialize SQLAlchemy
Base = declarative_base()

class Incident(Base):
    __tablename__ = 'incidents'
    
    id = Column(Integer, primary_key=True)
    incident_id = Column(String, unique=True, nullable=False)
    date = Column(DateTime, nullable=False)
    description = Column(String)
    location = Column(String)
    crime_type = Column(String)
    source = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_incidents_date', date),
        Index('ix_incidents_source', source),
        Index('ix_incidents_crime_type', crime_type),
    )

class Source(Base):
    __tablename__ = 'sources'
    
    id = Column(Integer, primary_key=True)
    source = Column(String, unique=True, nullable=False)
    last_fetch = Column(DateTime)
    status = Column(String)
    records_count = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_sources_source', source),
    )

class CrimeDatabase:
    def __init__(self):
        """Initialize database connection"""
        load_dotenv()
        
        # Get database URL from environment variable
        database_url = os.getenv('DATABASE_URI')
        if not database_url:
            raise ValueError("DATABASE_URI environment variable not set")
            
        # Convert postgres:// to postgresql://
        if database_url.startswith('postgres://'):
            database_url = database_url.replace('postgres://', 'postgresql://', 1)
            
        self.engine = create_engine(database_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        
        logging.info("PostgreSQL connection initialized")

    def save_incidents(self, df: pd.DataFrame) -> int:
        """Save incidents to database"""
        if df.empty:
            return 0
            
        saved_count = 0
        session = self.Session()
        
        try:
            # Convert dates to datetime if they're strings
            if 'date' in df.columns and df['date'].dtype == 'object':
                df['date'] = pd.to_datetime(df['date'])
            
            # Process each row
            for _, row in df.iterrows():
                try:
                    # Convert row to dict and handle NaN values
                    incident_data = row.where(pd.notnull(row), None).to_dict()
                    
                    # Create upsert statement
                    stmt = insert(Incident).values(
                        incident_id=str(incident_data['incident_id']),
                        date=incident_data['date'],
                        description=str(incident_data.get('description', '')),
                        location=str(incident_data.get('location', '')),
                        crime_type=str(incident_data.get('crime_type', '')),
                        source=str(incident_data['source'])
                    )
                    
                    # Add ON CONFLICT DO UPDATE clause
                    stmt = stmt.on_conflict_do_update(
                        constraint='incidents_incident_id_key',
                        set_=dict(
                            date=incident_data['date'],
                            description=str(incident_data.get('description', '')),
                            location=str(incident_data.get('location', '')),
                            crime_type=str(incident_data.get('crime_type', '')),
                            source=str(incident_data['source']),
                            updated_at=datetime.utcnow()
                        )
                    )
                    
                    session.execute(stmt)
                    saved_count += 1
                    
                except Exception as e:
                    logging.error(f"Error saving incident: {str(e)}")
                    logging.error(f"Problematic row: {row}")
                    
            session.commit()
            
        except Exception as e:
            logging.error(f"Database error: {str(e)}")
            session.rollback()
            raise
            
        finally:
            session.close()
            
        return saved_count

    def update_source(self, source: str, status: str, records_count: int):
        """Update source tracking information"""
        session = self.Session()
        
        try:
            stmt = insert(Source).values(
                source=source,
                last_fetch=datetime.utcnow(),
                status=status,
                records_count=records_count
            )
            
            stmt = stmt.on_conflict_do_update(
                constraint='sources_source_key',
                set_=dict(
                    last_fetch=datetime.utcnow(),
                    status=status,
                    records_count=records_count,
                    updated_at=datetime.utcnow()
                )
            )
            
            session.execute(stmt)
            session.commit()
            
        except Exception as e:
            logging.error(f"Error updating source {source}: {str(e)}")
            session.rollback()
            raise
            
        finally:
            session.close()

    def get_source_status(self) -> pd.DataFrame:
        """Get status of all sources"""
        session = self.Session()
        
        try:
            sources = session.query(Source).all()
            if not sources:
                return pd.DataFrame()
                
            data = [{
                'source': s.source,
                'last_fetch': s.last_fetch,
                'status': s.status,
                'records_count': s.records_count,
                'created_at': s.created_at,
                'updated_at': s.updated_at
            } for s in sources]
            
            return pd.DataFrame(data)
            
        except Exception as e:
            logging.error(f"Error getting source status: {str(e)}")
            return pd.DataFrame()
            
        finally:
            session.close()

    async def get_statistics(self):
        """Get database statistics"""
        stats = {}
        session = self.Session()
        
        try:
            # Total incidents
            stats['total_incidents'] = session.query(Incident).count()
            
            # Incidents by source
            source_counts = session.query(
                Incident.source,
                func.count(Incident.id).label('count')
            ).group_by(Incident.source).all()
            
            stats['by_source'] = {
                source: count for source, count in source_counts
            }
            
            # Date range
            date_range = session.query(
                func.min(Incident.date),
                func.max(Incident.date)
            ).first()
            
            stats['date_range'] = {
                'min': date_range[0],
                'max': date_range[1]
            }
            
        except Exception as e:
            logging.error(f"Error getting statistics: {str(e)}")
            
        finally:
            session.close()
            
        return stats