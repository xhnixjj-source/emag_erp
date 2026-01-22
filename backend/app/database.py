"""Database connection and session management"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import config

engine = create_engine(
    config.DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in config.DATABASE_URL else {}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Import all models to ensure they are registered with Base.metadata
# This is required for SQLAlchemy to create all tables and foreign key relationships
from app.models.user import User, UserRole, UserStatus
from app.models.keyword import Keyword, KeywordLink, KeywordStatus
from app.models.crawl_task import CrawlTask, ErrorLog, TaskType, TaskStatus, TaskPriority, ErrorType
from app.models.product import FilterPool
from app.models.monitor_pool import MonitorPool, MonitorHistory, MonitorStatus
from app.models.listing import ListingPool, ListingDetails, ProfitCalculation, ListingStatus
from app.models.operation_log import OperationLog


def get_db():
    """Dependency for getting database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)
