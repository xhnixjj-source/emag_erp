"""Database connection and session management"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import config

engine = create_engine(
    config.DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in config.DATABASE_URL else {},
    echo=False  # 设置为True可以查看SQL语句，便于调试
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
from app.models.operation_log import OperationLog
from app.models.profit_config import ProfitConfig
# Import profit configuration models (must be imported before ProfitCalculation uses them)
from app.models.profit_config_models import (
    LogisticsPrice, VatConfig, ExchangeRate, GeniusRule, GeniusRuleStep,
    PackagingTemplate, CommissionConfig, FeeTemplate
)
from app.models.listing import ListingPool, ListingDetails, ProfitCalculation, ListingStatus
from app.models.emag_sync import EmagShop, EmagAccount, EmagProduct, EmagOrder, EmagReturn, EmagInboundShipment, EmagInboundShipmentDetail
from app.models.emag_ads import AdsCampaign, AdsAdset, AdsProductPerformance


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
