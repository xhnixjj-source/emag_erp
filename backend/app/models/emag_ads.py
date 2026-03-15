"""eMAG Ads campaign / adset / product performance models"""
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Index, UniqueConstraint
from sqlalchemy.sql import func
from app.database import Base


class AdsCampaign(Base):
    """广告活动表"""
    __tablename__ = "ads_campaigns"

    id = Column(Integer, primary_key=True, index=True)
    campaign_id = Column(Integer, nullable=False, index=True)  # eMAG campaign ID
    marketplace = Column(String(10), nullable=False, default="ro", index=True)  # ro / bg / hu
    name = Column(String(500), nullable=True)
    status = Column(String(50), nullable=True)            # active / paused / ...
    budget = Column(Float, nullable=True)                 # 预算（如有）
    budget_type = Column(String(50), nullable=True)       # daily / total / ...
    synced_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("campaign_id", "marketplace", name="uq_campaign_marketplace"),
    )


class AdsAdset(Base):
    """广告组表"""
    __tablename__ = "ads_adsets"

    id = Column(Integer, primary_key=True, index=True)
    adset_id = Column(Integer, nullable=False, index=True)        # eMAG adset ID
    campaign_id = Column(Integer, nullable=False, index=True)     # 所属 campaign ID
    marketplace = Column(String(10), nullable=False, default="ro", index=True)  # ro / bg / hu
    name = Column(String(500), nullable=True)
    status = Column(String(50), nullable=True)
    bid = Column(Float, nullable=True)                            # 当前竞价
    synced_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("adset_id", "campaign_id", "marketplace", name="uq_adset_campaign_mp"),
        Index("idx_ads_adset_campaign_id", "campaign_id"),
    )


class AdsProductPerformance(Base):
    """广告产品表现数据表"""
    __tablename__ = "ads_product_performance"

    id = Column(Integer, primary_key=True, index=True)
    marketplace = Column(String(10), nullable=False, default="ro", index=True)  # ro / bg / hu
    campaign_id = Column(Integer, nullable=False, index=True)
    campaign_name = Column(String(500), nullable=True)
    adset_id = Column(Integer, nullable=False, index=True)
    adset_name = Column(String(500), nullable=True)
    product_id = Column(Integer, nullable=False, index=True)
    product_name = Column(String(500), nullable=True)
    part_number = Column(String(255), nullable=True)       # PNK (e.g. "NG06033")
    part_number_key = Column(String(255), nullable=True)   # Prd_Code (e.g. "DHXDJS3BM")
    date_start = Column(Date, nullable=False)
    date_end = Column(Date, nullable=False)

    # analytics 字段（来自 V4 PRD）
    clicks = Column(Integer, default=0)
    impressions = Column(Integer, default=0)
    ctr = Column(Float, default=0.0)
    actual_cpc = Column(Float, default=0.0)
    cost = Column(Float, default=0.0)
    sales = Column(Float, default=0.0)
    products_sold = Column(Integer, default=0)
    cps = Column(Float, default=0.0)
    cost_percentage = Column(Float, default=0.0)

    synced_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("product_id", "adset_id", "date_start", "date_end", "marketplace", name="uq_product_adset_date_mp"),
        Index("idx_ads_perf_campaign", "campaign_id"),
        Index("idx_ads_perf_adset", "adset_id"),
        Index("idx_ads_perf_product", "product_id"),
        Index("idx_ads_perf_date", "date_start", "date_end"),
        Index("idx_ads_perf_marketplace", "marketplace"),
    )

