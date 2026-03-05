"""Profit configuration management API"""
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel
from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.profit_config_models import (
    LogisticsPrice,
    VatConfig,
    ExchangeRate,
    GeniusRule,
    GeniusRuleStep,
    PackagingTemplate,
    CommissionConfig,
    FeeTemplate
)

router = APIRouter(prefix="/api/profit-config", tags=["profit-config"])


# ========== Logistics Price ==========

class LogisticsPriceResponse(BaseModel):
    """Logistics price response model"""
    id: int
    transport_mode: str
    price_per_kg_rmb: float
    effective_from: datetime
    effective_to: Optional[datetime]
    remark: Optional[str]

    class Config:
        from_attributes = True


class LogisticsPriceRequest(BaseModel):
    """Logistics price request model"""
    transport_mode: str
    price_per_kg_rmb: float
    effective_from: Optional[datetime] = None
    effective_to: Optional[datetime] = None
    remark: Optional[str] = None


@router.get("/logistics", response_model=List[LogisticsPriceResponse])
async def get_logistics_prices(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get all logistics prices"""
    prices = db.query(LogisticsPrice).order_by(
        LogisticsPrice.transport_mode,
        LogisticsPrice.effective_from.desc()
    ).all()
    return prices


@router.post("/logistics", response_model=LogisticsPriceResponse)
async def create_logistics_price(
    request: LogisticsPriceRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create a new logistics price configuration"""
    # 关闭旧配置
    if request.effective_from:
        db.query(LogisticsPrice).filter(
            LogisticsPrice.transport_mode == request.transport_mode,
            LogisticsPrice.effective_to.is_(None)
        ).update({"effective_to": request.effective_from})
    
    price = LogisticsPrice(
        transport_mode=request.transport_mode,
        price_per_kg_rmb=request.price_per_kg_rmb,
        effective_from=request.effective_from or datetime.utcnow(),
        effective_to=request.effective_to,
        remark=request.remark
    )
    db.add(price)
    db.commit()
    db.refresh(price)
    return price


# ========== VAT Config ==========

class VatConfigResponse(BaseModel):
    """VAT config response model"""
    id: int
    site: str
    vat_rate: float
    effective_from: datetime
    effective_to: Optional[datetime]

    class Config:
        from_attributes = True


class VatConfigRequest(BaseModel):
    """VAT config request model"""
    site: str = "emag_ro"
    vat_rate: float
    effective_from: Optional[datetime] = None
    effective_to: Optional[datetime] = None


@router.get("/vat", response_model=List[VatConfigResponse])
async def get_vat_configs(
    site: Optional[str] = Query(None),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get VAT configurations"""
    query = db.query(VatConfig)
    if site:
        query = query.filter(VatConfig.site == site)
    configs = query.order_by(VatConfig.effective_from.desc()).all()
    return configs


@router.post("/vat", response_model=VatConfigResponse)
async def create_vat_config(
    request: VatConfigRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create a new VAT configuration"""
    # 关闭旧配置
    if request.effective_from:
        db.query(VatConfig).filter(
            VatConfig.site == request.site,
            VatConfig.effective_to.is_(None)
        ).update({"effective_to": request.effective_from})
    
    config = VatConfig(
        site=request.site,
        vat_rate=request.vat_rate,
        effective_from=request.effective_from or datetime.utcnow(),
        effective_to=request.effective_to
    )
    db.add(config)
    db.commit()
    db.refresh(config)
    return config


# ========== Exchange Rate ==========

class ExchangeRateResponse(BaseModel):
    """Exchange rate response model"""
    id: int
    from_currency: str
    to_currency: str
    rate: float
    source: str
    effective_from: datetime
    effective_to: Optional[datetime]

    class Config:
        from_attributes = True


class ExchangeRateRequest(BaseModel):
    """Exchange rate request model"""
    from_currency: str = "RON"
    to_currency: str = "CNY"
    rate: float
    source: str = "manual"
    effective_from: Optional[datetime] = None
    effective_to: Optional[datetime] = None


@router.get("/exchange-rate", response_model=List[ExchangeRateResponse])
async def get_exchange_rates(
    from_currency: Optional[str] = Query(None),
    to_currency: Optional[str] = Query(None),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get exchange rate configurations"""
    query = db.query(ExchangeRate)
    if from_currency:
        query = query.filter(ExchangeRate.from_currency == from_currency)
    if to_currency:
        query = query.filter(ExchangeRate.to_currency == to_currency)
    rates = query.order_by(ExchangeRate.effective_from.desc()).all()
    return rates


@router.post("/exchange-rate", response_model=ExchangeRateResponse)
async def create_exchange_rate(
    request: ExchangeRateRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create a new exchange rate configuration"""
    # 关闭旧配置
    if request.effective_from:
        db.query(ExchangeRate).filter(
            ExchangeRate.from_currency == request.from_currency,
            ExchangeRate.to_currency == request.to_currency,
            ExchangeRate.effective_to.is_(None)
        ).update({"effective_to": request.effective_from})
    
    rate = ExchangeRate(
        from_currency=request.from_currency,
        to_currency=request.to_currency,
        rate=request.rate,
        source=request.source,
        effective_from=request.effective_from or datetime.utcnow(),
        effective_to=request.effective_to
    )
    db.add(rate)
    db.commit()
    db.refresh(rate)
    return rate


# ========== Genius Rule ==========

class GeniusRuleStepRequest(BaseModel):
    """Genius rule step request model"""
    min_sales_amount: float
    max_sales_amount: Optional[float] = None
    fee_amount: float


class GeniusRuleRequest(BaseModel):
    """Genius rule request model"""
    rule_name: str
    currency: str = "RON"
    is_active: bool = True
    steps: List[GeniusRuleStepRequest]


class GeniusRuleStepResponse(BaseModel):
    """Genius rule step response model"""
    id: int
    rule_id: int
    min_sales_amount: float
    max_sales_amount: Optional[float]
    fee_amount: float

    class Config:
        from_attributes = True


class GeniusRuleResponse(BaseModel):
    """Genius rule response model"""
    id: int
    rule_name: str
    currency: str
    is_active: bool
    steps: List[GeniusRuleStepResponse] = []

    class Config:
        from_attributes = True


@router.get("/genius-rule", response_model=List[GeniusRuleResponse])
async def get_genius_rules(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get all genius rules"""
    rules = db.query(GeniusRule).all()
    result = []
    for rule in rules:
        steps = db.query(GeniusRuleStep).filter(
            GeniusRuleStep.rule_id == rule.id
        ).order_by(GeniusRuleStep.min_sales_amount).all()
        result.append({
            "id": rule.id,
            "rule_name": rule.rule_name,
            "currency": rule.currency,
            "is_active": rule.is_active,
            "steps": steps
        })
    return result


@router.post("/genius-rule", response_model=GeniusRuleResponse)
async def create_genius_rule(
    request: GeniusRuleRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create or update a genius rule"""
    # 如果存在同名规则，先停用旧规则
    existing = db.query(GeniusRule).filter(
        GeniusRule.rule_name == request.rule_name
    ).first()
    
    if existing:
        existing.is_active = False
        # 删除旧规则的步骤
        db.query(GeniusRuleStep).filter(
            GeniusRuleStep.rule_id == existing.id
        ).delete()
    
    # 创建新规则
    rule = GeniusRule(
        rule_name=request.rule_name,
        currency=request.currency,
        is_active=request.is_active
    )
    db.add(rule)
    db.flush()
    
    # 创建步骤
    for step_req in request.steps:
        step = GeniusRuleStep(
            rule_id=rule.id,
            min_sales_amount=step_req.min_sales_amount,
            max_sales_amount=step_req.max_sales_amount,
            fee_amount=step_req.fee_amount
        )
        db.add(step)
    
    db.commit()
    db.refresh(rule)
    
    # 返回完整规则（包含步骤）
    steps = db.query(GeniusRuleStep).filter(
        GeniusRuleStep.rule_id == rule.id
    ).order_by(GeniusRuleStep.min_sales_amount).all()
    
    return {
        "id": rule.id,
        "rule_name": rule.rule_name,
        "currency": rule.currency,
        "is_active": rule.is_active,
        "steps": steps
    }


# ========== Commission Config ==========

class CommissionConfigResponse(BaseModel):
    """Commission config response model"""
    id: int
    site: str
    category_or_group: str
    commission_rate: float
    effective_from: datetime
    effective_to: Optional[datetime]

    class Config:
        from_attributes = True


class CommissionConfigRequest(BaseModel):
    """Commission config request model"""
    site: str = "emag_ro"
    category_or_group: str
    commission_rate: float  # 小数格式，如 0.15 表示 15%
    effective_from: Optional[datetime] = None
    effective_to: Optional[datetime] = None


@router.get("/commission", response_model=List[CommissionConfigResponse])
async def get_commission_configs(
    site: Optional[str] = Query(None),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get commission configurations"""
    query = db.query(CommissionConfig)
    if site:
        query = query.filter(CommissionConfig.site == site)
    configs = query.order_by(CommissionConfig.effective_from.desc()).all()
    return configs


@router.post("/commission", response_model=CommissionConfigResponse)
async def create_commission_config(
    request: CommissionConfigRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create a new commission configuration"""
    # 关闭旧配置（同一类目）
    if request.effective_from:
        db.query(CommissionConfig).filter(
            CommissionConfig.site == request.site,
            CommissionConfig.category_or_group == request.category_or_group,
            CommissionConfig.effective_to.is_(None)
        ).update({"effective_to": request.effective_from})
    
    config = CommissionConfig(
        site=request.site,
        category_or_group=request.category_or_group,
        commission_rate=request.commission_rate,
        effective_from=request.effective_from or datetime.utcnow(),
        effective_to=request.effective_to
    )
    db.add(config)
    db.commit()
    db.refresh(config)
    return config


# ========== Packaging Template ==========

class PackagingTemplateResponse(BaseModel):
    """Packaging template response model"""
    id: int
    name: str
    cost_rmb: float
    apply_scope: Optional[str]
    is_default: bool

    class Config:
        from_attributes = True


class PackagingTemplateRequest(BaseModel):
    """Packaging template request model"""
    name: str
    cost_rmb: float
    apply_scope: Optional[str] = None
    is_default: bool = False


@router.get("/packaging", response_model=List[PackagingTemplateResponse])
async def get_packaging_templates(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get all packaging templates"""
    templates = db.query(PackagingTemplate).order_by(
        PackagingTemplate.is_default.desc(),
        PackagingTemplate.name
    ).all()
    return templates


@router.post("/packaging", response_model=PackagingTemplateResponse)
async def create_packaging_template(
    request: PackagingTemplateRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create a new packaging template"""
    # 如果设置为默认，取消其他默认模板
    if request.is_default:
        db.query(PackagingTemplate).filter(
            PackagingTemplate.is_default == True
        ).update({"is_default": False})
    
    template = PackagingTemplate(
        name=request.name,
        cost_rmb=request.cost_rmb,
        apply_scope=request.apply_scope,
        is_default=request.is_default
    )
    db.add(template)
    db.commit()
    db.refresh(template)
    return template


# ========== Fee Template ==========

class FeeTemplateResponse(BaseModel):
    """Fee template response model"""
    id: int
    template_name: str
    fee_type: str
    currency: str
    calculation_method: str
    base_amount: float
    rate: float
    unit: Optional[str]

    class Config:
        from_attributes = True


class FeeTemplateRequest(BaseModel):
    """Fee template request model"""
    template_name: str
    fee_type: str  # 'order_handling' / 'storage'
    currency: str  # 'CNY' / 'RON'
    calculation_method: str  # 'per_order', 'per_item', 'per_weight', 'fixed'
    base_amount: float = 0.0
    rate: float = 0.0
    unit: Optional[str] = None


@router.get("/fee-template", response_model=List[FeeTemplateResponse])
async def get_fee_templates(
    fee_type: Optional[str] = Query(None),
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Get fee templates"""
    query = db.query(FeeTemplate)
    if fee_type:
        query = query.filter(FeeTemplate.fee_type == fee_type)
    templates = query.order_by(FeeTemplate.template_name).all()
    return templates


@router.post("/fee-template", response_model=FeeTemplateResponse)
async def create_fee_template(
    request: FeeTemplateRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db)
):
    """Create a new fee template"""
    template = FeeTemplate(
        template_name=request.template_name,
        fee_type=request.fee_type,
        currency=request.currency,
        calculation_method=request.calculation_method,
        base_amount=request.base_amount,
        rate=request.rate,
        unit=request.unit
    )
    db.add(template)
    db.commit()
    db.refresh(template)
    return template

