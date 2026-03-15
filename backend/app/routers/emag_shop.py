"""eMAG Shop (店铺) CRUD router"""
import logging
from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.middleware.auth_middleware import require_auth
from app.models.emag_sync import EmagShop

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/emag-shops", tags=["emag-shops"])

# Platform base URLs mapping (same as emag_sync.py)
PLATFORM_URLS = {
    "ro": "https://marketplace-api.emag.ro/api-3",
    "bg": "https://marketplace-api.emag.bg/api-3",
    "hu": "https://marketplace-api.emag.hu/api-3",
    "fashiondays-ro": "https://marketplace-ro-api.fashiondays.com/api-3",
    "fashiondays-bg": "https://marketplace-bg-api.fashiondays.com/api-3",
}


# ---------- Request / Response ----------

class ShopCreateRequest(BaseModel):
    name: str                             # 店铺名称
    platform: str                         # ro / bg / hu / …
    api_username: Optional[str] = None    # API 用户名
    api_password: Optional[str] = None    # API 密码
    login_email: Optional[str] = None     # 后台登录邮箱
    login_password: Optional[str] = None  # 后台登录密码


class ShopUpdateRequest(BaseModel):
    name: Optional[str] = None
    platform: Optional[str] = None
    api_username: Optional[str] = None
    api_password: Optional[str] = None
    login_email: Optional[str] = None
    login_password: Optional[str] = None
    is_active: Optional[int] = None


class ShopResponse(BaseModel):
    id: int
    name: str
    platform: str
    api_username: Optional[str] = None
    api_base_url: Optional[str] = None
    login_email: Optional[str] = None
    is_active: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ---------- Endpoints ----------

@router.get("", response_model=List[ShopResponse])
async def list_shops(
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """获取所有店铺列表"""
    shops = db.query(EmagShop).order_by(EmagShop.id).all()
    return shops


@router.get("/{shop_id}", response_model=ShopResponse)
async def get_shop(
    shop_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """获取单个店铺详情"""
    shop = db.query(EmagShop).filter(EmagShop.id == shop_id).first()
    if not shop:
        raise HTTPException(status_code=404, detail="店铺不存在")
    return shop


@router.post("", response_model=ShopResponse, status_code=status.HTTP_201_CREATED)
async def create_shop(
    payload: ShopCreateRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """新建店铺"""
    if payload.platform not in PLATFORM_URLS:
        raise HTTPException(status_code=400, detail=f"无效平台，可选: {', '.join(PLATFORM_URLS.keys())}")

    existing = db.query(EmagShop).filter(EmagShop.name == payload.name).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"店铺名称 '{payload.name}' 已存在")

    shop = EmagShop(
        name=payload.name,
        platform=payload.platform,
        api_username=payload.api_username,
        api_password=payload.api_password,
        api_base_url=PLATFORM_URLS[payload.platform],
        login_email=payload.login_email,
        login_password=payload.login_password,
        is_active=1,
    )
    db.add(shop)
    db.commit()
    db.refresh(shop)
    logger.info(f"创建店铺: {shop.name} (id={shop.id})")
    return shop


@router.put("/{shop_id}", response_model=ShopResponse)
async def update_shop(
    shop_id: int,
    payload: ShopUpdateRequest,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """更新店铺"""
    shop = db.query(EmagShop).filter(EmagShop.id == shop_id).first()
    if not shop:
        raise HTTPException(status_code=404, detail="店铺不存在")

    if payload.name is not None:
        dup = db.query(EmagShop).filter(EmagShop.name == payload.name, EmagShop.id != shop_id).first()
        if dup:
            raise HTTPException(status_code=400, detail=f"店铺名称 '{payload.name}' 已存在")
        shop.name = payload.name

    if payload.platform is not None:
        if payload.platform not in PLATFORM_URLS:
            raise HTTPException(status_code=400, detail=f"无效平台，可选: {', '.join(PLATFORM_URLS.keys())}")
        shop.platform = payload.platform
        shop.api_base_url = PLATFORM_URLS[payload.platform]

    if payload.api_username is not None:
        shop.api_username = payload.api_username
    if payload.api_password is not None:
        shop.api_password = payload.api_password
    if payload.login_email is not None:
        shop.login_email = payload.login_email
    if payload.login_password is not None:
        shop.login_password = payload.login_password
    if payload.is_active is not None:
        shop.is_active = payload.is_active

    shop.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(shop)
    logger.info(f"更新店铺: {shop.name} (id={shop.id})")
    return shop


@router.delete("/{shop_id}")
async def delete_shop(
    shop_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """删除店铺"""
    shop = db.query(EmagShop).filter(EmagShop.id == shop_id).first()
    if not shop:
        raise HTTPException(status_code=404, detail="店铺不存在")

    db.delete(shop)
    db.commit()
    logger.info(f"删除店铺: {shop.name} (id={shop_id})")
    return {"success": True, "message": f"店铺 '{shop.name}' 已删除"}


@router.get("/{shop_id}/credentials")
async def get_shop_credentials(
    shop_id: int,
    current_user: dict = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """获取店铺的完整凭据（含密码，仅供内部调用）"""
    shop = db.query(EmagShop).filter(EmagShop.id == shop_id).first()
    if not shop:
        raise HTTPException(status_code=404, detail="店铺不存在")

    return {
        "id": shop.id,
        "name": shop.name,
        "platform": shop.platform,
        "api_username": shop.api_username,
        "api_password": shop.api_password,
        "api_base_url": shop.api_base_url,
        "login_email": shop.login_email,
        "login_password": shop.login_password,
    }

