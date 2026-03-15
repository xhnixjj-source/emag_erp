"""初始化 eMAG API 账号配置"""
from app.database import SessionLocal, init_db
from app.models.emag_sync import EmagAccount

# Platform base URLs mapping
PLATFORM_URLS = {
    "ro": "https://marketplace-api.emag.ro/api-3",
    "bg": "https://marketplace-api.emag.bg/api-3",
    "hu": "https://marketplace-api.emag.hu/api-3",
    "fashiondays-ro": "https://marketplace-ro-api.fashiondays.com/api-3",
    "fashiondays-bg": "https://marketplace-bg-api.fashiondays.com/api-3",
}

def init_emag_account():
    """初始化 eMAG API 账号配置"""
    init_db()
    db = SessionLocal()
    try:
        # 账号信息
        platform = "ro"  # 默认使用 eMAG Romania，如需其他平台可修改
        username = "sea403464507@gmail.com"
        password = "g6jYDh0"
        base_url = PLATFORM_URLS[platform]
        
        # 检查是否已存在该平台的账号
        existing_account = db.query(EmagAccount).filter(
            EmagAccount.platform == platform
        ).first()
        
        if existing_account:
            # 更新现有账号
            existing_account.username = username
            existing_account.password = password
            existing_account.base_url = base_url
            existing_account.is_active = 1
            db.commit()
            print("=" * 50)
            print(f"eMAG API 账号已更新！")
            print(f"平台: {platform}")
            print(f"用户名: {username}")
            print(f"API URL: {base_url}")
            print("=" * 50)
        else:
            # 创建新账号
            account = EmagAccount(
                platform=platform,
                username=username,
                password=password,
                base_url=base_url,
                is_active=1
            )
            db.add(account)
            db.commit()
            print("=" * 50)
            print(f"eMAG API 账号初始化成功！")
            print(f"平台: {platform}")
            print(f"用户名: {username}")
            print(f"API URL: {base_url}")
            print("=" * 50)
    except Exception as e:
        print(f"初始化失败: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    init_emag_account()