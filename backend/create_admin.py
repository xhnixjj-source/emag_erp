"""创建初始管理员用户"""
from app.database import SessionLocal, init_db
from app.services.auth_service import create_user
from app.models.user import UserRole, User

def create_admin():
    """创建管理员用户"""
    init_db()
    db = SessionLocal()
    try:
        # 检查是否已有用户
        existing_user = db.query(User).first()
        if existing_user:
            print("用户已存在，跳过创建")
            print(f"已有用户: {existing_user.username} (角色: {existing_user.role.value})")
            return
        
        # 创建管理员用户
        admin = create_user(
            db=db,
            username="admin",
            password="admin123",  # 请修改为安全密码
            role=UserRole.ADMIN
        )
        print("=" * 50)
        print("管理员用户创建成功！")
        print(f"用户名: {admin.username}")
        print(f"密码: admin123")
        print("警告: 请登录后立即修改密码！")
        print("=" * 50)
    except Exception as e:
        print(f"创建用户失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    create_admin()