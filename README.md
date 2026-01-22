# EMAG ERP 选品上架管理系统

## 项目简介

这是一个基于 FastAPI + Vue 3 的 eMAG 选品上架管理系统，支持关键字搜索、产品筛选、监控、上架管理和利润测算等功能。

## 技术栈

### 后端
- FastAPI 0.104.1
- SQLAlchemy 2.0.23
- SQLite 数据库
- APScheduler 定时任务
- JWT 认证

### 前端
- Vue 3.3.0
- Element Plus 2.4.0
- Vue Router 4.2.0
- Axios 1.6.0
- Vite 5.0.0

## 快速开始

### 1. 环境要求

- Python 3.8+
- Node.js 16+
- npm 或 yarn

### 2. 后端设置

```bash
# 进入后端目录
cd backend

# 创建虚拟环境（如果还没有）
python -m venv venv

# 激活虚拟环境
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt
```

### 3. 创建初始管理员用户

在 `backend` 目录下创建 `create_admin.py`：

```python
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
            return
        
        # 创建管理员用户
        admin = create_user(
            db=db,
            username="admin",
            password="admin123",  # 请修改为安全密码
            role=UserRole.ADMIN
        )
        print(f"管理员用户创建成功！")
        print(f"用户名: {admin.username}")
        print(f"密码: admin123 (请登录后修改)")
    except Exception as e:
        print(f"创建用户失败: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    create_admin()
```

运行脚本：

```bash
python create_admin.py
```

### 4. 启动后端服务

**方式1：使用启动脚本（推荐）**

```bash
# Windows
start.bat

# 或使用Python脚本
python start.py
```

**方式2：使用命令行**

```bash
# 确保在backend目录下
cd backend

# 激活虚拟环境
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# 启动服务
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8888
```

后端服务将在 `http://127.0.0.1:8888` 启动

### 5. 前端设置

```bash
# 进入前端目录
cd frontend

# 安装依赖
npm install
```

### 6. 启动前端服务

```bash
# 在frontend目录下
npm run dev
```

前端服务将在 `http://localhost:3000` 启动

### 7. 访问系统

1. 打开浏览器访问：`http://localhost:3000`
2. 使用管理员账号登录：
   - 用户名：`admin`
   - 密码：`admin123`（如果使用上面的脚本创建）

## 项目结构

```
emag_erp/
├── backend/              # 后端服务
│   ├── app/
│   │   ├── main.py       # FastAPI应用入口
│   │   ├── config.py     # 配置文件
│   │   ├── database.py   # 数据库连接
│   │   ├── models/       # 数据模型
│   │   ├── routers/      # API路由
│   │   ├── services/     # 业务逻辑
│   │   ├── middleware/   # 中间件
│   │   └── utils/        # 工具函数
│   ├── requirements.txt  # Python依赖
│   ├── start.py          # 启动脚本
│   └── start.bat         # Windows启动脚本
├── frontend/             # 前端服务
│   ├── src/
│   │   ├── views/        # 页面组件
│   │   ├── api/          # API调用
│   │   ├── router/       # 路由配置
│   │   └── store/        # 状态管理
│   └── package.json      # Node依赖
└── emag_erp.db          # SQLite数据库（运行后自动创建）
```

## 功能模块

1. **关键字管理**：添加关键字、批量搜索、查看链接库
2. **筛选池**：产品筛选、条件设置、批量操作
3. **监控池**：产品监控、历史数据、定时任务
4. **上架管理**：产品上架、状态管理、锁定机制
5. **利润测算**：利润计算、费用设置
6. **操作日志**：日志查询、筛选、导出（仅管理员）

## 配置说明

### 环境变量（可选）

在 `backend` 目录下创建 `.env` 文件：

```env
# 数据库
DATABASE_URL=sqlite:///./emag_erp.db

# JWT密钥（生产环境必须修改）
SECRET_KEY=your-secret-key-change-in-production

# 代理配置
PROXY_ENABLED=false
PROXY_LIST=
PROXY_API_URL=
PROXY_API_KEY=
PROXY_API_USER_ID=
PROXY_API_IP_COUNT=10
PROXY_API_COUNTRY=ro
PROXY_API_IP_SI=5
PROXY_API_SB=1
PROXY_API_FETCH_INTERVAL=300

# 代理配置示例（动态住宅IP）
# 方式A：使用“旋转网关”代理（推荐，类似示例中的固定域名+端口）
# PROXY_ENABLED=true
# PROXY_LIST=ua-isp-pr.lunaproxy.net:16000
# 说明：系统会自动在请求中设置 http/https 代理；动态IP由代理服务商轮换
#
# 方式B：使用API拉取IP列表（返回多行 ip:port）
# PROXY_ENABLED=true
# PROXY_API_URL=https://tq.lunaproxy.com/get_dynamic_ip
# PROXY_API_USER_ID=你的用户ID
# PROXY_API_IP_COUNT=10
# PROXY_API_COUNTRY=ro

# 爬虫配置
CRAWLER_DELAY_MIN=1
CRAWLER_DELAY_MAX=3
CRAWLER_TIMEOUT=30

# 线程池配置
MAX_WORKER_THREADS=50
KEYWORD_SEARCH_THREADS=20
PRODUCT_CRAWL_THREADS=50
MONITOR_THREADS=30

# 重试配置
MAX_RETRY_COUNT=5
RETRY_BACKOFF_BASE=2
RETRY_BACKOFF_MAX=60

# 定时任务配置
SCHEDULER_TIMEZONE=Asia/Shanghai
MONITOR_SCHEDULE_HOUR=2
MONITOR_SCHEDULE_MINUTE=0
```

## 常见问题

### 1. 端口被占用

如果遇到端口占用错误，可以：

- 修改 `backend/start.py` 中的端口号（默认8888）
- 修改 `frontend/vite.config.js` 中的代理目标端口

### 2. 模块导入错误

确保：
- 在 `backend` 目录下运行启动命令
- 已激活虚拟环境
- 已安装所有依赖

### 3. 数据库错误

删除 `emag_erp.db` 文件，重新运行服务会自动创建数据库。

### 4. 前端无法连接后端

检查：
- 后端服务是否正常启动
- `vite.config.js` 中的代理配置是否正确
- 端口号是否匹配

## 开发说明

### API文档

后端启动后，可以访问：
- Swagger UI: `http://127.0.0.1:8888/docs`
- ReDoc: `http://127.0.0.1:8888/redoc`

### 数据库迁移

数据库表会在首次启动时自动创建。如需重置数据库，删除 `emag_erp.db` 文件即可。

## 注意事项

1. 首次运行会自动创建数据库和表结构
2. 默认使用SQLite数据库，数据文件在项目根目录
3. 生产环境请修改 `SECRET_KEY` 和数据库配置
4. 代理配置根据实际情况设置
5. 定时任务默认每天凌晨2点执行监控任务

## 许可证

MIT License

