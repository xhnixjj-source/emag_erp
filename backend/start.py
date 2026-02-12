"""启动脚本 - 使用8888端口避免冲突"""
import uvicorn
import sys
import os

# 确保在backend目录下运行
if __name__ == "__main__":
    # 获取脚本所在目录（backend目录）
    base_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(base_dir)
    
    # 添加当前目录到Python路径
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)
    
    print(f"Starting server in: {base_dir}")
    print(f"Python path: {sys.path[0]}")
    print("Server will start at: http://0.0.0.0:8888")
    print("Accessible from: http://localhost:8888 or http://<your-ip>:8888")
    print("Press Ctrl+C to stop the server")
    print("-" * 50)
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",  # 改为 0.0.0.0 允许所有网络接口访问（包括 ZeroTier）
        port=8888,          # 使用8888端口
        reload=True,
        log_level="info"
    )

