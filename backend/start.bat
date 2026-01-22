@echo off
echo Starting EMAG ERP Backend Server...
echo.

REM 切换到backend目录
cd /d %~dp0

REM 激活虚拟环境（如果存在）
if exist venv\Scripts\activate.bat (
    call venv\Scripts\activate.bat
)

REM 启动服务 - 使用 0.0.0.0 允许局域网访问
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8888

pause

