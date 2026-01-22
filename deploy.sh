#!/bin/bash
cd /opt/emag_erp

echo ">>> 拉取最新代码..."
git pull origin main

echo ">>> 更新后端依赖..."
cd backend
source venv/bin/activate
pip install -r requirements.txt

echo ">>> 构建前端..."
cd ../frontend
npm install
npm run build

echo ">>> 重启服务..."
sudo systemctl restart emag-backend

echo ">>> 部署完成！"

