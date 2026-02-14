# 服务器端 Git 连接问题解决方案

当服务器无法连接到 GitHub 时，可以使用以下方法解决。

## 问题现象

```
fatal: unable to access 'https://github.com/xhnixjj-source/emag_erp.git/': 
Failed to connect to github.com port 443 after 21074 ms: Could not connect to server
```

## 解决方案

### 方案 1：配置 Git HTTP 代理（推荐）

如果服务器有可用的 HTTP/HTTPS 代理，可以配置 Git 使用代理：

```bash
# 设置 HTTP 代理（替换为你的代理地址和端口）
git config --global http.proxy http://proxy.example.com:8080
git config --global https.proxy http://proxy.example.com:8080

# 如果代理需要认证
git config --global http.proxy http://username:password@proxy.example.com:8080
git config --global https.proxy http://username:password@proxy.example.com:8080

# 只对 GitHub 使用代理（推荐）
git config --global http.https://github.com.proxy http://proxy.example.com:8080
git config --global https.https://github.com.proxy http://proxy.example.com:8080

# 验证配置
git config --global --get http.proxy
git config --global --get https.proxy

# 测试连接
git pull origin main
```

**取消代理配置**（如果不需要代理时）：
```bash
git config --global --unset http.proxy
git config --global --unset https.proxy
```

### 方案 2：使用 SSH 方式（推荐，如果服务器有 SSH 密钥）

如果服务器配置了 SSH 密钥，可以改用 SSH 方式：

```bash
# 查看当前远程地址
git remote -v

# 将 HTTPS 地址改为 SSH 地址
git remote set-url origin git@github.com:xhnixjj-source/emag_erp.git

# 验证
git remote -v

# 测试连接
ssh -T git@github.com

# 拉取代码
git pull origin main
```

**如果没有 SSH 密钥，需要先配置**：
```bash
# 1. 生成 SSH 密钥（如果还没有）
ssh-keygen -t ed25519 -C "your_email@example.com"

# 2. 查看公钥
cat ~/.ssh/id_ed25519.pub

# 3. 将公钥添加到 GitHub：
#    - 登录 GitHub
#    - Settings -> SSH and GPG keys -> New SSH key
#    - 粘贴公钥内容

# 4. 测试连接
ssh -T git@github.com
```

### 方案 3：增加超时时间和重试次数

```bash
# 增加超时时间（秒）
git config --global http.lowSpeedLimit 0
git config --global http.lowSpeedTime 999999

# 或者只针对 GitHub
git config --global http.https://github.com.lowSpeedLimit 0
git config --global http.https://github.com.lowSpeedTime 999999
```

### 方案 4：使用镜像站点（如果在中国大陆）

如果服务器在中国大陆，可以使用 GitHub 镜像：

```bash
# 方法 1：使用 gitee 镜像（需要先在 Gitee 上导入仓库）
git remote set-url origin https://gitee.com/your-username/emag_erp.git

# 方法 2：使用 GitHub 镜像代理（如 ghproxy.com）
git config --global url."https://ghproxy.com/https://github.com/".insteadOf "https://github.com/"

# 或者只针对当前仓库
git config url."https://ghproxy.com/https://github.com/".insteadOf "https://github.com/"
```

### 方案 5：手动下载代码包（临时方案）

如果以上方法都不行，可以手动下载代码：

```bash
# 1. 在本地或能访问 GitHub 的机器上下载代码包
#    访问：https://github.com/xhnixjj-source/emag_erp/archive/refs/heads/main.zip

# 2. 上传到服务器并解压
cd /path/to/project
unzip emag_erp-main.zip
mv emag_erp-main/* .
mv emag_erp-main/.* . 2>/dev/null || true
rmdir emag_erp-main
rm emag_erp-main.zip

# 3. 重新初始化 git（如果需要）
git init
git remote add origin https://github.com/xhnixjj-source/emag_erp.git
git add .
git commit -m "Update from manual download"
```

### 方案 6：检查网络连接

```bash
# 测试是否能访问 GitHub
ping github.com

# 测试 HTTPS 连接
curl -I https://github.com

# 检查 DNS 解析
nslookup github.com

# 如果 DNS 有问题，可以尝试使用其他 DNS
# 编辑 /etc/resolv.conf 或使用 8.8.8.8
```

## 推荐配置流程

### 如果服务器有代理：

```bash
# 1. 配置代理（替换为实际代理地址）
git config --global http.https://github.com.proxy http://your-proxy:port
git config --global https.https://github.com.proxy http://your-proxy:port

# 2. 测试
git pull origin main
```

### 如果服务器有 SSH 密钥：

```bash
# 1. 切换到 SSH 方式
git remote set-url origin git@github.com:xhnixjj-source/emag_erp.git

# 2. 测试
git pull origin main
```

### 如果都没有：

```bash
# 使用镜像代理（ghproxy.com 是公开的 GitHub 代理）
git config --global url."https://ghproxy.com/https://github.com/".insteadOf "https://github.com/"

# 测试
git pull origin main
```

## 验证配置

```bash
# 查看所有 Git 配置
git config --global --list

# 查看远程仓库地址
git remote -v

# 测试连接
git ls-remote origin
```

## 常见问题

### Q: 配置代理后仍然无法连接？
A: 检查代理地址是否正确，代理服务是否正常运行，防火墙是否允许连接。

### Q: SSH 方式提示 "Permission denied"？
A: 检查 SSH 密钥是否正确添加到 GitHub，使用 `ssh -T git@github.com` 测试。

### Q: 使用镜像代理后速度很慢？
A: 可以尝试其他镜像服务，或者配置自己的代理服务器。

## 注意事项

1. **备份配置**：修改 Git 配置前，可以先备份：
   ```bash
   cp ~/.gitconfig ~/.gitconfig.backup
   ```

2. **仅当前仓库配置**：如果只想对当前仓库配置，去掉 `--global` 参数：
   ```bash
   git config http.proxy http://proxy.example.com:8080
   ```

3. **恢复默认配置**：
   ```bash
   git config --global --unset http.proxy
   git config --global --unset https.proxy
   git config --global --unset url."https://ghproxy.com/https://github.com/".insteadOf
   ```

