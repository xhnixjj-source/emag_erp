# Proxifier 配置指南 - 强制 Playwright 流量走 VPN

## 背景

当使用 Playwright 设置代理时，Chromium 会直接连接代理服务器，绕过系统的 TUN 虚拟网卡（VPN），
导致 lunaproxy 看到的是中国 IP 而非海外 IP，从而被拒绝连接。

Proxifier 可以强制指定程序的所有流量走代理，包括 Playwright 的代理连接请求。

## 安装 Proxifier

1. 下载 Proxifier：https://www.proxifier.com/download/
2. 安装并启动 Proxifier

## 配置步骤

### 步骤 1: 添加代理服务器

1. 打开 Proxifier，点击菜单 **Profile** → **Proxy Servers...**
2. 点击 **Add** 添加新代理
3. 配置如下：
   - **Address**: `127.0.0.1`
   - **Port**: `7890` (或您的 VPN/Clash 本地代理端口)
   - **Protocol**: 选择 `HTTP` 或 `SOCKS5`（根据您的 VPN 配置）
4. 点击 **Check** 验证代理是否工作
5. 点击 **OK** 保存

### 步骤 2: 创建代理规则

1. 点击菜单 **Profile** → **Proxification Rules...**
2. 点击 **Add** 添加新规则
3. 配置规则如下：

**规则 1: Chrome/Chromium 浏览器（Playwright 使用）**
```
Name: Playwright Browsers
Applications: chrome.exe; chromium.exe; msedge.exe
Target hosts: Any
Target ports: Any
Action: Proxy 127.0.0.1:7890
```

**规则 2: Python 进程**
```
Name: Python
Applications: python.exe; python3.exe; pythonw.exe
Target hosts: Any
Target ports: Any  
Action: Proxy 127.0.0.1:7890
```

### 步骤 3: 配置规则顺序

确保规则顺序如下（从上到下）：
1. **localhost** - Action: Direct (默认规则，保持不变)
2. **Playwright Browsers** - Action: Proxy 127.0.0.1:7890
3. **Python** - Action: Proxy 127.0.0.1:7890
4. **Default** - Action: Direct (默认规则)

### 步骤 4: 保存并启用

1. 点击 **OK** 保存所有规则
2. 确保 Proxifier 状态栏显示为 **Running**

## 验证配置

### 方法 1: Proxifier 日志

1. 点击菜单 **View** → **Log** 查看日志
2. 运行 Playwright 测试脚本
3. 观察日志中 `chrome.exe` 或 `chromium.exe` 的连接是否通过代理

### 方法 2: 运行测试脚本

```bash
cd backend
python scripts/test_api_proxy_to_emag.py
```

如果配置正确，测试脚本应该能够：
1. 成功从 lunaproxy API 获取 IP 列表
2. 使用获取的 IP 作为代理访问 emag.ro

## 常见问题

### Q: Proxifier 显示 "DNS Resolve Error"
A: 在代理服务器设置中，启用 **Resolve hostnames through this proxy**

### Q: 某些程序无法正常工作
A: 为该程序创建单独的规则，Action 选择 **Direct**

### Q: VPN 端口不是 7890
A: 请根据您的 VPN 客户端（Clash、V2Ray 等）配置的本地代理端口修改

## Proxifier 配置文件导出

配置完成后，建议导出配置文件备份：
1. 点击菜单 **File** → **Export Profile...**
2. 保存为 `.ppx` 文件

## 流量路径说明

配置完成后的流量路径：

```
Playwright → 设置 lunaproxy IP 为代理 → Proxifier 拦截
                                              ↓
                                       强制走 VPN (127.0.0.1:7890)
                                              ↓
                                       VPN 出口（海外 IP）
                                              ↓
                                       lunaproxy（看到海外 IP，通过）
                                              ↓
                                       emag.ro（成功访问）
```

