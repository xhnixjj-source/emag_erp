import requests
from curl_cffi import requests as crequests

def start_process():
    # 豌豆代理 API 地址
    api_url = "https://api.wandouapp.com"
    
    # 按照文档和报错提示，明确指定所有参数
    params = {
        "app_key": "539e6e384386e579a57140f0e5f72fe8", # 如果还报错，请尝试改为 "AppKey"
        "num": "1",
        "xy": "1",    # 1: http, 3: socks
        "type": "2",  # 2: json
        "nr": "99"    # 去重
    }

    try:
        print("正在从豌豆代理获取 IP...")
        # 使用 params 传参，由 requests 自动处理 URL 编码和拼接
        response = requests.get(api_url, params=params, timeout=10)
        res_json = response.json()
        
        # 调试：打印完整的响应内容，查看报错具体原因
        # print(f"API 原始响应: {res_json}")

        if res_json.get("code") == 200:
            # 豌豆代理 JSON 返回 data 是列表，取第一个
            data_list = res_json.get("data", [])
            if not data_list:
                print("❌ 提取成功但没有数据，请检查套餐余额")
                return

            ip_item = data_list[0]
            proxy_addr = f"{ip_item['ip']}:{ip_item['port']}"
            print(f"✅ 成功提取 IP: {proxy_addr} (城市: {ip_item['city']})")
            
            # 使用提取到的 IP 请求 eMAG
            request_emag(proxy_addr)
        else:
            print(f"❌ API 返回错误: {res_json.get('msg')}")
            
    except Exception as e:
        print(f"❌ 过程发生异常: {e}")

def request_emag(proxy):
    proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        print(f"正在通过代理 {proxy} 访问 emag.ro...")
        resp = crequests.get(
            "https://www.emag.ro",
            proxies=proxies,
            impersonate="chrome120",
            timeout=20,
            verify=False
        )
        print(f"🎯 访问状态码: {resp.status_code}")
    except Exception as e:
        print(f"❌ 请求 emag 失败: {e}")

if __name__ == "__main__":
    start_process()
