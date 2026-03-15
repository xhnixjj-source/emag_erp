import json
import time
import os
from playwright.sync_api import sync_playwright

# --- 配置区 ---
AUTH_JSON = "emag_auth.json"
OUTPUT_DIR = "reception_details_finalized" # 修改文件夹名以区分
LOGIN_URL = "https://auth.emag.net/login?adk=d6YV59dE4AM1EFWV"

def ensure_dir():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"📁 已创建目录: {OUTPUT_DIR}")

def save_json_file(filename, data):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"📄 已保存已完成运单: {filename}")

def run_sync():
    ensure_dir()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(storage_state=AUTH_JSON) if os.path.exists(AUTH_JSON) else browser.new_context()
        page = context.new_page()

        try:
            page.goto(LOGIN_URL)
            if "dashboard" not in page.url:
                print(">>> 登录失效，请手动登录...")
                page.wait_for_url("**/dashboard**", timeout=0)
            
            context.storage_state(path=AUTH_JSON)
            print("✅ 登录验证成功")

            # --- 1. 获取所有运单列表 ---
            print("正在获取运单列表...")
            list_payload = {
                "sort_by": "id",
                "sort_order": "desc",
                "page": 1,
                "rows": 50  # 增加行数以确保能覆盖更多历史单据
            }
            
            list_js = f"""
            async () => {{
                try {{
                    const res = await fetch('https://marketplace.emag.ro/api-ui/fio/reception/list', {{
                        method: 'POST',
                        headers: {{ 
                            'content-type': 'application/json',
                            'x-requested-with': 'XMLHttpRequest' 
                        }},
                        body: JSON.stringify({json.dumps(list_payload)})
                    }});
                    return await res.json();
                }} catch (e) {{
                    return "ERROR_JS_" + e.message;
                }}
            }}
            """
            list_res = page.evaluate(list_js)

            # --- 2. 筛选状态为 finalized 的运单 ---
            if isinstance(list_res, dict) and "data" in list_res and "rows" in list_res["data"]:
                all_receptions = list_res["data"]["rows"]
                # 过滤逻辑
                finalized_list = [item for item in all_receptions if item.get("status") == "finalized"]
            else:
                print("❌ 无法获取列表数据。")
                return

            if not finalized_list:
                print("⚠ 当前列表页中没有状态为 'finalized' 的运单。")
                return

            print(f"检测到 {len(all_receptions)} 条总记录，其中 {len(finalized_list)} 条为 finalized 状态。")

            # --- 3. 遍历并获取详情 ---
            for item in finalized_list:
                rec_id = item.get("id")
                print(f"--> 正在抓取 finalized 单号: {rec_id}")
                
                detail_url = f"https://marketplace.emag.ro/api-ui/fio/get-transferred-to-storage-quantity/{rec_id}"
                
                get_detail_js = f"""
                async () => {{
                    try {{
                        const res = await fetch('{detail_url}', {{
                            method: 'GET',
                            headers: {{ 'x-requested-with': 'XMLHttpRequest' }}
                        }});
                        return await res.json();
                    }} catch (e) {{
                        return "ERROR_DETAIL_JS_" + e.message;
                    }}
                }}
                """
                
                detail_data = page.evaluate(get_detail_js)
                save_json_file(f"finalized_reception_{rec_id}.json", detail_data)
                
                # 礼貌延迟
                time.sleep(1)

            print(f"\n🎉 任务完成！共有 {len(finalized_list)} 个已完成运单同步到 {OUTPUT_DIR}。")

        except Exception as e:
            print(f"❌ 运行异常: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    run_sync()