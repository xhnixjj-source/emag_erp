#!/usr/bin/env python3
"""Comprehensive Round 3 failure analysis with correct field names."""
import json
from collections import Counter, defaultdict

log_path = r"d:\emag_erp\.cursor\debug.log"

entries = []
with open(log_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            pass

entries.sort(key=lambda e: e.get("timestamp", 0))

# 1. Proper task failure breakdown
task_failed = [e for e in entries if e.get("location") == "crawler.py:task_failed"]
print(f"=== TASK FAILURES: {len(task_failed)} ===")
fail_cats = Counter()
for tf in task_failed:
    d = tf.get("data", {})
    err = d.get("error_message", "")
    if "ERR_EMPTY_RESPONSE" in err:
        fail_cats["ERR_EMPTY_RESPONSE"] += 1
    elif "ERR_CONNECTION_RESET" in err:
        fail_cats["ERR_CONNECTION_RESET"] += 1
    elif "ERR_CONNECTION_CLOSED" in err:
        fail_cats["ERR_CONNECTION_CLOSED"] += 1
    elif "Timeout 30000" in err:
        fail_cats["Timeout_30s"] += 1
    elif "Timeout 20000" in err:
        fail_cats["Timeout_20s"] += 1
    elif "Timeout 10000" in err:
        fail_cats["Timeout_10s"] += 1
    elif "Timeout" in err:
        fail_cats[f"Timeout_other"] += 1
    elif "店铺介绍页URL" in err or "\u5e97\u94fa\u4ecb\u7ecd\u9875" in err:
        fail_cats["Shop_URL_missing"] += 1
    elif "Captcha" in err or "captcha" in err:
        fail_cats["Captcha"] += 1
    elif "ECONNREFUSED" in err:
        fail_cats["ECONNREFUSED"] += 1
    elif "BitBrowser" in err or "\u7a97\u53e3" in err:
        fail_cats["BitBrowser"] += 1
    else:
        fail_cats[f"OTHER({err[:60]})"] += 1

for cat, cnt in fail_cats.most_common():
    print(f"  {cat}: {cnt}")

# 2. Check actual timeout_ms values used in different stages
print(f"\n=== TIMEOUT VALUES USED ===")
# Product page
product_goto = [e for e in entries if e.get("location") == "product_data_crawler.py:before_page_goto"]
product_timeouts = Counter(e.get("data", {}).get("timeout_ms", "?") for e in product_goto)
print(f"  Product page goto timeout_ms: {dict(product_timeouts)}")

# Category page
cat_goto = [e for e in entries if e.get("location") == "dynamic_data_extractor.py:before_category_page_goto"]
cat_timeouts = Counter(e.get("data", {}).get("timeout_ms", "?") for e in cat_goto)
print(f"  Category page goto timeout_ms: {dict(cat_timeouts)}")

# Store page
store_goto = [e for e in entries if e.get("location") == "dynamic_data_extractor.py:before_store_page_goto"]
store_timeouts = Counter(e.get("data", {}).get("timeout_ms", "?") for e in store_goto)
print(f"  Store page goto timeout_ms: {dict(store_timeouts)}")

# 3. Where do 10000ms timeouts come from?
print(f"\n=== 10000ms TIMEOUT ERRORS ===")
t10k = [e for e in entries if "10000" in str(e.get("data", {}).get("error_message", "")) or "10000" in str(e.get("data", {}).get("error", ""))]
for t in t10k[:5]:
    print(f"  loc={t.get('location')}, data={json.dumps(t.get('data', {}), ensure_ascii=False)[:200]}")

# 4. Rankings failure flow per task  
print(f"\n=== RANKINGS FAILURE BREAKDOWN ===")
rank_fail = [e for e in entries if "extract_rankings_failed" in e.get("location", "")]
rank_err_cats = Counter()
for rf in rank_fail:
    d = rf.get("data", {})
    err = d.get("error", "")
    if "ERR_EMPTY_RESPONSE" in err:
        rank_err_cats["ERR_EMPTY_RESPONSE"] += 1
    elif "Timeout 30000" in err:
        rank_err_cats["Timeout_30s"] += 1
    elif "Timeout 20000" in err:
        rank_err_cats["Timeout_20s"] += 1
    elif "Timeout 10000" in err:
        rank_err_cats["Timeout_10s"] += 1
    elif "Timeout" in err:
        rank_err_cats[f"Timeout_other({err[:50]})"] += 1
    elif "店铺介绍页" in err or "\u5e97\u94fa\u4ecb\u7ecd\u9875" in err:
        rank_err_cats["Shop_URL_missing"] += 1
    elif "Captcha" in err or "captcha" in err:
        rank_err_cats["Captcha"] += 1
    elif "Target page" in err:
        rank_err_cats["Target_closed"] += 1
    else:
        rank_err_cats[f"OTHER({err[:60]})"] += 1

for cat, cnt in rank_err_cats.most_common():
    print(f"  {cat}: {cnt}")

# 5. Overall success rate by unique task_id
print(f"\n=== OVERALL SUCCESS RATE (by unique task_id) ===")
failed_tasks = set()
for tf in task_failed:
    tid = tf.get("data", {}).get("task_id")
    if tid:
        failed_tasks.add(tid)

# Estimate total unique tasks from acquire_window logs
acquire_logs = [e for e in entries if "acquire_exclusive_window" in e.get("location", "")]
all_task_ids = set()
for al in acquire_logs:
    tid = al.get("data", {}).get("task_id")
    if tid:
        all_task_ids.add(tid)

# Also from task_failed
for tf in task_failed:
    tid = tf.get("data", {}).get("task_id")
    if tid:
        all_task_ids.add(tid)

# From dynamic_data_extracted
success_logs = [e for e in entries if e.get("location") == "product_data_crawler.py:dynamic_data_extracted"]
success_tasks = set()
for sl in success_logs:
    tid = sl.get("data", {}).get("task_id")
    if tid:
        success_tasks.add(tid)

print(f"  Total unique task_ids from window acquire: {len(all_task_ids)}")
print(f"  Unique tasks with dynamic_data extracted: {len(success_tasks)}")
print(f"  Unique tasks failed: {len(failed_tasks)}")
overlap = success_tasks & failed_tasks
print(f"  Tasks both succeeded and failed (retried): {len(overlap)}")
pure_success = success_tasks - failed_tasks
pure_fail = failed_tasks - success_tasks
print(f"  Pure successes (never failed): {len(pure_success)}")
print(f"  Pure failures (never succeeded): {len(pure_fail)}")
total_unique = len(pure_success) + len(pure_fail) + len(overlap)
final_success = len(pure_success) + len(overlap)
print(f"  Final success rate: {final_success}/{total_unique} = {final_success/total_unique*100:.1f}%" if total_unique > 0 else "  N/A")

# 6. Category page goto: same URL repeated failures
print(f"\n=== CATEGORY PAGES WITH REPEATED FAILURES ===")
cat_errors = [e for e in entries if e.get("location") == "dynamic_data_extractor.py:category_page_goto_error"]
cat_url_errors = defaultdict(list)
for ce in cat_errors:
    url = ce.get("data", {}).get("page_url", "")
    err = ce.get("data", {}).get("error_message", "")[:50]
    cat_url_errors[url].append(err)

for url, errs in sorted(cat_url_errors.items(), key=lambda x: -len(x[1]))[:10]:
    print(f"  {url}: {len(errs)} errors - {Counter(errs).most_common(3)}")

# 7. Check if shop_url extraction is the bottleneck
print(f"\n=== SHOP URL EXTRACTION ===")
shop_start = len([e for e in entries if e.get("location") == "dynamic_data_extractor.py:_extract_shop_url_from_page:start"])
shop_ok = len([e for e in entries if e.get("location") == "dynamic_data_extractor.py:_extract_shop_url_from_page:success"])
shop_intro_ok = len([e for e in entries if e.get("location") == "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_goto_ok"])
shop_retries = len([e for e in entries if e.get("location") == "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_goto_retry"])
print(f"  Started: {shop_start}")
print(f"  Intro page goto OK: {shop_intro_ok}")
print(f"  Intro goto retries: {shop_retries}")
print(f"  Successfully extracted: {shop_ok}")
print(f"  Success rate: {shop_ok/shop_start*100:.1f}%" if shop_start > 0 else "  N/A")

print("\n=== Analysis Complete ===")

