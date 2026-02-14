#!/usr/bin/env python3
"""Deep-dive Round 3 analysis: understand remaining failures."""
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

# Sort by timestamp
entries.sort(key=lambda e: e.get("timestamp", 0))

print(f"=== Total log entries: {len(entries)} ===")
if entries:
    t0 = entries[0].get("timestamp", 0)
    t1 = entries[-1].get("timestamp", 0)
    duration_min = (t1 - t0) / 60000
    print(f"Time span: {duration_min:.1f} minutes")

# 1. Task failure details
task_failed = [e for e in entries if e.get("location") == "crawler.py:task_failed"]
print(f"\n=== TASK FAILURES: {len(task_failed)} ===")
fail_reasons = Counter()
for tf in task_failed:
    data = tf.get("data", {})
    error = data.get("error", "")[:100]
    # Categorize
    if "ERR_EMPTY_RESPONSE" in error:
        fail_reasons["ERR_EMPTY_RESPONSE"] += 1
    elif "ERR_CONNECTION_RESET" in error:
        fail_reasons["ERR_CONNECTION_RESET"] += 1
    elif "Timeout 30000" in error:
        fail_reasons["Timeout_30s"] += 1
    elif "Timeout 20000" in error:
        fail_reasons["Timeout_20s"] += 1
    elif "Timeout 10000" in error:
        fail_reasons["Timeout_10s"] += 1
    elif "Timeout 120000" in error or "Timeout 60000" in error:
        fail_reasons["Timeout_nav"] += 1
    elif "店铺介绍页URL" in error and "商品列表URL" in error:
        fail_reasons["Shop_URL_missing"] += 1
    elif "Captcha" in error:
        fail_reasons["Captcha"] += 1
    elif "ECONNREFUSED" in error:
        fail_reasons["ECONNREFUSED"] += 1
    elif "BitBrowser" in error or "窗口" in error:
        fail_reasons["BitBrowser_window"] += 1
    elif "Timeout" in error:
        fail_reasons[f"Timeout_other({error[:60]})"] += 1
    else:
        fail_reasons[f"OTHER({error[:80]})"] += 1

print("Failure breakdown:")
for reason, cnt in fail_reasons.most_common():
    print(f"  {reason}: {cnt}")

# 2. Product page goto errors details
page_goto_errors = [e for e in entries if e.get("location") == "product_data_crawler.py:page_goto_error"]
print(f"\n=== PRODUCT PAGE GOTO ERRORS: {len(page_goto_errors)} ===")
pge_types = Counter()
for pge in page_goto_errors:
    data = pge.get("data", {})
    err = data.get("error", "")[:100]
    attempt = data.get("attempt", "?")
    will_retry = data.get("will_retry", "?")
    if "ERR_EMPTY_RESPONSE" in err:
        pge_types[f"ERR_EMPTY_RESPONSE (attempt={attempt}, retry={will_retry})"] += 1
    elif "Timeout" in err:
        pge_types[f"Timeout (attempt={attempt}, retry={will_retry})"] += 1
    elif "ERR_CONNECTION_RESET" in err:
        pge_types[f"ERR_CONNECTION_RESET (attempt={attempt}, retry={will_retry})"] += 1
    else:
        pge_types[f"OTHER: {err[:60]} (attempt={attempt}, retry={will_retry})"] += 1

for t, c in pge_types.most_common():
    print(f"  {t}: {c}")

# 3. Category page goto errors
cat_goto_errors = [e for e in entries if e.get("location") == "dynamic_data_extractor.py:category_page_goto_error"]
print(f"\n=== CATEGORY PAGE GOTO ERRORS: {len(cat_goto_errors)} ===")
cge_types = Counter()
for cge in cat_goto_errors:
    data = cge.get("data", {})
    err = data.get("error", "")[:100]
    attempt = data.get("attempt", "?")
    will_retry = data.get("will_retry", "?")
    if "ERR_EMPTY_RESPONSE" in err:
        cge_types[f"ERR_EMPTY_RESPONSE (attempt={attempt}, retry={will_retry})"] += 1
    elif "Timeout" in err:
        cge_types[f"Timeout (attempt={attempt}, retry={will_retry})"] += 1
    elif "ERR_CONNECTION_RESET" in err:
        cge_types[f"ERR_CONNECTION_RESET (attempt={attempt}, retry={will_retry})"] += 1
    else:
        cge_types[f"OTHER: {err[:60]} (attempt={attempt}, retry={will_retry})"] += 1

for t, c in cge_types.most_common():
    print(f"  {t}: {c}")

# 4. Store page goto errors
store_goto_errors = [e for e in entries if e.get("location") == "dynamic_data_extractor.py:store_page_goto_error"]
print(f"\n=== STORE PAGE GOTO ERRORS: {len(store_goto_errors)} ===")
sge_types = Counter()
for sge in store_goto_errors:
    data = sge.get("data", {})
    err = data.get("error", "")[:100]
    attempt = data.get("attempt", "?")
    will_retry = data.get("will_retry", "?")
    if "ERR_EMPTY_RESPONSE" in err:
        sge_types[f"ERR_EMPTY_RESPONSE (attempt={attempt}, retry={will_retry})"] += 1
    elif "Timeout" in err:
        sge_types[f"Timeout (attempt={attempt}, retry={will_retry})"] += 1
    else:
        sge_types[f"OTHER: {err[:60]} (attempt={attempt}, retry={will_retry})"] += 1

for t, c in sge_types.most_common():
    print(f"  {t}: {c}")

# 5. Intro page goto errors
intro_retries = [e for e in entries if e.get("location") == "dynamic_data_extractor.py:_extract_shop_url_from_page:intro_goto_retry"]
print(f"\n=== INTRO PAGE GOTO RETRIES: {len(intro_retries)} ===")
for ir in intro_retries[:5]:
    data = ir.get("data", {})
    print(f"  attempt={data.get('attempt')}, error={data.get('error', '')[:80]}")

# 6. Internal retry recovery rate
# For product page: count tasks that had goto error but then succeeded
product_tasks = defaultdict(list)  # group by some identifier
for e in entries:
    loc = e.get("location", "")
    if "product_data_crawler" in loc:
        url = e.get("data", {}).get("url", "") or e.get("data", {}).get("product_url", "")
        if url:
            product_tasks[url].append(e)

recovered_count = 0
failed_count = 0
for url, evts in product_tasks.items():
    locs = [ev.get("location", "") for ev in evts]
    had_error = any("page_goto_error" in l for l in locs)
    had_success = any("after_page_goto" in l for l in locs)
    if had_error and had_success:
        recovered_count += 1
    elif had_error and not had_success:
        failed_count += 1

print(f"\n=== PRODUCT PAGE INTERNAL RETRY RECOVERY ===")
print(f"  Recovered (had error then success): {recovered_count}")
print(f"  Failed (all attempts): {failed_count}")
print(f"  Recovery rate: {recovered_count/(recovered_count+failed_count)*100:.1f}%" if recovered_count + failed_count > 0 else "  N/A")

# 7. Error classification details
classify_logs = [e for e in entries if "classify" in e.get("location", "")]
print(f"\n=== ERROR CLASSIFICATION ({len(classify_logs)}) ===")
for cl in classify_logs[:10]:
    print(f"  loc={cl.get('location')}, msg={cl.get('message')}, data={json.dumps(cl.get('data', {}), ensure_ascii=False)[:120]}")

# 8. extract_rankings_failed_critical details
rankings_failed = [e for e in entries if "extract_rankings_failed" in e.get("location", "")]
print(f"\n=== RANKINGS EXTRACTION FAILURES: {len(rankings_failed)} ===")
rf_reasons = Counter()
for rf in rankings_failed:
    data = rf.get("data", {})
    error = data.get("error", "")[:100]
    if "ERR_EMPTY_RESPONSE" in error:
        rf_reasons["ERR_EMPTY_RESPONSE"] += 1
    elif "Timeout" in error:
        rf_reasons["Timeout"] += 1
    elif "店铺" in error:
        rf_reasons["Shop_URL"] += 1
    else:
        rf_reasons[error[:80]] += 1

for r, c in rf_reasons.most_common():
    print(f"  {r}: {c}")

# 9. Overall success rate
total_before_goto = len([e for e in entries if e.get("location") == "product_data_crawler.py:before_page_goto"])
total_after_goto = len([e for e in entries if e.get("location") == "product_data_crawler.py:after_page_goto"])
total_base_extracted = len([e for e in entries if e.get("location") == "product_data_crawler.py:base_info_extracted"])
total_dynamic_extracted = len([e for e in entries if e.get("location") == "product_data_crawler.py:dynamic_data_extracted"])
total_failed = len(task_failed)

print(f"\n=== PIPELINE FUNNEL ===")
print(f"  page.goto attempts: {total_before_goto}")
print(f"  page.goto success: {total_after_goto}")
print(f"  base_info extracted: {total_base_extracted}")
print(f"  dynamic_data extracted: {total_dynamic_extracted}")
print(f"  task_failed: {total_failed}")
print(f"  Success rate: {total_dynamic_extracted/(total_before_goto)*100:.1f}%" if total_before_goto > 0 else "  N/A")
print(f"  goto success rate: {total_after_goto/total_before_goto*100:.1f}%" if total_before_goto > 0 else "  N/A")

print("\n=== Analysis Complete ===")

