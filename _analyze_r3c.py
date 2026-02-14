#!/usr/bin/env python3
"""Check raw log entry structures."""
import json
from collections import Counter

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

# Show raw task_failed entries
task_failed = [e for e in entries if e.get("location") == "crawler.py:task_failed"]
print(f"=== TASK FAILURES RAW (first 5 of {len(task_failed)}) ===")
for tf in task_failed[:5]:
    print(json.dumps(tf, ensure_ascii=False, indent=2)[:500])
    print("---")

# Show raw page_goto_error entries
pge = [e for e in entries if e.get("location") == "product_data_crawler.py:page_goto_error"]
print(f"\n=== PAGE GOTO ERROR RAW (first 5 of {len(pge)}) ===")
for p in pge[:5]:
    print(json.dumps(p, ensure_ascii=False, indent=2)[:500])
    print("---")

# Show raw category_page_goto_error entries
cge = [e for e in entries if e.get("location") == "dynamic_data_extractor.py:category_page_goto_error"]
print(f"\n=== CATEGORY GOTO ERROR RAW (first 5 of {len(cge)}) ===")
for c in cge[:5]:
    print(json.dumps(c, ensure_ascii=False, indent=2)[:500])
    print("---")

# Show raw extract_rankings_failed_critical entries
rfc = [e for e in entries if "extract_rankings_failed" in e.get("location", "")]
print(f"\n=== RANKINGS FAILED RAW (first 5 of {len(rfc)}) ===")
for r in rfc[:5]:
    print(json.dumps(r, ensure_ascii=False, indent=2)[:500])
    print("---")

# Check data field names across all entries
all_data_keys = Counter()
for e in entries:
    data = e.get("data", {})
    for k in data.keys():
        all_data_keys[k] += 1

print(f"\n=== ALL DATA FIELD NAMES ===")
for k, c in all_data_keys.most_common(40):
    print(f"  {k}: {c}")

