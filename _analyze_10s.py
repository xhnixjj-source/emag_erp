import json
from datetime import datetime

log_path = r'\\10.147.19.69\emag_erp\.cursor\debug.log'
lines = open(log_path, 'r', encoding='utf-8').readlines()

entries = []
for line in lines:
    line = line.strip()
    if not line: continue
    try:
        entries.append(json.loads(line))
    except: continue

# Filter to new run only (after 16:18)
cutoff = 1771057134000  # 16:18:54
new_entries = [e for e in entries if e.get('timestamp', 0) >= cutoff]

print("=== ALL 10000ms TIMEOUT ENTRIES IN NEW RUN ===")
for obj in new_entries:
    data = obj.get('data', {})
    err = str(data.get('error_message', data.get('error', '')))
    if 'Timeout 10000' in err:
        ts = datetime.fromtimestamp(obj['timestamp']/1000).strftime('%H:%M:%S')
        loc = obj.get('location', '')
        url = data.get('url', data.get('page_url', data.get('product_url', '?')))
        print(f"  [{ts}] {loc}")
        print(f"    error: {err[:120]}")
        print(f"    url: {str(url)[:80]}")
        print()

print("\n=== ALL 20000ms TIMEOUT ENTRIES IN NEW RUN (new element wait) ===")
count_20 = 0
for obj in new_entries:
    data = obj.get('data', {})
    err = str(data.get('error_message', data.get('error', '')))
    if 'Timeout 20000' in err:
        count_20 += 1
        ts = datetime.fromtimestamp(obj['timestamp']/1000).strftime('%H:%M:%S')
        loc = obj.get('location', '')
        if count_20 <= 5:  # Show first 5
            print(f"  [{ts}] {loc}: {err[:100]}")
print(f"  Total: {count_20}")

print("\n=== CHECK: category_rank_timeout entries ===")
for obj in new_entries:
    loc = obj.get('location', '')
    if 'category_rank_timeout' in loc:
        data = obj.get('data', {})
        err = str(data.get('error', ''))[:80]
        ts = datetime.fromtimestamp(obj['timestamp']/1000).strftime('%H:%M:%S')
        print(f"  [{ts}] {err}")

print("\n=== Timeout source breakdown (by location) ===")
import collections
timeout_sources = collections.Counter()
for obj in new_entries:
    data = obj.get('data', {})
    err = str(data.get('error_message', data.get('error', '')))
    loc = obj.get('location', '')
    if 'Timeout' in err and 'ms exceeded' in err:
        timeout_sources[loc] += 1
for k, v in timeout_sources.most_common():
    print(f"  {k}: {v}")

