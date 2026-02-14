import json, collections, re
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

# New run cutoff (16:18:54)
cutoff = 1771057134000
new_entries = [e for e in entries if e.get('timestamp', 0) >= cutoff]

# Separate old-code vs new-code period
# Old code: before 16:43 (10s element timeouts)
# New code: after 16:43 (20s element timeouts)
code_boundary = 1771058580000  # 16:43:00

old_code_entries = [e for e in new_entries if e.get('timestamp', 0) < code_boundary]
new_code_entries = [e for e in new_entries if e.get('timestamp', 0) >= code_boundary]

def categorize(e):
    e = str(e)
    if 'ERR_EMPTY_RESPONSE' in e: return 'ERR_EMPTY_RESPONSE'
    if 'ERR_CONNECTION_RESET' in e: return 'ERR_CONNECTION_RESET'
    if 'ERR_CONNECTION_CLOSED' in e: return 'ERR_CONNECTION_CLOSED'
    if 'ERR_TUNNEL' in e: return 'ERR_TUNNEL'
    if 'ECONNREFUSED' in e: return 'ECONNREFUSED'
    if 'Timeout 10000' in e: return 'Timeout_10s'
    if 'Timeout 20000' in e: return 'Timeout_20s'
    if 'Timeout 30000' in e: return 'Timeout_30s'
    if 'Captcha' in e or 'captcha' in e: return 'Captcha'
    if 'Connection closed' in e: return 'CDP_disconnect'
    return 'Other'

# Count unique task URLs that failed
def analyze_period(period_entries, label):
    failures = collections.Counter()
    failed_urls = collections.Counter()
    cdp = sum(1 for e in period_entries if 'create_context_cdp:created' in e.get('location',''))
    
    for obj in period_entries:
        if 'task_failed' in obj.get('location', ''):
            data = obj.get('data', {})
            err = data.get('error_message', data.get('error', ''))
            url = str(data.get('url', data.get('product_url', '?')))
            cat = categorize(err)
            failures[cat] += 1
            failed_urls[url] += 1
    
    total = sum(failures.values())
    unique_urls = len(failed_urls)
    
    print(f'\n=== {label} ===')
    print(f'  CDP created: {cdp}')
    print(f'  Total failures: {total}')
    print(f'  Unique failed URLs: {unique_urls}')
    if cdp > 0:
        # Rough: unique tasks ~= unique_urls
        unique_success = cdp - total  # This isn't quite right but gives an idea
        print(f'  Estimated unique tasks: {unique_urls + max(0, cdp - total)}')
    print(f'  Failure breakdown:')
    for k, v in failures.most_common():
        print(f'    {k}: {v}')
    
    # Show URLs that failed most
    multi_fail = [(u, c) for u, c in failed_urls.most_common() if c > 1]
    if multi_fail:
        print(f'  URLs failing multiple times (external retries):')
        for url, count in multi_fail[:10]:
            m = re.search(r'/pd/([^/]+)', url)
            pid = m.group(1) if m else url[:40]
            print(f'    {pid}: {count} failures')
    
    return failures, failed_urls

f1, u1 = analyze_period(old_code_entries, "OLD CODE PERIOD (16:18-16:43)")
f2, u2 = analyze_period(new_code_entries, "NEW CODE PERIOD (16:43+)")

# Overall
print(f'\n=== COMPARISON ===')
print(f'  Old code period: {sum(f1.values())} failures, {len(u1)} unique URLs')
print(f'  New code period: {sum(f2.values())} failures, {len(u2)} unique URLs')
print(f'  10s element timeout eliminated: {"YES" if f2.get("Timeout_10s", 0) == 0 else "NO (" + str(f2.get("Timeout_10s",0)) + " remaining)"}')

# Internal retry effectiveness for new code period
print(f'\n=== INTERNAL RETRY EFFECTIVENESS (NEW CODE PERIOD) ===')
retry_data = collections.Counter()
for obj in new_code_entries:
    loc = obj.get('location', '')
    data = obj.get('data', {})
    if any(x in loc for x in ['page_goto_error', 'category_page_goto_error', 'store_page_goto_error']):
        attempt = data.get('attempt', '?')
        err_msg = str(data.get('error_message', ''))
        cat = categorize(err_msg)
        retry_data[f'{cat}_attempt{attempt}'] += 1

for k, v in sorted(retry_data.items()):
    print(f'  {k}: {v}')

