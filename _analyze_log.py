import json, collections
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

# Find service restart boundary by looking for first CDP creation after gap
# Based on time distribution, new run starts around 16:15
# Use 16:15 as cutoff (1771057500000 approx)
# Let me be more precise - find the gap

# Find entries with timestamps and look for restart markers
ts_list = [(e.get('timestamp',0), e) for e in entries if e.get('timestamp',0) > 0]
ts_list.sort(key=lambda x: x[0])

# Find largest gap
prev_ts = 0
max_gap = 0
restart_ts = 0
for ts, _ in ts_list:
    if prev_ts > 0:
        gap = ts - prev_ts
        if gap > max_gap and ts > 1771057000000:  # after 16:00
            max_gap = gap
            restart_ts = ts
    prev_ts = ts

if restart_ts:
    print(f"Detected restart at: {datetime.fromtimestamp(restart_ts/1000)} (gap: {max_gap/1000:.0f}s)")
    cutoff = restart_ts
else:
    cutoff = 1771057500000  # fallback to 16:18

# Filter entries to only new run
new_entries = [e for e in entries if e.get('timestamp', 0) >= cutoff]
print(f"Total entries in new run: {len(new_entries)}")
print(f"New run time range: {datetime.fromtimestamp(cutoff/1000)} to {datetime.fromtimestamp(max(e.get('timestamp',0) for e in new_entries)/1000)}")

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
    return 'Other: ' + e[:60]

errors_internal = collections.Counter()
errors_final = collections.Counter()
retry_attempts = collections.Counter()
retry_will = 0
retry_wont = 0

for obj in new_entries:
    loc = obj.get('location', '')
    data = obj.get('data', {})
    err_msg = data.get('error_message', data.get('error', ''))

    if any(x in loc for x in ['page_goto_error', 'category_page_goto_error', 'store_page_goto_error', 'intro_page_goto_error']):
        cat = categorize(err_msg)
        errors_internal[cat] += 1
        attempt = data.get('attempt')
        if attempt:
            retry_attempts[f'{cat}_attempt{attempt}'] += 1
        wr = data.get('will_retry')
        if wr is True: retry_will += 1
        elif wr is False: retry_wont += 1

    if 'task_failed' in loc:
        cat = categorize(err_msg)
        errors_final[cat] += 1

cdp_created = sum(1 for e in new_entries if 'create_context_cdp:created' in e.get('location',''))
cdp_released = sum(1 for e in new_entries if 'release_context' in e.get('location',''))
stale_ws = sum(1 for e in new_entries if 'stale_ws' in e.get('location',''))
classify_pw = sum(1 for e in new_entries if 'pw_connection_fallback' in e.get('location',''))
classify_other = sum(1 for e in new_entries if 'classify_error:fallback_other' in e.get('location',''))

timeout_10s_el = sum(1 for e in new_entries if 'category_rank_timeout' in e.get('location','') and 'Timeout 10000' in str(e.get('data',{}).get('error','')))
timeout_20s_el = sum(1 for e in new_entries if 'category_rank_timeout' in e.get('location','') and 'Timeout 20000' in str(e.get('data',{}).get('error','')))

print()
print('--- CDP Lifecycle (NEW RUN ONLY) ---')
print(f'  Created: {cdp_created}')
print(f'  Released: {cdp_released}')
print(f'  Stale WS caught: {stale_ws}')

print(f'\n--- Internal Goto Errors ---')
for k, v in errors_internal.most_common():
    print(f'  {k}: {v}')

print(f'\n--- Retry Decisions ---')
print(f'  will_retry=true: {retry_will}')
print(f'  will_retry=false: {retry_wont}')

print(f'\n--- Retry Attempt Distribution ---')
for k, v in sorted(retry_attempts.items()):
    print(f'  {k}: {v}')

print(f'\n--- Element Timeout Evidence ---')
print(f'  Timeout 10000ms (should be ZERO now): {timeout_10s_el}')
print(f'  Timeout 20000ms (new element wait): {timeout_20s_el}')

print(f'\n--- Classify Error ---')
print(f'  PW fallback->CONNECTION: {classify_pw}')
print(f'  Fallback->OTHER: {classify_other}')

print(f'\n=== FINAL TASK FAILURES (NEW RUN) ===')
total_final = sum(errors_final.values())
for k, v in errors_final.most_common():
    print(f'  {k}: {v}')
print(f'  TOTAL: {total_final}')

if cdp_created > 0:
    success_rate = (cdp_created - total_final) / cdp_created * 100
    print(f'\n=== SUCCESS RATE ===')
    print(f'  Tasks started: {cdp_created}')
    print(f'  Tasks failed: {total_final}')
    print(f'  Success rate: {success_rate:.1f}%')
    print(f'\n  Previous run: ~43% fail rate')
    print(f'  Current run:  {total_final/max(cdp_created,1)*100:.1f}% fail rate')

# Detailed: show Timeout_30s by location
print(f'\n--- Timeout_30s Detail (by location) ---')
for obj in new_entries:
    loc = obj.get('location', '')
    data = obj.get('data', {})
    err_msg = str(data.get('error_message', data.get('error', '')))
    if 'task_failed' in loc and 'Timeout 30000' in err_msg:
        # Try to find the URL
        url = data.get('url', data.get('page_url', data.get('product_url', '?')))
        print(f'  [{loc}] url={str(url)[:60]}')
