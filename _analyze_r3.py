#!/usr/bin/env python3
"""Analyze Round 3 debug logs to evaluate fix effectiveness."""
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

print(f"=== Total log entries: {len(entries)} ===\n")

# 1. Group by location
location_counts = Counter(e.get("location", "unknown") for e in entries)
print("--- Log entry counts by location ---")
for loc, cnt in location_counts.most_common(30):
    print(f"  {loc}: {cnt}")

# 2. Check soft failure logs (element wait timeout -> soft fail)
soft_fails = [e for e in entries if "soft_fail" in e.get("hypothesisId", "")]
print(f"\n--- Element wait soft failures (H_soft_fail): {len(soft_fails)} ---")
for sf in soft_fails[:5]:
    print(f"  URL: {sf.get('data', {}).get('page_url', 'N/A')[:80]}")
    print(f"  Error: {sf.get('data', {}).get('error', 'N/A')[:120]}")

# 3. Check error classification logs
classify_logs = [e for e in entries if "classify" in e.get("location", "")]
classify_types = Counter()
for cl in classify_logs:
    msg = cl.get("message", "")
    classify_types[msg] += 1
print(f"\n--- Error classification distribution ({len(classify_logs)} total) ---")
for msg, cnt in classify_types.most_common():
    print(f"  {msg}: {cnt}")

# 4. Check retry logs
retry_logs = [e for e in entries if "retry" in e.get("location", "").lower() and "execute" in e.get("location", "").lower()]
print(f"\n--- Retry execution logs: {len(retry_logs)} ---")
# Group by retry attempt
retry_attempts = Counter()
retry_decisions = Counter()
for rl in retry_logs:
    data = rl.get("data", {})
    attempt = data.get("retry_count", "?")
    decision = data.get("will_retry", "?")
    error_type = data.get("error_type", "?")
    retry_attempts[f"attempt_{attempt}_{error_type}"] += 1
    retry_decisions[f"will_retry={decision}_{error_type}"] += 1

print("  By attempt+type:")
for k, v in sorted(retry_attempts.items()):
    print(f"    {k}: {v}")
print("  By decision+type:")
for k, v in sorted(retry_decisions.items()):
    print(f"    {k}: {v}")

# 5. Check goto retry logs (internal retries)
goto_retries = [e for e in entries if "goto" in e.get("location", "").lower()]
print(f"\n--- Internal goto retry logs: {len(goto_retries)} ---")
goto_by_loc = Counter(e.get("location", "") for e in goto_retries)
for loc, cnt in goto_by_loc.most_common():
    print(f"  {loc}: {cnt}")

# Check recovery from goto retries
goto_success_after_retry = [e for e in goto_retries if "成功" in e.get("message", "") and e.get("data", {}).get("attempt", 0) > 0]
goto_fail_all = [e for e in goto_retries if "最终失败" in e.get("message", "") or "final_fail" in e.get("message", "")]
print(f"  Recovered after retry: {len(goto_success_after_retry)}")
print(f"  Failed all attempts: {len(goto_fail_all)}")

# 6. CDP leak check
cdp_logs = [e for e in entries if "cdp" in e.get("location", "").lower() or "cdp" in e.get("hypothesisId", "").lower()]
print(f"\n--- CDP connection logs: {len(cdp_logs)} ---")
cdp_by_loc = Counter(e.get("location", "") for e in cdp_logs)
for loc, cnt in cdp_by_loc.most_common():
    print(f"  {loc}: {cnt}")

# Track CDP context count over time
cdp_create = [e for e in cdp_logs if "create" in e.get("location", "").lower()]
cdp_release = [e for e in cdp_logs if "release" in e.get("location", "").lower()]
print(f"  CDP creates: {len(cdp_create)}, CDP releases: {len(cdp_release)}")
if cdp_create:
    ctx_counts = [e.get("data", {}).get("current_contexts_count", "?") for e in cdp_create[-5:]]
    print(f"  Last 5 CDP create context counts: {ctx_counts}")

# 7. ws_url stale detection
ws_stale = [e for e in entries if "stale_ws" in e.get("location", "")]
ws_reopen = [e for e in entries if "reopened" in e.get("location", "")]
print(f"\n--- WS URL stale/reopen ---")
print(f"  Stale detections: {len(ws_stale)}")
print(f"  Successful reopens: {len(ws_reopen)}")

# 8. ECONNREFUSED check
econnrefused = [e for e in entries if "econnrefused" in str(e.get("data", {})).lower() or "econnrefused" in e.get("message", "").lower()]
print(f"\n--- ECONNREFUSED occurrences: {len(econnrefused)} ---")

# 9. Final task outcomes from retry manager
final_outcomes = [e for e in entries if "execute_with_retry" in e.get("location", "")]
success_count = sum(1 for e in final_outcomes if "成功" in e.get("message", ""))
fail_count = sum(1 for e in final_outcomes if "失败" in e.get("message", "") or "放弃" in e.get("message", ""))
print(f"\n--- Task outcomes from retry manager ---")
print(f"  Success: {success_count}")
print(f"  Failed/Abandoned: {fail_count}")

# 10. Timeout error analysis
timeout_errors = [e for e in entries if "timeout" in str(e.get("data", {})).lower() or "timeout" in e.get("message", "").lower()]
timeout_by_ms = Counter()
for te in timeout_errors:
    data_str = str(te.get("data", {}))
    msg = te.get("message", "")
    combined = data_str + msg
    if "10000" in combined:
        timeout_by_ms["10s"] += 1
    elif "20000" in combined:
        timeout_by_ms["20s"] += 1
    elif "30000" in combined:
        timeout_by_ms["30s"] += 1
    elif "120000" in combined:
        timeout_by_ms["120s"] += 1
    else:
        timeout_by_ms["other"] += 1
print(f"\n--- Timeout distribution ---")
for ms, cnt in timeout_by_ms.most_common():
    print(f"  {ms}: {cnt}")

# 11. ERR_EMPTY_RESPONSE recovery
empty_resp = [e for e in entries if "empty_response" in str(e.get("data", {})).lower() or "err_empty" in str(e.get("data", {})).lower()]
print(f"\n--- ERR_EMPTY_RESPONSE logs: {len(empty_resp)} ---")

print("\n=== Analysis Complete ===")

