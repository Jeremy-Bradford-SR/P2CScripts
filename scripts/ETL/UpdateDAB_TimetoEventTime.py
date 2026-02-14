
import sys
import re
from datetime import datetime
import os
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import shared_utils # Use shared utils for API
from shared_utils import APIClient

def try_parse_formats(date_string, raw_time_str, rule_name):
    formats = [
        '%H:%M, %m/%d/%Y',       # 14:30, 11/20/2025
        '%m/%d/%Y %I:%M:%S %p',  # 11/20/2025 2:57:00 PM
        '%m/%d/%Y %I:%M %p',     # 11/20/2025 2:57 PM
        '%m/%d/%Y %H:%M',        # 11/22/2025 23:42
        '%m/%d/%Y'               # 11/23/2025
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_string, fmt)
            # print(f"MATCH {rule_name}: '{date_string}' (fmt: {fmt}) -> {dt}")
            return dt
        except ValueError:
            continue
    
    # print(f"FAIL {rule_name}: '{date_string}' - No format matched")
    return None

def parse_time_with_regex(time_str):
    """
    Uses regex to find the timestamp after 'Reported: '.
    Returns a datetime object if successful, otherwise tries other patterns.
    """
    if not time_str:
        return None
    
    # Rule 1: Prioritize 'Reported:' time.
    reported_match = re.search(r"Reported:\s*(.+?)\.", time_str)
    if reported_match:
        date_string = reported_match.group(1).strip()
        dt = try_parse_formats(date_string, time_str, "Rule 1 (Reported)")
        if dt: return dt

    # Rule 2: Handle 'between...and...' format, taking the second time.
    between_match = re.search(r"and\s+(.+?)\.", time_str)
    if between_match:
        date_string = between_match.group(1).strip()
        dt = try_parse_formats(date_string, time_str, "Rule 2 (Between)")
        if dt: return dt

    # Rule 3: Handle simple 'on...' format.
    on_match = re.search(r"on\s+(.+?)\.", time_str)
    if on_match:
        date_string = on_match.group(1).strip()
        dt = try_parse_formats(date_string, time_str, "Rule 3 (On)")
        if dt: return dt

    # Rule 4: Handle 'On ... at ...' format (e.g. "On 11/15/2025 at 11:00")
    on_at_match = re.search(r"On\s+(.+?)\s+at\s+(.+?)(?:\.|$)", time_str, re.IGNORECASE)
    if on_at_match:
        date_part = on_at_match.group(1).strip()
        time_part = on_at_match.group(2).strip()
        combined = f"{date_part} {time_part}"
        dt = try_parse_formats(combined, time_str, "Rule 4 (On .. at)")
        if dt: return dt

    return None # Return None if no patterns match

def update_event_time(target_ids=None):
    api = APIClient()
    updated_count = 0
    failed_count = 0
    total_processed = 0

    while True:
        candidates = []
        try:
            if target_ids:
                candidates = api.post("tools/dab-time/fetch-details", {"ids": target_ids})
                # Single pass for target_ids
            else:
                # Fetch batch of 100
                candidates = api.get("tools/dab-time/candidates?count=100")
        except Exception as ex:
            print(f"[ERROR] API Fetch failed: {ex}")
            # Raise exception to propagate failure to caller (Orchestrator/Ingestion)
            raise ex
        
        if not candidates:
            # print("No candidates found.")
            break

        updates = []
        for row in candidates:
            rec_id = row.get("id") or row.get("Id")
            raw_time = row.get("time") or row.get("TimeText")
            
            event_time = parse_time_with_regex(raw_time)
            
            if event_time:
                 updates.append({
                     "Id": str(rec_id),
                     "EventTime": event_time.isoformat()
                 })
                 updated_count += 1
            else:
                 failed_count += 1
        
        if updates:
            try:
                api.post("tools/dab-time/update", updates)
                print(f"Updated batch of {len(updates)}")
            except Exception as e:
                print(f"[ERROR] Batch Update failed: {e}")
                # Raise exception to propagate failure
                raise e

        total_processed += len(candidates)
        
        if target_ids:
            break
        
        if not candidates:
             break

    print(f"\n[SUCCESS] Script finished.")
    print(f"  - Rows successfully updated: {updated_count}")
    print(f"  - Rows that could not be parsed: {failed_count}")
    print(f"  - Total Processed: {total_processed}")

if __name__ == "__main__":
    update_event_time()