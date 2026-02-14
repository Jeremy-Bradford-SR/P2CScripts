from datetime import datetime
import json

def to_iso(dt):
    return dt.isoformat() if dt else None

def parse_date(date_str):
    if not date_str: return None
    try:
        return datetime.strptime(date_str, "%m/%d/%Y")
    except:
        return None

# Simulation of what DOC script does
dt = parse_date("01/01/2023")
iso_str = to_iso(dt)

data = {
    "date": iso_str
}

print(f"Original: {dt}")
print(f"ISO: {iso_str}")
print(f"JSON: {json.dumps(data)}")
