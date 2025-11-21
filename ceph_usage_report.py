import requests
import datetime as dt
from collections import defaultdict
from typing import List, Dict

#====Config====
PROM_URL = "http://171.244.195.198:9095"
METRIC = "ceph_pool_bytes_used"
STEP = "1h"
#==============

def parse_date(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%d")

def build_query(pool_id: str) -> str:
    return f'{METRIC}{{pool_id="{pool_id}"}}'

def query_range(pool_id: str,
                start: dt.datetime,
                end: dt.datetime,
                step: str = STEP) -> List[Dict]:
    query = build_query(pool_id)
    params = {
        "query": query,
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z",
        "step": step,
    }
    r = requests.get(f'{PROM_URL}/api/v1/query_range', params=params)
    r.raise_for_status()
    data = r.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus error: {data}")
    result = data["data"]["result"]
    if not result:
        return []
    
    values = result[0]["values"]
    return [(float(ts), float(val)) for ts, val in values]

def bucket_key(ts: float, group_by: str) -> str:
    t = dt.datetime.utcfromtimestamp(ts)
    if group_by == "day":
        return t.date().isoformat()
    elif group_by == "month":
        return f"{t.year:04d}-{t.month:02d}"
    else:
        raise ValueError("group_by must be 'day' or 'month'")
    
def aggregate_usage(pool_id: str,
                    from_date: str,
                    to_date: str,
                    group_by: str = "day") -> List[Dict]:
    start = parse_date(from_date)
    end = parse_date(to_date) + dt.timedelta(days=1)

    samples = query_range(pool_id, start, end)
    if not samples:
        raise ValueError("No data in this interval")
    
    buckets = defaultdict(list)
    for ts, val in samples:
        k = bucket_key(ts, group_by)
        buckets[k].append(val)

    rows = []
    for period in sorted(buckets.keys()):
        vals = buckets[period]
        avg_bytes = sum(vals) / len(vals)
        max_bytes = max(vals)
        rows.append({
            "period": period,
            "avg_bytes": avg_bytes,
            "max_bytes": max_bytes,
            "avg_gib": avg_bytes / (1024 ** 3),
            "max_gib": max_bytes / (1024 ** 3)
        })
    
    return rows

def main():
    pool_id = "7"

    from_date = "2025-10-01"
    to_date = "2025-11-30"

    daily = aggregate_usage(pool_id, from_date, to_date, group_by="day")
    print("=== DAILY USAGE ===")
    for row in daily[:10]:
        print(f"{row['period']}: avg={row['avg_gib']:.2f} GiB, "
              f"max={row['max_gib']:.2f} GiB")
        
    monthly = aggregate_usage(pool_id, from_date, to_date, group_by="month")
    print("\=== MONTHLY USAGE ===")
    for row in monthly:
        print(f"{row['period']}: avg={row['avg_gib']:.2f} GiB, "
              f"max={row['max_gib']:.2f} GiB")
        
if __name__ == "__main__":
    main()