import argparse
import os
import requests
import datetime as dt
from collections import defaultdict
from typing import List, Dict, Tuple, Any

GRAFANA_URL = "http://192.168.169.30:3000"
DATASOURCE_ID = 2
API_TOKEN = os.getenv("GRAFANA_API_TOKEN")

METRIC = "ceph_pool_bytes_used"
STEP = "1h"


def parse_date(s: str) -> dt.datetime:
    return dt.datetime.strptime(s, "%Y-%m-%d")

def build_query(pool_id: str) -> str:
    return f'{METRIC}{{pool_id="{pool_id}"}}'

def grafana_headers() -> Dict[str, str]:
    headers = {}
    if API_TOKEN:
        headers["Authorization"] = f"Bearer {API_TOKEN}"
    return headers

def query_range(pool_id: str,
                start: dt.datetime,
                end: dt.datetime,
                step: str = STEP) -> List[Tuple[float, float]]:
    query = build_query(pool_id)

    params = {
        "query": query,
        "start": start.isoformat() + "Z",
        "end": end.isoformat() + "Z",
        "step": step,
    }

    url = f"{GRAFANA_URL}/api/datasources/proxy/{DATASOURCE_ID}/api/v1/query_range"
    r = requests.get(url, params=params, headers=grafana_headers(), timeout=30)
    r.raise_for_status()
    data: Dict[str, Any] = r.json()

    if data.get("status") != "success":
        raise RuntimeError(f"Grafana/Prometheus error: {data}")

    result = data["data"]["result"]
    if not result:
        return []

    # Prometheus format: "values": [ [ <timestamp>, "<value>" ], ... ]
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
                    start: dt.datetime,
                    end: dt.datetime,
                    group_by: str = "day") -> List[Dict[str, Any]]:
    """
    start: datetime từ đầu khoảng
    end  : datetime cuối khoảng (đã cộng thêm 1 ngày để làm end-exclusive)
    """
    samples = query_range(pool_id, start, end)
    if not samples:
        raise ValueError("No data in this interval")

    buckets: Dict[str, list] = defaultdict(list)
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
            "max_gib": max_bytes / (1024 ** 3),
        })

    return rows


def parse_args():
    parser = argparse.ArgumentParser(
        description="Ceph pool usage report from Grafana (Prometheus proxy)"
    )

    parser.add_argument(
        "--pool-id",
        required=True,
        help="ID of Ceph pool"
    )
    parser.add_argument(
        "--from-date",
        required=True,
        help="Start Date, format YYYY-MM-DD (inclusive)"
    )
    parser.add_argument(
        "--to-date",
        required=True,
        help="End Date, format YYYY-MM-DD (inclusive)"
    )
    parser.add_argument(
        "--group-by",
        choices=["day", "month"],
        default="day",
        help="Group by day or month"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    pool_id = args.pool_id
    from_date = parse_date(args.from_date)
    # to_date inclusive → cộng thêm 1 ngày để thành end-exclusive cho query_range
    to_date = parse_date(args.to_date) + dt.timedelta(days=1)

    print(f"Pool ID: {pool_id}")
    print(f"Range : {from_date} -> {to_date}")
    print(f"Group : {args.group_by}")
    print("-" * 60)

    usage = aggregate_usage(pool_id, from_date, to_date, group_by=args.group_by)

    for item in usage:
        print(
            f"{item['period']}: "
            f"avg={item['avg_gib']:.2f} GiB, "
            f"max={item['max_gib']:.2f} GiB"
        )


if __name__ == "__main__":
    main()