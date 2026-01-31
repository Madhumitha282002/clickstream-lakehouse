import argparse
import json
import random
from datetime import datetime, timedelta, timezone

from faker import Faker

fake = Faker()

EVENT_TYPES = ["view", "add_to_cart", "checkout", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "CA", "GB", "DE", "IN", "BR", "AU"]
PAGES = ["/", "/search", "/product", "/cart", "/checkout", "/purchase", "/category"]
REFERRERS = ["google", "bing", "direct", "newsletter", "twitter", "reddit"]

def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def weighted_event(prev: str | None) -> str:
    """
    Simple funnel-ish behavior:
    - views are most common
    - add_to_cart usually follows view
    - checkout follows add_to_cart
    - purchase follows checkout (rare)
    """
    if prev is None:
        return random.choices(EVENT_TYPES, weights=[0.92, 0.06, 0.015, 0.005])[0]
    if prev == "view":
        return random.choices(EVENT_TYPES, weights=[0.80, 0.17, 0.02, 0.01])[0]
    if prev == "add_to_cart":
        return random.choices(EVENT_TYPES, weights=[0.50, 0.15, 0.30, 0.05])[0]
    if prev == "checkout":
        return random.choices(EVENT_TYPES, weights=[0.30, 0.05, 0.25, 0.40])[0]
    return random.choices(EVENT_TYPES, weights=[0.90, 0.07, 0.02, 0.01])[0]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output JSONL file path")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--users", type=int, default=20000)
    ap.add_argument("--events", type=int, default=500000)
    ap.add_argument("--start_date", default=None, help="YYYY-MM-DD (optional)")
    args = ap.parse_args()

    if args.start_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        # deterministic-ish baseline: last N days ending today
        today = datetime.now(timezone.utc).date()
        start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc) - timedelta(days=args.days)

    user_ids = [f"user_{i:06d}" for i in range(args.users)]
    # pre-generate some product ids
    product_ids = [f"sku_{i:05d}" for i in range(5000)]

    with open(args.out, "w", encoding="utf-8") as f:
        prev_event_by_user = {}
        for i in range(args.events):
            user = random.choice(user_ids)

            # random time within window
            day_offset = random.randint(0, max(0, args.days - 1))
            base_day = start + timedelta(days=day_offset)
            event_time = base_day + timedelta(seconds=random.randint(0, 86399))

            prev = prev_event_by_user.get(user)
            etype = weighted_event(prev)
            prev_event_by_user[user] = etype

            page = random.choice(PAGES)
            if page == "/product":
                page_url = f"/product/{random.choice(product_ids)}"
            elif page == "/category":
                page_url = f"/category/{random.randint(1, 50)}"
            else:
                page_url = page

            record = {
                "event_id": f"ev_{i:09d}",
                "user_id": user,
                "event_time": iso(event_time),
                "event_type": etype,
                "page_url": page_url,
                "referrer": random.choice(REFERRERS),
                "device": random.choice(DEVICES),
                "country": random.choice(COUNTRIES),
            }
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {args.events} events to {args.out}")

if __name__ == "__main__":
    main()
