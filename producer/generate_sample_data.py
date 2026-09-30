"""
generate_sample_data.py

Assignment 2 — Streaming Data Analytics
Generates sample e-commerce event data for the 4 sources identified in
Assignment 1: clickstream, cart events, transactions, and ad/marketing
touchpoints. Field names follow the reference schema used in Assignment 1
(based on the Kaggle "eCommerce Behavior Data from Multi-Category Store"
dataset), extended with marketing fields for the ad-click source.

Output: writes one JSON-lines (.jsonl) file per source into ./sample_data/
Each line is a single event record, ready to be streamed by producer.py.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = Path("sample_data")
OUTPUT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------------------------------------
# Shared reference data (kept small and consistent across all 4 sources so
# the same user_id / product_id / session shows up across event types —
# mirrors how a real customer journey would look)
# ---------------------------------------------------------------------------

CATEGORIES = [
    ("electronics.smartphone", "samsung"),
    ("electronics.laptop", "apple"),
    ("electronics.headphones", "boat"),
    ("apparel.shoes", "nike"),
    ("apparel.tshirt", "levis"),
    ("appliances.kitchen", "prestige"),
    ("furniture.office", "ikea"),
    ("beauty.skincare", "nivea"),
]

NUM_USERS = 40
USER_IDS = [random.randint(100000, 999999) for _ in range(NUM_USERS)]

NUM_PRODUCTS = 60
PRODUCTS = []
for _ in range(NUM_PRODUCTS):
    category_code, brand = random.choice(CATEGORIES)
    PRODUCTS.append({
        "product_id": random.randint(1000000, 9999999),
        "category_code": category_code,
        "brand": brand,
        "price": round(random.uniform(199, 89999), 2),
    })

CAMPAIGNS = [
    ("SUMMER_SALE_25", "google", "cpc"),
    ("FESTIVE_OFFER", "facebook", "social"),
    ("NEW_LAUNCH_PROMO", "instagram", "social"),
    ("EMAIL_WINBACK", "newsletter", "email"),
    ("RETARGETING_CART", "google", "display"),
]

# Realistic e-commerce search terms, grouped by category so they connect
# naturally to the CATEGORIES list above -- replaces fake.word(), which
# was producing generic dictionary words ("blue", "fall", "offer") with
# no relation to what a real shopper searches for.
SEARCH_TERMS_BY_CATEGORY = {
    "electronics.smartphone": ["smartphone under 20000", "5g phone", "phone with good camera", "samsung galaxy", "iphone"],
    "electronics.laptop": ["laptop for gaming", "macbook", "laptop under 50000", "lightweight laptop", "laptop for students"],
    "electronics.headphones": ["wireless earbuds", "noise cancelling headphones", "bluetooth headphones", "gaming headset"],
    "apparel.shoes": ["running shoes", "sneakers", "formal shoes men", "sports shoes", "shoes for women"],
    "apparel.tshirt": ["cotton t shirt", "graphic tshirt", "oversized t shirt", "plain white tshirt"],
    "appliances.kitchen": ["air fryer", "mixer grinder", "microwave oven", "induction cooktop"],
    "furniture.office": ["ergonomic office chair", "study table", "standing desk", "office chair"],
    "beauty.skincare": ["face serum", "sunscreen", "moisturizer for dry skin", "skincare kit"],
}
# A smaller set of category-agnostic terms real shoppers also type.
GENERIC_SEARCH_TERMS = ["discount code", "free shipping", "sale offers", "best deals today", "new arrivals"]

START_TIME = datetime(2026, 8, 20, 9, 0, 0)


def random_timestamp(base, max_offset_minutes=60 * 24 * 10):
    return base + timedelta(minutes=random.randint(0, max_offset_minutes))


def new_session_id():
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# 1. Clickstream events — page views, product views, searches
# ---------------------------------------------------------------------------

def generate_clickstream(n=300):
    events = []
    for _ in range(n):
        user_id = random.choice(USER_IDS)
        product = random.choice(PRODUCTS)
        event_type = random.choices(
            ["view", "search"], weights=[0.8, 0.2]
        )[0]
        event = {
            "event_time": random_timestamp(START_TIME).isoformat(),
            "event_type": event_type,
            "product_id": product["product_id"],
            "category_code": product["category_code"],
            "brand": product["brand"],
            "price": product["price"],
            "user_id": user_id,
            "user_session": new_session_id(),
        }
        if event_type == "search":
            # ~80% of the time, search something related to the category
            # the event already rolled (mimics a shopper browsing within
            # an interest area); ~20% a generic site-wide term.
            if random.random() < 0.8 and product["category_code"] in SEARCH_TERMS_BY_CATEGORY:
                event["search_query"] = random.choice(SEARCH_TERMS_BY_CATEGORY[product["category_code"]])
            else:
                event["search_query"] = random.choice(GENERIC_SEARCH_TERMS)
        events.append(event)
    events.sort(key=lambda e: e["event_time"])
    return events


# ---------------------------------------------------------------------------
# 2. Cart events — add-to-cart, remove-from-cart, checkout-started
# ---------------------------------------------------------------------------

def generate_cart_events(n=150):
    events = []
    for _ in range(n):
        user_id = random.choice(USER_IDS)
        product = random.choice(PRODUCTS)
        event_type = random.choices(
            ["cart", "remove_from_cart", "checkout_started"],
            weights=[0.55, 0.2, 0.25],
        )[0]
        event = {
            "event_time": random_timestamp(START_TIME).isoformat(),
            "event_type": event_type,
            "product_id": product["product_id"],
            "category_code": product["category_code"],
            "brand": product["brand"],
            "price": product["price"],
            "user_id": user_id,
            "user_session": new_session_id(),
        }
        events.append(event)
    events.sort(key=lambda e: e["event_time"])
    return events


# ---------------------------------------------------------------------------
# 3. Transaction events — completed purchases
# ---------------------------------------------------------------------------

def generate_transactions(n=90):
    events = []
    for _ in range(n):
        user_id = random.choice(USER_IDS)
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 3)
        event = {
            "event_time": random_timestamp(START_TIME).isoformat(),
            "event_type": "purchase",
            "order_id": str(uuid.uuid4()),
            "product_id": product["product_id"],
            "category_code": product["category_code"],
            "brand": product["brand"],
            "price": product["price"],
            "quantity": quantity,
            "order_value": round(product["price"] * quantity, 2),
            "user_id": user_id,
            "user_session": new_session_id(),
        }
        events.append(event)
    events.sort(key=lambda e: e["event_time"])
    return events


# ---------------------------------------------------------------------------
# 4. Marketing / ad touchpoints — impressions, clicks, email clicks
# ---------------------------------------------------------------------------

def generate_ad_events(n=200):
    """
    Generates impression events, then derives clicks/email_clicks as a
    SUBSET of those impressions -- never independently. This matters:
    a real ad platform can never have more clicks than impressions for a
    campaign (a click requires an impression first), so CTR must be
    structurally bounded to 0-100%. The earlier version sampled
    "ad_impression"/"ad_click"/"email_click" as three independent event
    types from the same pool, which meant a campaign could end up with
    MORE clicks than impressions by pure chance -- producing an
    impossible >100% CTR no matter how the dashboard query aggregates it.
    """
    events = []

    # Step 1: generate the impression pool (this is now the only
    # independently-sampled event type).
    num_impressions = int(n * 0.70)
    impressions = []
    for _ in range(num_impressions):
        user_id = random.choice(USER_IDS)
        campaign, utm_source, utm_medium = random.choice(CAMPAIGNS)
        impression_time = random_timestamp(START_TIME)
        impressions.append({
            "event_time": impression_time.isoformat(),
            "event_type": "ad_impression",
            "user_id": user_id,
            "campaign": campaign,
            "utm_source": utm_source,
            "utm_medium": utm_medium,
            "cost": 0.0,
            "_impression_time": impression_time,  # kept for deriving a later click time; stripped before writing
        })
    events.extend(impressions)

    # Step 2: a fraction of impressions ALSO become a click (or email
    # click), a few seconds/minutes later, for the SAME user/campaign --
    # this is what guarantees clicks <= impressions by construction.
    click_rate = 0.35  # ~35% of impressions get clicked -- realistic-ish CTR
    num_clicks = int(num_impressions * click_rate)
    clicked_impressions = random.sample(impressions, min(num_clicks, len(impressions)))
    for imp in clicked_impressions:
        click_event_type = random.choices(
            ["ad_click", "email_click"], weights=[0.75, 0.25]
        )[0]
        click_time = imp["_impression_time"] + timedelta(seconds=random.randint(5, 600))
        events.append({
            "event_time": click_time.isoformat(),
            "event_type": click_event_type,
            "user_id": imp["user_id"],
            "campaign": imp["campaign"],
            "utm_source": imp["utm_source"],
            "utm_medium": imp["utm_medium"],
            "cost": round(random.uniform(0.5, 15), 2),
        })

    # Strip the internal-only helper field before returning.
    for e in events:
        e.pop("_impression_time", None)

    events.sort(key=lambda e: e["event_time"])
    return events


def write_jsonl(filename, records):
    path = OUTPUT_DIR / filename
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"Wrote {len(records):>4} records -> {path}")


if __name__ == "__main__":
    write_jsonl("clickstream.jsonl", generate_clickstream())
    write_jsonl("cart_events.jsonl", generate_cart_events())
    write_jsonl("transactions.jsonl", generate_transactions())
    write_jsonl("ad_events.jsonl", generate_ad_events())
    print("\nSample data generation complete. Files are in ./sample_data/")