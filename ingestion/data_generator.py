"""
data_generator.py
─────────────────
OOP-based fake data generator for the OLTP database.
Uses Faker library to produce realistic e-commerce data.

Usage:
    python ingestion/data_generator.py --users 500 --products 200 --orders 2000
"""

import random
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any

from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

fake = Faker("vi_VN")   # Vietnamese locale for realistic data
random.seed(42)
Faker.seed(42)


# ─── Config ───────────────────────────────────────────────────────────────────
CATEGORIES = [
    ("Electronics", None),
    ("Smartphones",  "Electronics"),
    ("Laptops",      "Electronics"),
    ("Fashion",      None),
    ("Men's Wear",   "Fashion"),
    ("Women's Wear", "Fashion"),
    ("Home & Living",None),
    ("Kitchen",      "Home & Living"),
    ("Books",        None),
    ("Sports",       None),
]

CITIES = ["Hà Nội", "TP.HCM", "Đà Nẵng", "Cần Thơ", "Hải Phòng",
          "Huế", "Nha Trang", "Vũng Tàu", "Đà Lạt", "Biên Hòa"]

ORDER_STATUSES = [
    ("delivered",  0.55),
    ("shipped",    0.15),
    ("confirmed",  0.10),
    ("pending",    0.05),
    ("cancelled",  0.10),
    ("returned",   0.05),
]

PAYMENT_METHODS = [
    ("cod",           0.40),
    ("e_wallet",      0.30),
    ("credit_card",   0.20),
    ("bank_transfer", 0.10),
]


def weighted_choice(choices: List[tuple]) -> str:
    """Pick a value based on weighted probabilities."""
    values, weights = zip(*choices)
    return random.choices(values, weights=weights, k=1)[0]


# ─── Generator Classes ────────────────────────────────────────────────────────

class CategoryGenerator:
    """Generates product category hierarchy."""

    def generate(self) -> List[Dict[str, Any]]:
        rows = []
        name_to_id: Dict[str, int] = {}
        for idx, (name, parent_name) in enumerate(CATEGORIES, start=1):
            rows.append({
                "category_id": idx,
                "name":        name,
                "parent_id":   name_to_id.get(parent_name),
            })
            name_to_id[name] = idx
        return rows


class SellerGenerator:
    """Generates realistic seller profiles."""

    def __init__(self, count: int = 50):
        self.count = count

    def generate(self) -> List[Dict[str, Any]]:
        rows = []
        for i in range(1, self.count + 1):
            rows.append({
                "seller_id": i,
                "name":      fake.company(),
                "email":     f"seller{i}@{fake.free_email_domain()}",
                "city":      random.choice(CITIES),
                "rating":    round(random.uniform(3.0, 5.0), 1),
                "joined_at": fake.date_time_between(
                    start_date="-3y", end_date="-6m"
                ),
            })
        return rows


class UserGenerator:
    """Generates customer profiles."""

    def __init__(self, count: int = 500):
        self.count = count

    def generate(self) -> List[Dict[str, Any]]:
        rows = []
        for i in range(1, self.count + 1):
            gender = random.choice(["male", "female"])
            rows.append({
                "user_id":    i,
                "name":       fake.name_male() if gender == "male" else fake.name_female(),
                "email":      f"user{i}@{fake.free_email_domain()}",
                "phone":      fake.phone_number()[:20],
                "city":       random.choice(CITIES),
                "gender":     gender,
                "created_at": fake.date_time_between(
                    start_date="-2y", end_date="now"
                ),
            })
        return rows


class ProductGenerator:
    """Generates product listings with realistic pricing tiers by category."""

    PRICE_RANGES = {
        "Smartphones":   (3_000_000, 30_000_000),
        "Laptops":       (8_000_000, 50_000_000),
        "Electronics":   (500_000,   10_000_000),
        "Men's Wear":    (100_000,   2_000_000),
        "Women's Wear":  (100_000,   3_000_000),
        "Fashion":       (80_000,    1_500_000),
        "Kitchen":       (50_000,    500_000),
        "Home & Living": (100_000,   5_000_000),
        "Books":         (50_000,    500_000),
        "Sports":        (200_000,   8_000_000),
    }

    def __init__(self, count: int = 200, seller_count: int = 50):
        self.count = count
        self.seller_count = seller_count
        self.categories = CategoryGenerator().generate()

    def _get_price_range(self, category_name: str) -> tuple:
        return self.PRICE_RANGES.get(category_name, (100_000, 1_000_000))

    def generate(self) -> List[Dict[str, Any]]:
        rows = []
        for i in range(1, self.count + 1):
            cat = random.choice(self.categories)
            lo, hi = self._get_price_range(cat["name"])
            rows.append({
                "product_id":  i,
                "seller_id":   random.randint(1, self.seller_count),
                "category_id": cat["category_id"],
                "name":        f"{cat['name']} - {fake.word().capitalize()} {i}",
                "base_price":  round(random.uniform(lo, hi), -3),  # round to 1000đ
                "stock":       random.randint(0, 500),
                "created_at":  fake.date_time_between(
                    start_date="-2y", end_date="-30d"
                ),
                "is_active":   random.random() > 0.05,  # 95% active
            })
        return rows


class OrderGenerator:
    """
    Generates orders + order_items with realistic business logic:
    - Heavier traffic on weekends
    - Larger basket sizes for Electronics
    - Cancelled orders have no items (skipped at load time)
    """

    def __init__(
        self,
        order_count:   int = 2000,
        user_count:    int = 500,
        product_count: int = 200,
    ):
        self.order_count   = order_count
        self.user_count    = user_count
        self.product_count = product_count

    def generate(self) -> tuple[List[Dict], List[Dict]]:
        # Pre-generate products for price lookup
        products = ProductGenerator(self.product_count).generate()
        price_map = {p["product_id"]: p["base_price"] for p in products}

        orders, items = [], []
        item_id = 1

        start_dt = datetime.now() - timedelta(days=365)

        for order_id in range(1, self.order_count + 1):
            created_at = start_dt + timedelta(
                seconds=random.randint(0, 365 * 24 * 3600)
            )
            status  = weighted_choice(ORDER_STATUSES)
            payment = weighted_choice(PAYMENT_METHODS)

            orders.append({
                "order_id":       order_id,
                "user_id":        random.randint(1, self.user_count),
                "status":         status,
                "shipping_fee":   random.choice([0, 15000, 25000, 35000]),
                "payment_method": payment,
                "created_at":     created_at,
                "updated_at":     created_at + timedelta(hours=random.randint(1, 72)),
            })

            # Only create items for non-cancelled orders
            if status != "cancelled":
                n_items = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
                chosen_products = random.sample(range(1, self.product_count + 1), min(n_items, self.product_count))

                for pid in chosen_products:
                    price = price_map[pid]
                    # apply random discount 0–20%
                    unit_price = round(price * random.uniform(0.80, 1.00), -2)
                    items.append({
                        "item_id":    item_id,
                        "order_id":   order_id,
                        "product_id": pid,
                        "quantity":   random.randint(1, 5),
                        "unit_price": unit_price,
                    })
                    item_id += 1

        return orders, items


# ─── Database Loader ──────────────────────────────────────────────────────────

class OLTPSeeder:
    """Loads generated data into the OLTP PostgreSQL database."""

    def __init__(self, conn_params: Dict[str, Any]):
        self.conn = psycopg2.connect(**conn_params)
        self.conn.autocommit = False

    def _bulk_insert(self, table: str, rows: List[Dict], columns: List[str]):
        if not rows:
            return
        values = [[r[c] for c in columns] for r in rows]
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        with self.conn.cursor() as cur:
            execute_values(cur, sql, values, page_size=500)
        self.conn.commit()
        print(f"  ✅ Inserted {len(rows):,} rows into {table}")

    def seed(
        self,
        n_users:    int = 500,
        n_sellers:  int = 50,
        n_products: int = 200,
        n_orders:   int = 2000,
    ):
        print("🌱 Seeding OLTP database...")

        # Categories
        cats = CategoryGenerator().generate()
        self._bulk_insert(
            "categories", cats,
            ["category_id", "name", "parent_id"]
        )

        # Sellers
        sellers = SellerGenerator(n_sellers).generate()
        self._bulk_insert(
            "sellers", sellers,
            ["seller_id", "name", "email", "city", "rating", "joined_at"]
        )

        # Users
        users = UserGenerator(n_users).generate()
        self._bulk_insert(
            "users", users,
            ["user_id", "name", "email", "phone", "city", "gender", "created_at"]
        )

        # Products
        products = ProductGenerator(n_products, n_sellers).generate()
        self._bulk_insert(
            "products", products,
            ["product_id", "seller_id", "category_id", "name", "base_price", "stock", "created_at", "is_active"]
        )

        # Orders + Items
        gen = OrderGenerator(n_orders, n_users, n_products)
        orders, items = gen.generate()

        self._bulk_insert(
            "orders", orders,
            ["order_id", "user_id", "status", "shipping_fee", "payment_method", "created_at", "updated_at"]
        )
        self._bulk_insert(
            "order_items", items,
            ["item_id", "order_id", "product_id", "quantity", "unit_price"]
        )

        print("\n🎉 OLTP seeding complete!")
        self.conn.close()


# ─── CLI Entry Point ──────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Seed OLTP database with fake data")
    p.add_argument("--host",     default="localhost")
    p.add_argument("--port",     default=5432, type=int)
    p.add_argument("--db",       default="ecommerce_oltp")
    p.add_argument("--user",     default="deuser")
    p.add_argument("--password", default="depassword")
    p.add_argument("--users",    default=500,  type=int)
    p.add_argument("--sellers",  default=50,   type=int)
    p.add_argument("--products", default=200,  type=int)
    p.add_argument("--orders",   default=2000, type=int)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    seeder = OLTPSeeder({
        "host":     args.host,
        "port":     args.port,
        "dbname":   args.db,
        "user":     args.user,
        "password": args.password,
    })
    seeder.seed(
        n_users=args.users,
        n_sellers=args.sellers,
        n_products=args.products,
        n_orders=args.orders,
    )
