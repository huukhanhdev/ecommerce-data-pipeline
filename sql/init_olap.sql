-- ============================================================
-- ClickHouse OLAP: staging (raw copies) + analytics (dbt output)
-- ============================================================

CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS analytics;

-- ── DATABASE: staging ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.stg_users (
    user_id     Int32,
    name        String,
    email       String,
    phone       String,
    city        String,
    gender      String,
    created_at  DateTime,
    _loaded_at  DateTime
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS staging.stg_sellers (
    seller_id   Int32,
    name        String,
    email       String,
    city        String,
    rating      Decimal(3,1),
    joined_at   DateTime,
    _loaded_at  DateTime
) ENGINE = MergeTree()
ORDER BY seller_id;

CREATE TABLE IF NOT EXISTS staging.stg_categories (
    category_id Int32,
    name        String,
    parent_id   Nullable(Int32),
    _loaded_at  DateTime
) ENGINE = MergeTree()
ORDER BY category_id;

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id  Int32,
    seller_id   Int32,
    category_id Int32,
    name        String,
    base_price  Decimal(12,2),
    stock       Int32,
    created_at  DateTime,
    is_active   UInt8,
    _loaded_at  DateTime
) ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    order_id        Int32,
    user_id         Int32,
    status          String,
    shipping_fee    Decimal(10,2),
    payment_method  String,
    created_at      DateTime,
    updated_at      DateTime,
    _loaded_at      DateTime
) ENGINE = MergeTree()
ORDER BY order_id;

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    item_id     Int32,
    order_id    Int32,
    product_id  Int32,
    quantity    Int32,
    unit_price  Decimal(12,2),
    _loaded_at  DateTime
) ENGINE = MergeTree()
ORDER BY item_id;
