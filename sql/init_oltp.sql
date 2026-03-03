-- ============================================================
-- OLTP Schema: ecommerce_oltp
-- Simulates a normalized transactional database (3NF)
-- ============================================================

-- ── Sellers ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sellers (
    seller_id   SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    city        VARCHAR(100),
    rating      NUMERIC(2,1) CHECK (rating BETWEEN 1.0 AND 5.0),
    joined_at   TIMESTAMP DEFAULT NOW()
);

-- ── Product Categories ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS categories (
    category_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    parent_id     INT REFERENCES categories(category_id)
);

-- ── Products ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id   SERIAL PRIMARY KEY,
    seller_id    INT NOT NULL REFERENCES sellers(seller_id),
    category_id  INT NOT NULL REFERENCES categories(category_id),
    name         VARCHAR(255) NOT NULL,
    base_price   NUMERIC(12,2) NOT NULL,
    stock        INT DEFAULT 0,
    created_at   TIMESTAMP DEFAULT NOW(),
    is_active    BOOLEAN DEFAULT TRUE
);

-- ── Users ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    user_id      SERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    email        VARCHAR(150) UNIQUE NOT NULL,
    phone        VARCHAR(20),
    city         VARCHAR(100),
    gender       VARCHAR(10) CHECK (gender IN ('male', 'female', 'other')),
    created_at   TIMESTAMP DEFAULT NOW()
);

-- ── Orders ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id       SERIAL PRIMARY KEY,
    user_id        INT NOT NULL REFERENCES users(user_id),
    status         VARCHAR(20) NOT NULL
                       CHECK (status IN ('pending','confirmed','shipped','delivered','cancelled','returned')),
    shipping_fee   NUMERIC(10,2) DEFAULT 0,
    payment_method VARCHAR(30) CHECK (payment_method IN ('cod','credit_card','e_wallet','bank_transfer')),
    created_at     TIMESTAMP DEFAULT NOW(),
    updated_at     TIMESTAMP DEFAULT NOW()
);

-- ── Order Items ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
    item_id      SERIAL PRIMARY KEY,
    order_id     INT NOT NULL REFERENCES orders(order_id),
    product_id   INT NOT NULL REFERENCES products(product_id),
    quantity     INT NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(12,2) NOT NULL   -- snapshotted at order time
);

-- ── Indexes for common query patterns ────────────────────────
CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_products_seller_id     ON products(seller_id);
CREATE INDEX IF NOT EXISTS idx_products_category_id   ON products(category_id);
