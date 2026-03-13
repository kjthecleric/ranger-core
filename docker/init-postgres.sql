-- =============================================================================
-- Ranger Dev: Seed data for PostgreSQL relational source testing
-- =============================================================================

-- Sample orders table
CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    customer_id INTEGER      NOT NULL,
    product     VARCHAR(128) NOT NULL,
    quantity    INTEGER      NOT NULL DEFAULT 1,
    price       NUMERIC(10,2) NOT NULL,
    status      VARCHAR(32)  NOT NULL DEFAULT 'pending',
    order_date  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Sample customers table
CREATE TABLE IF NOT EXISTS customers (
    id         SERIAL PRIMARY KEY,
    name       VARCHAR(128) NOT NULL,
    email      VARCHAR(256) UNIQUE NOT NULL,
    region     VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Insert seed customers
INSERT INTO customers (name, email, region) VALUES
    ('Alice Johnson',  'alice@example.com',   'US-West'),
    ('Bob Smith',      'bob@example.com',     'US-East'),
    ('Carlos Garcia',  'carlos@example.com',  'EU-South'),
    ('Diana Lee',      'diana@example.com',   'APAC'),
    ('Evan Williams',  'evan@example.com',    'US-Central');

-- Insert seed orders
INSERT INTO orders (customer_id, product, quantity, price, status, order_date) VALUES
    (1, 'Widget A',    2,  19.99, 'shipped',   NOW() - INTERVAL '5 days'),
    (1, 'Widget B',    1,  49.99, 'delivered',  NOW() - INTERVAL '10 days'),
    (2, 'Gadget X',    3,  12.50, 'pending',    NOW() - INTERVAL '1 day'),
    (3, 'Widget A',    5,  19.99, 'shipped',    NOW() - INTERVAL '3 days'),
    (3, 'Gadget Y',    1, 149.00, 'processing', NOW() - INTERVAL '2 days'),
    (4, 'Widget C',   10,   9.99, 'delivered',  NOW() - INTERVAL '15 days'),
    (4, 'Gadget X',    2,  12.50, 'shipped',    NOW() - INTERVAL '7 days'),
    (5, 'Widget B',    1,  49.99, 'pending',    NOW()),
    (5, 'Gadget Z',    4,  34.95, 'processing', NOW() - INTERVAL '4 hours'),
    (2, 'Widget A',    1,  19.99, 'delivered',  NOW() - INTERVAL '20 days');

-- Enable logical replication for Debezium CDC
-- (requires wal_level=logical in postgresql.conf — the default Postgres image
--  supports ALTER SYSTEM if needed)
ALTER SYSTEM SET wal_level = 'logical';

-- Grant replication privileges (for Debezium connector)
-- NOTE: Debezium will use the 'ranger' superuser by default in dev mode.

-- Helpful view for testing discover_schema()
CREATE VIEW order_summary AS
    SELECT
        c.name        AS customer_name,
        c.region,
        COUNT(o.id)   AS total_orders,
        SUM(o.price * o.quantity) AS total_revenue
    FROM orders o
    JOIN customers c ON c.id = o.customer_id
    GROUP BY c.name, c.region;
