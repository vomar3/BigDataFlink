CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    quantity INTEGER,
    pet_category TEXT,
    weight NUMERIC(12, 2),
    color TEXT,
    size_name TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(3, 1),
    reviews INTEGER,
    release_date TEXT,
    expiry_date TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id TEXT PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    city TEXT,
    state_name TEXT,
    country TEXT,
    phone TEXT,
    email TEXT
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id TEXT PRIMARY KEY,
    supplier_name TEXT,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id TEXT PRIMARY KEY,
    customer_id BIGINT,
    seller_id BIGINT,
    product_id BIGINT,
    store_id TEXT,
    supplier_id TEXT,
    sale_date TEXT,
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12, 2),
    source_file TEXT,
    source_line INTEGER
);
