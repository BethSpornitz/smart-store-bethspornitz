import sqlite3

# Establish connection to SQLite database (creates it if not exists)
conn = sqlite3.connect("data_warehouse.db")
cursor = conn.cursor()

# ---------------------- CREATE DIMENSION TABLES ---------------------- #
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name TEXT NOT NULL,
    region TEXT,
    join_date DATE,
    lifetime_value INTEGER,
    customer_tier TEXT
);
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_category TEXT,
    unit_price REAL,
    brand TEXT,
    manufacturing_cost INTEGER
);
""")

# ---------------------- CREATE FACT TABLE (SALES) ---------------------- #
cursor.execute("""
CREATE TABLE IF NOT EXISTS sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    campaign_id INTEGER,
    sale_amount REAL NOT NULL,
    quantity_sold INTEGER NOT NULL,
    payment_method TEXT NOT NULL,
    sales_channel TEXT NOT NULL,
    sale_date DATE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)

);
""")

# ---------------------- INSERT SAMPLE DATA ---------------------- #
cursor.executemany("""
INSERT INTO customers (customer_id, customer_name, region, join_date, lifetime_value, customer_tier)
VALUES (?, ?, ?, ?, ?, ?)
""", [
    (1, "Alice Johnson", "North", "2021-05-10", 5000, "Gold"),
    (2, "Bob Smith", "South", "2020-07-23", 3000, "Silver"),
])

cursor.executemany("""
INSERT INTO products (product_id, product_name, product_category, unit_price, brand, manufacturing_cost)
VALUES (?, ?, ?, ?, ?, ?)
""", [
    (101, "Laptop", "Electronics", 999.99, "BrandA", 600),
    (102, "Headphones", "Accessories", 199.99, "BrandB", 50),
])




cursor.executemany("""
INSERT INTO sales (customer_id, product_id, store_id, campaign_id, sale_amount, quantity_sold, payment_method, sales_channel, sale_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""", [
    (1, 101, 1, 10, 999.99, 1, "Credit Card", "Online", "2023-12-15"),
    (2, 102, 2, 11, 199.99, 2, "Debit Card", "In-Store", "2023-08-15"),
])

# Commit and close connection
conn.commit()
conn.close()

print("Database and tables created, and sample data inserted successfully!")