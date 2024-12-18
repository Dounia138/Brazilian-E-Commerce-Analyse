import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


connection = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)

cursor = connection.cursor()

# Script de création des tables
create_tables_sql = """
-- Table dim_customers: Informations sur les clients
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_unique_id VARCHAR(32),
    customer_city VARCHAR(255),
    customer_state VARCHAR(2)
);

-- Table dim_products: Informations sur les produits
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(255),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT
);

-- Table dim_sellers: Informations sur les vendeurs
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(255),
    seller_state VARCHAR(2)
);

-- Table dim_time: Informations sur le temps (dates, mois, trimestre)
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    weekday INT
);

-- Table fact_sales: Informations sur les ventes
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id SERIAL PRIMARY KEY,
    product_id VARCHAR(32),
    customer_id VARCHAR(32),
    seller_id VARCHAR(32),
    payment_value FLOAT,
    delivery_date DATE,
    order_purchase_date DATE,
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id)
);
-- Table Dimension : dim_time
CREATE TABLE dim_time (
    date DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    week INT,
    day_of_week INT
);
CREATE TABLE IF NOT EXISTS dim_payments (
    payment_id SERIAL PRIMARY KEY,
    order_id VARCHAR(32),
    payment_type VARCHAR(50),
    payment_value FLOAT,
    payment_date TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES dim_orders(order_id)
);
"""

cursor.execute(create_tables_sql)

connection.commit()

cursor.close()
connection.close()

print("Tables créées avec succès dans la base de données.")
