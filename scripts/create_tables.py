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
    customer_id VARCHAR(255) PRIMARY KEY,
    customer_city VARCHAR(255),
    customer_state VARCHAR(255)
);

-- Table dim_products: Informations sur les produits
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(255) PRIMARY KEY,
    category VARCHAR(255)
);

-- Table dim_sellers: Informations sur les vendeurs
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id VARCHAR(255) PRIMARY KEY,
    seller_city VARCHAR(255)
);

-- Table dim_time: Informations sur le temps (dates, mois, trimestre)
CREATE TABLE IF NOT EXISTS dim_time (
    date_id VARCHAR PRIMARY KEY,
    order_date DATE,
    year INT,
    quarter VARCHAR(10),
    month VARCHAR(20)
);

-- Table fact_sales: Informations sur les ventes
CREATE TABLE IF NOT EXISTS fact_sales (
    order_id VARCHAR(255) PRIMARY KEY, 
    date_id VARCHAR(255),
    product_id VARCHAR(255),
    customer_id VARCHAR(255),
    seller_id VARCHAR(255),
    order_item INT,
    payment_value FLOAT,
    item_price FLOAT,
    freight_value FLOAT,
    quantity INT DEFAULT '1',
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id), 
    FOREIGN KEY (date_id) REFERENCES dim_time(date_id)
);

"""

cursor.execute(create_tables_sql)

connection.commit()

cursor.close()
connection.close()

print("Tables créées avec succès dans la base de données.")
