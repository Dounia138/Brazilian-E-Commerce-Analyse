import os
import pandas as pd
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
DATA_LAKE_PATH = "../DataLake"

def process_fact_sales():
    print("Traitement de la table fact_sales...")
    orders = pd.read_csv(os.path.join(DATA_LAKE_PATH, "orders/olist_orders_dataset.csv"))
    payments = pd.read_csv(os.path.join(DATA_LAKE_PATH, "payments/olist_order_payments_dataset.csv"))
    customers = pd.read_csv(os.path.join(DATA_LAKE_PATH, "customers/olist_customers_dataset.csv"))
    products = pd.read_csv(os.path.join(DATA_LAKE_PATH, "products/olist_products_dataset.csv"))
    
    sales = orders.merge(payments, on='order_id', how='left') \
                  .merge(customers, on='customer_id', how='left') \
                  .merge(products, on='product_id', how='left')
    
    sales_clean = sales.dropna().drop_duplicates()
    
    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    sales_clean.to_sql('fact_sales', engine, if_exists='replace', index=False)
    print("fact_sales traitée et chargée avec succès.")

if __name__ == "__main__":
    process_fact_sales()
