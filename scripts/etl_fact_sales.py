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
    orders = pd.read_csv(os.path.join(DATA_LAKE_PATH, "orders/olist_orders_dataset.csv", columns = ["order_id","customer_id"]))
    items = pd.read_csv(os.path.join(DATA_LAKE_PATH, "items/olist_order_items_dataset.csv",  columns = ["order_id","product_id", "seller_id", "price", "freight_value", "order_item_id"]))
    payments = pd.read_csv(os.path.join(DATA_LAKE_PATH, "payments/olist_order_payments_dataset.csv",  columns = ["order_id","payment_value"]))

    sales = orders.merge(items, on='order_id', how='full') \
                .merge(payments, on = 'order_id', how='full')
                  
    sales_clean = sales.dropna().drop_duplicates()
    
    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    sales_clean.to_sql('fact_sales', engine, if_exists='append', index=False)
    print("fact_sales traitée et chargée avec succès.")

if __name__ == "__main__":
    process_fact_sales()
