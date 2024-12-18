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
    df_order = pd.read_csv(os.path.join(DATA_LAKE_PATH, "orders/olist_orders_dataset.csv"))
    df_order_item = pd.read_csv(os.path.join(DATA_LAKE_PATH, "items/olist_order_items_dataset.csv"))
    df_payment = pd.read_csv(os.path.join(DATA_LAKE_PATH, "payments/olist_order_payments_dataset.csv"))

    df_sales = pd.merge(df_order_item,df_order, on='order_id', how='outer')
    dff_sales = pd.merge(df_sales,df_payment, on = 'order_id', how = 'outer')
    dff_sales['date_id'] = pd.to_datetime(dff_sales['order_purchase_timestamp']).dt.date
    dff_sales['row_id'] = dff_sales['order_id'].astype(str) + '_' + dff_sales['order_item_id'].astype(str)
    dff_sales = dff_sales[['order_id','order_item_id','product_id','seller_id','date_id','freight_value','price','payment_value','customer_id','row_id']]
    df_sales_rename = dff_sales.rename(columns = {'order_item_id':'order_item','price':'item_price'})


    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    df_sales_rename.to_sql('fact_sales', engine, if_exists='append', index=False)
    print("fact_sales traitée et chargée avec succès.")

if __name__ == "__main__":
    process_fact_sales()
