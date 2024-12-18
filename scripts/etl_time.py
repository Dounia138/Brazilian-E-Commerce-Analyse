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

def process_time():
    print("Traitement de la table dim_time...")
    df_order = pd.read_csv(os.path.join(DATA_LAKE_PATH, "orders/olist_orders_dataset.csv"))
    
    df_order['order_date'] = pd.to_datetime(df_order['order_purchase_timestamp'])

    time_df = pd.DataFrame({
        'date_id': df_order['order_date'].dt.date,
        'order_date': df_order['order_date'].dt.date,
        'year': df_order['order_date'].dt.year,
        'month': df_order['order_date'].dt.month,
        'quarter': df_order['order_date'].dt.quarter
    }).drop_duplicates()
    
    
    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    time_df.to_sql('dim_time', engine, if_exists='append', index=False)
    print("dim_time traitée et chargée avec succès.")

if __name__ == "__main__":
    process_time()
