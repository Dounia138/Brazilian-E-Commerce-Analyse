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
    orders = pd.read_csv(os.path.join(DATA_LAKE_PATH, "orders/olist_orders_dataset.csv"))
    
    # Conversion des timestamps en date
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    time_df = pd.DataFrame({
        'date': orders['order_purchase_timestamp'].dt.date,
        'year': orders['order_purchase_timestamp'].dt.year,
        'month': orders['order_purchase_timestamp'].dt.month,
        'day': orders['order_purchase_timestamp'].dt.day,
        'week': orders['order_purchase_timestamp'].dt.isocalendar().week,
        'day_of_week': orders['order_purchase_timestamp'].dt.dayofweek
    }).drop_duplicates()
    
    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    time_df.to_sql('dim_time', engine, if_exists='replace', index=False)
    print("dim_time traitée et chargée avec succès.")

if __name__ == "__main__":
    process_time()