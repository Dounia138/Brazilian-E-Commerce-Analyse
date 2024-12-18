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


def process_customers():
    customers = pd.read_csv(
        os.path.join(DATA_LAKE_PATH, "customers/olist_customers_dataset.csv") , usecols= ["customer_id","customer_city","customer_state"])

    # Nettoyage des données
    customers_clean = customers.dropna().drop_duplicates(subset=["customer_id"])

    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    customers_clean.to_sql("dim_customers", engine, if_exists="append", index=False)
    print("dim_customers traitée et chargée avec succès.")


if __name__ == "__main__":
    process_customers()
