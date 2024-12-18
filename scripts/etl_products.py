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

def process_products():
    print("Traitement de la table dim_products...")
    products = pd.read_csv(
        os.path.join(DATA_LAKE_PATH, "products/olist_products_dataset.csv")
    )

    # Nettoyage des doublons
    products_clean = products.dropna().drop_duplicates()

    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    products_clean.to_sql("dim_products", engine, if_exists="replace", index=False)
    print("dim_products traitée et chargée avec succès.")


if __name__ == "__main__":
    process_products()
