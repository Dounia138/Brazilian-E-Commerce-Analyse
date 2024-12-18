import os
import pandas as pd
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
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
