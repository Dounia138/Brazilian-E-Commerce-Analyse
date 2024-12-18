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

    df_products_name = pd.read_csv(os.path.join(DATA_LAKE_PATH, "product_name/product_category_name_translation.csv"))
    df_products_select = df_products[['product_id','product_category_name']]
    df_product_translation = pd.merge(df_products_select, df_products_name, how = 'left', on = 'product_category_name')
    df_product_translation = df_product_translation[['product_id','product_category_name_english']]
    df_product_final = df_product_translation.rename(columns = {'product_category_name_english':'category'})

    # Insertion dans PostgreSQL
    engine = create_engine(DATABASE_URL)
    products_clean.to_sql("dim_products", engine, if_exists="append", index=False)
    print("dim_products traitée et chargée avec succès.")


if __name__ == "__main__":
    process_products()
