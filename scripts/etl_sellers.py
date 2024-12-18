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


def process_sellers():
    sellers = pd.read_csv(
        os.path.join(DATA_LAKE_PATH, "sellers/olist_sellers_dataset.csv", columns = ["seller_id", "seller_city"])
    )

    df_sellers_filtered = df_sellers[['seller_id', 'seller_city']]

    engine = create_engine(DATABASE_URL)
    df_sellers_filtered.to_sql("dim_sellers", engine, if_exists="append", index=False)
    print("dim_sellers traitée et chargée avec succès.")


if __name__ == "__main__":
    process_sellers()
