import os
import psycopg2
import subprocess
from dotenv import load_dotenv


load_dotenv()

db_config = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Liste des scripts ETL à exécuter
SCRIPTS = [
    "scripts/etl_customers.py",
    "scripts/etl_products.py",
    "scripts/etl_sellers.py",
    "scripts/etl_time.py",
    "scripts/etl_fact_sales.py",
]

def truncate_tables():
    """Fonction pour vider les tables avant chaque exécution"""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    truncate_queries = [
        "TRUNCATE TABLE dim_customers RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE dim_products RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE dim_sellers RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE dim_time RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE fact_sales RESTART IDENTITY CASCADE;",
    ]

    # Exécution des commandes TRUNCATE
    for query in truncate_queries:
        try:
            print(f"Exécution de la commande: {query}")
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Erreur lors de l'exécution de {query}: {e}")
            conn.rollback()

    # Fermeture de la connexion
    cur.close()
    conn.close()
    print("Tables vidées avec succès.")

def run_etl():
    """Exécution de tous les scripts ETL après avoir vidé les tables"""
    truncate_tables()

    for script in SCRIPTS:
        print(f"Exécution de {script}...")
        subprocess.run(["python", script], check=True)
    
    print("ETL complet exécuté avec succès !")

if __name__ == "__main__":
    run_etl()