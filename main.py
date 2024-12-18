import subprocess

SCRIPTS = [
    "scripts/etl_customers.py",
    "scripts/etl_products.py",
    "scripts/etl_sellers.py",
    #"scripts/etl_time.py",
]

def run_etl():
    for script in SCRIPTS:
        print(f"Exécution de {script}...")
        subprocess.run(["python", script], check=True)
    print("ETL complet exécuté avec succès !")

if __name__ == "__main__":
    run_etl()
