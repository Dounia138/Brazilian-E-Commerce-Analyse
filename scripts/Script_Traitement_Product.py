#!/usr/bin/env python
# coding: utf-8

# In[151]:


#Importation des librairies

import os
import pandas as pd
from sqlalchemy import create_engine, text
#pip install psycopg2
import psycopg2


# In[153]:


#Chemin
datalake_path = 'Datalake'
products_path = '/products/olist_products_dataset.csv'
products_name = '/product_name/product_category_name_translation.csv'

#Création des dataframe
df_products = pd.read_csv(datalake_path + products_path)
df_products_name = pd.read_csv(datalake_path + products_name)


# In[155]:


#Compte des valeurs nulls
df_products.info()


# In[157]:


#Compte des valeurs uniques par colonne
nombre_uniques_par_colonne = df_products.apply(pd.Series.nunique)
print("Nombre de lignes total df_products : ", df_products.shape[0])
print("Nombre de valeurs uniques par colonne :")
print(nombre_uniques_par_colonne)


# In[159]:


#Selection des colonnes à garder : 
df_products_select = df_products[['product_id','product_category_name']]


# In[173]:


#Merge avec le dataset name_translation pour récupérer les noms anglais
df_product_translation = pd.merge(df_products_select, df_products_name, how = 'left', on = 'product_category_name')


# In[179]:


#Selection des colonnes à garder
df_product_translation = df_product_translation[['product_id','product_category_name_english']]


# In[189]:


df_product_final = df_product_translation.rename(columns = {'product_category_name_english':'category'})


# In[ ]:


# Informations de connexion
db_config = {
    "dbname": "olist_dwh",
    "user": "postgres",
    "password": "Taha123",
    "host": "localhost",
    "port": 5432
}


# Connexion à la base de données
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

    # Étape 1 : Récupérer les données sources
select_query = """
TRUNCATE TABLE dim_products; 
"""
cur.execute(select_query)

# Connexion à PostgreSQL
engine = create_engine('postgresql+psycopg2://postgres:Taha123@localhost/olist_dwh')
with engine.connect() as conn:
    df_product_final.to_sql('dim_products', conn, if_exists='append', index=False)

