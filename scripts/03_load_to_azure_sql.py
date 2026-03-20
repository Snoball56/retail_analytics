#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
import urllib
import pyodbc

GOLD_PATH = Path("../data/gold")

SERVER = "retail-sql-server12465.database.windows.net"
DATABASE = "retail-analytics-db"
USERNAME = "sqladmin"
PASSWORD = "YOUR_PASSWORD"

LOAD_DIMENSIONS = True
LOAD_FACT = False
CLEAR_FACT_BEFORE_LOAD = False

# Daten laden

dim_product = pd.read_parquet(GOLD_PATH / "dim_product.parquet")
dim_customer = pd.read_parquet(GOLD_PATH / "dim_customer.parquet")
dim_date = pd.read_parquet(GOLD_PATH / "dim_date.parquet")
fact_sales = pd.read_parquet(GOLD_PATH / "fact_sales.parquet")

print("Lokale Daten geladen:")
print(f"dim_product:  {dim_product.shape}")
print(f"dim_customer: {dim_customer.shape}")
print(f"dim_date:     {dim_date.shape}")
print(f"fact_sales:   {fact_sales.shape}")

print("\nVerfügbare ODBC-Treiber:")
print(pyodbc.drivers())

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER=tcp:{SERVER},1433;"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=60;"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)

# Verbindung testen

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(f"\nVerbindungstest erfolgreich: {result.scalar()}")

# Hilfsfunktion

def get_table_count(table_name: str) -> int:
    query = f"SELECT COUNT(*) AS cnt FROM {table_name}"
    with engine.connect() as conn:
        return int(pd.read_sql(query, conn).iloc[0, 0])


# Dimensionstabellen laden

if LOAD_DIMENSIONS:
    print("\nPrüfe Dimensionstabellen ...")

    dim_product_count = get_table_count("dim_product")
    dim_customer_count = get_table_count("dim_customer")
    dim_date_count = get_table_count("dim_date")

    print(f"dim_product vorhanden:  {dim_product_count} Zeilen")
    print(f"dim_customer vorhanden: {dim_customer_count} Zeilen")
    print(f"dim_date vorhanden:     {dim_date_count} Zeilen")

    if dim_product_count == 0:
        dim_product.to_sql("dim_product", engine, if_exists="append", index=False)
        print("dim_product geladen.")
    else:
        print("dim_product übersprungen, da bereits Daten vorhanden sind.")

    if dim_customer_count == 0:
        dim_customer.to_sql("dim_customer", engine, if_exists="append", index=False)
        print("dim_customer geladen.")
    else:
        print("dim_customer übersprungen, da bereits Daten vorhanden sind.")

    if dim_date_count == 0:
        dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
        print("dim_date geladen.")
    else:
        print("dim_date übersprungen, da bereits Daten vorhanden sind.")

if LOAD_FACT:
    engine.dispose()

    current_fact_count = get_table_count("fact_sales")
    print(f"\nAktueller Stand fact_sales in SQL: {current_fact_count} Zeilen")

    if CLEAR_FACT_BEFORE_LOAD:
        print("Leere fact_sales vor dem Laden ...")
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM fact_sales"))
        print("fact_sales wurde geleert.")

    elif current_fact_count > 0:
        print(
            "Warnung: fact_sales enthält bereits Daten. "
            "Ein erneutes Laden mit 'append' erzeugt Dubletten."
        )

    fact_sales.to_sql(
        "fact_sales",
        engine,
        if_exists="append",
        index=False,
        chunksize=200
    )

    print("fact_sales geladen.")


engine.dispose()

print("\nZeilenanzahlen nach dem Laden:")
print(f"dim_product:  {get_table_count('dim_product')}")
print(f"dim_customer: {get_table_count('dim_customer')}")
print(f"dim_date:     {get_table_count('dim_date')}")
print(f"fact_sales:   {get_table_count('fact_sales')}")

print("\nUpload nach Azure SQL abgeschlossen.")


# In[ ]:




