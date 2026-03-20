#!/usr/bin/env python
# coding: utf-8

# In[4]:


import pandas as pd
from pathlib import Path

# Ordner anlegen
Path("../data/bronze").mkdir(parents=True, exist_ok=True)
Path("../data/silver").mkdir(parents=True, exist_ok=True)
Path("../data/gold").mkdir(parents=True, exist_ok=True)


# In[3]:


# Rohdaten laden
df = pd.read_excel(
    "../data/Online Retail.xlsx",
    parse_dates = ["InvoiceDate"],
    dtype={
        "InvoiceNo": "string",
        "StockCode": "string",
        "Description": "string",
        "CustomerID": "string",
        "Country": "string",
    }
)

# print("Raw shape:", df.shape)
# print(df.head())


# In[5]:


# Bronze speichern
df.to_parquet("../data/bronze/online_retail_raw.parquet", engine="pyarrow")


# In[7]:


# df.isnull().sum()


# In[8]:


# Quality Checks
total_rows = len(df)

returns_count = (df["Quantity"] < 0).sum()
negative_price_count = (df["UnitPrice"] <0).sum()
missing_description_count = df["Description"].isna().sum()
missing_customer_count = df["CustomerID"].isna().sum()

print("Returns:", returns_count, f"({returns_count/total_rows:.2%})")
print("Negative prices:", negative_price_count, f"({negative_price_count/total_rows:.2%})")
print("Missing descriptions:", missing_description_count, f"({missing_description_count/total_rows:.2%})")
print("Missing customer IDs:", missing_customer_count, f"({missing_customer_count/total_rows:.2%})")


# In[9]:


# Silver Layer
sales = df[df["Quantity"] > 0].copy()
sales = sales[sales["UnitPrice"] > 0].copy()
sales = sales[sales["Description"].notna()].copy()
sales = sales[sales["CustomerID"].notna()].copy()

sales["Description"] = (
    sales["Description"]
    .str.upper()
    .str.strip()
)

# Kennzahl berechnen
sales["Revenue"] = sales["Quantity"] * sales["UnitPrice"]
#df[["Quantity", "UnitPrice", "Revenue"]].head()


# In[10]:


# Zeitfelder erzeugen
sales["Year"] = sales["InvoiceDate"].dt.year
sales["Month"] = sales["InvoiceDate"].dt.month
sales["Day"] = sales["InvoiceDate"].dt.day
sales["Weekday"] = sales["InvoiceDate"].dt.day_name()


# In[11]:


# Zwischenstand speichern (Silver Layer)
sales.to_parquet("../data/silver/online_retail_clean.parquet", engine="pyarrow")


# In[12]:


# Check auf Zeilenverlust im Silver Layer
print("Raw rows:", len(df))
print("Silver rows:", len(sales))
print("Rows removed:", len(df) - len(sales))


# In[74]:


# print("Silver shape:", sales.shape)


# In[13]:


# Product Consistency Checks
product_desc_check = (
    sales.groupby("StockCode")["Description"].nunique()
)

product_desc_issues = product_desc_check[product_desc_check > 1]

if len(product_desc_issues) > 0:
    print("Products with multiple descriptions:", len(product_desc_issues))
    print(product_desc_issues.head())

# Customer consistency check
customer_country_check = (
    sales.groupby("CustomerID")["Country"].nunique()
)

customer_country_issues = customer_country_check[customer_country_check > 1]

if len(customer_country_issues) > 0:
    print("Customers with multiple countries:", len(customer_country_issues))
    print(customer_country_issues.head())


# In[18]:


# dim_product erstellen
# Zählen wie oft jede Description vorkommt, nach Häufigkeit sortieren und die häufigste Description pro StockCode nehmen
dim_product = (
    sales.groupby(["StockCode", "Description"])
    .size()
    .reset_index(name = "count")
    .sort_values(["StockCode", "count"], ascending = [True, False])
    .drop_duplicates(subset=["StockCode"])
    .drop(columns = "count")
    .reset_index(drop = True)
)
# ProductKey erzeugen
dim_product["ProductKey"] = dim_product.index + 1 
dim_product = dim_product[
    ["ProductKey", "StockCode", "Description"]
]


# In[19]:


# dim_customer erstellen
dim_customer = (
    sales.groupby(["CustomerID", "Country"])
    .size()
    .reset_index(name = "count")
    .sort_values(["CustomerID", "count"], ascending = [True, False])
    .drop_duplicates(subset=["CustomerID"])
    .drop(columns = "count")
    .reset_index(drop=True)
)
# CustomerKey erzeugen
dim_customer["CustomerKey"] = dim_customer.index + 1

dim_customer = dim_customer[
    ["CustomerKey", "CustomerID", "Country"]
]

# dim_customer.head()


# In[20]:


# dim_date erstellen
date_range = pd.date_range(
    start=sales["InvoiceDate"].min().normalize(),
    end=sales["InvoiceDate"].max().normalize(),
    freq="D"
)

dim_date = pd.DataFrame({"Date": date_range})

# DateKey erzeugen
dim_date["DateKey"] = dim_date["Date"].dt.strftime("%Y%m%d").astype(int)
dim_date["Year"] = dim_date["Date"].dt.year
dim_date["Quarter"] = dim_date["Date"].dt.quarter

dim_date["MonthNumber"] = dim_date["Date"].dt.month
dim_date["MonthName"] = dim_date["Date"].dt.month_name()

dim_date["Day"] = dim_date["Date"].dt.day

dim_date["YearMonth"] = dim_date["Date"].dt.strftime("%Y-%m")
dim_date["Weekday"] = dim_date["Date"].dt.day_name()
dim_date["WeekdayNumber"] = dim_date["Date"].dt.weekday
dim_date["IsWeekend"] = dim_date["WeekdayNumber"].isin([5,6])

dim_date = dim_date[
    ["DateKey", "Date", "Year", "Quarter", "MonthNumber", "MonthName", "YearMonth", "Day", "Weekday", "WeekdayNumber", "IsWeekend"]
]

# dim_date.head()


# In[21]:


# fact_sales erstellen
fact_sales = sales[
    [
    "InvoiceNo",
    "StockCode",
    "CustomerID",
    "InvoiceDate",
    "Quantity",
    "UnitPrice",
    "Revenue"
    ]
].copy()


# In[22]:


# fact_sales mit Keys verbinden
fact_sales = fact_sales.merge(
    dim_product[["ProductKey", "StockCode"]],
    on = "StockCode",
    how ="left"
)


# In[23]:


fact_sales = fact_sales.merge(
    dim_customer[["CustomerKey", "CustomerID"]],
    on = "CustomerID",
    how = "left"
)


# In[24]:


fact_sales["DateKey"] = fact_sales["InvoiceDate"].dt.floor("D").dt.strftime("%Y%m%d").astype(int)


# In[25]:


fact_sales = fact_sales[
    [
        "InvoiceNo",
        "DateKey",
        "ProductKey",
        "CustomerKey",
        "Quantity",
        "UnitPrice",
        "Revenue",
    ]
].copy()
# fact_sales.head()


# In[26]:


# fact_sales[["ProductKey", "CustomerKey", "DateKey"]].isna().sum()


# In[27]:


# Data Quality Checks 
print("Sales shape:", sales.shape)
print("fact_sales shape:", fact_sales.shape)

# Pipeline Integrity: Row Count Check Gehen Zeilen verloren bei der Transformation?
assert len(sales) == len(fact_sales), "Row count mismatch between sales and fact_sales"

# Foreign Key Check
assert fact_sales["ProductKey"].isna().sum() == 0, "Missing ProductKey detected"
assert fact_sales["CustomerKey"].isna().sum() == 0, "Missing CustomerKey detected"
assert fact_sales["DateKey"].isna().sum() == 0, "Missing DateKey detected"

# Business Rules
assert fact_sales["Quantity"].min() > 0 # keine Returns
assert fact_sales["UnitPrice"].min() > 0 # keine negativen Preise

# Dimension Checks
assert dim_product["StockCode"].duplicated().sum() == 0
assert dim_customer["CustomerID"].duplicated().sum() == 0
assert dim_date["DateKey"].duplicated().sum() == 0


# In[28]:


dim_product = dim_product.sort_values("ProductKey").reset_index(drop=True)
dim_customer = dim_customer.sort_values("CustomerKey").reset_index(drop=True)
dim_date = dim_date.sort_values("DateKey").reset_index(drop=True)
fact_sales = fact_sales.sort_values(["DateKey", "InvoiceNo"]).reset_index(drop=True)


# In[29]:


dim_product.to_parquet("../data/gold/dim_product.parquet", engine = "pyarrow")
dim_customer.to_parquet("../data/gold/dim_customer.parquet", engine = "pyarrow")
dim_date.to_parquet("../data/gold/dim_date.parquet", engine = "pyarrow")
fact_sales.to_parquet("../data/gold/fact_sales.parquet", engine="pyarrow")


# In[ ]:




