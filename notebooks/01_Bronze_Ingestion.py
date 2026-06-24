# Bronze Layer Ingestion

#Objective:
#Read raw CSV files from Azure Data Lake Storage Gen2 and convert them into Delta tables in the Bronze layer using PySpark.

#Features:
#- Parameterized notebook
#- Dynamic folder and file processing
#- Delta Lake storage format
#- ADF integration using notebook parameters
# Databricks notebook source
# MAGIC %md
# MAGIC **Configure ADLS Access in Notebook**

# COMMAND ----------

# Retrieved from Azure Key Vault in production
storage_account_name = "<storage_account_name>"
storage_account_key = "<storage_account_key>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("ADLS Connection Configured Successfully")

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://raw@stsupermarketcapstoness.dfs.core.windows.net/"
    )
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "abfss://raw@stsupermarketcapstoness.dfs.core.windows.net/Products/"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading data from source**

# COMMAND ----------

products_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://raw@stsupermarketcapstoness.dfs.core.windows.net/Products/products.csv")


# COMMAND ----------

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Standard Validation of the Data**

# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

print("Total rows :",products_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC **Write Data to Bronze Layer**

# COMMAND ----------

products_df.write\
           .format("delta")\
           .mode("overwrite")\
           .save("abfss://bronze@stsupermarketcapstoness.dfs.core.windows.net/products")
          
               

# COMMAND ----------

product_df = spark.read.format("delta").load("abfss://bronze@stsupermarketcapstoness.dfs.core.windows.net/products")
display(product_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Automating the Ingestion using workflow**

# COMMAND ----------

dbutils.widgets.text("folder_name","")
dbutils.widgets.text("file_name","")
folder_name = dbutils.widgets.get("folder_name")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"abfss://raw@stsupermarketcapstoness.dfs.core.windows.net/{folder_name}/{file_name}")

print(f"Reading: {folder_name}/{file_name}")

# COMMAND ----------

df.write\
        .format("delta")\
        .mode("overwrite")\
        .save(f"abfss://bronze@stsupermarketcapstoness.dfs.core.windows.net/{folder_name.lower()}")

print(f"Successfully loaded {folder_name} into Bronze layer")
