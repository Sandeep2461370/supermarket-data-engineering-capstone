# Databricks notebook source
# MAGIC %md
# MAGIC **Silver Layer Transformation**
# MAGIC
# MAGIC Objective
# MAGIC Read Bronze Delta tables, perform data quality checks and cleansing, and store curated data in the Silver layer.
# MAGIC
# MAGIC **Features**
# MAGIC - Dynamic dataset processing
# MAGIC - Data quality validation
# MAGIC - Null handling
# MAGIC - Duplicate removal
# MAGIC - Delta Lake storage
# MAGIC
# MAGIC **Technologies**
# MAGIC - Azure Databricks
# MAGIC - PySpark
# MAGIC - Delta Lake
# MAGIC - Azure Data Lake Storage Gen2

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

# MAGIC %md
# MAGIC **Read Products from Bronze**

# COMMAND ----------

bronze_products_df = spark.read.format("delta").load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/products")
display(bronze_products_df)
                       
                         

# COMMAND ----------

# MAGIC %md
# MAGIC **Product Table Transformation** 

# COMMAND ----------

from pyspark.sql.functions import *

silver_products_df = bronze_products_df.filter(col("unit_price") > 0)
silver_products_df.write\
                   .mode("overwrite")\
                   .format("delta")\
                   .save(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/products")








# COMMAND ----------

# MAGIC %md
# MAGIC **Stores Table Transformation**

# COMMAND ----------

stores_bronze_df = spark.read.format("delta").load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/stores")
display(stores_bronze_df)

# COMMAND ----------

stores_silver_df = stores_bronze_df.fillna("Unknown", subset=["manager_name"])
display(stores_silver_df)
stores_silver_df.write\
                 .mode("overwrite")\
                     .format("delta")\
                         .save(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/stores")

print("Stored silver data in delta format")


# COMMAND ----------

# MAGIC %md
# MAGIC **Supplier table transformation**

# COMMAND ----------

suppliers_df = spark.read.format("delta").load(
    "abfss://bronze@stsupermarketcapstoness.dfs.core.windows.net/suppliers"
)
display(suppliers_df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StringType

suppliers_silver_df = (
    suppliers_df
    .dropDuplicates()
    .withColumn("phone",regexp_replace(col("phone"), "[^0-9]", ""))
    .withColumn("phone",
        when(
            length(col("phone")) > 10,
            expr("right(phone,10)")
        ).otherwise(col("phone"))
    )
)
display(suppliers_silver_df)

# COMMAND ----------

suppliers_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/suppliers")

# COMMAND ----------

# MAGIC %md
# MAGIC **Inventry Table Transformation**

# COMMAND ----------

inventory_df = spark.read.format("delta").load(f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/inventory")

inventory_df.filter(
    col("quantity_on_hand") < 0
).show()
inventory_silver_df = inventory_df


# COMMAND ----------

inventory_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC **Purchase order table Transformation**

# COMMAND ----------

purchase_orders_df = spark.read.format("delta").load(
    f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/purchaseorders"
)
display(purchase_orders_df)

# COMMAND ----------

from pyspark.sql.functions import *

purchase_orders_silver_df = (
    purchase_orders_df.dropDuplicates()
    .withColumn("delivery_status",
        when(
            col("delivery_date").isNull(),"Cancelled").otherwise("Delivered")
    )
)
display(purchase_orders_silver_df)

# COMMAND ----------

purchase_orders_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        f"abfss://silver@{storage_account_name}.dfs.core.windows.net/purchaseorders"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Silver Table Transformation**
# MAGIC

# COMMAND ----------

sales_df = spark.read.format("delta").load(
    f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/salestransactions"
)

display(sales_df)

# COMMAND ----------

sales_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Duplicate Sales record**

# COMMAND ----------

sales_df.groupBy(
    "store_id",
    "product_id",
    "sale_date",
    "quantity_sold"
).count() \
.filter("count > 1") \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check Invalid Store IDs**
# MAGIC

# COMMAND ----------

sales_df.join(
    stores_silver_df,
    "store_id",
    "left_anti"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check Future Dates**

# COMMAND ----------

sales_df.filter(
    col("sale_date") > current_date()
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Check Amount Consistency**

# COMMAND ----------

sales_df.filter(
    col("total_amount") !=
    (col("quantity_sold") * col("unit_price"))
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Sales Silver Transformation**

# COMMAND ----------

sales_silver_df = (
    sales_df.dropDuplicates(
        [
            "store_id",
            "product_id",
            "sale_date",
            "quantity_sold"
        ]
    )
)
print("Bronze Sales Rows :", sales_df.count())
print("Silver Sales Rows :", sales_silver_df.count())


# COMMAND ----------

sales_silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        f"abfss://silver@{storage_account_name}.dfs.core.windows.net/salestransactions"
    )
