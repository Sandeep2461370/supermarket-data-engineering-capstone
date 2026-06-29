# Gold Layer Transformation

## Objective
#Create business-ready Delta tables from the Silver layer.

### Gold Tables

#1. gold_store_sales
#2. gold_product_sales
#3. gold_inventory
#4. gold_supplier_performance
#5. gold_monthly_sales 

storage_account_name = "<storage_account_name>"
storage_account_key = "<storage_account_key>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

print("ADLS Connection Configured Successfully")

sales_df = spark.read.format("delta").load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/salestransactions")
products_df = spark.read.format("delta").load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/products")
stores_df = spark.read.format("delta").load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/stores")
orders_df = spark.read.format("delta").load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/purchaseorders")
inventry_df = spark.read.format("delta").load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/inventory")
supplier_df = spark.read.format("delta").load(f"abfss://silver@{storage_account_name}.dfs.core.windows.net/suppliers")

sales_df.createOrReplaceTempView("salestransactions")
products_df.createOrReplaceTempView("products")
stores_df.createOrReplaceTempView("stores")
orders_df.createOrReplaceTempView("purchaseorders")
inventry_df.createOrReplaceTempView("inventory")
supplier_df.createOrReplaceTempView("suppliers")

### Gold Table 1 : gold_sales_summary

#Calculating the total chain revenue which is required for market share
from pyspark.sql.functions import *

total_chain_revenue = (
    sales_df
    .agg(sum("total_amount").alias("total_revenue"))
    .collect()[0]["total_revenue"]
)

print(f"Total Chain Revenue : {total_chain_revenue}")




gold_sales_summary_df = sales_df \
    .join(stores_df, on="store_id", how="inner") \
    .groupBy(
        "store_id",
        "store_name",
        "location"
    ) \
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_transactions"),
        round(avg("total_amount"), 2).alias("average_transaction_value")
    ) \
    .withColumn(
        "market_share_percentage",
        round(
            (col("total_revenue") / lit(total_chain_revenue)) * 100,
            2
        )
    ) \
    .orderBy(desc("total_revenue"))

display(gold_sales_summary_df)

print("Total Stores :", gold_sales_summary_df.count())

gold_sales_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"abfss://gold@{storage_account_name}.dfs.core.windows.net/gold_sales_summary")

### Gold Table 2 : gold_product_performance

#Calculating the number of days covered by your sales data.

from pyspark.sql.functions import *

sales_period = sales_df.select(
    datediff(
        max("sale_date"),
        min("sale_date")
    ).alias("days")
).collect()[0]["days"] + 1

print(f"Sales Period : {sales_period} days")




gold_product_performance_df = sales_df \
    .join(products_df, on="product_id", how="inner") \
    .join(supplier_df, on="supplier_id", how="inner") \
    .groupBy(
        "product_id",
        "product_name",
        "category",
        "supplier_name"
    ) \
    .agg(
        sum("quantity_sold").alias("total_quantity_sold"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_transactions"),
        round(avg("quantity_sold"), 2).alias("average_quantity_per_transaction")
    ) \
    .withColumn(
        "sales_velocity",
        round(
            col("total_quantity_sold") / lit(sales_period),
            2
        )
    ) \
    .orderBy(desc("total_quantity_sold"))

display(gold_product_performance_df)

print("Total Products :", gold_product_performance_df.count())

gold_product_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        f"abfss://gold@{storage_account_name}.dfs.core.windows.net/gold_product_performance"
    )

### Gold Table 3 : gold_monthly_sales

gold_monthly_sales_df = sales_df \
    .join(stores_df, on="store_id", how="inner") \
    .withColumn(
        "sales_month",
        date_format(col("sale_date"), "MMMM")
    ) \
    .withColumn(
        "month_number",
        month(col("sale_date"))
    ) \
    .groupBy(
        "store_id",
        "store_name",
        "location",
        "sales_month",
        "month_number"
    ) \
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_transactions")
    ) \
    .orderBy(
        "store_id",
        "month_number"
    )

display(gold_monthly_sales_df)

print("Total Records :", gold_monthly_sales_df.count())

gold_monthly_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        f"abfss://gold@{storage_account_name}.dfs.core.windows.net/gold_monthly_sales"
    )

### Gold Table 4 : gold_inventory_status

#Creating total quantity sold for each Store + Product.

sales_summary_df = sales_df \
    .groupBy(
        "store_id",
        "product_id"
    ) \
    .agg(
        sum("quantity_sold").alias("total_quantity_sold")
    )

gold_inventory_status_df = inventry_df \
    .join(
        products_df,
        on="product_id",
        how="inner"
    ) \
    .join(
        stores_df,
        on="store_id",
        how="inner"
    ) \
    .join(
        sales_summary_df,
        on=["store_id", "product_id"],
        how="left"
    ) \
    .fillna(
        {"total_quantity_sold": 0}
    ) \
    .withColumn(
        "sales_velocity",
        round(
            col("total_quantity_sold") / lit(sales_period),
            2
        )
    ) \
    .withColumn(
        "inventory_status",
        when(col("quantity_on_hand") == 0, "Out of Stock")
        .when(col("quantity_on_hand") < 50, "Low Stock")
        .otherwise("In Stock")
    ) \
    .withColumn(
        "reorder_point",
        round(
            (col("sales_velocity") * 7) + 20,
            2
        )
    ) \
    .orderBy(
        "store_id",
        "product_id"
    )

display(gold_inventory_status_df)

print("Total Records :", gold_inventory_status_df.count())

gold_inventory_status_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        f"abfss://gold@{storage_account_name}.dfs.core.windows.net/gold_inventory_status"
    )


### Gold Table 5 : gold_supplier_performance

gold_supplier_performance_df = orders_df \
    .join(
        supplier_df,
        on="supplier_id",
        how="inner"
    ) \
    .withColumn(
        "delivery_days",
        datediff(
            col("delivery_date"),
            col("order_date")
        )
    ) \
    .groupBy(
        "supplier_id",
        "supplier_name"
    ) \
    .agg(
        count("po_id").alias("total_orders"),

        sum(
            when(
                col("delivery_status") == "Delivered",
                1
            ).otherwise(0)
        ).alias("delivered_orders"),

        sum(
            when(
                col("delivery_status") == "Cancelled",
                1
            ).otherwise(0)
        ).alias("cancelled_orders"),

        round(
            avg("delivery_days"),
            2
        ).alias("average_delivery_days")
    ) \
    .withColumn(
        "delivery_completion_rate",
        round(
            (col("delivered_orders") / col("total_orders")) * 100,
            2
        )
    ) \
    .orderBy(desc("delivery_completion_rate"))

display(gold_supplier_performance_df)

print(
    "Total Suppliers :",
    gold_supplier_performance_df.count()
)

gold_supplier_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(
        f"abfss://gold@{storage_account_name}.dfs.core.windows.net/gold_supplier_performance"
    )