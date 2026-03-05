

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==========================================================
# CONFIGURATION
# ==========================================================

jdbc_url = "Your_JDBC_Address"

connection_props = {
    "user": "Your_username",
    "password": "Your_password",   # Use secrets in production
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

tables_config = {
    "customers": {
        "layer":  "02_silver",
        "target": "02_silver.dim_customers",
        "pk":     "customer_id"
    },
    "restaurants": {
        "layer":  "02_silver",
        "target": "02_silver.dim_restaurants",
        "pk":     "restaurant_id"
    }
}

CDC_COLS = [
    "__$start_lsn",
    "__$end_lsn",
    "__$seqval",
    "__$operation",
    "__$update_mask",
    "__$command_id"
]

# ==========================================================
# CREATE WATERMARK TABLE
# ==========================================================

spark.sql("""
CREATE TABLE IF NOT EXISTS default.cdc_watermark (
    table_name STRING,
    last_lsn   STRING,
    layer      STRING,
    last_run   TIMESTAMP
)
""")

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================

def get_max_lsn():
    df = spark.read.jdbc(
        url=jdbc_url,
        table="(SELECT CONVERT(VARCHAR(100), sys.fn_cdc_get_max_lsn(), 1) AS max_lsn) AS t",
        properties=connection_props
    )
    return df.first()["max_lsn"]

def get_watermark(table_name):
    df = spark.sql(f"""
        SELECT last_lsn
        FROM default.cdc_watermark
        WHERE table_name = '{table_name}'
    """)
    row = df.first()
    return row["last_lsn"] if row else None

def update_watermark(table_name, lsn, layer):
    spark.sql(f"""
        MERGE INTO default.cdc_watermark t
        USING (
            SELECT '{table_name}' AS table_name,
                   '{lsn}' AS last_lsn,
                   '{layer}' AS layer,
                   current_timestamp() AS last_run
        ) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            t.last_lsn = s.last_lsn,
            t.layer    = s.layer,
            t.last_run = s.last_run
        WHEN NOT MATCHED THEN
            INSERT (table_name, last_lsn, layer, last_run)
            VALUES (s.table_name, s.last_lsn, s.layer, s.last_run)
    """)

def read_cdc(table_name, from_lsn, to_lsn):
    query = f"""(
        SELECT *
        FROM cdc.fn_cdc_get_all_changes_dbo_{table_name}(
            CONVERT(binary(10), '{from_lsn}', 1),
            CONVERT(binary(10), '{to_lsn}', 1),
            'all'
        )
    ) AS cdc_data"""

    return spark.read.jdbc(
        url=jdbc_url,
        table=query,
        properties=connection_props
    )

# ==========================================================
# STEP 1 — INITIALIZE WATERMARK
# ==========================================================

print("="*60)
print("STEP 1: WATERMARK INITIALIZATION")
print("="*60)

current_max_lsn = get_max_lsn()

for table_name, config in tables_config.items():

    last_lsn = get_watermark(table_name)

    if last_lsn:
        print(f"✅ {table_name} already initialized")
    else:
        update_watermark(table_name, current_max_lsn, config["layer"])
        print(f"🆕 {table_name} initialized")

# ==========================================================
# STEP 2 — CDC INCREMENTAL LOAD
# ==========================================================

print("\n" + "="*60)
print("STEP 2: CDC INCREMENTAL LOAD")
print("="*60)

current_max_lsn = get_max_lsn()

for table_name, config in tables_config.items():

    layer  = config["layer"]
    target = config["target"]
    pk     = config["pk"]

    print(f"\nProcessing → {table_name}")

    last_lsn = get_watermark(table_name)

    if not last_lsn:
        print("No watermark. Skipping.")
        continue

    if last_lsn == current_max_lsn:
        print("No new changes.")
        continue

    # ----------------------------------------------
    # READ CDC DATA
    # ----------------------------------------------
    cdc_df = read_cdc(table_name, last_lsn, current_max_lsn)

    if not cdc_df.head(1):
        print("No records in LSN range.")
        update_watermark(table_name, current_max_lsn, layer)
        continue

    print("CDC changes detected.")

    # ----------------------------------------------
    # PROCESS SILVER LAYER
    # ----------------------------------------------
    if layer == "02_silver":

        # Separate operations
        upserts_raw_df = cdc_df.filter(F.col("__$operation").isin(2,4))
        deletes_df     = cdc_df.filter(F.col("__$operation") == 1).select(pk)

        # ==============================
        # UPSERTS (DEDUPLICATED)
        # ==============================
        if upserts_raw_df.head(1):

            window_spec = Window.partitionBy(pk).orderBy(
                F.col("__$start_lsn").desc(),
                F.col("__$seqval").desc()
            )

            upserts_df = (
                upserts_raw_df
                .withColumn("rn", F.row_number().over(window_spec))
                .filter(F.col("rn") == 1)
                .drop("rn")
                .drop(*CDC_COLS)
            )

            upserts_df.createOrReplaceTempView("upserts")

            silver_cols = [c.name for c in spark.table(target).schema]
            set_clause  = ", ".join([f"t.{c} = s.{c}" for c in silver_cols if c != pk])
            insert_cols = ", ".join(silver_cols)
            insert_vals = ", ".join([f"s.{c}" for c in silver_cols])

            spark.sql(f"""
                MERGE INTO {target} t
                USING upserts s
                ON t.{pk} = s.{pk}
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols})
                VALUES ({insert_vals})
            """)

            print("Upserts applied.")

        # ==============================
        # DELETES
        # ==============================
        if deletes_df.head(1):

            deletes_df.createOrReplaceTempView("deletes")

            spark.sql(f"""
                MERGE INTO {target} t
                USING deletes s
                ON t.{pk} = s.{pk}
                WHEN MATCHED THEN DELETE
            """)

            print("Deletes applied.")

    # ----------------------------------------------
    # UPDATE WATERMARK
    # ----------------------------------------------
    update_watermark(table_name, current_max_lsn, layer)
    print("Watermark updated.")

print("\n" + "="*60)
print("CDC PIPELINE COMPLETED")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC Silver_pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `02_silver`.dim_customers
# MAGIC where customer_id='CUST-10000'

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC WaterMark_Table

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS default.cdc_watermark (
        table_name STRING,
        last_lsn STRING,
        last_run TIMESTAMP
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC CDC impelmented

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

