# Databricks notebook source
# MAGIC %run ./0_ingest_raw

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

display(rawDF)

# COMMAND ----------

rawDF.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
moiveSchema = StructType([
                   StructField("movie", ArrayType(
                                         StructType([
                                                 StructField("BackdropUrl", StringType()),
                                                 StructField("Budget", DoubleType()),
                                                 StructField("CreatedBy",StringType()),
                                                 StructField("CreatedDate", TimestampType()),
                                                 StructField("Id", LongType()),
                                                 StructField("ImdbUrl", StringType()),
                                                 StructField("OriginalLanguage", StringType()),
                                                 StructField("Overview", StringType()),
                                                 StructField("PosterUrl", StringType()),
                                                 StructField("Price", DoubleType()),
                                                 StructField("ReleaseDate", TimestampType()),
                                                 StructField("Revenue", DoubleType()),
                                                 StructField("RunTime", LongType()),
                                                 StructField("Tagline", StringType()),
                                                 StructField("Title", StringType()),
                                                 StructField("TmdbUrl", StringType()),
                                                 StructField("UpdatedBy", StringType()),
                                                 StructField("UpdatedDate", TimestampType()),
                                                 StructField("genres", ArrayType(
                                                                        StructType([
                                                                                  StructField("id", LongType()),
                                                                                  StructField("name", StringType())
                                                                                  ])))
                     ]))
                              )])

# COMMAND ----------

rawDF=(spark.read
                .option("multiLine", "true")
                .schema(moiveSchema)
                .json(rawPath)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten nested array

# COMMAND ----------

from pyspark.sql.functions import explode
rawDF=rawDF.withColumn("movie", explode("movie"))

# COMMAND ----------

display(rawDF)

# COMMAND ----------

rawDF.count()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
DF = rawDF.select(row_number().over(Window.orderBy(col("movie"))).alias("surrogateKEY"),
                                      col("movie")
                             )

# COMMAND ----------

DF.display()

# COMMAND ----------

DF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Metadata
# MAGIC - ingestion time (`ingesttime`)
# MAGIC - status (`status`), use `"new"`
# MAGIC - ingestion date (`ingestdate`)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

meta_DF = DF.select(
    "surrogateKEY",
    "movie",
    current_timestamp().alias("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate")
)

# COMMAND ----------

display(meta_DF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table
# MAGIC ### This would be the last step of raw_to_bronze pipeline

# COMMAND ----------

from pyspark.sql.functions import col

(
    meta_DF.select(
        "surrogateKEY",
        "movie",
        "ingesttime",
        "status",
        col("ingestdate").alias("p_ingestdate"),
    )
    .write.format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(bronzePath)
)

# COMMAND ----------

meta_DF.printSchema()

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore 
# MAGIC ### this step is for analyis purpose and is not quired 

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_bronze
"""
)                                                                     

spark.sql(
    f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
"""
)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------



# COMMAND ----------

dbutils.fs.rm(rawPath, recurse=True)
