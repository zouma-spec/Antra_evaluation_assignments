# Databricks notebook source
# MAGIC %run ./Configuration

# COMMAND ----------

# dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")

# COMMAND ----------

bronzeDF = spark.read.load(bronzePath)

# COMMAND ----------

bronzeDF.count()

# COMMAND ----------

bronzeDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract the Nested JSON from the Bronze Records
# MAGIC #### Extract the Nested JSON from the `movie` column

# COMMAND ----------

extractedDF = bronzeDF.select("surrogateKEY","movie", "p_ingestdate", "movie.*")

# COMMAND ----------

display(extractedDF)

# COMMAND ----------

extractedDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Some movies have a negative runtime
# MAGIC     - First gather all such records, put them in “quarantine” area
# MAGIC         - Mark the record as “quarantined” in your bronze table
# MAGIC         - Separate these records from the “clean” records, and work on fixing them after you have loaded all the “clean” records from the 8 movie files into silver tables.
# MAGIC     - To fix the runtime, simply take the absolute value of the original negative value.

# COMMAND ----------

silver_runtime_clean = extractedDF.filter("RunTime >= 0")
silver_runtime_quarantine = extractedDF.filter("RunTime < 0")

# COMMAND ----------

print(silver_runtime_clean.count())
print(silver_runtime_quarantine.count())

# COMMAND ----------

display(silver_runtime_clean)

# COMMAND ----------

display(silver_runtime_quarantine)

# COMMAND ----------

dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE Clean Batch to a Silver Table

# COMMAND ----------

(   silver_runtime_clean.drop('movie'
     )
    .write
    .format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

spark.read.load(silverPath).count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Register clean runtime table in the Metastore 

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silver_runtime_clean_table
"""
)

spark.sql(
    f"""
CREATE TABLE silver_runtime_clean_table
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM silver_runtime_clean_table

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update Bronze table to Reflect the Loads
# MAGIC  Update the records in the Bronze table to reflect updates.
# MAGIC 
# MAGIC ##### Step 1: Update Clean records
# MAGIC Clean records that have been loaded into the Silver table and should have
# MAGIC    their Bronze table `status` updated to `"loaded"`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = silver_runtime_clean.withColumn("status", lit("loaded"))

update_match = "bronze.surrogateKEY = clean.surrogateKEY"  #matching surrogateKEY column in clean sliver dataframe to the surrogateKEY column in the bronze table
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Update Quarantined records
# MAGIC Quarantined records should have their Bronze table `status` updated to `"quarantined"`.

# COMMAND ----------

silverAugmented = silver_runtime_quarantine.withColumn(
    "status", lit("quarantined")
)

update_match = "bronze.surrogateKEY = quarantine.surrogateKEY"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_bronze         --loads have been correctly reflected on bronze table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fix quarantined records

# COMMAND ----------

# silver_runtime_quarantine = extractedDF.filter("RunTime < 0")

# COMMAND ----------

display(silver_runtime_quarantine)

# COMMAND ----------

silver_runtime_quarantine_cleaned = silver_runtime_quarantine.withColumn("RunTime", abs(col("RunTime")))

# COMMAND ----------

display(silver_runtime_quarantine_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Append runtime_fixed Batch to the Silver Table

# COMMAND ----------

( silver_runtime_quarantine.drop('movie'
     )
    .write
    .format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

spark.read.load(silverPath).count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Update table in metastore

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS silver_runtime_clean_table
"""
)

spark.sql(
    f"""
CREATE TABLE silver_runtime_clean_table
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from silver_runtime_clean_table

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Genre
# MAGIC - Certain movies have valid ID for the genre, but the name of the genre is missing
# MAGIC     - Do we need to fix this? If we do, where should we fix this?
# MAGIC     - If not, why don’t we need to fix this?

# COMMAND ----------

# MAGIC %md
# MAGIC ### We can create a Genres lookup table for reference

# COMMAND ----------

# MAGIC %run ./Operation_Functions

# COMMAND ----------

dbutils.fs.rm(genrePath, recurse=True)
dbutils.fs.rm(movieGenrePath, recurse=True)
dbutils.fs.rm(languagePath, recurse=True)

# dbutils.fs.mkdirs(genrePath)
# dbutils.fs.mkdirs(movieGenrePath)
# dbutils.fs.mkdirs(languagePath)

# COMMAND ----------

from pyspark.sql.functions import explode
genreDF=(extractedDF.select(explode("genres")).alias("genres")).distinct()

# COMMAND ----------

display(genreDF)

# COMMAND ----------

# genreDF = (explode_bronze(extractedDF, "genres", "genres")
#            .distinct())

# COMMAND ----------

# display(genreDF)

# COMMAND ----------

genreDF=genreDF.select(
    'col',
    'col.*'
)

# COMMAND ----------

display(genreDF)

# COMMAND ----------

genreDF = (genreDF.filter(col("name") != ''))

# COMMAND ----------

display(genreDF) # the final genre dataframe

# COMMAND ----------

#write data to path
(
    genreDF
    .write
    .format("delta")
    .mode("overwrite")
    .save(genrePath)
)

# COMMAND ----------

#register table to dbfs
spark.sql(
    """
DROP TABLE IF EXISTS genre
"""
)

spark.sql(
    f"""
CREATE TABLE genre
USING DELTA
LOCATION "{genrePath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a MovieGenre Junction Table

# COMMAND ----------

movieGenreDF=extractedDF.select(*["Id"], explode(col("genres")).alias("genres"))

# COMMAND ----------

movieGenreDF.display()

# COMMAND ----------

movieGenreDF = movieGenreDF.select(row_number().over(Window.orderBy(col("id"))).alias("moviegenre_id"),
                                      col("id").alias("movie_id"),
                                      col("genres.id").alias("genre_id"))

# COMMAND ----------

movieGenreDF.display()

# COMMAND ----------

movieGenreDF.count()

# COMMAND ----------

#write data to path
(
    movieGenreDF
    .write
    .format("delta")
    .mode("overwrite")
    .save(movieGenrePath)
)

# COMMAND ----------

#register table to dbfs
spark.sql(
    """
DROP TABLE IF EXISTS movieGenre
"""
)

spark.sql(
    f"""
CREATE TABLE movieGenre
USING DELTA
LOCATION "{movieGenrePath}"
"""
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Language Lookup Table

# COMMAND ----------

languageDF = (extractedDF.select(col("OriginalLanguage").alias("language"))
            .distinct())

# COMMAND ----------

 languageDF=languageDF.select(
                       row_number().over(Window.orderBy(col("language"))).alias("language_id"),
                       col("language").alias("name")
                      )

# COMMAND ----------

display(languageDF)

# COMMAND ----------

#write data to path
(
    languageDF
    .write
    .format("delta")
    .mode("overwrite")
    .save(languagePath)
)

# COMMAND ----------

#register table to dbfs
spark.sql(
    """
DROP TABLE IF EXISTS languageDF
"""
)

spark.sql(
    f"""
CREATE TABLE languageDF
USING DELTA
LOCATION "{languagePath}"
"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## budget
# MAGIC - Let’s assume all the movies should have a minimum budget of 1 million
# MAGIC     - Where should we fix this? (Raw/Bronze/Silver)  --Sliver
# MAGIC     - If a movie has a budget of less than 1 million, we should replace it with 1 million

# COMMAND ----------

#Retrieve data from silverPath which has been cleaned for runtime
#display(dbutils.fs.ls(silverPath))
df = spark.read.load(silverPath)

# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

df_budget_clean=df.filter("Budget>=1000000")
df_budget_quarantined=df.filter("Budget<1000000")

# COMMAND ----------

df_budget_clean.display()

# COMMAND ----------

df_budget_clean.count()

# COMMAND ----------

df_budget_quarantined.display()

# COMMAND ----------

df_budget_quarantined.count()

# COMMAND ----------

df_budget_quarantined_repaired = df_budget_quarantined.withColumn("Budget", lit(1000000).cast("Double"))

# COMMAND ----------

df_budget_quarantined_repaired.display()

# COMMAND ----------

df_budget_quarantined_repaired.count()

# COMMAND ----------

df_budget_clean.printSchema()

# COMMAND ----------

(   df_budget_clean
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

spark.read.load(silverPath).count()

# COMMAND ----------

(   df_budget_quarantined_repaired
    .write
    .format("delta")
    .mode("append")
    .partitionBy("p_ingestdate")
    .save(silverPath)
)

# COMMAND ----------

spark.read.load(silverPath).count()

# COMMAND ----------

df=spark.read.load(silverPath).display()

# COMMAND ----------

spark.read.load(silverPath).count()

# COMMAND ----------

df.filter('Budget=1000000').display()

# COMMAND ----------

df.filter('Budget=1000000').count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### We have some movies that are showing up in more than one movie file.
# MAGIC     - How do we ensure only one record shows up in our silver table?
# MAGIC         - Recall in the moovio notebooks we had a column called “status” in our bronze table to specify “new” and “loaded”...

# COMMAND ----------

extractedDF.display()

# COMMAND ----------

# Mark duplicate records as duplicated, non-duplicate ercords as non-duplicated
DF_duplicates = extractedDF.groupBy("movie").count().filter("count > 1")
DF_non_duplicates = extractedDF.groupBy("movie").count().filter("count = 1")

# COMMAND ----------

extractedDF.display()

# COMMAND ----------

DF_duplicates.display()

# COMMAND ----------

#mark duplicates in bronze table
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = DF_duplicates.withColumn(
    "status", lit("duplicated")
    )

update_match = "bronze.movie = quarantine.movie"
update = {"status": "quarantine.status"}
(
   bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
    )

# COMMAND ----------

bronzeDF_duplicates.display()

# COMMAND ----------



# COMMAND ----------

#mark non_duplicates in bronze table
bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = DF_non_duplicates.withColumn(
    "status", lit("non_duplicated")
    )

update_match = "bronze.movie = quarantine.movie"
update = {"status": "quarantine.status"}
(
        bronzeTable.alias("bronze")
        .merge(silverAugmented.alias("quarantine"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

# COMMAND ----------

# Fix Duplicates
