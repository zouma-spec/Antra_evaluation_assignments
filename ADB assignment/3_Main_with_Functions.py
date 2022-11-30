# Databricks notebook source
# MAGIC %run ./Configuration

# COMMAND ----------

# MAGIC %run ./Operation_Functions

# COMMAND ----------

dbutils.fs.rm(moviePath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest Raw

# COMMAND ----------

ingest_raw(uploadPath, rawPath)

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw to Bronze

# COMMAND ----------

# read in data
rawDF=read_raw(rawPath, moiveSchema)

# COMMAND ----------

#flatten nest json array
rawDF=explode_function(rawDF,'movie','movie')

# COMMAND ----------

#Add surrogate key
DF=add_surrogateKEY(rawDF,"movie","surrogateKEY")

# COMMAND ----------

#Ingest Metadata
meta_DF=ingest_meta(DF)

# COMMAND ----------

#write to bronzePath
batch_writer_append(meta_DF,"p_ingestdate").save(bronzePath)

# COMMAND ----------

# Register the Bronze Table in the Metastore 
register_table("movie_bronze",bronzePath)

# COMMAND ----------

dbutils.fs.rm(rawPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze to Silver

# COMMAND ----------

extractedDF=Extract_Nested_JSON()

# COMMAND ----------

extractedDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning Data
# MAGIC ### Runtime, Budget, Duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Runtime

# COMMAND ----------

silver_runtime_clean=runtime_clean_and_quarantine_dataframes(extractedDF)[0]
silver_runtime_quarantine=runtime_clean_and_quarantine_dataframes(extractedDF)[1]

# COMMAND ----------

# WRITE Clean Batch to a Silver Table
batch_writer_append(silver_runtime_clean,"p_ingestdate",['movie']).save(silverPath)
#register table
register_table('silver_runtime_clean_table',silverPath)

# COMMAND ----------

# Update Bronze table to Reflect the Loads
# Step 1: Update Clean records
Update_Clean_records(silver_runtime_clean)
# Step 2: Update Quarantined records
Update_Quarantined_records(silver_runtime_quarantine)

# COMMAND ----------

# Fix quarantined records
silver_runtime_quarantine_cleaned = fix_runtime(silver_runtime_quarantine)

# COMMAND ----------

#Append runtime_fixed Batch to the Silver Table
batch_writer_append(silver_runtime_clean,"p_ingestdate",['movie']).save(silverPath)
# register table
register_table('silver_runtime_clean_table',silverPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Budget

# COMMAND ----------

df_budget_clean=fix_budget()[0]
df_budget_quarantined_repaired= fix_budget()[1]
#write data
batch_writer_overwrite(df_budget_clean,"p_ingestdate").save(silverPath)
batch_writer_append(df_budget_quarantined_repaired,"p_ingestdate").save(silverPath)

# register table
register_table('silver_runtime_clean_table',silverPath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Duplicates

# COMMAND ----------

# Mark duplicate records as duplicated, non-duplicate ercords as non-duplicated
duplicated_non_duplicated_seperated()
mark_duplicates()
mark_non_duplicates()

# COMMAND ----------

# filter records that are non-duplicated
bronzeDF = spark.read.table("movie_bronze").filter("status = 'non-duplicated'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genre Lookup Table

# COMMAND ----------

genreDF=genre_lookup(extractedDF,'genres','genres')

# COMMAND ----------

write_delta(genreDF,"overwrite").save(genrePath)

# COMMAND ----------

register_table("genre",genrePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MovieGenre Junction Table

# COMMAND ----------

movieGenreDF=movieGenreJunction(extractedDF,"genres","genres",["id"])

# COMMAND ----------

write_delta(movieGenreDF,"overwrite").save(movieGenrePath)
register_table("movieGenre",movieGenrePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Language Lookup Table

# COMMAND ----------

languageDF=language_lookup(extractedDF,'OriginalLanguage','OriginalLanguage')

# COMMAND ----------

write_delta(languageDF,"overwrite").save(languagePath)
register_table("languageLookup",languagePath)
