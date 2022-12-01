# Databricks notebook source
username='Rui'

# COMMAND ----------

uploadPath = f"/FileStore/{username}/ADB"

moviePath = f"/mnt/{username}/movie"
rawPath = moviePath + '/0_raw'
bronzePath = moviePath + '/1_bronze'
silverPath = moviePath + '/2_silver'

genrePath = moviePath + '/silver_genre'
movieGenrePath = moviePath + '/silver_movieGenre'
languagePath = moviePath + '/silver_language'

spark.sql(f"CREATE DATABASE IF NOT EXISTS movie_{username}")
spark.sql(f"USE movie_{username}")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


