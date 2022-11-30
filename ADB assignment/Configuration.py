# Databricks notebook source
username='Rui'

# COMMAND ----------

uploadPath = f"/FileStore/{username}/ADB"

moviePath = f"/mnt/{username}/movie"
rawPath = moviePath + '/0_raw'
bronzePath = moviePath + '/1_bronze'
silverPath = moviePath + '/2_silver'

genrePath = silverPath + '/genre'
movieGenrePath = silverPath + '/movieGenre'
languagePath = silverPath + '/language'

spark.sql(f"CREATE DATABASE IF NOT EXISTS movie_{username}")
spark.sql(f"USE movie_{username}")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


