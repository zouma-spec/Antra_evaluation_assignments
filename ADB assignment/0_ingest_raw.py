# Databricks notebook source
# MAGIC %run ./Configuration

# COMMAND ----------

username = "Rui"

# COMMAND ----------

display(dbutils.fs.ls(uploadPath))

# COMMAND ----------

file_list = [[file.path, file.name] for file in dbutils.fs.ls(uploadPath)]

# COMMAND ----------

display(file_list)

# COMMAND ----------

for file in file_list:
    dbutils.fs.cp(file[0], rawPath + "/" +file[1])

# COMMAND ----------

dbutils.fs.ls(rawPath)

# COMMAND ----------

rawDF = (spark.read
                .option("multiLine", "true")
                .json(rawPath))

# COMMAND ----------

display(rawDF)
