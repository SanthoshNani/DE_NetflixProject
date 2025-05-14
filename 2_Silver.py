# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook Lookup Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

dbutils.widgets.text('sourceFolder', 'netflix_directors')
dbutils.widgets.text('targetFolder', 'netflix_directors')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get('sourceFolder')
var_trg_folder = dbutils.widgets.get('targetFolder')

# COMMAND ----------

df = spark.read.format('csv')\
    .option('header', True)\
        .option('inferSchema', True)\
            .load(f'abfss://bronze@sanetflixdl.dfs.core.windows.net/{var_src_folder}')

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format('delta')\
    .mode('append')\
        .option('path', f'abfss://silver@sanetflixdl.dfs.core.windows.net/{var_trg_folder}')\
        .save()

# COMMAND ----------

