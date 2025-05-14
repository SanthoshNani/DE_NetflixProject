# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Data Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('delta')\
    .option('header', True)\
        .option('inferSchema', True)\
            .load('abfss://bronze@sanetflixdl.dfs.core.windows.net/netflix_titles')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.fillna({'duration_minutes': 0, 'duration_seasons': 1})

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('duration_minutes', col('duration_minutes').cast('int'))\
        .withColumn('duration_seasons', col('duration_seasons').cast('int'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('shortTitle', split('title', ':')[0])
df.display()

# COMMAND ----------

df = df.withColumn('rating', split('rating', '-')[0])
df.display()

# COMMAND ----------

df = df.withColumn('type_flag', when(col('type')=='Movie', 1)\
                                .when(col('type')=='TV Show', 2)\
                                    .otherwise(0))
                                    
df.display()

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

df = df.withColumn('duration_ranking', dense_rank().over(Window.orderBy(col('duration_minutes').desc())))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregate the data

# COMMAND ----------

df_vis = df.groupBy('type').agg(count('*').alias('total_count'))
df_vis.display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data into Silver Container's titles folder

# COMMAND ----------

df.write.format('delta')\
        .mode('overwrite')\
            .option('path', 'abfss://silver@sanetflixdl.dfs.core.windows.net/netflix_titles')\
                .save()

# COMMAND ----------

