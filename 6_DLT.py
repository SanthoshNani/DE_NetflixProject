# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Notebook - GOLD Layer

# COMMAND ----------

looktables_rules = {
    'rule1': 'show_id is NOT NULL'
}

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)

@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
    df = spark.readStream.format('delta').load('abfss://silver@sanetflixdl.dfs.core.windows.net/netflix_directors')
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)

@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
    df = spark.readStream.format('delta').load('abfss://silver@sanetflixdl.dfs.core.windows.net/netflix_cast')
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcountries"
)

@dlt.expect_all_or_drop(looktables_rules)

def myfunc():
    df = spark.readStream.format('delta').load('abfss://silver@sanetflixdl.dfs.core.windows.net/netflix_countries')
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcategory"
)

# if there is only 1 rule, you can hard code
@dlt.expect_or_drop('rule1', 'show_id is NOT NULL')

def myfunc():
    df = spark.readStream.format('delta').load('abfss://silver@sanetflixdl.dfs.core.windows.net/netflix_category')
    return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#if the table name is not provided, it will take function name as table name

@dlt.table

def gold_stg_netflixtitles():
    # a staging table
    df = spark.readStream.format('delta').load('abfss://silver@sanetflixdl.dfs.core.windows.net/netflix_titles')
    return df


# COMMAND ----------

@dlt.table

def gold_trans_netflixtitles():
    # a view with LIVE stream from a staging table
    df = spark.readStream.table('LIVE.gold_stg_netflixtitles')
    # delta live tables allows to transform the data in the streaming mode
    df = df.withColumn('newflag', lit(1))
    return df

# COMMAND ----------

masterdata_rules = {
    'rule1': 'newflag is NOT NULL',
    'rule2': 'show_id is NOT NULL'
}

# COMMAND ----------

# Final gold table using the view

@dlt.table
@dlt.expect_all_or_drop(masterdata_rules)
def gold_netflixtitles():
    df = spark.readStream.table('LIVE.gold_trans_netflixtitles')

    return df
