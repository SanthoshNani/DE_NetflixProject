# Databricks notebook source
# MAGIC %md
# MAGIC ### Array Parameter

# COMMAND ----------

files = [
    {
        'sourceFolder': 'netflix_directors',
        'targetFolder': 'netflix_directors'
    },
    {
        'sourceFolder': 'netflix_cast',
        'targetFolder': 'netflix_cast'
    },
    {
        'sourceFolder': 'netflix_countries',
        'targetFolder': 'netflix_countries'
    },
    {
        'sourceFolder': 'netflix_category',
        'targetFolder': 'netflix_category'
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Utility to pass the variable between the tasks

# COMMAND ----------

dbutils.jobs.taskValues.set(key='my_arr', value=files)

# COMMAND ----------

