# Databricks notebook source
# DBTITLE 1,Print info about the dataset we'll use
f = open('/dbfs/databricks-datasets/COVID/covid-19-data/README.md', 'r')
print(f.read())

# COMMAND ----------

# DBTITLE 1,Read the dataset
df=spark.read.csv('/databricks-datasets/COVID/covid-19-data/us-states.csv', header=True,inferSchema=True)


# COMMAND ----------

display(df).filter()

# COMMAND ----------


