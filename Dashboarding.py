# Databricks notebook source
# DBTITLE 1,Print info about the dataset we'll use
f = open('/dbfs/databricks-datasets/COVID/covid-19-data/README.md', 'r')
print(f.read())

# COMMAND ----------

# DBTITLE 1,Read the dataset using PySpark
df=spark.read.csv('/databricks-datasets/COVID/covid-19-data/us-states.csv', header=True,inferSchema=True)


# COMMAND ----------

# DBTITLE 1,All-time cummulative cases per state 
display(df)

# COMMAND ----------

# DBTITLE 1,Create a tempview from the dataframe above, to analyze using SQL
df.createOrReplaceTempView("COVID_by_state")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN date DEFAULT '2021-03-11' CHOICES (select distinct(date) date from COVID_by_state order by date desc)

# COMMAND ----------

# MAGIC %sql
# MAGIC select  '${date}' date,state,fips, cases, deaths, deaths/cases*100 mortality
# MAGIC from COVID_by_state 
# MAGIC where 
# MAGIC date='${date}'
# MAGIC order by mortality desc
