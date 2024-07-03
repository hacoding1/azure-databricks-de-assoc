# Databricks notebook source
print('Hello from Databricks!')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Hello from Databricks!'

# COMMAND ----------

# MAGIC %md
# MAGIC # This is a Markdown

# COMMAND ----------

# MAGIC %run ./includes/setup

# COMMAND ----------

print(f'the value of "env" from notebook setup is {env}')

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
display(files)

# COMMAND ----------

dbutils.help()

# COMMAND ----------


