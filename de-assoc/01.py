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

!ls /databricks-datasets/

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE 
# MAGIC   employees 
# MAGIC -- USING DELTA 
# MAGIC (
# MAGIC   id INT, 
# MAGIC   name STRING, 
# MAGIC   salary DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO 
# MAGIC   employees 
# MAGIC VALUES 
# MAGIC   (1,'BEN',180.0),
# MAGIC   (2,'GWEN',150.0),
# MAGIC   (3,'Kevin',140.0);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

files = dbutils.fs.ls('/user/hive/warehouse/employees')
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE 
# MAGIC   employees
# MAGIC SET 
# MAGIC   name = 'KEVIN'
# MAGIC WHERE 
# MAGIC   name = 'Kevin';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   employees;

# COMMAND ----------

files = dbutils.fs.ls('/user/hive/warehouse/employees')
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

files = dbutils.fs.ls('/user/hive/warehouse/employees/_delta_log')
display(files)

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000003.json'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TIMETRAVEL - Version History
# MAGIC SELECT * FROM employees VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees@v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees TIMESTAMP AS OF '2024-07-03T09:34:49.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO 
# MAGIC   employees 
# MAGIC VALUES 
# MAGIC   (4,'MAX',180.0);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees TO TIMESTAMP AS OF '2024-07-03T09:34:49.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees TO VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZING - Compacting Small Files To Large Files To Improve Table Performance
# MAGIC OPTIMIZE employees;

# COMMAND ----------

display(dbutils.fs.ls('/user/hive/warehouse/employees'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INDEXING - Co-Locate Column Information 
# MAGIC -- Save n To n+x files in one file and n+x+1 to n+x+N in next file and so on...
# MAGIC OPTIMIZE 
# MAGIC   employees
# MAGIC ZORDER BY (id);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM - clean-up unused data files or uncommitted files (files that are no longer in latest table state)
# MAGIC -- default retention period is 7 days 
# MAGIC -- VACUUM <TABLE_NAME> RETAIN <n> <HOURS/DAYS>;
# MAGIC -- after VACUUM, TIME TRAVEL is not possible
# MAGIC VACUUM employees;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

display(dbutils.fs.ls('/user/hive/warehouse/employees'))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

display(dbutils.fs.ls('/user/hive/warehouse/employees'))

# COMMAND ----------


