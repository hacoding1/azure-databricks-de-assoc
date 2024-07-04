# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE managed_default (width INT, length INT, height INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO managed_default VALUES (3,2,1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM managed_default;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_default

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/managed_default'))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_default;

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/user/hive/warehouse/managed_default'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default (width INT, length INT, height INT) LOCATION '/Workspace/Test';
# MAGIC INSERT INTO external_default VALUES (3,2,1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Workspace/Test'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Workspace/Test'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA db1; -- or use CREATE DATABASE db1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DATABASE EXTENDED db1;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DB1;
# MAGIC CREATE TABLE managed_db1 (width INT, length INT, height INT);
# MAGIC INSERT INTO managed_db1 VALUES (3,2,1);

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED managed_db1;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/db1.db/managed_db1'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_db1;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/db1.db/managed_db1'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE db1;
# MAGIC CREATE TABLE external_db1 (width INT, length INT, height INT) LOCATION '/Workspace/Test';
# MAGIC INSERT INTO external_db1 VALUES (3,2,1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_db1;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Workspace/Test'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_db1;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Workspace/Test'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE external_db LOCATION '/Workspace/Test1/external_db.db';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED external_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE external_db;
# MAGIC CREATE TABLE managed_tbl (width INT, length INT, height INT);
# MAGIC INSERT INTO managed_tbl VALUES (3,2,1);
# MAGIC CREATE TABLE external_tbl (width INT, length INT, height INT) LOCATION '/Workspace/Test2';
# MAGIC INSERT INTO external_tbl VALUES (3,2,1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE managed_tbl;
# MAGIC DROP TABLE external_tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE external_db;
# MAGIC -- DROP DATABASE db1;

# COMMAND ----------


