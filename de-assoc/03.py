# Databricks notebook source
# MAGIC %sql
# MAGIC -- CREATE TABLE tbl1 (width INT, length INT, height INT);
# MAGIC INSERT INTO tbl1 VALUES (3,2,1);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS - create table as 
# MAGIC -- CREATE TABLE new_table 
# MAGIC -- COMMENT 'add comments'
# MAGIC -- PARTITIONED BY (col1, col2)
# MAGIC -- LOCATION 'path for external table'
# MAGIC -- AS (SELECT * FROM old_table)
# MAGIC CREATE TABLE tbl2 AS (SELECT * FROM tbl1);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tbl1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tbl2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CONSTRAINTS
# MAGIC ALTER TABLE tbl2 ADD CONSTRAINT height_not_null CHECK (height IS NOT NULL);
# MAGIC ALTER TABLE tbl2 ADD CONSTRAINT width_greater_than_2 CHECK (width > 2);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED tbl2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CLONING TABLE 
# MAGIC -- DEEP CLONE - copy data + metadata, sync changes, takes quite a while for large datasets
# MAGIC -- SHALLOW CLONE - quick copy of table, h=just copies delta transaction logs 
# MAGIC -- changes to source table after cloning will not be reflected in target after cloning for both deep and shallow cloning
# MAGIC -- modifications to data in target will not affect source
# MAGIC CREATE TABLE tbl2_shallow_clone SHALLOW CLONE tbl2;
# MAGIC CREATE TABLE tbl2_deep_clone DEEP CLONE tbl2;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tbl2 VALUES (4,3,2);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tbl2_deep_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tbl2_shallow_clone;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIEWS - virtual table - no physical data - logical query is executed on actual table everytime when view is queried
# MAGIC -- STORED VIEWS - pesistent
# MAGIC CREATE VIEW tbl2_v AS (SELECT * FROM tbl2);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tbl2_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPORARY VIEW - attached to spark session (session scoped)
# MAGIC -- new spark session created when - opening new notebook, attaching / detaching a cluster, installing a python package
# MAGIC CREATE TEMP VIEW tbl2_tv AS (SELECT * FROM tbl2);
# MAGIC SELECT * FROM tbl2_tv;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GLOBAL TEMPORARY VIEW - attached to the cluster (cluster scoped) - accessible between multiple spark session
# MAGIC CREATE GLOBAL TEMP VIEW tbl2_gtv AS (SELECT * FROM tbl2);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.tbl2_gtv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE tbl1;
# MAGIC DROP TABLE tbl2;
# MAGIC DROP TABLE tbl2_deep_clone;
# MAGIC DROP TABLE tbl2_shallow_clone;
# MAGIC DROP VIEW tbl2_v;
# MAGIC

# COMMAND ----------


