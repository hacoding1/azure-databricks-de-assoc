# Databricks notebook source
# MAGIC %sql
# MAGIC -- STORED VIEW
# MAGIC SELECT * FROM tbl2_v;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TEMPORARY VIEW 
# MAGIC SELECT * FROM tbl2_tv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.tbl2_gtv;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------


