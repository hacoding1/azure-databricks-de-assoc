# Databricks notebook source
# MAGIC %sql
# MAGIC -- CREATE TABLE FROM FILES
# MAGIC -- CTAS - DELTA TABLES - CTREATE TABLE table_name AS (SELECT * FROM file_format.`path_of_files`); - does not support other file options
# MAGIC -- FROM EXTERNAL DATA SOURCE - CREATE TABLE table_name USING data_source_like_csv_jdbc OPTIONS (options_key_value_pairs) LOCATION = path - it is not a delta table - directly points to data source
# MAGIC -- this does not garuntee the advantage of delta table
# MAGIC -- SOLUTION - create temp view from data source and create CTAS table from temp view

# COMMAND ----------

# MAGIC %run ./Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f'{dataset_bookstore}/customers-json')
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_001.json`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/*.json`;
# MAGIC -- SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_*.json`;
# MAGIC -- SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, input_file_name() AS source_file FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEXT.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM BINARYFILE.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CSV.`dbfs:/mnt/demo-datasets/bookstore/books-csv/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE 
# MAGIC   books_csv 
# MAGIC   (
# MAGIC     book_id STRING, 
# MAGIC     title STRING, 
# MAGIC     author STRING, 
# MAGIC     category STRING, 
# MAGIC     price DOUBLE
# MAGIC   )
# MAGIC USING 
# MAGIC   CSV 
# MAGIC OPTIONS (
# MAGIC   header = 'true',
# MAGIC   delimiter = ';'
# MAGIC )
# MAGIC LOCATION 'dbfs:/mnt/demo-datasets/bookstore/books-csv/';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books_csv;

# COMMAND ----------

files = dbutils.fs.ls(f'{dataset_bookstore}/books-csv')
display(files)

# COMMAND ----------

spark.read.table('books_csv').write.mode('append').format('csv').option('header','true').option('delimiter',';').save(f'{dataset_bookstore}/books-csv')

# COMMAND ----------

files = dbutils.fs.ls(f'{dataset_bookstore}/books-csv')
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_csv;
# MAGIC -- we get less count even after new files are added, as spark quries the cached data

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE books_csv;
# MAGIC SELECT COUNT(*) FROM books_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE customers AS (SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`);
# MAGIC DESCRIBE EXTENDED customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_unparsed AS SELECT * FROM CSV.`${dataset.bookstore}/books-csv`;
# MAGIC SELECT * FROM books_unparsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW 
# MAGIC   books_tv
# MAGIC   (
# MAGIC     book_id STRING, 
# MAGIC     title STRING, 
# MAGIC     author STRING, 
# MAGIC     category STRING, 
# MAGIC     price DOUBLE
# MAGIC   ) 
# MAGIC USING
# MAGIC   CSV
# MAGIC OPTIONS 
# MAGIC (
# MAGIC   path = '${dataset.bookstore}/books-csv/export_*.csv',
# MAGIC   header = 'true',
# MAGIC   delimiter = ';'
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE books AS (SELECT * FROM books_tv);
# MAGIC SELECT * FROM books;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED books;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE TABLE order AS (SELECT * FROM PARQUET.`${dataset.bookstore}/orders`);
# MAGIC SELECT * FROM order;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED order;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE order AS (SELECT * FROM PARQUET.`${dataset.bookstore}/orders`);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY order;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- same output as above, but applicable only if table exists already
# MAGIC INSERT OVERWRITE order SELECT * FROM PARQUET.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE order SELECT *, current_timestamp() FROM PARQUET.`${dataset.bookstore}/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO order SELECT * FROM PARQUET.`${dataset.bookstore}/orders-new`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM order;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY order;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- to avoid inserting duplicate records 
# MAGIC -- can insert, update, delete
# MAGIC CREATE OR REPLACE TEMP VIEW customers_updates AS 
# MAGIC SELECT * FROM JSON.`${dataset.bookstore}/customers-json-new`;
# MAGIC
# MAGIC MERGE INTO customers c USING customers_updates u 
# MAGIC ON c.customer_id = u.customer_id
# MAGIC WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN UPDATE SET 
# MAGIC email = u.email, updated = u.updated 
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW books_updates 
# MAGIC (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV 
# MAGIC OPTIONS 
# MAGIC (
# MAGIC   path = '${dataset.bookstore}/books-csv-new',
# MAGIC   header = 'true',
# MAGIC   delimiter = ';'
# MAGIC );
# MAGIC SELECT * FROM books_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO books b 
# MAGIC USING books_updates bu 
# MAGIC ON b.book_id = bu.book_id AND b.title = bu.title 
# MAGIC WHEN NOT MATCHED AND bu.category = 'Computer Science' THEN 
# MAGIC INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, profile:first_name, profile:address:country FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT profile FROM customers LIMIT 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_customers AS 
# MAGIC SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) AS profile_struct FROM customers;
# MAGIC SELECT * FROM parsed_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE parsed_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, profile_struct.first_name, profile_struct.address.country from parsed_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TEMP VIEW customer_final AS SELECT customer_id, profile_struct.* FROM parsed_customers;
# MAGIC SELECT * FROM customer_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, books FROM order;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, explode(books) as book FROM order;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COLLECT SET 
# MAGIC SELECT customer_id,
# MAGIC COLLECT_SET(order_id) AS orders_set,
# MAGIC COLLECT_SET(books.book_id) AS books_set 
# MAGIC FROM order 
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC COLLECT_SET(books.book_id) AS before_flatten,
# MAGIC ARRAY_DISTINCT(FLATTEN(COLLECT_SET(books.book_id))) AS after_flatten 
# MAGIC FROM order 
# MAGIC GROUP BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_enriched AS 
# MAGIC SELECT * FROM (SELECT *, explode(books) AS book FROM order) o 
# MAGIC INNER JOIN books b
# MAGIC ON o.book.book_id = b.book_id;
# MAGIC SELECT 8 FROM orders_enriched;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TEMP VIEW orders_update 
# MAGIC -- AS SELECT * FROM PARQUET.`${dataset.bookstore}/orders-new`;
# MAGIC
# MAGIC SELECT * FROM order
# MAGIC UNION 
# MAGIC SELECT * FROM orders_update;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order 
# MAGIC INTERSECT 
# MAGIC SELECT * FROM orders_update;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order 
# MAGIC MINUS 
# MAGIC SELECT * FROM orders_update;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REPLACE TABLE transactions AS 
# MAGIC -- (SELECT customer_id, book.book_id AS book_id, book.quantity AS quantity FROM orders_enriched)
# MAGIC -- PIVOT (sum(quantity) FOR book_id in ('B01', 'B02', 'B03', 'B04', 'B05', 'B06','B07', 'B08', 'B09', 'B10', 'B11', 'B12'));
# MAGIC
# MAGIC CREATE OR REPLACE TABLE transactions AS
# MAGIC
# MAGIC SELECT * FROM (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     book.book_id AS book_id,
# MAGIC     book.quantity AS quantity
# MAGIC   FROM orders_enriched
# MAGIC ) PIVOT (
# MAGIC   sum(quantity) FOR book_id in (
# MAGIC     'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
# MAGIC     'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, books,
# MAGIC FILTER (books, i -> i.quantity >= 2) AS multiple_copies 
# MAGIC FROM order;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT order_id, books,
# MAGIC FILTER (books, i -> i.quantity >= 2) AS multiple_copies 
# MAGIC FROM order 
# MAGIC ) WHERE size(multiple_copies) > 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, books,
# MAGIC TRANsFORM (books, b -> CAST(b.subtotal * 0.8 AS INT)) AS subtotal_after_discount 
# MAGIC from order;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_url(email STRING)
# MAGIC RETURNS STRING 
# MAGIC RETURN CONCAT('https://www.',SPLIT(email,'@')[1])

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, get_url(email) domain FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION get_url;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE FUNCTION EXTENDED get_url;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION site_type(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE 
# MAGIC           WHEN email like "%.com" THEN "Commercial business"
# MAGIC           WHEN email like "%.org" THEN "Non-profits organization"
# MAGIC           WHEN email like "%.edu" THEN "Educational institution"
# MAGIC           ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
# MAGIC        END;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT email, site_type(email) AS domain_category FROM customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- functions are evaluated atively in spark, so optimized for parallel execution 
# MAGIC DROP FUNCTION get_url;
# MAGIC DROP FUNCTION site_type;

# COMMAND ----------


