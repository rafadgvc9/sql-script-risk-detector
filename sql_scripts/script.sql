-- Bases de datos
CREATE OR REPLACE DATABASE db_example;
CREATE DATABASE db_tmp;
ALTER DATABASE db_example SET DATA_RETENTION_TIME_IN_DAYS=1;
DROP DATABASE db_tmp;
UNDROP DATABASE db_tmp;

-- Schemas
CREATE OR REPLACE SCHEMA db_example.silver;
CREATE SCHEMA db_example.geld;
ALTER SCHEMA db_example.geld RENAME TO gold;
DROP SCHEMA db_example.gold;
UNDROP SCHEMA db_example.gold;

-- Tablas
CREATE OR REPLACE TABLE db_example.gold.dim_where (id INT, place_name VARCHAR);
CREATE HYBRID TABLE db_example.gold.dim_who (id INT, username STRING);
CREATE OR ALTER EXTERNAL TABLE db_example.gold.dim_where (id INT, place_name VARCHAR);
DROP TABLE db_example.gold.dim_where;
ALTER TABLE db_example.gold.dim_where ADD COLUMN place_description STRING;
ALTER TABLE db_example.gold.dim_where DROP COLUMN place_description;
ALTER TABLE db_example.gold.dim_where ALTER COLUMN place_name TYPE VARCHAR(100);
TRUNCATE TABLE db_example.gold.dim_where;
UNDROP TABLE db_example.gold.dim_where;

-- Vistas
CREATE OR REPLACE MATERIALIZED VIEW db_example.gold.view_dim_who AS SELECT * FROM db_example.gold.dim_who;
CREATE MATERIALIZED VIEW db_example.gold.view_dim_where AS SELECT id, nombre FROM db_example.gold.dim_where;
ALTER VIEW db_example.gold.view_dim_where RENAME TO dim_where_view;
DROP VIEW db_example.gold.dim_where_view;

-- Warehouses
CREATE OR ALTER WAREHOUSE wh_compute;
CREATE WAREHOUSE wh_bi;
ALTER WAREHOUSE wh_compute SET WAREHOUSE_SIZE='LARGE';
DROP WAREHOUSE wh_bi;

-- Shares
CREATE SHARE new_share;
ALTER SHARE new_share ADD ACCOUNTS = ('account1');
DROP SHARE new_share;

-- Tags
CREATE OR REPLACE TAG IF NOT EXISTS ANALYSTS_TAG;
CREATE OR ALTER TAG ANALYSTS_TAG;
ALTER TAG IF EXISTS ANALYSTS_TAG RENAME TO TAG_ANALYST;
DROP TAG TAG_ANALYST;
UNDROP TAG TAG_ANALYST;


-- DML
INSERT INTO db_example.gold.dim_where (id, nombre) VALUES (10, 'Nombre ejemplo');
MERGE INTO db_example.gold.dim_who USING db_example.silver.who ON who.id = dim_who.id WHEN MATCHED THEN UPDATE SET username = dim_who.username;
DELETE FROM db_example.gold.fct_transactions WHERE insertion_date < CURRENT_DATE() - 180;