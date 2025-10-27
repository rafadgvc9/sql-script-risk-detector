USE DATABASE db_eximple;
USE SCHEMA db_exemple.gold;
USE WAREHOUSE db_exemple.wh_bi;
CREATE DATABASE db_tmp;
CALL PROCEDURE db_example.gold.proc_replace();