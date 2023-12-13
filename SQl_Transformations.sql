-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE TABLE employee_data(
-- MAGIC     ID INT,
-- MAGIC     Name string,
-- MAGIC     Age INT,
-- MAGIC     City string
-- MAGIC );
-- MAGIC
-- MAGIC INSERT INTO employee_data(ID, Name, Age, City) VALUES
-- MAGIC     (1, 'John', 25, 'New York'),
-- MAGIC     (2, 'Alice', 30, 'San Francisco'),
-- MAGIC     (3, 'Bob', 22, 'Los Angeles'),
-- MAGIC     (4, 'Eve', 28, 'Chicago');
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO employee_data(ID, Name, Age, City) VALUES
-- MAGIC     (5, NULL, 35, 'Houston');
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC select * from employee_data

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO employee_data(ID, Name, Age, City) VALUES (6, 'Mary', NULL, 'Seattle');
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC select * from employee_data

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DELETE FROM employee_data
-- MAGIC WHERE Name is NULL or Age IS NULL;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC select * from employee_data

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE employee_info(
-- MAGIC     ID INT,
-- MAGIC     Department string,
-- MAGIC     Salary INT
-- MAGIC );
-- MAGIC
-- MAGIC INSERT INTO employee_info(ID, Department, Salary) VALUES
-- MAGIC     (1, 'HR', 50000),
-- MAGIC     (2, 'IT', 75000),
-- MAGIC     (3, 'Finance', 60000),
-- MAGIC     (4, 'Sales', 80000),
-- MAGIC     (5,'HR',40000);
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- inner join on the ID column
-- MAGIC SELECT *
-- MAGIC FROM employee_data
-- MAGIC JOIN employee_info ON employee_data.ID = employee_info.ID;
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- left inner join on the ID column
-- MAGIC SELECT *
-- MAGIC FROM employee_data
-- MAGIC LEFT JOIN employee_info ON employee_data.ID = employee_info.ID;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- right inner join on the ID column
-- MAGIC SELECT *
-- MAGIC FROM employee_data
-- MAGIC RIGHT JOIN employee_info ON employee_data.ID = employee_info.ID;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- full outer inner join on the ID column
-- MAGIC -- Create the combined_table with aliases to avoid column conflicts
-- MAGIC CREATE TABLE combined_table AS
-- MAGIC SELECT
-- MAGIC     employee_data.ID AS example_id,
-- MAGIC     employee_data.Name,
-- MAGIC     employee_data.Age,
-- MAGIC     employee_info.Department,
-- MAGIC     employee_info.Salary
-- MAGIC FROM employee_data FULL OUTER JOIN employee_info ON employee_data.ID = employee_info.ID;
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from combined_table

-- COMMAND ----------

-- Delete rows where the ID is NULL
DELETE FROM combined_table
WHERE example_id IS NULL;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from combined_table

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from combined_table order by example_id asc;

-- COMMAND ----------

select max(Salary) from combined_table ;

-- COMMAND ----------

SELECT DISTINCT Salary
FROM combined_table
ORDER BY Salary DESC
LIMIT 1 ;

-- COMMAND ----------

SELECT MAX(Salary) AS SecondHighestSalary
FROM combined_table
WHERE Salary < (SELECT MAX(Salary) FROM combined_table);


-- COMMAND ----------

--In SQL, the OFFSET clause is used in combination with the LIMIT clause to skip a specified number of rows from the beginning of the result set.
  
SELECT DISTINCT Salary
FROM combined_table
ORDER BY Salary DESC
LIMIT 1 OFFSET (3 - 1);


-- COMMAND ----------

select max(Salary),Department,Name from combined_table group by Department, Name limit 2;

-- COMMAND ----------

select * from combined_table;

-- COMMAND ----------

insert into combined_table values (4,'sreekanth',24,'DE',30000)

-- COMMAND ----------

select distinct(*) from combined_table;

-- COMMAND ----------

select example_id,Name from combined_table group by example_id,Name  having count(*)>1;

-- COMMAND ----------

select example_id from combined_table group by example_id having count(*)>1;

-- COMMAND ----------

SELECT DISTINCT *
FROM combined_table;

-- COMMAND ----------

delete  from combined_table where Name='sreekanth';

-- COMMAND ----------

select * from combined_table;

-- COMMAND ----------

update combined_table set Salary=Salary*2 

-- COMMAND ----------

select * from combined_table;

-- COMMAND ----------

update combined_table set Name='sreekanth' where Name like 'Eve';

-- COMMAND ----------

select * from combined_table;

-- COMMAND ----------


