# Databricks notebook source
import requests
response=requests.get('https://gist.githubusercontent.com/pradeeppaikateel/a5caf3b8dea7cf215b1e0cf8ebbbba4d/raw/79125d55b44de60f519ad3fe12ce24329492e3e3/nest.json')
db=spark.sparkContext.parallelize([response.text])
df=spark.read.option("multiline",True).json(db)
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

employees=[StructField('emp_id',IntegerType(),True),
           StructField('emp_name',StringType(),True)]

properties=[StructField('name',StringType(),True),
           StructField('store_size',StringType(),True)]

schema=StructType([StructField('employees',ArrayType(StructType(employees),True),
                   StructField('id',IntegerType(),True),
                   StructField('properties',StructType(properties),True))])

# COMMAND ----------

# import requests
# response=requests.get('https://gist.githubusercontent.com/pradeeppaikateel/a5caf3b8dea7cf215b1e0cf8ebbbba4d/raw/79125d55b44de60f519ad3fe12ce24329492e3e3/nest.json')

# json_rdd=spark.sparkContext.parallelize([response.text])
# df=spark.read.option("multiline",True).schema(schema).json(json_rdd)
# df.show(truncate=False)

# COMMAND ----------

df=spark.range(2)
df.show()

# COMMAND ----------


from pyspark.sql.functions import *

df1=df.withColumn('currentTimeStamp',current_timestamp())
df1.show(truncate=False)

# COMMAND ----------

df2=df1.withColumn('stringdateformat',lit('2023-12-15 11-08-34'))
df2.show()

# COMMAND ----------

df3=df2.withColumn('timestamp',to_timestamp(df2.stringdateformat,'yyyy-MM-dd HH-mm-ss'))
df3.show()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema=StructType([StructField('Name',StringType(),True),
                   StructField('Department',StringType(),True)])

# COMMAND ----------

csv_options={'header':True,
             'inferschema':True,
             'delimiter':','}
        
def read_csv(path,csv_options,schema):
    return spark.read.options(**csv_options).schema(schema).csv(path)

df=read_csv("dbfs:/FileStore/kanth/Book4_1.csv",csv_options,schema)
display(df)

# COMMAND ----------

# In the given code, sets are used to efficiently compare the columns of a DataFrame with an expected set of column names.

expected_col=set(["Name","Department","Salary"])
if set(df.columns)==expected_col:
    print('all columns is available')
else:
    missing_columns=expected_col-set(df.columns)
    print(f'columns are missing:{missing_columns}')

# COMMAND ----------


