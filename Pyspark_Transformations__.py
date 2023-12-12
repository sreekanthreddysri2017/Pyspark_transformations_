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

dbutils.fs.mount(
source='wasbs://input@lateststorageaccunt.blob.core.windows.net/',
mount_point= '/mnt/sreekanth',
extra_configs={'fs.azure.account.key.lateststorageaccunt.blob.core.windows.net':'uZ+UfqgOimTkMBIzENI8XqmGKbhPq+sCZWpj2fq1kw+F8869w1bErU7YJbmylZBAVIdj+8FnEiOj+AStL50/6g=='}
)

# COMMAND ----------

# MAGIC %fs ls
# MAGIC
# MAGIC dbfs:/mnt/sreekanth/bronze/

# COMMAND ----------

option={'multiline':'true'}

def read_json(format,path,option):
    return spark.read.format(format).options(**option).load(path)

# COMMAND ----------

df=read_json('json','dbfs:/mnt/sreekanth/bronze/sample.json',option)
display(df)

# COMMAND ----------

from pyspark.sql.functions import*

split_data =df.withColumn("Final_price1", split(df['Final price'],","))

# COMMAND ----------

df2=split_data.select('*',posexplode('Final_price1').alias('pos_Final_price_new','Final_price_new'))
display(df2)

# COMMAND ----------

df3=df2.withColumn("Product_Quantity1", split(df['Product Quantity'],",")).withColumn("Product_basePrice1", split(df['Product basePrice'],","))
display(df3)

# COMMAND ----------

df4=df3.select('*',posexplode('Product_Quantity1').alias('pos_Product_Quantity','Product_Quantity_new'))
display(df4)

# COMMAND ----------

df5=df4.select('*',posexplode('Product_basePrice1').alias('pos_Product_basePrice1','Product_basePrice1_new'))

# COMMAND ----------

display(df5)

# COMMAND ----------

df6=df5.drop('Final_price1','Product_Quantity1','Product_basePrice1')
display(df6)

# COMMAND ----------

df7=df6.filter((df6['pos_Final_price_new']==df6['pos_Product_Quantity']) & (df6['pos_Product_Quantity']==df6['pos_Product_basePrice1']))

display(df7)
#
