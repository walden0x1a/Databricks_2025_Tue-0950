# Databricks notebook source
from pyspark.sql.functions import col
# File location and type
#file_location = "/FileStore/tables/telco.csv"
file_location = "/Workspace/Users/pawel.rubach@sgh.waw.pl/data/telco.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

from pyspark.sql.functions import trim, when

#df_tc_dbl = df.withColumn('TotalCharges', col('TotalCharges').cast('double'))

#display(df.select(trim(col("TotalCharges"))))
#.cast('double')
df_tc_dbl = df.withColumn('TotalCharges', trim(col("TotalCharges")))

#df_tc_dbl['TotalCharges Trimmed'] = df_tc_dbl['TotalCharges Trimmed'].apply(lambda x: float(x) if x != '' else 0)
#df_tc_dbl.replace()
#df_tc_dbl['TotalCharges Trimmed'] = df_tc_dbl['TotalCharges Trimmed'].replace('', 0)

df_tc_dbl = df.withColumn("TotalCharges", \
       when(col('TotalCharges')==" " , None) \
          .otherwise(col('TotalCharges')))
df_tc_dbl = df_tc_dbl.withColumn('TotalCharges', col('TotalCharges').cast('double'))

#df_tc_dbl = df.withColumn('TotalCharges', df.select(trim(col("TotalCharges"))).cast('double'))
display(df_tc_dbl)

# COMMAND ----------

res = df_tc_dbl.groupBy(col('PhoneService'), col('InternetService')).count()
res.show()
display(res)

# COMMAND ----------

res = df_tc_dbl.groupBy(df_tc_dbl['PhoneService'], df_tc_dbl['InternetService']).agg({'TotalCharges': 'avg', 'MonthlyCharges': 'min', 'Tenure': 'avg'})
display(res)

# COMMAND ----------

#from pyspark.sql.functions import avg, max, round
#res = df_tc_dbl.groupBy(col('PhoneService'), col('InternetService')).agg(round(avg(col('TotalCharges')), 2),  max(col('MonthlyCharges')), round(avg(col('Tenure')), 2))

import pyspark.sql.functions as f
res = df_tc_dbl.groupBy(col('PhoneService'), col('InternetService')).agg(f.round(f.avg(col('TotalCharges')), 2).alias('Avg Total Charges'),  f.max(col('MonthlyCharges')).alias('Max Monthly Charges'), f.round(f.avg(col('Tenure')), 2).alias('Avg Tenure'))
display(res)

# COMMAND ----------

row_list = res.collect()
print(type(row_list))
print(row_list)

#r = row_list[1]

for r in row_list:
  row_dict = r.asDict()
  print(row_dict)
  print(row_dict['Avg Total Charges'])  


# COMMAND ----------

df_res = df_tc_dbl.withColumn('Tenure x Monthly Charges', f.round(col('Tenure')*col('MonthlyCharges'), 2))
display(df_res)

# COMMAND ----------

df_res2 = df_res.withColumn('Abs Diff Tenure x Monthly Charges and TotalCharges', f.abs(f.round(col('Tenure x Monthly Charges')- col('TotalCharges'), 2)))
display(df_res2)

# COMMAND ----------

df_res3 = df_res2.withColumn('Diff Tenure x Monthly Charges and TotalCharges greater MonthlyCharges', col('Abs Diff Tenure x Monthly Charges and TotalCharges') > col('MonthlyCharges'))
display(df_res3)

# COMMAND ----------

df_res4 = df_res3[col('Diff Tenure x Monthly Charges and TotalCharges greater MonthlyCharges')==True]
display(df_res4)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def convert_contract(cntr):
  return cntr.replace('One', '1').replace('Two', '2').replace('-to-month', 'ly')

convert_contract_func = udf(lambda x: convert_contract(x), StringType())


# COMMAND ----------

df_cnv_cntr = df_res4.withColumn('Conv contract', convert_contract_func(col('Contract')))
display(df_cnv_cntr)
