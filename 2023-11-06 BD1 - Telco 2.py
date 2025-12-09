# Databricks notebook source
from pyspark.sql.functions import col, trim
# File location and type
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

display(df)

# COMMAND ----------

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
#df_tc_dbl = df.withColumn('TotalCharges', col('TotalCharges').cast('Double'))
#df_tc_dbl.printSchema()
#display(df_tc_dbl)

# COMMAND ----------

df_grp_contract_phone = df_tc_dbl.groupBy(df_tc_dbl['Contract'], df_tc_dbl['PhoneService']).count()
#df_grp_contract_phone = df_tc_dbl.groupBy(col('Contract'), col('PhoneService')).count()

#avg()
display(df_grp_contract_phone)

# COMMAND ----------

df_tc_dbl.groupBy(df_tc_dbl['Contract'], df_tc_dbl['PhoneService']).agg({"MonthlyCharges": "avg", "PhoneService": "count", "Tenure": "max"}).show()

# COMMAND ----------

df_tc_dbl.groupBy(df_tc_dbl['Contract'], df_tc_dbl['PhoneService']).agg({"Contract": "count", "PhoneService": "count"}).show()

# COMMAND ----------

from pyspark.sql.functions import round, avg, max, count

df_tc_dbl_new_col = df_tc_dbl.withColumn('MonthlyCharges x Tenure', round(col('MonthlyCharges')*col('Tenure'), 2))
display(df_tc_dbl_new_col)

# COMMAND ----------

df_results = df_tc_dbl.groupBy(df_tc_dbl['Contract'], df_tc_dbl['PhoneService']).agg(round(avg("MonthlyCharges"),2).alias('AVG Monthly Charges'), count("PhoneService"), max("Tenure"))
df_results.show()

# COMMAND ----------

results_list = df_results.collect()
print(type(results_list))
print(results_list)

# COMMAND ----------

results_dict = results_list[1].asDict()
print(results_dict)
print(results_dict['AVG Monthly Charges'])

# COMMAND ----------

for r in results_list:
  #print(r)
  r_dict = r.asDict()
  #print('Charges: ' + r['Contract'] + ": " + str(r['AVG Monthly Charges']))
  print('Charges: {}: {}'.format(r['Contract'], r['AVG Monthly Charges']))


# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def convert_contract(cntr):
  cntr = cntr.replace('Month-to-month', 'monthly')
  cntr = cntr.replace('One', '1')
  cntr = cntr.replace('Two', '2')
  return cntr

convert_contract_function = udf(lambda z: convert_contract(z), StringType())


# COMMAND ----------

df_results_converted = df_results.withColumn('Converted Contract', convert_contract_function(df_results['Contract']))
display(df_results_converted.drop(col('Contract')))
#display(df_results_converted)

# COMMAND ----------

# MAGIC %md
# MAGIC TODO
# MAGIC 1. Select columns: gender, tenure, InternetService, Contract, MonthlyCharges, TotalCharges, MultipleLines
# MAGIC
# MAGIC group it   gender, Contract, MultipleLines
# MAGIC
# MAGIC avg TotalCharges, count on MultipleLines
# MAGIC
# MAGIC 2. Check if customers with dependents pay on average more than those without dependents. The answer should be returned as a single bool (True or False)
# MAGIC
