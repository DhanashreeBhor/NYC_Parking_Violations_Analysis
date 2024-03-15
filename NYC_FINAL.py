# Databricks notebook source
# MAGIC %run ./Connection

# COMMAND ----------

data1 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2020.csv",header='True').fillna('')
data2 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2021.csv",header='True').fillna('')
data3 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2022.csv",header='True').fillna('')
data4 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2023.csv",header='True').fillna('')
data5 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2024.csv",header='True').fillna('')

# COMMAND ----------

data = data1.union(data2).union(data3).union(data4).union(data5)

# COMMAND ----------

nyc = data.toDF(*(col.replace(" ","_") for col in data.columns))
nyc.createOrReplaceTempView("data")
# data.count()

# COMMAND ----------

nyc_columns = spark.sql("""SELECT DISTINCT Summons_Number,Plate_ID,Registration_State,Plate_Type,
                         LEFT(replace(Issue_Date,'/','') ,8) AS Issue_Date,Violation_Code,
                         Vehicle_Body_Type,Vehicle_Make,Issuer_Precinct,Issuer_Code,
                         Violation_Time,Violation_County
                         FROM nyc""")

# COMMAND ----------



# COMMAND ----------

#Converting Issued_Date column string datatype to date datatype 
from pyspark.sql.functions import *
nyc = nyc_columns.withColumn("Issue_Date", to_date('Issue_Date','MMddyyyy'))

# COMMAND ----------

nyc.createOrReplaceTempView("data")

# COMMAND ----------

#Extracting data for fiscal year 2019-06-01 to 2024-01-15
nyc = spark.sql(f"""
                    SELECT * FROM data WHERE Issue_Date >= '2019-06-01' AND Issue_Date <= '2024-01-15'
                 """)

# COMMAND ----------

nyc.count()

# COMMAND ----------

nyc.show()

# COMMAND ----------

#checking null values
from pyspark.sql.functions import col

for i in nyc.columns:
    print(i,(20-len(i))*"-",((nyc.filter(nyc[i].isNull()).count())/nyc.count())*100)

# COMMAND ----------

#Dropping null values from data 
nyc = nyc.na.drop()

# COMMAND ----------

# Check for the number of rows 
nyc.count()

# COMMAND ----------

#Replacihg the state named 99 with NY, as NY has the maximum violations.

nyc = nyc.withColumn('Registration_State', regexp_replace('Registration_State', '99', 'NY'))

# COMMAND ----------

#Removing the rows containing value as BLANKPLATE for plate_id 
nyc = nyc[nyc.Plate_ID != 'BLANKPLATE'] 

# COMMAND ----------

#Removing the rows containing value as 999 for plate_id 
nyc = nyc[nyc.Plate_ID != '999']

# COMMAND ----------

#Codes other than those between 1 and 99 are invalid so removing rows with 0 as violation code
nyc = nyc[nyc.Violation_Code != 0 ]

# COMMAND ----------

#Issuing Precinct having invalid entry So removing from columns
nyc = nyc[nyc.Issuer_Precinct != 0 ]

# COMMAND ----------

from pyspark.sql.functions import col, when, unix_timestamp

# Extracting hours, minutes, and AM/PM values from Violation Time column
nyc = nyc.withColumn("violation_hour", col("Violation_Time").substr(1,2).cast("int"))
nyc = nyc.withColumn("violation_minutes", col("Violation_Time").substr(3,2).cast("int"))
nyc = nyc.withColumn("violation_ampm", col("Violation_Time").substr(5,1))

# Converting AM/PM time to absolute hours ranging from 0-23
nyc = nyc.withColumn("violation_hour", 
                   when((col("violation_hour") == 12) & (col("violation_ampm") == 'A'), 0)
                   .when((col("violation_hour") != 12) & (col("violation_ampm") == 'P'), col("violation_hour") + 12)
                   .otherwise(col("violation_hour")))

# After extracting the information, these columns are not required and hence dropped
nyc = nyc.drop("Violation_Time", "violation_ampm")

# Printing the schema now
nyc.printSchema()

# COMMAND ----------

nyc.count()

# COMMAND ----------

nyc.show()

# COMMAND ----------

nyc.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save(f"{Adls_silver_path}/finalnyc/nycparking/track2/")

# COMMAND ----------


