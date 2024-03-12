# Databricks notebook source
# MAGIC %run ./ConWidgets

# COMMAND ----------

# Import the numpy, pandas, datetime, matplotlib, & seaborn packages

import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
# import findspark
# findspark.init()
sns.set_style("whitegrid", {'axes.grid' : False})

# COMMAND ----------

# # Import the numpy, pandas, datetime, matplotlib, & seaborn packages

# import numpy as np
# import pandas as pd
# import datetime
# import matplotlib.pyplot as plt
# import seaborn as sns
# import findspark
# findspark.init()
# sns.set_style("whitegrid", {'axes.grid' : False})

# COMMAND ----------

df1 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2020.csv", header = "True" )
df1=df1.toDF(*(col.replace(' ','_') for col in df1.columns)).createOrReplaceTempView('df1')

# COMMAND ----------

# MAGIC %sql select * from df1

# COMMAND ----------

display(spark.sql("""
                  SELECT to_date(LEFT(REPLACE(trim(Issue_Date),'/','') , 8),'MMddyyyy') FROM df1
                  """))

# COMMAND ----------

# df1.count()

# COMMAND ----------

# len(df1.columns)

# COMMAND ----------

df11=df1

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date

# Initialize SparkSession
spark = SparkSession.builder.appName("Date Transformation").getOrCreate()

# Step 1: Remove hidden spaces
df11 = df11.withColumn("Issue Date", regexp_replace(col("Issue Date"), " ", ""))

# Step 2: Convert "-" to "/" and standardize date format to MM/dd/yyyy
df11 = df11.withColumn("Issue Date", regexp_replace(col("Issue Date"), "-", "/"))

# Since the datetime might be in different formats and lengths, first extract the relevant portion
# Assuming the maximum length of the date string we're interested in is 10 characters (MM/dd/yyyy)
df11 = df11.withColumn("Issue Date", regexp_replace(col("Issue Date"), "(\\d{2}/\\d{2}/\\d{4}).*", "$1"))


# COMMAND ----------

df11.display()

# COMMAND ----------

# # get the record count
# df11.count()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

# Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# Convert "Issue Date" column to date type (ensure this matches your column's name and format)
df6 = df11.withColumn("Issue Date", to_date(col("Issue Date"), "MM/dd/yyyy"))

# Create or replace a temporary view to use SQL
df6.createOrReplaceTempView("parking")

# SQL query to find the total number of tickets for each month and year
month_year_wise_tickets = spark.sql("""
SELECT 
    year(`Issue Date`) AS year, 
    month(`Issue Date`) AS month, 
    count(`Summons Number`) AS no_of_tickets 
FROM parking 
GROUP BY year, month 
ORDER BY year DESC, month DESC
""")

# # Show the results
# month_year_wise_tickets.show()

# COMMAND ----------

df6.display()

# COMMAND ----------

df2 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2021.csv", header = "True")
# display(df2)

# COMMAND ----------

# df2.count()

# COMMAND ----------

# len(df2.columns)

# COMMAND ----------

df21=df2

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date

# Initialize SparkSession
spark = SparkSession.builder.appName("Date Transformation").getOrCreate()

# Step 1: Remove hidden spaces
df21 = df21.withColumn("Issue Date", regexp_replace(col("Issue Date"), " ", ""))

# Step 2: Convert "-" to "/" and standardize date format to MM/dd/yyyy
df21 = df21.withColumn("Issue Date", regexp_replace(col("Issue Date"), "-", "/"))

# Since the datetime might be in different formats and lengths, first extract the relevant portion
# Assuming the maximum length of the date string we're interested in is 10 characters (MM/dd/yyyy)
df21 = df21.withColumn("Issue Date", regexp_replace(col("Issue Date"), "(\\d{2}/\\d{2}/\\d{4}).*", "$1"))



# # Show the transformed dates
# df21.show(5)

# COMMAND ----------

# # get the record count
# df21.count()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

# Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# Convert "Issue Date" column to date type (ensure this matches your column's name and format)
df7 = df21.withColumn("Issue Date", to_date(col("Issue Date"), "MM/dd/yyyy"))

# Create or replace a temporary view to use SQL
df6.createOrReplaceTempView("parking")

# SQL query to find the total number of tickets for each month and year
month_year_wise_tickets = spark.sql("""
SELECT 
    year(`Issue Date`) AS year, 
    month(`Issue Date`) AS month, 
    count(`Summons Number`) AS no_of_tickets 
FROM parking 
GROUP BY year, month 
ORDER BY year DESC, month DESC
""")

# # Show the results
# month_year_wise_tickets.show(5)

# COMMAND ----------

df3 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2022.csv", header = "True")
# display(df3)


# COMMAND ----------

# len(df3.columns)

# COMMAND ----------

df31=df3

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date

# Initialize SparkSession
spark = SparkSession.builder.appName("Date Transformation").getOrCreate()


# Step 1: Remove hidden spaces
df31 = df31.withColumn("Issue Date", regexp_replace(col("Issue Date"), " ", ""))

# Step 2: Convert "-" to "/" and standardize date format to MM/dd/yyyy
df31 = df31.withColumn("Issue Date", regexp_replace(col("Issue Date"), "-", "/"))

# Since the datetime might be in different formats and lengths, first extract the relevant portion
# Assuming the maximum length of the date string we're interested in is 10 characters (MM/dd/yyyy)
df31 = df31.withColumn("Issue Date", regexp_replace(col("Issue Date"), "(\\d{2}/\\d{2}/\\d{4}).*", "$1"))



# # Show the transformed dates
# df31.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

# Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# Convert "Issue Date" column to date type (ensure this matches your column's name and format)
df8 = df31.withColumn("Issue Date", to_date(col("Issue Date"), "MM/dd/yyyy"))

# Create or replace a temporary view to use SQL
df8.createOrReplaceTempView("parking")

# SQL query to find the total number of tickets for each month and year
month_year_wise_tickets = spark.sql("""
SELECT 
    year(`Issue Date`) AS year, 
    month(`Issue Date`) AS month, 
    count(`Summons Number`) AS no_of_tickets 
FROM parking 
GROUP BY year, month 
ORDER BY year DESC, month DESC
""")

# # Show the results
# month_year_wise_tickets.show(5)

# COMMAND ----------

df4 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2023.csv", header = "True")
# display(df4)


# COMMAND ----------

# len(df4.columns)

# COMMAND ----------

df41=df4

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date

# Initialize SparkSession
spark = SparkSession.builder.appName("Date Transformation").getOrCreate()

# Step 1: Remove hidden spaces
df41 = df41.withColumn("Issue Date", regexp_replace(col("Issue Date"), " ", ""))

# Step 2: Convert "-" to "/" and standardize date format to MM/dd/yyyy
df41 = df41.withColumn("Issue Date", regexp_replace(col("Issue Date"), "-", "/"))

# Since the datetime might be in different formats and lengths, first extract the relevant portion
# Assuming the maximum length of the date string we're interested in is 10 characters (MM/dd/yyyy)
df41 = df41.withColumn("Issue Date", regexp_replace(col("Issue Date"), "(\\d{2}/\\d{2}/\\d{4}).*", "$1"))


# # Show the transformed dates
# df41.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

# Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# Convert "Issue Date" column to date type (ensure this matches your column's name and format)
df9 = df41.withColumn("Issue Date", to_date(col("Issue Date"), "MM/dd/yyyy"))

# Create or replace a temporary view to use SQL
df9.createOrReplaceTempView("parking")

# SQL query to find the total number of tickets for each month and year
month_year_wise_tickets = spark.sql("""
SELECT 
    year(`Issue Date`) AS year, 
    month(`Issue Date`) AS month, 
    count(`Summons Number`) AS no_of_tickets 
FROM parking 
GROUP BY year, month 
ORDER BY year DESC, month DESC
""")

# # Show the results
# month_year_wise_tickets.show(5)

# COMMAND ----------

df5 = spark.read.csv(f"{Adls_raw_path}/Parking_Violations_Issued_Fiscal_Year_2024.csv", header = "True")
# display(df5)

# COMMAND ----------

# len(df5.columns)

# COMMAND ----------

df51=df5

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date

# Initialize SparkSession
spark = SparkSession.builder.appName("Date Transformation").getOrCreate()

# # Sample DataFrame creation (assuming df is your DataFrame with 'Issue Date')
# data = [
#     ("05-08-1972 00:00",),
#     ("08/29/1977 12:00:00 AM",),
#     # Add the rest of your dates here...
#     ("01/24/2000 12:00:00 AM",)
# ]
# df = spark.createDataFrame(data, ["Issue Date"])

# Step 1: Remove hidden spaces
df51 = df51.withColumn("Issue Date", regexp_replace(col("Issue Date"), " ", ""))

# Step 2: Convert "-" to "/" and standardize date format to MM/dd/yyyy
df51 = df51.withColumn("Issue Date", regexp_replace(col("Issue Date"), "-", "/"))

# Since the datetime might be in different formats and lengths, first extract the relevant portion
# Assuming the maximum length of the date string we're interested in is 10 characters (MM/dd/yyyy)
df51 = df51.withColumn("Issue Date", regexp_replace(col("Issue Date"), "(\\d{2}/\\d{2}/\\d{4}).*", "$1"))


# # Show the transformed dates
# df51.show()

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month

# Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# Convert "Issue Date" column to date type (ensure this matches your column's name and format)
df10 = df51.withColumn("Issue Date", to_date(col("Issue Date"), "MM/dd/yyyy"))

# Create or replace a temporary view to use SQL
df10.createOrReplaceTempView("parking")

# SQL query to find the total number of tickets for each month and year
month_year_wise_tickets = spark.sql("""
SELECT 
    year(`Issue Date`) AS year, 
    month(`Issue Date`) AS month, 
    count(`Summons Number`) AS no_of_tickets 
FROM parking 
GROUP BY year, month 
ORDER BY year DESC, month DESC
""")

# # Show the results
# month_year_wise_tickets.show(5)

# COMMAND ----------

df10.show()

# COMMAND ----------

# Union all the DataFrames together to get the final df

final_df = df6.unionAll(df7).unionAll(df8).unionAll(df9).unionAll(df10)

# COMMAND ----------

# Total count of columns
len(final_df.columns)

# COMMAND ----------

# Total count of records
final_df.counts()

# COMMAND ----------

# checking the schema of the table
final_df.printSchema()

# COMMAND ----------

df=final_df

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, year, month

# # Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# # Convert "Issue Date" column to date type (ensure this matches your column's name and format)
# df = df.withColumn("Issue Date", to_date(col("Issue Date"), "MM/dd/yyyy"))

# # Create or replace a temporary view to use SQL
# df.createOrReplaceTempView("parking")

# # SQL query to find the total number of tickets for each month and year
# month_year_wise_tickets = spark.sql("""
# SELECT 
#     year(`Issue Date`) AS year, 
#     month(`Issue Date`) AS month, 
#     count(`Summons Number`) AS no_of_tickets 
# FROM parking 
# GROUP BY year, month 
# ORDER BY year DESC, month DESC
# """)

# # Show the results
# month_year_wise_tickets.show(5)

# COMMAND ----------

# printing null values in the table 
from pyspark.sql.functions import col, sum as pyspark_sum

# Replace 'df' with your DataFrame
null_counts = df.select([pyspark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])

# Display the null counts
null_counts.show()

# COMMAND ----------

# Calculate the threshold for 80% non-null values
threshold = int(df.count() * 0.8)

# List of columns that meet the condition
good_columns = [col for col in df.columns if df.filter(df[col].isNotNull()).count() >= threshold]

# Select these columns
df1 = df.select(*good_columns)

# Show the resulting DataFrame
df1.show(1)

# COMMAND ----------

# Schema of the table
df1.printSchema()

# COMMAND ----------

# Selecting necessary column
columns_to_select = ['Summons Number','Plate ID','Registration State','Plate Type','Issue Date','Violation Code','Vehicle Body Type','Vehicle Make','Issuer Precinct','Issuer Code','Violation Time','Violation County']

# Select the specified columns
df2 = df1.select(*columns_to_select)

# COMMAND ----------

from pyspark.sql.functions import col

# Define a function to apply the renaming logic
def rename_column(name):
    return name.lower().strip().replace(' ', '_')

# Apply the renaming logic to each column
for column in df.columns:
    df2 = df2.withColumnRenamed(column, rename_column(column))

# COMMAND ----------

df2.show(1)

# COMMAND ----------

# Drop duplicate rows, keeping only the first occurrence of each set of duplicated rows
df3 = df2.dropDuplicates()

# COMMAND ----------

from pyspark.sql import functions as F

# Count the number of duplicate rows by grouping and counting
duplicate_count = df3.groupBy(df3.columns).count().where(F.col('count') > 1).count()

# Display the total number of duplicate rows
print("Total number of duplicate rows:", duplicate_count)

# COMMAND ----------

# Check count before dorping null values
print('Count of records including null values: ',df3.count())

# Drop rows with missing values in the columns
df3 = df3.na.drop()

# Check count after dorping null values
print('Count of records excluding null values: ',df3.count())

# COMMAND ----------

# printing null values in the table 
from pyspark.sql.functions import col, sum as pyspark_sum

# Replace 'df3' with your DataFrame
null_counts = df3.select([pyspark_sum(col(c).isNull().cast("int")).alias(c) for c in df3.columns])

# Display the null counts
null_counts.show()

# COMMAND ----------

df3.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track1/')

# COMMAND ----------

# df3.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track2/')

# COMMAND ----------

# # Import SparkSession
# from pyspark.sql import SparkSession

# # Create a SparkSession
# spark = SparkSession.builder \
#     .appName("CSV Reader") \
#     .getOrCreate()

# # Read CSV file into DataFrame
# df4 = spark.read.format("delta") \
#     .option("header", "true") \
#     .load("abfs://silver@azdac24we.dfs.core.windows.net/raw/track1/")

# # Show the DataFrame schema
# df4.printSchema()

# COMMAND ----------

df4=df3

# COMMAND ----------

# Drop duplicate values based on the summons number column

# Check the count before dropping duplicate summon numbers
print('Count before dropping duplicate summon numbers : ', df4.count())

# drop duplicate summon numbers
df4.select('summons_number').dropDuplicates()

# Check the count after dropping duplicate summon numbers
print('Count before dropping duplicate summon numbers : ', df4.count())

# COMMAND ----------

df5=df4

# COMMAND ----------

# Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# Convert "Issue Date" column to date type (ensure this matches your column's name and format)
df5 = df5.withColumn("issue_date", to_date(col("issue_date"), "MM/dd/yyyy"))

# Create or replace a temporary view to use SQL
df5.createOrReplaceTempView("parking")

# # SQL query to find the total number of tickets for each month and year
# month_year_wise_tickets = spark.sql("""
# SELECT 
#     year(`issue_date`) AS year, 
#     month(`issue_date`) AS month, 
#     count(`summons_number`) AS no_of_tickets 
# FROM parking 
# GROUP BY year, month 
# ORDER BY year DESC, month DESC
# """)

# # Show the results
# month_year_wise_tickets.show(150)

# SQL query to find the total number of tickets for each year
year_wise_tickets = spark.sql("""
SELECT 
    year(`issue_date`) AS year, 
    count(`summons_number`) AS no_of_tickets 
FROM parking 
GROUP BY year 
ORDER BY year DESC
""")

# Show the results
year_wise_tickets.show(50)

# COMMAND ----------

# month_year_wise_tickets.count()

# COMMAND ----------

# # SQL query to find the total number of tickets for each year
# year_wise_tickets = spark.sql("""
# SELECT 
#     year(`issue_date`) AS year, 
#     count(`summons_number`) AS no_of_tickets 
# FROM parking 
# GROUP BY year 
# ORDER BY year DESC
# """)

# # Show the results
# year_wise_tickets.show(50)


# COMMAND ----------

year_wise_tickets.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Following can be inferred from the above results :
# MAGIC
# MAGIC - The data contains information about the parking tickets for 53 years between 1977 to 2067
# MAGIC
# MAGIC - 2021 has maximum number of parking tickets followed by 2020
# MAGIC
# MAGIC - Since we would only be analysing 2020 to 2024 related data, let us filter out only 2010 to 2024 related data.

# COMMAND ----------

# df5.count()

# COMMAND ----------

# df5.show()

# COMMAND ----------

from pyspark.sql.functions import to_date

# # Ensure Issue_Date is in the correct date format, if it's not already
# # This step is necessary only if Issue_Date is still a string
# df = df.withColumn("Issue_Date", to_date("Issue_Date", "MM/dd/yyyy"))

# Now, filter using Spark SQL or DataFrame API
# Spark SQL:
# parking_filtered = spark.sql("""
# SELECT * 
# FROM parking
# WHERE Issue_Date BETWEEN to_date('2019-06-01', 'yyyy-MM-dd') AND to_date('2024-01-31', 'yyyy-MM-dd')
# """)

# Or using DataFrame API:
parking_filtered = df5.filter((col("Issue_Date") >= lit("2019-06-01")) & (col("Issue_Date") <= lit("2024-01-31")))

# Count how many rows meet this criteria
print(parking_filtered.count())


# COMMAND ----------

parking_filtered.show(5)

# COMMAND ----------

df7=parking_filtered

# COMMAND ----------

df7.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track2/')

# COMMAND ----------

df8=df7

# COMMAND ----------

# Assuming df51 is your DataFrame and has been prepared as per the previous transformations

# Convert "Issue Date" column to date type (ensure this matches your column's name and format)
# df8 = df8.withColumn("Issue Date", to_date(col("Issue Date"), "MM/dd/yyyy"))

# Create or replace a temporary view to use SQL
df8.createOrReplaceTempView("parking")

# # SQL query to find the total number of tickets for each month and year
# month_year_wise_tickets = spark.sql("""
# SELECT 
#     year(`issue_date`) AS year, 
#     month(`issue_date`) AS month, 
#     count(`summons_number`) AS no_of_tickets 
# FROM parking 
# GROUP BY year, month 
# ORDER BY year DESC, month DESC
# """)

# # Show the results
# month_year_wise_tickets.count()

# SQL query to find the total number of tickets for each year
year_wise_tickets = spark.sql("""
SELECT 
    year(`issue_date`) AS year, 
    count(`summons_number`) AS no_of_tickets 
FROM parking 
GROUP BY year 
ORDER BY year DESC
""")

# Show the results
year_wise_tickets.show()

# COMMAND ----------

# month_year_wise_tickets.show(55)

# COMMAND ----------

# # SQL query to find the total number of tickets for each year
# year_wise_tickets = spark.sql("""
# SELECT 
#     year(`issue_date`) AS year, 
#     count(`summons_number`) AS no_of_tickets 
# FROM parking 
# GROUP BY year 
# ORDER BY year DESC
# """)

# # Show the results
# year_wise_tickets.show()


# COMMAND ----------

# Check the Plate Id for any erroneous data and if exists remove the erroneous data
# Check the tickets issued based on plate ids

check_plate_id = spark.sql("select Plate_ID as plate_id, count(*) as ticket_count \
                          from parking \
                          group by plate_id \
                          having count(*) > 1 \
                          order by ticket_count desc")

check_plate_id.show()



# COMMAND ----------


# Remove the rows containing value as BLANKPLATE for plate_id

df8 = df8.filter((col("plate_id") != "BLANKPLATE") & (col("plate_id") != "NS"))

# COMMAND ----------

# update the temp table with the current data

df8.createOrReplaceTempView("parking")

# Check the count now 

spark.sql("select count(*) as count FROM parking").show()

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track3/')

# COMMAND ----------

# df8.show()

# COMMAND ----------

# df8.count()

# COMMAND ----------


# update the temp table with the current data

df8.createOrReplaceTempView("parking")

# Check the ticket counts based in registered state
registered_state_wise_tickets = spark.sql("select registration_state as reg_state, count(*) as ticket_count \
                               from parking \
                               group by reg_state \
                               order by ticket_count desc")

registered_state_wise_tickets.show(70)

# COMMAND ----------

# Count of distinct Registration states

spark.sql("select count(distinct registration_state) as count from parking").show()


# COMMAND ----------

## replacing the Registration state having '99' value with 'NY' state (state with max entries)

from pyspark.sql.functions import when,lit


df8 = df8.withColumn('registration_state', \
                                     when(df8["registration_state"] == "99", lit('NY'))\
                                     .otherwise(df8["registration_state"]))



# COMMAND ----------

# Count of distinct Registration states

spark.sql("select count(distinct registration_state) as count from parking").show()

# COMMAND ----------

# update the temp table with the current data

df8.createOrReplaceTempView("parking")

# How often does each plate_type occur? Display the frequency of the top five plate_type

registration_state_frequency = spark.sql("select registration_state as registration_states, count(*) as ticket_frequency \
                                      from parking \
                                      group by registration_states \
                                      order by ticket_frequency desc \
                                      limit 5")
registration_state_frequency.show()

# COMMAND ----------

# create a dataframe with the registration_state_frequency

registration_state_frequency_df = registration_state_frequency.toPandas()

# plot a graph
plt.clf()
registration_state_frequency_df.plot(x='registration_states', y='ticket_frequency', kind='bar')
plt.title('Frequency Of Top 5 Registration State', fontsize = 14)
plt.xlabel("Registration State", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track4/')

# COMMAND ----------

# Let us check which month in the year 2017 has maximum summons
df8.createOrReplaceTempView("parking")

month_wise_tickets = spark.sql("select month(issue_date) as month, count(*) as ticket_count \
                               from parking \
                               group by month(issue_date) \
                               order by ticket_count desc")

month_wise_tickets.show()

# COMMAND ----------

# create a dataframe with the month wise tickets data

month_wise_tickets_df = month_wise_tickets.toPandas()

# plot a graph
plt.clf()
month_wise_tickets_df.plot(x= 'month', y='ticket_count', kind='bar', color='blue')
plt.title('Month wise tickets count', fontsize = 14)
plt.xlabel("Month", fontsize = 12)
plt.ylabel("Ticket Count", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Top 5 Plate Ids with maximum violations

top_5_plate_ids = spark.sql("select plate_id as plateid, count(*) as ticket_count \
                             from parking \
                             group by plateid \
                             order by ticket_count desc \
                             limit 5")
top_5_plate_ids.show()

# COMMAND ----------

# create a dataframe with the month wise tickets data

top_5_plate_ids_df = top_5_plate_ids.toPandas()

# plot a graph
plt.clf()
top_5_plate_ids_df.plot(x='plateid', y='ticket_count', kind='bar', color='brown')
plt.title("Top 5 License Plate Id with Maximum Parking Violations", fontsize = 14)
plt.xlabel("Plate Id", fontsize = 12)
plt.ylabel("Ticket Count", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track5/')

# COMMAND ----------

from pyspark.sql.functions import col
df8.createOrReplaceTempView("parking")

# Display the frequency of the top five Vehicle Body Type getting a parking ticket

vehicle_body_type_frequency = spark.sql("select vehicle_body_type as vehicle_body_types, count(*) as ticket_frequency \
                                      from parking \
                                      group by vehicle_body_types \
                                      order by ticket_frequency desc \
                                      limit 5")

vehicle_body_type_frequency.show()

# COMMAND ----------

#create a dataframe with the vehicle_body_type_frequency

vehicle_body_type_frequency_df = vehicle_body_type_frequency.toPandas()

# plot a graph
plt.clf()
vehicle_body_type_frequency_df.head(5).plot(x='vehicle_body_types', y='ticket_frequency', kind='bar')
plt.title('Frequency Of Top 5 Parking Violations Based On Vehicle Body Type', fontsize = 14)
plt.xlabel("Vehicle Body Type", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import col
df8.createOrReplaceTempView("parking")

# Group by 'violation_code', count, and sort in descending order
parking = parking.groupBy("violation_code") \
    .count() \
    .sort(col("count").desc())

# Collecting the results to the driver
results = parking.collect()

# Printing the results
for row in results:
    print(row)

# COMMAND ----------

# Remove the rows containing value as 0 for violation_code

df8 = df8[df8.violation_code != 0]
df8.count()

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track6/')

# COMMAND ----------



# COMMAND ----------

# update the temp table with the current data

df8.createOrReplaceTempView("parking")

# How often does each violation code occur? Display the frequency of the top five violation codes

violation_code_frequency = spark.sql("select violation_code as viol_code, count(*) as ticket_frequency \
                                      from parking \
                                      group by viol_code \
                                      order by ticket_frequency desc \
                                      limit 5")
violation_code_frequency.show()

# COMMAND ----------

# create a dataframe with the violation_code_frequency

violation_code_frequency_df = violation_code_frequency.toPandas()

# plot a graph
plt.clf()
violation_code_frequency_df.plot(x='viol_code', y='ticket_frequency', kind='bar')
plt.title('Frequency Of Top 5 Violation Code', fontsize = 14)
plt.xlabel("Violation Code", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

from pyspark.sql.functions import col

df8.createOrReplaceTempView("parking")

# Assigning the DataFrame to the 'parking' variable
parking = spark.sql("SELECT * FROM parking")

# Group by 'vehicle_make', count, and sort in descending order
parking = parking.groupBy("vehicle_make") \
    .count() \
    .sort(col("count").desc())

# Collecting the results to the driver
results = parking.collect()

# Printing the results
for row in results:
    print(row)

# COMMAND ----------

# update the temp table with the current data

df8.createOrReplaceTempView("parking")

# How often does each vehicle_make occur? Display the frequency of the top five vehicle_make

vehicle_make_frequency = spark.sql("select vehicle_make as vehicles_make, count(*) as ticket_frequency \
                                      from parking \
                                      group by vehicles_make \
                                      order by ticket_frequency desc \
                                      limit 5")
vehicle_make_frequency.show()

# COMMAND ----------

# create a dataframe with the violation_code_frequency

vehicle_make_frequency_df = vehicle_make_frequency.toPandas()

# plot a graph
plt.clf()
vehicle_make_frequency_df.plot(x='vehicles_make', y='ticket_frequency', kind='bar')
plt.title('Frequency Of Top 5 Vehicle Make', fontsize = 14)
plt.xlabel("Vehicle Make", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track8/')

# COMMAND ----------

# from pyspark.sql.functions import col
# df8.createOrReplaceTempView("parking")

# # Group by 'issuer_precinct', count, and sort in descending order
# parking = parking.groupBy("issuer_precinct") \
#     .count() \
#     .sort(col("count").desc())
# 
# # Collecting the results to the driver
# results = parking.collect()

# # Printing the results
# for row in results:
#     print(row)


# COMMAND ----------

# Remove the rows containing value as 0 for issuer_precinct

df8 = df8[df8.issuer_precinct != 0]
df8.count()


# COMMAND ----------

# Remove the rows containing value as 0 for issuer_precinct

df8 = df8[df8.issuer_precinct != 114]
df8.count()


# COMMAND ----------

# update the temp table with the current data

df8.createOrReplaceTempView("parking")

# How often does each issuer_precinct occur? Display the frequency of the top five issuer_precinct

issuer_precinct_frequency = spark.sql("select issuer_precinct as issuers_precinct, count(*) as ticket_frequency \
                                      from parking \
                                      group by issuers_precinct \
                                      order by ticket_frequency desc \
                                      limit 5")
issuer_precinct_frequency.show()

# COMMAND ----------

# create a dataframe with the violation_code_frequency

issuer_precinct_frequency_df = issuer_precinct_frequency.toPandas()

# plot a graph
plt.clf()
issuer_precinct_frequency_df.plot(x='issuers_precinct', y='ticket_frequency', kind='bar')
plt.title('Frequency Of Top 5 Issuer Precinct', fontsize = 14)
plt.xlabel("Issuer Precinct", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 19 

violation_code_frequency_precinct19 = spark.sql("select violation_code as violations_code, count(*) as ticket_frequency \
                                                from parking \
                                                where issuer_precinct = 19 \
                                                group by violations_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct19.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 14

violation_code_frequency_precinct19 = spark.sql("select violation_code as viol_code, count(*) as ticket_frequency \
                                                from parking \
                                                where issuer_precinct = 14 \
                                                group by viol_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct19.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 18

violation_code_frequency_precinct19 = spark.sql("select violation_code as viol_code, count(*) as ticket_frequency \
                                                from parking \
                                                where issuer_precinct = 18 \
                                                group by viol_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct19.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 13 

violation_code_frequency_precinct19 = spark.sql("select violation_code as viol_code, count(*) as ticket_frequency \
                                                from parking \
                                                where issuer_precinct = 13 \
                                                group by viol_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct19.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 1

violation_code_frequency_precinct19 = spark.sql("select violation_code as viol_code, count(*) as ticket_frequency \
                                                from parking \
                                                where issuer_precinct = 1 \
                                                group by viol_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct19.show()

# COMMAND ----------

# Common violation Codes across issuer precincts 19, 14, 18, 13 and 1

common_precincts_violation_codes = spark.sql("select violation_code as violations_code , count(*) as ticket_frequency \
                                              from parking \
                                              where issuer_precinct in (19, 14, 18, 13, 1) \
                                              group by violations_code \
                                              order by ticket_frequency desc \
                                              limit 5 ")

common_precincts_violation_codes.show()

# COMMAND ----------

# create a dataframe with the common_precincts_violation_codes

common_precincts_violation_codes_df = common_precincts_violation_codes.toPandas()

# plot a graph
plt.clf()
common_precincts_violation_codes_df.plot(x='violations_code', y='ticket_frequency', kind='bar')
plt.title('Violation Codes Across Issuer Precincts 19, 14, 18, 13 and 1', fontsize = 14)
plt.xlabel("Violation Codes", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track9/')

# COMMAND ----------

# First let us divide the year into seasons based on the Issue Date

# We shall divide the 4 seasons based on Issue Date as follows

#     Spring = March to May
#     Summer = June to August
#     Autumn = September to November
#     winter = December to February

seasons = spark.sql("select summons_number, issue_date, violation_code,  \
                        case \
                            when MONTH(TO_DATE(issue_date, 'MM/dd/yyyy')) between 03 and 05 \
                                then 'spring' \
                            when MONTH(TO_DATE(issue_date, 'MM/dd/yyyy')) between 06 and 08 \
                                then 'summer' \
                            when MONTH(TO_DATE(issue_date, 'MM/dd/yyyy')) between 09 and 11 \
                                then 'autumn' \
                            when MONTH(TO_DATE(issue_date, 'MM/dd/yyyy')) in (1,2,12) \
                                then 'winter' \
                            else 'unknown' \
                        end as Season \
                        from parking")

seasons.show(10)

# COMMAND ----------

# Create/Replace a Temp View

seasons.createOrReplaceTempView("seasons")

# COMMAND ----------

# Frequency of tickets based on season

parking_violations_on_seasons = spark.sql("select Season as season, count(*) as ticket_frequency \
                                           from seasons \
                                           group by season \
                                           order by ticket_frequency desc")
parking_violations_on_seasons.show()

# COMMAND ----------

# Three most commonly occuring violation for spring i.e. from March to May

spring = spark.sql("select violation_code as violations_code, count(*) as violation_count \
                    from seasons \
                    where Season == 'spring' \
                    group by violations_code \
                    order by violation_count desc \
                    limit 3 ")
spring.show()

# COMMAND ----------

# Three most commonly occuring violation for winter i.e. from December to February

winter = spark.sql("select violation_code as violations_code, count(*) as violation_count \
                    from seasons \
                    where Season == 'winter' \
                    group by violations_code \
                    order by violation_count desc \
                    limit 3 ")
winter.show()

# COMMAND ----------

# Three most commonly occuring violation for summer i.e. from June to September

summer = spark.sql("select violation_code as violations_code, count(*) as violation_count \
                    from seasons \
                    where Season == 'summer' \
                    group by violations_code \
                    order by violation_count desc \
                    limit 3 ")
summer.show()

# COMMAND ----------

# Three most commonly occuring violation for autumn i.e. from September to November

autumn = spark.sql("select violation_code as violations_code, count(*) as violation_count \
                    from seasons \
                    where Season == 'autumn' \
                    group by violations_code \
                    order by violation_count desc \
                    limit 3 ")
autumn.show()


# COMMAND ----------

top_3_common_violations = spark.sql("select violation_code as violations_code, count(*) as ticket_frequency \
                                    from parking \
                                    group by violations_code \
                                    order by ticket_frequency desc \
                                    limit 3")
top_3_common_violations.show()

# COMMAND ----------

# From the above result we know the top three violation codes.

# As per the website, the average prices for the three violation codes are as follows:
#   For violation code 21 = (65 + 45)/2 = $55
#   For violation code 38 = (65 + 35)/2 = $50
#   For violation code 14 = (115 + 115)/2 = $115

from pyspark.sql.functions import when

common_violations_fine_amount = top_3_common_violations.withColumn("fine_amount",
    when(top_3_common_violations.violations_code == 21, top_3_common_violations.ticket_frequency * 55)
    .when(top_3_common_violations.violations_code == 38, top_3_common_violations.ticket_frequency * 50)
    .otherwise(top_3_common_violations.ticket_frequency * 115)
)

common_violations_fine_amount.show()

# COMMAND ----------

# Total amount collected for the three violation codes with maximum tickets

from pyspark.sql import functions as F

total = common_violations_fine_amount.agg(F.sum("fine_amount")).collect()
print('Total amount collected for the three violation codes with maximum tickets : ', total)

# COMMAND ----------

# State the code that has the highest total collection

common_violations_fine_amount.show(1)

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track10/')

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()

# Read CSV file into DataFrame
df10 = spark.read.format("delta") \
    .option("header", "true") \
    .load("abfs://silver@azdac24we.dfs.core.windows.net/raw/track10/")

# Show the DataFrame schema
df10.printSchema()

# COMMAND ----------

df10.repartition(1).write.mode("overwrite").option("header",'true').csv('abfs://powerbicontainer@azdac24we.dfs.core.windows.net/')

# COMMAND ----------

df10.count()

# COMMAND ----------

df10.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track7/')

# COMMAND ----------

# Import SparkSession
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSV Reader") \
    .getOrCreate()

# Read CSV file into DataFrame
df8 = spark.read.format("delta") \
    .option("header", "true") \
    .load("abfs://silver@azdac24we.dfs.core.windows.net/raw/track7/")

# Show the DataFrame schema
df8.printSchema()

# COMMAND ----------

# from pyspark.sql.functions import col

# df8.createOrReplaceTempView("parking")

# # Assigning the DataFrame to the 'parking' variable
# parking = spark.sql("SELECT * FROM parking")

# # Group by 'plate_type', count, and sort in descending order
# parking = parking.groupBy("plate_type") \
#     .count() \
#     .sort(col("count").desc())

# # Collecting the results to the driver
# results = parking.collect()

# # Printing the results
# for row in results:
#     print(row)

# COMMAND ----------

#  Remove the rows containing value as 999 for plate_type

df8 = df8[df8.plate_type != '999']
df8.count()

# COMMAND ----------

df8.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save('abfs://silver@azdac24we.dfs.core.windows.net/raw/track8/')

# COMMAND ----------

# update the temp table with the current data

df8.createOrReplaceTempView("parking")

# How often does each plate_type occur? Display the frequency of the top five plate_type

plate_type_frequency = spark.sql("select plate_type as plate_types, count(*) as ticket_frequency \
                                      from parking \
                                      group by plate_types \
                                      order by ticket_frequency desc \
                                      limit 5")
plate_type_frequency.show()

# COMMAND ----------

# create a dataframe with the violation_code_frequency

plate_type_frequency_df = plate_type_frequency.toPandas()

# plot a graph
plt.clf()
plate_type_frequency_df.plot(x='plate_types', y='ticket_frequency', kind='bar')
plt.title('Frequency Of Top 5 plate type', fontsize = 14)
plt.xlabel("Plate Type", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

from pyspark.sql.functions import col
df8.createOrReplaceTempView("parking")
# Group by 'vehicle_body_type', count, and sort in descending order
parking = parking.groupBy("vehicle_body_type") \
    .count() \
    .sort(col("count").desc())

# Collecting the results to the driver
results = parking.collect()

# Printing the results
for row in results:
    print(row)

# COMMAND ----------

df8.printSchema()

# COMMAND ----------

df9=df8

# COMMAND ----------

df9.count()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Initialize Spark Session
spark = SparkSession.builder.appName("RandomForestClassifierExample").getOrCreate()

# Assuming df9 is your DataFrame after loading data
# Example: df9 = spark.read...

# Indexing categorical columns to transform to numeric, excluding 'issue_date' (if present) and the target 'violation_code'
categoricalColumns = ['summons_number', 'plate_id', 'registration_state', 'plate_type', 'vehicle_body_type', 'vehicle_make', 'issuer_precinct', 'issuer_code', 'violation_time', 'violation_county']
indexers = [StringIndexer(inputCol=c, outputCol="{0}_index".format(c)).fit(df9) for c in categoricalColumns]

# Assembling indexed features into a vector
assemblerInputs = [c + "_index" for c in categoricalColumns]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")

# Indexing the label column (violation_code)
labelIndexer = StringIndexer(inputCol="violation_code", outputCol="label").fit(df9)

# Initialize the Random Forest Classifier
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)

# Construct the pipeline
pipeline = Pipeline(stages=indexers + [assembler, labelIndexer, rf])

# Split the data into training and test sets
(trainingData, testData) = df9.randomSplit([0.7, 0.3])

# Train the model
model = pipeline.fit(trainingData)

# Make predictions
predictions = model.transform(testData)

# Evaluate the model
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Accuracy = %g" % accuracy)

# Stop the SparkSession
spark.stop()


# COMMAND ----------


