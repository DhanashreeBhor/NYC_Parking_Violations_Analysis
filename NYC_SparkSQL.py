# Databricks notebook source
# MAGIC %run ./Connection

# COMMAND ----------

# nyc.write.format("delta").mode('overWrite').option('overwriteSchema', 'true').save(f"{Adls_silver_path}/finalnyc/nycparking/track1/")

# COMMAND ----------

# Import the numpy, pandas, datetime, matplotlib, & seaborn packages

import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid", {'axes.grid' : False})

# COMMAND ----------

nyc.repartition(1).write.mode("overwrite").option("header",'true').csv(f"{Adls_silver_path}/finalnyc")

# COMMAND ----------

# Import SparkSession
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Delta Reader") \
    .getOrCreate()

# Read Delta table into DataFrame
nyc = spark.read.format("delta") \
    .load(f"{Adls_silver_path}/finalnyc/nycparking/track1/")

# Show the DataFrame schema
nyc.printSchema()


# COMMAND ----------

# Create/Replace a Temp View

nyc.createOrReplaceTempView("parking")

# COMMAND ----------

# Run a sample query on the parking table

spark.sql('select * from parking')

# COMMAND ----------

# Find the total number of tickets for each year

year_wise_tickets = spark.sql("select year(Issue_Date) as year, count(Summons_Number) as no_of_tickets from parking \
                              group by year order by year desc")

year_wise_tickets.show(70)

# COMMAND ----------

# Check the count of years for which the summons have been raised

year_wise_tickets.count()

# COMMAND ----------

# create a dataframe with the year wise tickets data

year_wise_tickets_df = year_wise_tickets.toPandas()

# plot a graph
plt.clf()
year_wise_tickets_df.plot(x= 'year', y='no_of_tickets', kind='bar', color='blue')
plt.title('Year wise tickets count', fontsize = 14)
plt.xlabel("Year", fontsize = 12)
plt.ylabel("Ticket Count", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Check the tickets issued based on plate ids

check_plate_id = spark.sql("select Plate_ID as plate_id, count(*) as ticket_count \
                          from parking \
                          group by plate_id \
                          having count(*) > 1 \
                          order by ticket_count desc")

check_plate_id.show()

# COMMAND ----------

# Check the ticket counts based in registered state

registered_state_wise_tickets = spark.sql("select Registration_State as registration_state, count(*) as ticket_count \
                               from parking \
                               group by registration_state \
                               order by ticket_count desc")

registered_state_wise_tickets.show(70)

# COMMAND ----------

# create a dataframe with the ragistered state wise tickets

registered_state_wise_tickets_df = registered_state_wise_tickets.toPandas()

# plot a graph
plt.clf()
registered_state_wise_tickets_df.head(5).plot(x='registration_state', y='ticket_count', kind='bar', color ='blue')
plt.title('Top 5 States with Maximum Parking Violations for 2017', fontsize = 14)
plt.xlabel("State", fontsize = 12)
plt.ylabel("Ticket Count", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Let us check which month in the year has maximum summons

month_wise_tickets = spark.sql("select month(Issue_Date) as month, count(*) as ticket_count \
                               from parking \
                               group by month(Issue_Date) \
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

top_5_plate_ids = spark.sql("select Plate_ID as plate_id, count(*) as ticket_count \
                             from parking \
                             group by plate_id \
                             order by ticket_count desc \
                             limit 5")
top_5_plate_ids.show()

# COMMAND ----------

# create a dataframe with the month wise tickets data

top_5_plate_ids_df = top_5_plate_ids.toPandas()

# plot a graph
plt.clf()
top_5_plate_ids_df.plot(x='plate_id', y='ticket_count', kind='bar', color='blue')
plt.title("Top 5 License Plate Id with Maximum Parking Violations", fontsize = 14)
plt.xlabel("Licence Plate Id", fontsize = 12)
plt.ylabel("Ticket Count", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Top 5 Plate Types with maximum violations

top_5_plate_types = spark.sql("select Plate_Type as plate_type, count(*) as ticket_count \
                             from parking \
                             group by plate_type \
                             order by ticket_count desc \
                             limit 5")
top_5_plate_types.show()

# COMMAND ----------

# create a dataframe with the month wise tickets data

top_5_plate_types_df = top_5_plate_types.toPandas()

# plot a graph
plt.clf()
top_5_plate_types_df.plot(x='plate_type', y='ticket_count', kind='bar', color='blue')
plt.title("Top 5 Plate Type with Maximum Parking Violations", fontsize = 14)
plt.xlabel("Licence Plate Type", fontsize = 12)
plt.ylabel("Ticket Count", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# How often does each violation code occur? Display the frequency of the top five violation codes

violation_code_frequency = spark.sql("select Violation_Code as violation_code, count(*) as ticket_frequency \
                                      from parking \
                                      group by violation_code \
                                      order by ticket_frequency desc \
                                      limit 5")
violation_code_frequency.show()

# COMMAND ----------

# create a dataframe with the violation_code_frequency

violation_code_frequency_df = violation_code_frequency.toPandas()

# plot a graph
plt.clf()
violation_code_frequency_df.plot(x='violation_code', y='ticket_frequency', kind='bar', color='blue')
plt.title('Frequency Of Top 5 Violation Code', fontsize = 14)
plt.xlabel("Violation Code", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Display the frequency of the top five Vehicle Body Type getting a parking ticket

vehicle_body_type_frequency = spark.sql("select Vehicle_Body_Type as vehicle_body_type, count(*) as ticket_frequency \
                                      from parking \
                                      group by vehicle_body_type \
                                      order by ticket_frequency desc \
                                      limit 5")

vehicle_body_type_frequency.show()

# COMMAND ----------

#create a dataframe with the vehicle_body_type_frequency

vehicle_body_type_frequency_df = vehicle_body_type_frequency.toPandas()

# plot a graph
plt.clf()
vehicle_body_type_frequency_df.head(5).plot(x='vehicle_body_type', y='ticket_frequency', kind='bar', color='blue')
plt.title('Frequency Of Top 5 Parking Violations Based On Vehicle Body Type', fontsize = 14)
plt.xlabel("Vehicle Body Type", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Display the frequency of the top five Vehicle Make getting a parking ticket

vehicle_make_ticket_frequency = spark.sql("select Vehicle_Make as vehicle_make, count(*) as ticket_frequency \
                                          from parking \
                                          group by vehicle_make \
                                          order by ticket_frequency desc \
                                          limit 5")
vehicle_make_ticket_frequency.show()

# COMMAND ----------

# create a dataframe with the vehicle_make_ticket_frequency
vehicle_make_ticket_frequency_df = vehicle_make_ticket_frequency.toPandas()

# plot a graph
plt.clf()
vehicle_make_ticket_frequency_df.plot(x='vehicle_make', y='ticket_frequency', kind='bar', color='blue')
plt.title('Frequency Of Top 5 Parking Violations Based On Vehicle Make', fontsize = 14)
plt.xlabel("Vehicle Make", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Display the frequency of the top six issuer precinct

issuer_precinct_ticket_frequency = spark.sql("select Issuer_Precinct as issuer_precinct, count(*) as ticket_frequency \
                                                from parking \
                                                group by issuer_precinct \
                                                order by ticket_frequency desc")

issuer_precinct_ticket_frequency.show(5)

# COMMAND ----------

# create a dataframe with the issuer_precinct_ticket_frequency

issuer_precinct_ticket_frequency_df = issuer_precinct_ticket_frequency.toPandas()

# plot a graph
plt.clf()
issuer_precinct_ticket_frequency_df[issuer_precinct_ticket_frequency_df.issuer_precinct != 0].head(5)\
                                        .plot(x='issuer_precinct', y='ticket_frequency', kind='bar', color='blue')

plt.title('Frequency Of Top 5 Issuer Precinct', fontsize = 14)
plt.xlabel("Issuer Precinct", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 19 

violation_code_frequency_precinct19 = spark.sql("select Violation_Code as violation_code, count(*) as ticket_frequency \
                                                from parking \
                                                where Issuer_Precinct = 19 \
                                                group by violation_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct19.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 114

violation_code_frequency_precinct114 = spark.sql("select Violation_Code as violation_code, count(*) as ticket_frequency \
                                                from parking \
                                                where Issuer_Precinct = 114 \
                                                group by violation_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct114.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 14

violation_code_frequency_precinct14 = spark.sql("select Violation_Code as violation_code, count(*) as ticket_frequency \
                                                from parking \
                                                where Issuer_Precinct = 14 \
                                                group by violation_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct14.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 18

violation_code_frequency_precinct18 = spark.sql("select Violation_Code as violation_code, count(*) as ticket_frequency \
                                                from parking \
                                                where Issuer_Precinct = 18 \
                                                group by violation_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct18.show()

# COMMAND ----------

# Violation code Frquency for Issuer Precinct 13

violation_code_frequency_precinct13 = spark.sql("select Violation_Code as violation_code, count(*) as ticket_frequency \
                                                from parking \
                                                where Issuer_Precinct = 13 \
                                                group by violation_code \
                                                order by ticket_frequency desc \
                                                limit 5 ")

violation_code_frequency_precinct13.show()

# COMMAND ----------

# Common violation Codes across issuer precincts 19,114, 14, 18 and 13

common_precincts_violation_codes = spark.sql("select Violation_Code as violation_code , count(*) as ticket_frequency \
                                              from parking \
                                              where Issuer_Precinct in (19, 114, 14, 18, 13) \
                                              group by violation_code \
                                              order by ticket_frequency desc \
                                              limit 5 ")

common_precincts_violation_codes.show()

# COMMAND ----------

# create a dataframe with the common_precincts_violation_codes

common_precincts_violation_codes_df = common_precincts_violation_codes.toPandas()

# plot a graph
plt.clf()
common_precincts_violation_codes_df.plot(x='violation_code', y='ticket_frequency', kind='bar', color='blue')
plt.title('Violation Codes Across Issuer Precincts 19,114, 14, 18 and 13', fontsize = 14)
plt.xlabel("Violation Codes", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------

# First let us divide the year into seasons based on the Issue Date

# We shall divide the 4 seasons based on Issue Date as follows

#     Spring = March to May
#     Summer = June to August
#     Autumn = September to November
#     winter = December to February

seasons = spark.sql("select Summons_Number, Issue_Date, Violation_Code,  \
                        case \
                            when MONTH(TO_DATE(Issue_Date, 'MM/dd/yyyy')) between 03 and 05 \
                                then 'spring' \
                            when MONTH(TO_DATE(Issue_Date, 'MM/dd/yyyy')) between 06 and 08 \
                                then 'summer' \
                            when MONTH(TO_DATE(Issue_Date, 'MM/dd/yyyy')) between 09 and 11 \
                                then 'autumn' \
                            when MONTH(TO_DATE(Issue_Date, 'MM/dd/yyyy')) in (1,2,12) \
                                then 'winter' \
                            else 'unknown' \
                        end as Season \
                        from parking")

seasons.show(5)

# COMMAND ----------

# Create/Replace a Temp View

seasons.createOrReplaceTempView("nyc_seasons")

# COMMAND ----------

# Frequency of tickets based on season

parking_violations_on_seasons = spark.sql("select Season as season, count(*) as ticket_frequency \
                                           from nyc_seasons \
                                           group by season \
                                           order by ticket_frequency desc")
parking_violations_on_seasons.show()

# COMMAND ----------

# Three most commonly occuring violation for spring i.e. from March to May

spring = spark.sql("select Violation_Code as violation_code, count(*) as violation_count \
                    from nyc_seasons \
                    where Season == 'spring' \
                    group by violation_code \
                    order by violation_count desc \
                    limit 5 ")
spring.show()

# COMMAND ----------

# Three most commonly occuring violation for winter i.e. from December to February

winter = spark.sql("select Violation_Code as violation_code, count(*) as violation_count \
                    from nyc_seasons \
                    where Season == 'winter' \
                    group by violation_code \
                    order by violation_count desc \
                    limit 5 ")
winter.show()

# COMMAND ----------

# Three most commonly occuring violation for summer i.e. from June to September

summer = spark.sql("select Violation_Code as violation_code, count(*) as violation_count \
                    from nyc_seasons \
                    where Season == 'summer' \
                    group by violation_code \
                    order by violation_count desc \
                    limit 5 ")
summer.show()

# COMMAND ----------

# Three most commonly occuring violation for autumn i.e. from September to November

autumn = spark.sql("select Violation_Code as violation_code, count(*) as violation_count \
                    from nyc_seasons \
                    where Season == 'autumn' \
                    group by violation_code \
                    order by violation_count desc \
                    limit 5 ")
autumn.show()

# COMMAND ----------

# Total occurrences of the three most common violation codes.  
top_3_common_violations = spark.sql("select Violation_Code as violation_code, count(*) as ticket_frequency \
                                    from parking \
                                    group by violation_code \
                                    order by ticket_frequency desc \
                                    limit 3")
top_3_common_violations.show()

# COMMAND ----------

# From the above result we know the top five violation codes.

# As per the website, the average prices for the three violation codes are as follows:
#   For violation code 21 = (65 + 45)/2 = $55
#   For violation code 38 = (65 + 35)/2 = $50
#   For violation code 14 = (115 + 115)/2 = $115

from pyspark.sql.functions import when

common_violations_fine_amount = top_3_common_violations.withColumn("fine_amount",
    when(top_3_common_violations.ticket_frequency == 21, top_3_common_violations.ticket_frequency * 55)
    .when(top_3_common_violations.ticket_frequency == 38, top_3_common_violations.ticket_frequency * 50)
    .otherwise(top_3_common_violations.ticket_frequency * 115)
)

common_violations_fine_amount.show()

# COMMAND ----------

# Total amount collected for the three violation codes with maximum tickets

from pyspark.sql import functions as F

total = common_violations_fine_amount.agg(F.sum("fine_amount")).collect()
print('Total amount collected for the three violation codes with maximum tickets : ', total)

# COMMAND ----------

# Display the frequency of the top five Issuer Code getting a parking ticket

Issuer_Code_ticket_frequency = spark.sql("select Issuer_Code as issuer_code, count(*) as ticket_frequency \
                                          from parking \
                                          group by issuer_code \
                                          order by ticket_frequency desc \
                                          limit 5")
Issuer_Code_ticket_frequency.show()


# COMMAND ----------

# create a dataframe with the Issuer_Code_ticket_frequency
Issuer_Code_ticket_frequency_df = Issuer_Code_ticket_frequency.toPandas()

# plot a graph
plt.clf()
Issuer_Code_ticket_frequency_df.plot(x='issuer_code', y='ticket_frequency', kind='bar', color='blue')
plt.title('Frequency Of Top 5 Parking Violations Based On Issuer Code', fontsize = 14)
plt.xlabel("Issuer Code", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()


# COMMAND ----------




# COMMAND ----------

# Display the frequency of the top five Vehicle Body Type getting a parking ticket

violation_county_type_frequency = spark.sql("select Violation_County as violation_county, count(*) as ticket_frequency \
                                      from parking \
                                      group by violation_county \
                                      order by ticket_frequency desc \
                                      limit 5")

violation_county_type_frequency.show()

# COMMAND ----------

# create a dataframe with the violation_county_type_frequency
violation_county_type_frequency_df = violation_county_type_frequency.toPandas()

# plot a graph
plt.clf()
violation_county_type_frequency_df.plot(x='violation_county', y='ticket_frequency', kind='bar', color='blue')
plt.title('Frequency Of Top 5 Parking Violations Based On Violation County', fontsize = 14)
plt.xlabel("Violation County", fontsize = 12)
plt.ylabel("Ticket Frequency", fontsize = 12)
plt.legend('')
plt.show()

# COMMAND ----------


