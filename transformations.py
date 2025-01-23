# -*- coding: utf-8 -*-

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime,timedelta

# Initialize Spark session
spark = SparkSession.builder.appName("Name").getOrCreate()

#For Existing data Transformation
days_n=0

# Get current date in 'YYYY-MM-DD' format
current_date = (datetime.now() - timedelta(days=days_n)).strftime("%Y-%m-%d")

# Construct file paths using the current date
user_data_path = "/user/spark/vedant/project_data/" + current_date + "/user_data"
card_data_path = "/user/spark/vedant/project_data/" + current_date + "/card_data"
transaction_data_path = "/user/spark/vedant/project_data/" + current_date + "/transaction_data"


# Read Parquet files using the constructed paths
user_df = spark.read.format("parquet").load(user_data_path)
card_data_df = spark.read.format("parquet").load(card_data_path)
transaction_df = spark.read.format("parquet").load(transaction_data_path)



#User Data Transformation
#Convert per_capita_income, yearly_income, and total_debt fields to numeric formats (e.g., DoubleType) to enable calculations.
user_df = user_df.withColumn("per_capita_income",regexp_replace("per_capita_income","[$]",''))\
                    .withColumn("yearly_income",regexp_replace("yearly_income","[$]",''))\
                        .withColumn("total_debt",regexp_replace("total_debt","[$]",''))

user_df = user_df.withColumn("per_capita_income",user_df["per_capita_income"].cast(DoubleType()))\
                    .withColumn("yearly_income",user_df["yearly_income"].cast('double'))\
                        .withColumn("total_debt",user_df["total_debt"].cast('double'))
                                             
                                             
#Derive age from birth_year and birth_month to ensure consistency with current_age.
#Calculate years_until_retirement as retirement_age - current_age.
#user_df = user_df.withColumn("birth_year_month",concate(col("birth_year"),"-",col("birth_month")))                                                                                    
from pyspark.sql.functions import *
user_df = user_df.withColumn("birth_date", expr("birth_year || '-' || birth_month"))\
                    .withColumn("birth_date",to_date("birth_date"))
user_df = user_df.withColumn("current_date",current_date())


#now creating the column name "current_new_age" and deleting the Old "current_age" Column
user_df = user_df.withColumn("current_new_age",(months_between(col("current_date"),col("birth_date"))/12).cast("int"))
user_df = user_df.drop("current_age")

#Create a debt_to_income_ratio by dividing total_debt by yearly_income.
user_df= user_df.withColumn("dept_to_income_ration",round(user_df["total_debt"]/user_df["yearly_income"],2))


#Verify that latitude and longitude fall within valid ranges; handle invalid entries by filtering or imputing values.
#The range for latitude is -90° to 90°, and the range for longitude is -180° to 180°
user_df = user_df.withColumn("latitude",when((col("latitude") > -90) & (col("latitude") < 90), col("latitude")).otherwise(None))\
                    .withColumn("latitude",when((col("latitude") > -90) & (col("latitude") < 90), col("latitude")).otherwise(None))
                                       

#Standardize gender values (e.g., "M", "F", "Other") and handle inconsistent or missing values
user_df = user_df.withColumn(
    "gender",
    when(col("gender") == "Male", "M")
    .when(col("gender") == "Female", "F")
)


#Segment credit_score into categories (e.g., “Poor,” “Fair,” “Good”) to facilitate analysis.
#For a score with a range between 300 and 850, a credit score of 670 to 739 is considered good. 
#Credit scores above 740 are very good and above 800 are excellent.
user_df = user_df.withColumn("credit_score_indicator",when((col("credit_score").between('670',"739")),"Fair")
                             .when((col("credit_score") >= 740) & (col("credit_score") < 8000),"Good" )
                             .when((col("credit_score") > 800),"Excellent").otherwise("Poor"))


user_df= user_df.withColumnRenamed("id","user_id")


#Card Data transformation

#Data Type Conversion:
card_data_df= card_data_df.withColumn("expires",regexp_replace("expires","[/]",'-'))\
                    .withColumn("acct_open_date",regexp_replace("acct_open_date","[/]",'-'))


#Convert credit_limit to a numeric type (e.g., DoubleType) for numerical analysis.
#Ensure acct_open_date and expires are in DateType for easier date calculations.
card_data_df = card_data_df.withColumn("expires",to_date("expires","MM-yyyy"))\
                                .withColumn("acct_open_date",to_date("acct_open_date","MM-yyyy"))\
                                    .withColumn("year_pin_last_changed",col("year_pin_last_changed").cast("string"))\
                                        .withColumn("year_pin_last_changed",to_date("year_pin_last_changed"))\
                                        .withColumn("credit_limit",regexp_replace("credit_limit","[$]",'').cast("double"))
                                        
## Date Calculations:
#Derive the account_age in years from acct_open_date and current date.
card_data_df = card_data_df.withColumn("current_date",current_date())
card_data_df = card_data_df.withColumn("account_age",(months_between(col("current_date"),col("acct_open_date"))/12).cast("int"))


#Calculate years_since_pin_change using year_pin_last_changed.
card_data_df = card_data_df.withColumn("years_since_pin_change",(months_between(col("current_date"),col("year_pin_last_changed"))/12).cast("int"))


#Categorize card_type (e.g., "Credit," "Debit") and card_brand (e.g., "Visa," "Mastercard") into predefined groups.
from pyspark.sql import functions as F
card_data_df = card_data_df.withColumn(
    "card_type_category",
    F.when(F.col("card_type") == "Credit", "Credit Card")
    .when(F.col("card_type").like("Debit%"), "Debit Card")
    .otherwise("Other")
).withColumn(
    "card_brand_category",
    F.when(F.col("card_brand") == "Visa", "Visa")
    .when(F.col("card_brand") == "Mastercard", "Mastercard")
    .when(F.col("card_brand")== "Discover","Discover")
    .when(F.col("card_brand")== "Amex","Amex")
    .otherwise("Other Brand")
)


#Convert has_chip and card_on_dark_web to booleans (e.g., True/False) for consistency.
card_data_df = card_data_df.withColumn("has_chip",col("has_chip").cast("boolean"))\
                            .withColumn("card_on_dark_web",col("card_on_dark_web").cast("boolean"))

card_data_df = card_data_df.withColumnRenamed("id","card_id")


## Transaction Data transformation

#### Data Type Conversion:
#Convert amount to DoubleType for numerical operations.
#Convert use_chip and errors to a standardized format (e.g., "Yes/No" or Boolean).
transaction_df = transaction_df.withColumn("amount",regexp_replace("amount","[$,-]",'').cast("double"))\
                                    .withColumn("zip",col("zip").cast("integer"))\
                                        .withColumn("use_card",when(col("use_chip") == "Swipe Transaction","true").otherwise("false").cast("boolean"))\
                                            .withColumn("use_online",when(col("use_chip") == "Online Transaction","true").otherwise("false").cast("boolean"))

#Extract date components (day, month, year) from date for time-based analysis.
from pyspark.sql.functions import dayofmonth  # Import the correct function

# Extract date components (day, month, year) from date for time-based analysis
transaction_df = transaction_df.withColumn("year", year("date"))\
                                .withColumn("month", month("date"))\
                                .withColumn("day", dayofmonth("date"))  # Use dayofmonth instead of day



#Calculate hour_of_day to understand transaction timing patterns.
#Calculate transaction_date to understand transaction date patterns.
transaction_df = transaction_df.withColumn("hour_of_day",date_format("date", "HH:mm:ss"))\
                                .withColumn("transaction_date", to_date(col("date")))

#### Date and Time Features:
#Extract date components (day, month, year) from date for time-based analysis.
transaction_df = transaction_df.withColumn("year", year("date"))\
                                .withColumn("month", month("date"))\
                                .withColumn("day", dayofmonth("date"))  # Use dayofmonth instead of day



#Calculate hour_of_day to understand transaction timing patterns.
#Calculate transaction_date to understand transaction date patterns.

transaction_df = transaction_df.withColumn("hour_of_day",date_format("date", "HH:mm:ss"))\
                                .withColumn("transaction_date", to_date(col("date")))

#### Standardize Location Data:
#Ensure consistent formatting for merchant_city, merchant_state, and zip (e.g., uppercase city/state, string format for zip).


transaction_df = transaction_df.withColumn("zip",col("zip").cast("string"))\
                                .withColumn("merchant_city",upper(col("merchant_city")))\
                                .withColumn("merchant_state",upper(col("merchant_state")))
                                
#### Merchant Category Code (MCC) Grouping:
#Map mcc to categories like "Retail," "Travel," or "Food" to facilitate BI analysis.

transaction_df = transaction_df.withColumn(
    "category",
    when(col("mcc").between(5400, 5499), "Retail") \
    .when(col("mcc").between(5812, 5814), "Food") \
    .when(col("mcc").between(3000, 3299), "Travel") \
    .when(col("mcc").between(4814, 4899), "Telecom") \
    .otherwise("Other")
)

#### Error Handling:
#Standardize values in the errors field for easier analysis and reporting.

transaction_df = transaction_df.withColumn(
    "errors",
    when(col("errors").isNull(), "No Error") \
    .when(col("errors").isin("NULL", "N/A", "NA", "null"), "No Error") \
    .when(col("errors").like("%timeout%"), "Timeout Error") \
    .when(col("errors").like("%decline%"), "Transaction Declined") \
    .when(col("errors").like("%invalid%"), "Invalid Entry") \
    .otherwise(col("errors"))
)

transaction_df = transaction_df.withColumnRenamed("id","Transaction_id")


user_df = user_df.withColumnRenamed("client_id", "user_client_id")
card_data_df = card_data_df.withColumnRenamed("client_id", "card_client_id")
card_data_df = card_data_df.withColumnRenamed("current_date", "card_current_date")
card_data_df = card_data_df.withColumnRenamed("id", "card_id")


## Final Data Joins
new_df = user_df.join(card_data_df, user_df.user_id == card_data_df.card_client_id, "inner")
new_df = new_df.drop("card_client_id", "card_current_date")
new2_df = new_df.join(transaction_df, new_df.user_id == transaction_df.client_id, "inner")
nov_df = new2_df.drop("client_id", "card_id")

#final_data_path = "/user/spark/vedant/project_data/" + current_date + "/Final_transformed_with_joins_csv.csv"

# Write the final DataFrame to a Parquete file
#nov_df.repartition(5).write.option("header", "true").csv("/user/spark/vedant/project_data/final_data.csv")
nov_df.repartition(5).write.option("header", "true").parquet("/user/spark/vedant/project_data/final_data.parquet")

