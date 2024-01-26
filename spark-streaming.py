# Import Dependencies 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json

# Initialising Spark Session
spark = SparkSession  \
        .builder  \
        .appName("Retail_Project")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read data from Kafka  
raw_data = spark  \
        .readStream  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","18.211.252.152:9092")  \
        .option("subscribe","real-time-project")  \
        .load()

# Define Schema
schema = StructType() \
        .add("invoice_no", LongType()) \
	    .add("country",StringType()) \
        .add("timestamp", TimestampType()) \
        .add("type", StringType()) \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ])))


order_stream = raw_data.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")


# Utility functions

def is_order(type):
   if type=="ORDER":
       return 1
   else:
       return 0

def is_return(type):
   if type=="RETURN":
       return 1
   else:
       return 0
       
def total_items_count(items):
   total_count = 0
   for item in items:
       total_count = total_count + item['quantity']
   return total_count

def calculate_total_cost(items,type):
   total_price = 0
   for item in items:
       total_price = total_price + item['unit_price'] * item['quantity']
   if type=="RETURN":
       return total_price * -1
   else:
       return total_price



# Define the UDFs with the utility functions
flag_order = udf(is_order, IntegerType())
flag_return = udf(is_return, IntegerType())
calculate_item = udf(total_items_count, IntegerType())
calculate_cost = udf(calculate_total_cost, FloatType())


# Console Output
order_stream_extended = order_stream \
 .withColumn("total_items", calculate_item(order_stream.items)) \
  .withColumn("total_cost",calculate_cost(order_stream.items,order_stream.type))\
   .withColumn("is_order", flag_order(order_stream.type)) \
    .withColumn("is_return", flag_return(order_stream.type))



order_table_console = order_stream_extended \
       .select("invoice_no", "country", "timestamp","type","total_items","total_cost","is_order","is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

# Calculate time based KPIs
agg_time = order_stream_extended \
    .withWatermark("timestamp","1 minute") \
    .groupby(window("timestamp", "1 minute","1 minute")) \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        avg("total_cost").alias("average_transaction_size"),
        avg("is_return").alias("rate_of_return")) \
    .select("window.start","window.end","total_volume_of_sales","average_transaction_size","rate_of_return")

# Calculate time and country based KPIs
agg_time_country = order_stream_extended \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window("timestamp", "1 minute","1 minute"), "country") \
    .agg(sum("total_cost").alias("total_volume_of_sales"),
        count("invoice_no").alias("OPM"),
        avg("is_return").alias("rate_of_return")) \
    .select("window.start","window.end","country", "OPM","total_volume_of_sales","rate_of_return")


# Write time based KPI values
console_by_time = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "user/ec2-user/time_kpi") \
    .option("checkpointLocation", "user/ec2-user/time_kpi_checkpoints") \
    .trigger(processingTime="1 minute") \
    .start()


# Write time and country based KPI values
console_by_country = agg_time_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "user/ec2-user/country_kpi") \
    .option("checkpointLocation", "user/ec2-user/country_kpi_checkpoints") \
    .trigger(processingTime="1 minute") \
    .start()

order_table_console.awaitTermination()
console_by_time.awaitTermination()
console_by_country.awaitTermination()