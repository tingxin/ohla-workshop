import sys
from pyspark.sql import functions as F

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

input_item_path = "s3://ohla-workshop/input/item.csv"
s3_output = "s3://ohla-workshop/glue_output/{0}/"

item_schema = StructType([
    StructField("id", StringType()),
    StructField("item_value1", StringType()),
    StructField("item_value2", StringType()),
    StructField("item_value3", StringType()),
    StructField("item_value4", StringType()),
    StructField("item_value5", StringType()),
    StructField("item_value6", StringType()),
    StructField("item_value7", StringType()),
    StructField("item_value8", StringType()),
    StructField("item_value9", StringType()),
    StructField("item_value10", StringType()),
    StructField("item_value11", StringType()),
    StructField("item_value12", StringType())
])


@udf(returnType=item_schema)
def extract_item(message_str):
    t = message_str.split("_!_")
    u = {"item_value{0}".format(i): t[i] for i in range(1, len(t))}
    u["id"] = t[0]
    return u


@udf(returnType=ArrayType(StringType()))
def split_field(message_str):
    if message_str:
        return message_str.split("|")
    return []


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
glue_spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = SparkSession.builder.getOrCreate()

item_df = spark.read.format("text").option("multiLine", "false").text(input_item_path)
# filter empty row
item_df = item_df.filter("value is not null")

item_df = item_df.withColumn("data", extract_item(F.col("value")))
item_df = item_df.select("data.*")

df = item_df.select("id", "item_value2", "item_value5", "item_value6")
df = df.withColumn("name", F.col("item_value2"))
df = df.withColumn("actors", split_field(F.col("item_value5")))
df = df.withColumn("movie_type", split_field(F.col("item_value6")))
movie_df = df.select("id", "name", "actors", "movie_type")

movie_detail_df_dyn_df = DynamicFrame.fromDF(movie_df, glueContext, "nested")

# used to next step etl
glueContext.write_dynamic_frame.from_options(frame=movie_detail_df_dyn_df, connection_type="s3", format="json",
                                             connection_options={
                                                 "path": s3_output.format("movie_detail"),
                                                 "partitionKeys": []},
                                             transformation_ctx="sink2")




