import sys
import os
import json
from pyspark.sql import functions as F

from pyspark.sql import SparkSession

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType

input_item_path = "../resource/item.csv"
input_action_path = "../resource/action.csv"

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


action_schema = StructType([
    StructField("user_id", StringType()),
    StructField("item_id", StringType()),
])


@udf(returnType=action_schema)
def extract_action(message_str):
    t = message_str.split("_!_")
    return {
        "user_id": t[0],
        "item_id": t[1]
    }


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

action_df = spark.read.format("text").option("multiLine", "false").text(input_action_path)
# filter empty row
action_df = action_df.filter("value is not null")
action_df = action_df.withColumn("data", extract_action(F.col("value")))

action_df = action_df.withColumn("user_id", F.col("data.user_id"))
action_df = action_df.withColumn("item_id", F.col("data.item_id"))
action_df = action_df.select("user_id", "item_id")


combine_df = action_df.join(df, action_df["item_id"] == movie_df["id"], "left")
combine_df = combine_df.select("user_id", "actors", "movie_type")

user_actor_df = combine_df.select("user_id", F.col("actors"))
user_actor_df = user_actor_df.withColumn("actors", F.explode(F.col("actors")))
user_actor_df = user_actor_df.groupby("user_id", "actors").agg(F.count("actors")
                                                               .cast(IntegerType())
                                                               .alias("count"))

user_movie_type_df = combine_df.select("user_id", "movie_type")
user_movie_type_df = user_movie_type_df.withColumn("movie_type", F.explode(F.col("movie_type")))
user_movie_type_df = user_movie_type_df.groupby("user_id", "movie_type").agg(F.count("movie_type")
                                                                             .cast(IntegerType())
                                                                             .alias("count"))

