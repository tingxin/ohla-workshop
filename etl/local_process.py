from pyspark.sql import functions as F

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, BooleanType, IntegerType
from pyspark.sql import SparkSession


input_item_path = "../resource/item.csv"
input_action_path = "../resource/action.csv"
input_movie_path = "../resource/temp/"
folder_output = "../temp/output-{0}"

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
action_df = spark.read.format("text").option("multiLine", "false").text(input_action_path)
# filter empty row
action_df = action_df.filter("value is not null")
action_df = action_df.withColumn("data", extract_action(F.col("value")))

action_df = action_df.withColumn("user_id", F.col("data.user_id"))
action_df = action_df.withColumn("item_id", F.col("data.item_id"))
action_df = action_df.select("user_id", "item_id")

movie_df = spark.read.option("multiLine", "false").json(input_movie_path)

combine_df = action_df.join(movie_df, action_df["item_id"] == movie_df["id"], "left")
combine_df = combine_df.select("user_id", "item_id", "actors", "movie_type")

u_df = combine_df.withColumn("actor", F.explode(F.col("actors")))
u_df = u_df.withColumn("movie_type", F.explode(F.col("movie_type")))

analytics_df = u_df.select("user_id", "actor", "movie_type")

actor_item_df = u_df.select("item_id", "actor").distinct()
move_type_actor_df = u_df.select("item_id", "movie_type").distinct()

user_actor_df = u_df.select("user_id", "actor").distinct()

user_actor_df = user_actor_df.join(actor_item_df, user_actor_df["actor"] == actor_item_df["actor"], "left")
user_actor_df = user_actor_df.select("user_id", "item_id")

user_movie_type_df = u_df.select("user_id", "movie_type")
user_movie_type_df = user_movie_type_df.join(move_type_actor_df,
                                             user_movie_type_df["movie_type"] == move_type_actor_df["movie_type"],
                                             "left")
user_movie_type_df = user_movie_type_df.select("user_id", "item_id").distinct()

recommend_df = user_movie_type_df.union(user_actor_df).distinct()
recommend_df = recommend_df.orderBy(F.col("user_id"))
print(recommend_df.count())
recommend_df.show(10)