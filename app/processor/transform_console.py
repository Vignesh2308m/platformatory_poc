from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, sum as _sum, abs as _abs
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import window


# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaAvroExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "broker:29092"
kafka_topic = "purchase-topic"
avro_schema_path = "./purchase_schema.json"

# Postgres Connection properties
jdbc_url = "jdbc:postgresql://postgres:5432/plf_training"
connection_properties = {
    "user": "platformatory",
    "password": "plf_password",
    "driver": "org.postgresql.Driver"
}

product_df = spark.read.jdbc(url=jdbc_url, table="public.product", properties=connection_properties)

product_df.show()

# Read Avro schema from file
with open(avro_schema_path, 'r') as schema_file:
    avro_schema = schema_file.read()

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()


# Select the 'value' field and decode Avro data
avro_df = kafka_df\
            .select(
                to_timestamp(col("key")).alias("event_timestamp"), 
                from_avro(col("value"), avro_schema).alias("data")
            ).select(
                col("event_timestamp"),col("data.*")
            ).withColumn(
                "id",_abs(col("id")%100)
            )

purchase_df=avro_df.join(
                product_df,on="id",how="inner"
            ).select(
                col("event_timestamp"),col("name"),col("category"),col("price"),
                col("quantity"),col("discount")
            ).withColumn(
                "total_price", col("price")*col("quantity")-col("discount")
            )

windowed_df = purchase_df \
            .withWatermark(
                "event_timestamp", "1 hours"
            ).groupBy(
                window(col("event_timestamp"), "1 hours", "5 minutes"),
                col("name"),col("category")
            ).agg(
                _sum(col("total_price")).alias("total_1hr")
            )


# Print the decoded data
query = windowed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
