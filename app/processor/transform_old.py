from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import os



conf = SparkConf()

conf.setAll(
    [
        (
            "spark.master",
            os.environ.get("SPARK_MASTER_URL", "spark://spark:7077"),
        ),
        ("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "local[*]")),
        ("spark.submit.deployMode", "client"),
        ("spark.driver.bindAddress", "0.0.0.0"),
        ("spark.app.name", "Platformatory"),
    ]
)

spark= SparkSession.builder.config(conf=conf).getOrCreate()


data = [
    (1, "Alice", 34),
    (2, "Bob", 45),
    (3, "Catherine", 29),
    (4, "David", 54)
]

# Define the schema
columns = ["ID", "Name", "Age"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show DataFrame
df.show()

