from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("silver").getOrCreate()

books   = spark.read.parquet("bronze/tablets")


books = (books
    .withColumn("precio", col("price").cast(DoubleType()))
)

books.write.mode("overwrite").parquet("silver/tablets")

print("Silver listo.")
spark.stop()


