from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min, max, avg, format_number, when, round,
    lit
)

spark = SparkSession.builder.appName("gold-tablets").getOrCreate()

# Leer las tablets desde silver
tablets = spark.read.parquet("silver/tablets")

tablets.show(5)

# ==============================================================
# 1) Estadísticas principales (media, min, max precio)
# ==============================================================

tabletStatistics = tablets.agg(
    format_number(avg(col("price")), 2).alias("average_price"),
    max(col("price")).alias("maximum_price"),
    min(col("price")).alias("minimum_price"),
)

tabletStatistics.show()

tabletStatistics.write.mode("overwrite").parquet("gold/tablets_statistics")

# ==============================================================
# 2) Clasificación de tablets por rango de precio
# ==============================================================

tablets_with_range = tablets.withColumn(
    "price_range",
    when(col("price") < 60, lit("Barato"))
    .when((col("price") >= 60) & (col("price") <= 120), lit("Medio"))
    .otherwise(lit("Caro"))
)

tablets_with_range.show(50, truncate=False)

tablets_with_range.write.mode("overwrite").parquet("gold/tablets_price_ranges")

# ==============================================================
# 3) Top 3 tablets con mejor relación calidad-precio
#    Fórmula: ratio = price / rating  (más bajo = mejor relación)
# ==============================================================

tablets_ratio = tablets.withColumn(
    "quality_price_ratio",
    round(col("price") / col("rating"), 3)
)

# Filtrar tablets que tienen rating > 0
tablets_ratio = tablets_ratio.filter(col("rating") > 0)

top3_quality_price = (
    tablets_ratio
    .orderBy(col("quality_price_ratio").asc())
    .limit(3)
)

print("Top 3 tablets con mejor relación calidad-precio:")
top3_quality_price.show(truncate=False)

top3_quality_price.write.mode("overwrite").parquet("gold/tablets_top3_quality_price")

spark.stop()
