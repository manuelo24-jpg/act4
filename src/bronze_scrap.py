#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("bronze-scraping").getOrCreate()

BASE_URL = "https://webscraper.io/test-sites/e-commerce/static/computers/tablets?page={}"

data = []

page = 1
while True:
    print(f"Scraping página {page}...")

    url = BASE_URL.format(page)
    response = requests.get(url)

    if response.status_code != 200:
        print("No hay más páginas. Finalizando.")
        break

    soup = BeautifulSoup(response.text, "html.parser")
    tablets = soup.select(".product-wrapper")

    if not tablets:
        print("No hay tablets en esta página. Fin.")
        break

    for t in tablets:
        try:
            # Nombre de la tablet
            title = t.select_one(".caption a.title").get_text(strip=True)
            print(title)

            # Precio
            price_raw = t.select_one(".caption .price span").get_text(strip=True)
            price = float(price_raw.replace("$", "").strip())

            # Rating numérico (viene en data-rating)
            rating_tag = t.select_one(".ratings p[data-rating]")
            rating = int(rating_tag["data-rating"]) if rating_tag else 0

            data.append((title, price, rating))

        except Exception as e:
            print("Error procesando tablet:", e)

    page += 1


# =======================================================
# SPARK: SCHEMA + DATAFRAME
# =======================================================

schema = StructType([
    StructField("title", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("rating", IntegerType(), True)
])

df_scraping = spark.createDataFrame(data, schema)
df_scraping.show(50, truncate=False)

df_scraping.write.mode("overwrite").parquet("bronze/tablets")

print("Scraping tablets COMPLETO → bronze/tablets")

spark.stop()
