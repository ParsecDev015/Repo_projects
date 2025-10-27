from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, when

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisResenasAmazon_Batch") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# Leer el archivo Excel
# Asegúrate de tener el conector spark-excel en /opt/spark/jars
df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/amazon.xlsx")

# Filtrar filas con reseñas no nulas
df = df.na.drop(subset=["review_content"])

# Convertir texto a minúsculas
df = df.withColumn("review_lower", lower(col("review_content")))

# Clasificación simple de sentimiento por palabras clave
df = df.withColumn(
    "sentimiento",
    when(col("review_lower").contains("good"), "Positiva")
    .when(col("review_lower").contains("excellent"), "Positiva")
    .when(col("review_lower").contains("amazing"), "Positiva")
    .when(col("review_lower").contains("great"), "Positiva")
    .when(col("review_lower").contains("bad"), "Negativa")
    .when(col("review_lower").contains("poor"), "Negativa")
    .when(col("review_lower").contains("terrible"), "Negativa")
    .otherwise("Neutra")
)

# Mostrar un resumen
df.groupBy("sentimiento").count().show(truncate=False)

# Guardar resultados
df.select("product_name", "rating", "review_title", "review_content", "sentimiento") \
    .write.mode("overwrite").csv("resultados/resenas_clasificadas", header=True)

spark.stop()
