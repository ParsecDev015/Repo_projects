from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.streaming import StreamingQueryException

#  Función de clasificación simple
def clasificar_sentimiento(texto):
    texto = texto.lower()
    if any(p in texto for p in ["excelente", "bueno", "encantó", "perfecto", "recomendado", "superó"]):
        return "Positiva"
    elif any(n in texto for n in ["mala", "horrible", "no recomiendo", "defectuoso", "dañado"]):
        return "Negativa"
    else:
        return "Neutra"

#  Registrar la función como UDF
from pyspark.sql.functions import udf
clasificar_udf = udf(clasificar_sentimiento, StringType())

#  Crear sesión Spark
spark = SparkSession.builder \
    .appName("KafkaAmazonReviews") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#  Definir el esquema
schema = StructType([
    StructField("review", StringType()),
    StructField("timestamp", StringType())
])

#  Leer desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "amazon_reviews") \
    .load()

#  Parsear JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

#  Aplicar clasificación
result_df = parsed_df.withColumn("sentimiento", clasificar_udf(col("review")))

#  Mostrar resultados en consola
query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

try:
    query.awaitTermination()
except StreamingQueryException as e:
    print(f"Error: {e}")


