# Análisis de Reseñas de Productos con Apache Spark y Kafka

Este proyecto implementa un pipeline de procesamiento de datos en **Apache Spark**, utilizando **Kafka** para simular un flujo de datos en tiempo real.  
El objetivo es analizar reseñas de productos de Amazon, clasificarlas por sentimiento y realizar un procesamiento tanto **batch** como **streaming**.

---

## Estructura del proyecto

bigdata_3/
├── data/
│   └── amazon.xlsx                 # Dataset base (fuente: Kaggle)
├── resultados/
│   └── resenas_clasificadas/       # Resultados del procesamiento batch
├── batch_processing.py             # Proceso batch (análisis estático)
├── streaming_processing.py         # Proceso streaming (análisis en tiempo real)
├── kafka_producer.py               # Emisor de mensajes a Kafka (productor)
├── kafka_consumer.py               # Receptor de mensajes desde Kafka (consumidor)
├── zookeeper_kafka_commands.txt    # Comandos para ejecutar Zookeeper, Kafka, Producer y Consumer
├── requirements.txt                # Dependencias Python
└── README.md                       # Este archivo

---

## Objetivos del proyecto

- Procesar y analizar reseñas de productos de Amazon usando **Spark SQL** y **PySpark**.  
- Clasificar reseñas como **positivas**, **negativas** o **neutras** según palabras clave.  
- Simular flujo de datos en tiempo real con **Kafka** y **Spark Streaming**.  
- Almacenar resultados en formato **CSV** para posterior análisis.

---

## Requisitos del entorno

- Ubuntu o Debian (se recomienda usar una **máquina virtual** o **WSL**)
- Java 11+
- Apache Spark ≥ 3.5.1
- Apache Kafka ≥ 3.6.0
- Python 3.10+  
- pip

---

## Instalación del entorno Python

Desde el directorio raíz del proyecto:

# Crear entorno virtual
python3 -m venv env
source env/bin/activate

# Instalar dependencias
pip install -r requirements.txt

---

## Instalación de Spark y configuración del conector Excel

1. Instala Apache Spark:
   sudo apt install openjdk-11-jdk -y
   cd /opt
   sudo wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
   sudo tar -xvf spark-3.5.1-bin-hadoop3.tgz
   sudo mv spark-3.5.1-bin-hadoop3 /opt/spark

2. Configura las variables de entorno en ~/.bashrc o ~/.zshrc:
   export SPARK_HOME=/opt/spark
   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
   export PYSPARK_PYTHON=python3

3. Descarga el conector de Excel compatible con Spark:
   sudo wget https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/3.5.1_0.20.4/spark-excel_2.12-3.5.1_0.20.4.jar -P /opt/spark/jars/

4. Verifica que Spark lo reconozca correctamente:
   spark-shell --jars /opt/spark/jars/spark-excel_2.12-3.5.1_0.20.4.jar

---

## Ejecución del procesamiento por lotes (Batch)

spark-submit --jars /opt/spark/jars/spark-excel_2.12-3.5.1_0.20.4.jar batch_processing.py

**Salida esperada:**
- Muestra el conteo de reseñas por sentimiento.
- Genera los resultados en resultados/resenas_clasificadas/.

---

## Ejecución del procesamiento en tiempo real (Streaming)

1. Inicia **Zookeeper**:
   sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties

2. Inicia **Kafka**:
   sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties

3. Crea el **topic**:
   /opt/Kafka/bin/kafka-topics.sh --create --topic reseñas --bootstrap-server localhost:9092

4. Ejecuta el **Productor** (envía reseñas al topic):
   python3 kafka_producer.py

5. Ejecuta el **Consumidor** (lee y analiza las reseñas):
   spark-submit streaming_processing.py

---

## Resultados esperados

El procesamiento batch mostrará algo similar a:
+-----------+-----+
|sentimiento|count|
+-----------+-----+
|Positiva   |1430 |
|Negativa   |11   |
|Neutra     |24   |
+-----------+-----+

El procesamiento streaming imprimirá los resultados en tiempo real conforme se produzcan mensajes desde Kafka.

---

## Licencia

Proyecto desarrollado como parte del curso de **Big Data y Procesamiento de Datos con Apache Spark**.  
Uso educativo y demostrativo.

---

## Autor

**William Gómez**  
Desarrollador Full Stack / Entusiasta de Big Data  
Colombia 🇨🇴
