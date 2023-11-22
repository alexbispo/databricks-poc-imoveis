# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType, DoubleType
from imoveis import transformation as tf

# COMMAND ----------

catalog = "poc_imoveis"
schema = "bronze"
volume = f"/Volumes/{catalog}/{schema}/land_zone"
fileName = "imoveis.json"
tableName = "imoveis_bronze"
fullTableName = f"{catalog}.{schema}.{tableName}"

# COMMAND ----------

imoveisDF = spark.read.json(f"{volume}/{fileName}")

# COMMAND ----------

imoveisDF.printSchema()

# COMMAND ----------

imoveisDF.count()

# COMMAND ----------

imoveisDF = tf.select_columns(df=imoveisDF)

# COMMAND ----------

imoveisDF = tf.add_timestamp(df=imoveisDF)

# COMMAND ----------

imoveisDF.printSchema()

# COMMAND ----------

imoveisDF = tf.fix_data_types(df=imoveisDF)

# COMMAND ----------

imoveisDF.printSchema()

# COMMAND ----------

imoveisDF.write.saveAsTable(name=fullTableName, mode="append")

# COMMAND ----------

spark.sql(f"select count(*) from {fullTableName}").display()
