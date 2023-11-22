from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, IntegerType

def select_columns(df):
    return df \
        .select("ident.customerID", 
                "listing.types.*", 
                "listing.address.*", 
                "listing.features.*", 
                "listing.prices.price", "listing.prices.tax.*") \
        .drop("city", "location", "totalAreas")

def add_timestamp(df):
    return df.withColumn("bronze_timestamp", f.current_timestamp())

def fix_data_types(df):
    return df \
            .withColumn("usableAreas", f.col("usableAreas").cast(IntegerType())) \
            .withColumn("price", f.col("price").cast(DoubleType())) \
            .withColumn("condo", f.col("condo").cast(DoubleType())) \
            .withColumn("iptu", f.col("iptu").cast(DoubleType()))