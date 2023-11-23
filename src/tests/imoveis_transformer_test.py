from etl.bronze.imoveis import transformation as tf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

jsonString = '''
[
  {
    "ident": {
      "customerID": "800002-TESTAAAC",
      "source": "Website"
    },
    "listing": {
      "types": {
        "unit": "Outros",
        "usage": "Residencial"
      },
      "address": {
        "city": "Rio de Janeiro",
        "location": {
          "lon": -43.413557,
          "lat": -22.909429
        },
        "zone": "Zona Oeste",
        "neighborhood": "Taquara"
      },
      "prices": {
        "price": "45000",
        "tax": {
          "iptu": "0",
          "condo": "150"
        }
      },
      "features": {
        "bedrooms": 0,
        "bathrooms": 0,
        "suites": 0,
        "parkingSpaces": 1,
        "usableAreas": "62",
        "totalAreas": "62",
        "floors": 0,
        "unitsOnTheFloor": 0,
        "unitFloor": 0
      }
    }
  }
]
'''

def test_select_columns():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("POC Unit Tests") \
        .getOrCreate()


    df = spark.read.json(spark.sparkContext.parallelize([jsonString]))

    df = tf.select_columns(df)

    assert type(df) == DataFrame
    #spark.stop()
