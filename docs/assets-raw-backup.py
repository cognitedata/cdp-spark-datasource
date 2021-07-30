# This example shows how to copy data already in CDF to RAW
# For the sake of simplicity we'll backup all assets into a RAW table

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

# initialize Spark and a few helper functions
spark = SparkSession.builder.config("spark.jars.packages","com.cognite.spark.datasource:cdf-spark-datasource_2.12:1.4.26").getOrCreate()

apikey = os.environ['COGNITE_API_KEY']
baseUrl = "https://greenfield.cognitedata.com"

def cdfData(**options):
	reader = spark.read.format("cognite.spark.v1")
	reader.option("apiKey", apikey)
	reader.option("maxRetryDelay", 5) # fail faster during testing
	reader.option("baseUrl", baseUrl)
	for k in options:
		reader.option(k, options[k])
	return reader


def cdfRaw(database, table, inferSchema=False):
    # For writing and reading we have to specify the table and database

    # For reading, you can set the inferSchema option, that will give you the column
    # names from the table, instead of one JSON object per row. Obviously, this will
    # only work if the table is not empty
	return cdfData(type="raw", database=database, table=table, inferSchema=inferSchema)

def loadIntoRaw(database, table, source: DataFrame):
    # RAW is a bit special since it does not have a fixed schema
    # When writing, we order it to use the same schema as the source dataset
	destination = cdfRaw(database, table).schema(source.schema).load()
	destination.createOrReplaceTempView("destinationRawTable")
	source.select(*destination.columns).write.insertInto("destinationRawTable")

# create a Spark view for assets
cdfData(type="assets").load().createOrReplaceTempView("cdf_assets")

assetsRawCopy = spark.sql("""
    -- raw rows require key column, we'll just use the asset's externalId
    select externalId as key, *
    from cdf_assets
    -- since the key column is required, we'll only backup the assets with externalId set
    where externalId is not null
""")
loadIntoRaw("test", "assets-backup", assetsRawCopy)
