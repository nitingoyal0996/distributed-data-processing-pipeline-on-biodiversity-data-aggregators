from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType

# Define the schema for the JSON-like message
schema = StructType([
    StructField("basisOfRecord", StringType(), True),
    StructField("bibliographicCitation", StringType(), True),
    StructField("catalogNumber", StringType(), True),
    StructField("class", StringType(), True),
    StructField("collectionCode", StringType(), True),
    StructField("coordinateUncertaintyInMeters", StringType(), True),
    StructField("country", StringType(), True),
    StructField("decimalLatitude", DoubleType(), True),
    StructField("decimalLongitude", DoubleType(), True),
    StructField("eventDate", StringType(), True),
    StructField("eventID", StringType(), True),
    StructField("family", StringType(), True),
    StructField("footprintWKT", StringType(), True),
    StructField("identificationReferences", StringType(), True),
    StructField("identificationRemarks", StringType(), True),
    StructField("institutionCode", StringType(), True),
    StructField("kingdom", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("materialSampleID", StringType(), True),
    StructField("maximumDepthInMeters", IntegerType(), True),
    StructField("minimumDepthInMeters", IntegerType(), True),
    StructField("modified", StringType(), True),
    StructField("occurrenceID", StringType(), True),
    StructField("occurrenceStatus", StringType(), True),
    StructField("order", StringType(), True),
    StructField("organismQuantity", StringType(), True),
    StructField("organismQuantityType", StringType(), True),
    StructField("phylum", StringType(), True),
    StructField("recordedBy", StringType(), True),
    StructField("sampleSizeUnit", StringType(), True),
    StructField("sampleSizeValue", StringType(), True),
    StructField("samplingProtocol", StringType(), True),
    StructField("scientificName", StringType(), True),
    StructField("scientificNameID", StringType(), True),
    StructField("taxonRank", StringType(), True),
    StructField("id", StringType(), True),
    StructField("dataset_id", StringType(), True),
    StructField("node_id", ArrayType(StringType()), True),
    StructField("depth", IntegerType(), True),
    StructField("date_start", StringType(), True),
    StructField("date_mid", StringType(), True),
    StructField("date_end", StringType(), True),
    StructField("date_year", IntegerType(), True),
    StructField("dropped", BooleanType(), True),
    StructField("absence", BooleanType(), True),
    StructField("superdomain", StringType(), True),
    StructField("superdomainid", IntegerType(), True),
    StructField("kingdomid", IntegerType(), True),
    StructField("phylumid", IntegerType(), True),
    StructField("classid", IntegerType(), True),
    StructField("orderid", IntegerType(), True),
    StructField("familyid", IntegerType(), True),
    StructField("aphiaID", IntegerType(), True),
    StructField("originalScientificName", StringType(), True),
    StructField("flags", ArrayType(StringType()), True),
    StructField("bathymetry", IntegerType(), True),
    StructField("shoredistance", IntegerType(), True),
    StructField("sst", DoubleType(), True),
    StructField("sss", DoubleType(), True)
])