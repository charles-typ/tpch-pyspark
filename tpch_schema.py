from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    DateType
)

schema_lineitem = StructType([
    StructField("L_ORDERKEY", IntegerType(), nullable=False),
    StructField("L_PARTKEY", IntegerType(), False),
    StructField("L_SUPPKEY", IntegerType(), False),
    StructField("L_LINENUMBER", IntegerType(), True),
    StructField("L_QUANTITY", DoubleType(), True),
    StructField("L_EXTENDEDPRICE", DoubleType(), True),
    StructField("L_DISCOUNT", DoubleType(), True),
    StructField("L_TAX", DoubleType(), True),
    StructField("L_RETURNFLAG", StringType(), True),
    StructField("L_LINESTATUS", StringType(), True),
    StructField("L_SHIPDATE", DateType(), True),
    StructField("L_COMMITDATE", DateType(), True),
    StructField("L_RECEIPTDATE", DateType(), True),
    StructField("L_SHIPINSTRUCT", StringType(), True),
    StructField("L_SHIPMODE", StringType(), True),
    StructField("L_COMMENT", StringType(), True)
])
