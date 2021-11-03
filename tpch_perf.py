from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
from time import time

from tpch_schema import schema_lineitem
from queries import q1


def conf_setup():
    conf = SparkConf()
    conf.setAppName("tpch")
    conf.set("spark.driver.memory", "8g")
    spark_session = SparkSession.builder.config(conf=conf).getOrCreate()

    spark_context = spark_session.sparkContext

    return spark_session, spark_context


def read_table(spark_session, spark_context):
    lineitem_split = spark_context.textFile(
        DATA_PATH+"lineitem.tbl").map(lambda l: l.split('|'))

    lineitem = lineitem_split.map(lambda l: [int(l[0]), int(l[1]), int(l[2]), int(l[3]), float(l[4]), float(l[5]), float(l[6]), float(l[7]), str(l[8]), str(
        l[9]), datetime.strptime(l[10], "%Y-%m-%d"), datetime.strptime(l[11], "%Y-%m-%d"), datetime.strptime(l[12], '%Y-%m-%d'), str(l[13]), str(l[14]), str(l[15])])

    # create a dataframe for lineitem
    df_lineitem = spark_session.createDataFrame(lineitem, schema_lineitem)
    df_lineitem.registerTempTable("lineitem")
    df_lineitem.cache().count()


def run_query(query, sc, query_name, result_path):
    result_file = Path(result_path + 'result_' + query_name)
    if not result_file.is_file():
        result_file.touch()

    with result_file.open('a') as f:
        start = time()
        query_result = sc.sql(query)
        end = time()
        f.write(str(query_result))
        f.write("\n{} result time: {} seconds".format(query_name, (end-start)))


if __name__ == "__main__":
    DATA_PATH = '/home/ruiliu/Development/tpch-pyspark/dbgen/'
    RESULT_PATH = '/home/ruiliu/Development/tpch-pyspark/query_results/'
    spark_sess, spark_ctx = conf_setup()
    read_table(spark_sess, spark_ctx)

    run_query(q1.query, spark_sess, "q1", RESULT_PATH)
