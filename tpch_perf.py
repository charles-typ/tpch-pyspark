from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from datetime import datetime
from pathlib import Path
import argparse
from time import time

from tpch_schema import *
# import all queries in __init__.py in queries folder
from queries import *


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

    part_split = spark_context.textFile(
        DATA_PATH+"part.tbl").map(lambda l: l.split('|'))

    part = part_split.map(lambda l: [int(l[0]), str(l[1]), str(l[2]), str(
        l[3]), str(l[4]), int(l[5]), str(l[6]), float(l[7]), str(l[8])])

    supplier_split = spark_context.textFile(
        DATA_PATH+"supplier.tbl").map(lambda l: l.split('|'))

    supplier = supplier_split.map(lambda l: [int(l[0]), str(l[1]), str(
        l[2]), int(l[3]), str(l[4]), float(l[5]), str(l[6])])

    partsupp_split = spark_context.textFile(
        DATA_PATH+"partsupp.tbl").map(lambda l: l.split('|'))

    partsupp = partsupp_split.map(
        lambda l: [int(l[0]), int(l[1]), int(l[2]), float(l[3]), str(l[4])])

    customer_split = spark_context.textFile(
        DATA_PATH+"customer.tbl").map(lambda l: l.split('|'))

    customer = customer_split.map(lambda l: [int(l[0]), str(l[1]), str(l[2]), int(
        l[3]), str(l[4]), float(l[5]), str(l[6]), str(l[7])])

    orders_split = spark_context.textFile(
        DATA_PATH+"orders.tbl").map(lambda l: l.split('|'))

    orders = orders_split.map(lambda l: [int(l[0]), int(l[1]), str(l[2]), float(l[3]), datetime.strptime(
        l[4], '%Y-%m-%d'), str(l[5]), str(l[6]), int(l[7]), str(l[8])])

    nation_split = spark_context.textFile(
        DATA_PATH+"nation.tbl").map(lambda l: l.split('|'))

    nation = nation_split.map(
        lambda l: [int(l[0]), str(l[1]), int(l[2]), str(l[3])])

    region_split = spark_context.textFile(
        DATA_PATH+"region.tbl").map(lambda l: l.split('|'))

    region = region_split.map(lambda l: [int(l[0]), str(l[1]), str(l[2])])

    df_lineitem = spark_session.createDataFrame(lineitem, schema_lineitem)
    df_lineitem.registerTempTable("lineitem")
    df_lineitem.cache().count()

    df_part = spark_session.createDataFrame(part, schema_part)
    df_part.registerTempTable("part")
    df_part.cache().count()

    df_supplier = spark_session.createDataFrame(supplier, schema_supplier)
    df_supplier.registerTempTable("supplier")
    df_supplier.cache().count()

    df_partsupp = spark_session.createDataFrame(partsupp, schema_partsupp)
    df_partsupp.registerTempTable("partsupp")
    df_partsupp.cache().count()

    df_customer = spark_session.createDataFrame(customer, schema_customer)
    df_customer.registerTempTable("customer")
    df_customer.cache().count()

    df_orders = spark_session.createDataFrame(orders, schema_orders)
    df_orders.registerTempTable("orders")
    df_orders.cache().count()

    df_nation = spark_session.createDataFrame(nation, schema_nation)
    df_nation.registerTempTable("nation")
    df_nation.cache().count()

    df_region = spark_session.createDataFrame(region, schema_region)
    df_region.registerTempTable("region")
    df_region.cache().count()


def run_query(query, sc, query_name, result_path, open_mode='w+'):
    result_file = Path(result_path + query_name + '.answer')

    with result_file.open(open_mode) as f:
        start = time()
        query_result = sc.sql(query)
        end = time()

        for idx, d in enumerate(query_result.collect()):
            if idx == 0:
                header = ''
                for r in d.asDict().keys():
                    header = header + r + ' | '
                f.write(header + '\n')
            row_record = ''
            for v in d.asDict().values():
                row_record = row_record + str(v) + ' | '
            f.write(row_record + '\n')

        f.write("\n{} query time: {} seconds\n".format(query_name, (end-start)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', '-q', type=str,
                        default='q1', required=True, help='query number')
    args = parser.parse_args()
    query_select = args.query

    DATA_PATH = '/home/ruiliu/Development/tpch-pyspark/dbgen/'
    RESULT_PATH = '/home/ruiliu/Development/tpch-pyspark/answers/'
    spark_sess, spark_ctx = conf_setup()

    read_table(spark_sess, spark_ctx)

    exec_query = globals()[query_select].query

    if isinstance(exec_query, list):
        for idx, query in enumerate(exec_query):
            if idx == 0:
                run_query(query, spark_sess, query_select, RESULT_PATH)
            else:
                run_query(query, spark_sess, query_select, RESULT_PATH, 'a+')
    else:
        run_query(exec_query, spark_sess, query_select, RESULT_PATH)
