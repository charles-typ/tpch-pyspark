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
    lineitem_split = spark_context.textFile(data_path + "/lineitem.tbl").map(lambda l: l.split('|'))

    lineitem = lineitem_split.map(lambda l: [int(l[0].strip() or 0),
                                             int(l[1].strip() or 0),
                                             int(l[2].strip() or 0),
                                             int(l[3].strip() or 0),
                                             float(l[4].strip() or 0.0),
                                             float(l[5].strip() or 0.0),
                                             float(l[6].strip() or 0.0),
                                             float(l[7].strip() or 0.0),
                                             str(l[8]),
                                             str(l[9]),
                                             datetime.strptime(l[10], "%Y-%m-%d"),
                                             datetime.strptime(l[11], "%Y-%m-%d"),
                                             datetime.strptime(l[12], '%Y-%m-%d'),
                                             str(l[13]),
                                             str(l[14]),
                                             str(l[15])])

    part_split = spark_context.textFile(data_path+"/part.tbl").map(lambda l: l.split('|'))

    part = part_split.map(lambda l: [int(l[0].strip() or 0),
                                     str(l[1]),
                                     str(l[2]),
                                     str(l[3]),
                                     str(l[4]),
                                     int(l[5].strip() or 0),
                                     str(l[6]),
                                     float(l[7].strip() or 0.0),
                                     str(l[8])])

    supplier_split = spark_context.textFile(data_path+"/supplier.tbl").map(lambda l: l.split('|'))

    supplier = supplier_split.map(lambda l: [int(l[0].strip() or 0),
                                             str(l[1]),
                                             str(l[2]),
                                             int(l[3].strip() or 0),
                                             str(l[4]),
                                             float(l[5].strip() or 0.0),
                                             str(l[6])])

    partsupp_split = spark_context.textFile(data_path+"/partsupp.tbl").map(lambda l: l.split('|'))

    partsupp = partsupp_split.map(lambda l: [int(l[0].strip() or 0),
                                             int(l[1].strip() or 0),
                                             int(l[2].strip() or 0),
                                             float(l[3].strip() or 0.0),
                                             str(l[4])])

    customer_split = spark_context.textFile(data_path + "/customer.tbl").map(lambda l: l.split('|'))

    customer = customer_split.map(lambda l: [int(l[0].strip() or 0),
                                             str(l[1]),
                                             str(l[2]),
                                             int(l[3].strip() or 0),
                                             str(l[4]),
                                             float(l[5].strip() or 0.0),
                                             str(l[6]),
                                             str(l[7])])

    orders_split = spark_context.textFile(data_path + "/orders.tbl").map(lambda l: l.split('|'))

    orders = orders_split.map(lambda l: [int(l[0].strip() or 0),
                                         int(l[1].strip() or 0),
                                         str(l[2]),
                                         float(l[3].strip() or 0.0),
                                         datetime.strptime(l[4], '%Y-%m-%d'),
                                         str(l[5]),
                                         str(l[6]),
                                         int(l[7].strip() or 0),
                                         str(l[8])])

    nation_split = spark_context.textFile(data_path + "/nation.tbl").map(lambda l: l.split('|'))

    nation = nation_split.map(lambda l: [int(l[0].strip() or 0),
                                         str(l[1]),
                                         int(l[2].strip() or 0),
                                         str(l[3])])

    region_split = spark_context.textFile(data_path + "/region.tbl").map(lambda l: l.split('|'))

    region = region_split.map(lambda l: [int(l[0].strip() or 0),
                                         str(l[1]),
                                         str(l[2])])

    df_lineitem = spark_session.createDataFrame(lineitem, lineitem_schema)
    df_lineitem.registerTempTable("lineitem")
    df_lineitem.cache().count()

    df_part = spark_session.createDataFrame(part, part_schema)
    df_part.registerTempTable("part")
    df_part.cache().count()

    df_supplier = spark_session.createDataFrame(supplier, supplier_schema)
    df_supplier.registerTempTable("supplier")
    df_supplier.cache().count()

    df_partsupp = spark_session.createDataFrame(partsupp, partsupp_schema)
    df_partsupp.registerTempTable("partsupp")
    df_partsupp.cache().count()

    df_customer = spark_session.createDataFrame(customer, customer_schema)
    df_customer.registerTempTable("customer")
    df_customer.cache().count()

    df_orders = spark_session.createDataFrame(orders, orders_schema)
    df_orders.registerTempTable("orders")
    df_orders.cache().count()

    df_nation = spark_session.createDataFrame(nation, nation_schema)
    df_nation.registerTempTable("nation")
    df_nation.cache().count()

    df_region = spark_session.createDataFrame(region, region_schema)
    df_region.registerTempTable("region")
    df_region.cache().count()


def run_query(query, sc, query_name, result_path, open_mode='w+'):
    result_file = Path(result_path + "/" + query_name + '.answer')

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
    parser.add_argument('--query', '-q', type=str, required=True, help='indicate TPC-H query id')
    parser.add_argument('--data_path', '-d', type=str, required=True, help='indicate generated TPC-H dataset')
    parser.add_argument('--results_path', '-r', type=str, required=True, help='indicate the output of TPC-H ')
    args = parser.parse_args()

    query_id = args.query
    data_path = args.data_path
    results_path = args.results_path

    spark_sess, spark_ctx = conf_setup()

    read_table(spark_sess, spark_ctx)

    exec_query = globals()[query_id].query

    if isinstance(exec_query, list):
        for idx, query in enumerate(exec_query):
            if idx == 0:
                run_query(query, spark_sess, query_id, results_path)
            else:
                run_query(query, spark_sess, query_id, results_path, 'a+')
    else:
        run_query(exec_query, spark_sess, query_id, results_path)
