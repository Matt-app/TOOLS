import datetime
import json
import sys
import time
import traceback
from collections import defaultdict

import requests
from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, expr, create_map, col, avg, lit, split, struct, concat, explode, \
    map_from_entries
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, IntegerType, MapType, ArrayType, StringType

t = datetime.datetime.now()
TODAY = '%d-%02d-%02d' % (t.year, t.month, t.day)
yd = t - datetime.timedelta(days=1)
YESTERDAY = '%d-%02d-%02d' % (yd.year, yd.month, yd.day)


def struct_to_map_combine(row):
    _dict = row.asDict()
    # print(_dict)
    # print(_dict[1].asDict())
    _map = defaultdict(list)
    # _map = {}
    # for sr in _dict['value_map']:
    #     _sd = sr.asDict()
    #     _map[_sd['InvoiceNo']].append(_sd['Description'])
    # _map.setdefault('InvoiceNo', []).append(_sd['Description'])
    # print('---------------------------------------------', Row(officialpoiid=_dict['StockCode'], value_map=_map))
    return Row(officialpoiid=_dict['StockCode'], value_map=dict(_map))


if __name__ == '__main__':
    spark = SparkSession.builder.appName('poi_data').getOrCreate()
    spark.read.csv('data/Online Retail.csv', header=True, maxColumns=100).createOrReplaceTempView('retail_data')
    df_group = spark.table('retail_data').where('Quantity >= 6') \
        .select('StockCode', concat('Description', lit(','), 'InvoiceNo').alias('value_list'), 'UnitPrice')
        # .agg(collect_list(struct(lit('111'), 'value_list')).alias('value_map')).rdd.map(struct_to_map_combine) \
        # .toDF(StructType([StructField('id', StringType(), True),
        #                   StructField('value', MapType(StringType(), ArrayType(StringType())), True)]))
    df_group.filter('StockCode > 20000 or StockCode < 70000').show(5, truncate=False)
    # _v = df_group.collect()
    # df_broadcast = spark.sparkContext.broadcast({key['id']: key['value'] for key in _v})
    # df_group.show(5, truncate=False)
    # df_group.selectExpr('Ds[0] as D0').show(5)
    # df = df_group.groupBy('StockCode').agg(collect_list('value_struct').alias('struct_list')).\
    #     rdd.map(struct_to_map_combine).toDF()
    # df_group.explain()
    # df.show(5, truncate=False)
