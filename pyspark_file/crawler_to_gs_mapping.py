import datetime
import json
import sys
import time

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

T = datetime.datetime.now()
BATCHNO = '%d%02d%02d' % (T.year, T.month, T.day) + '10003'
BATCHNO_ES = '21%02d%02d' % (T.month, T.day) + '3'


def map_status3(row):
    status = row['status']
    poi_id = row['officialpoiid']
    if status == 3:
        content = json.loads(row['content'])
        r = poi_map(content, poi_id)
        r_json = json.loads(r)
        map_status = r_json.get('mapStatus', 0)
        if map_status == 1 or map_status == 4:
            r = poi_map(content, poi_id, 1)
        elif map_status == 2 or map_status == 3:
            pass
        return Row(officialpoiid=poi_id, sourcepoiid=row['sourcepoiid'], status=row['status'], response=r
                   , mapstatus=map_status, content=row['content'])
    else:
        return Row(officialpoiid=poi_id, sourcepoiid=row['sourcepoiid'], status=row['status'], response=''
                   , mapstatus=None, content=row['content'])


def map_status3_23(row):
    content = json.loads(row['content'])
    r = save_es(source_poi=content, mode=2, gs_poi=row.asDict())
    return Row(officialpoiid=row['officialpoiid'], sourcepoiid=row['sourcepoiid'], status=row['status'], response=r
               , mapstatus=row['mapstatus'], content=row['content'])


def map_status12(row):
    source_id = row['sourcepoiid']
    r = save_es(gs_poi=row.asDict(), mode=1, source_poi={'sourcePoiId': source_id})
    return Row(officialpoiid=row['officialpoiid'], sourcepoiid=source_id, status=row['status'], response=r
               , mapstatus='', content=row['content'])


def poi_map(content, poi_id, is_add_pending_data=0):
    content['batchNo'] = BATCHNO
    content['poiType'] = '3'
    url = 'http://poidata-mapping.you.ctripcorp.com/mapping'
    _data = {
        'args':
            {
                "poitype": [3, 66],
                "searchtype": [5],
                'targetPois': poi_id
            },
        'isAddPendingData': is_add_pending_data,
        'sourcePoi': content
    }
    headers = {
        'Content-Type': 'application/json;charset=UTF-8',
    }
    try:
        r = requests.post(url=url, json=_data, headers=headers).text
    except:
        time.sleep(1)
        r = requests.post(url=url, json=_data, headers=headers).text
    return r


def save_es(gs_poi, mode, source_poi=None):
    if source_poi is None:
        source_poi = {}
    source_poi_id = source_poi.get('sourcePoiId', 0)
    info1 = {
        "gsPoiId": gs_poi['officialpoiid'],
        "gsDitrcitName": gs_poi['districtname'],
        "gsPoiName": gs_poi['poiname'],
        "gsPoiAddress": gs_poi['poiaddress'],
        "gsTel": gs_poi['telephone'],
        "gsTag": gs_poi['tagname'],
        "gsBusinessStatus": gs_poi['businessstatus'],
        "gsLinkUrl": "<a target='_blank' href='http://poiadminsite.you.ctripcorp.com/#/app/allpoi/detail/{0}/baseMenu'>后台链接</a>"
            .format(str(gs_poi['officialpoiid'])),
        "gsMappingUrl": "<a target='_blank' href='http://poiadminsite.you.ctripcorp.com/#/app/poimapping/detail/{0}'>匹配关系</a>"
            .format(str(gs_poi['officialpoiid']))
    }
    if mode == 2:
        info2 = {
            "sourcePoiId": source_poi_id,
            "sourceDitrcitName": source_poi.get('breadCrumb', ''),
            "sourcePoiName": ','.join([x['value'] for x in source_poi['poiName']]),
            "sourcePoiAddress": ','.join([x['value'] for x in source_poi['poiAddress']]),
            "sourceTel": ','.join([str(x) for x in source_poi.get('telephone', [''])]),
            "sourceTag": ','.join(source_poi.get('sourceTag', [''])),
            "sourceBusinessStatus": source_poi.get('businessStatus', ''),
            "sourceLinkUrl": "<a target='_blank' href='http://www.dianping.com/shop/{0}'>竞品链接</a>"
                .format(str(source_poi_id))
        }
    elif mode == 1:
        info2 = {
            "sourcePoiId": source_poi_id,
            "sourceDitrcitName": None,
            "sourcePoiName": None,
            "sourcePoiAddress": None,
            "sourceTel": None,
            "sourceTag": None,
            "sourceBusinessStatus": None,
            "sourceLinkUrl": None
        }
    else:
        return ''

    indexid = BATCHNO_ES + str(mode) + str(gs_poi.get('officialpoiid', 0))
    data1 = {"index": {"_id": indexid}}
    info_id = int(gs_poi.get('officialpoiid', 0)) + int(source_poi_id) \
        if str(source_poi_id).isdigit() else int(gs_poi.get('officialpoiid', 0))
    data2 = {
        "infoid": info_id,
        "note": json.dumps(info1),
        "value": json.dumps(info2),
        "batch": '202107271000' + str(mode),
        "id": indexid,
        "domain": "",
        "type": 1019,
        "checkresult": "",
        "datachange_lasttime": time.time() * 1000
    }

    _data = json.dumps(data1) + "\r\n" + json.dumps(data2) + '\r\n'
    headers = {
        'Authorization': 'Basic ZWxhc3RpYzplOFp3OFI5RTlaeExmdDI1NUdzNTQ5SE8=',
        'Content-Type': 'application/json;charset=UTF-8',
    }
    url = "http://poidatasearch-application.rb.es.gs.ctripcorp.com/infocheck/infocheck/_bulk"

    response = requests.post(url, _data, headers=headers).text
    if response == "timed out":
        response = requests.post(url, _data, headers=headers).text
    return response


if __name__ == '__main__':
    spark = SparkSession.builder.appName('crawler_to_gs_mapping').enableHiveSupport().getOrCreate()
    spark.conf.set('spark_dir.sql.execution.arrow.pyspark.enabled', 'true')
    print('++++++++++++++++++++++++++++++++++++++++ begin +++++++++++++++++++++++++++++++++++++++++++++++++')
    print('spark_dir version', spark.version)
    print('start spark_dir ')
    print('')
    print('Python version', sys.version_info[0])
    print('encoding', sys.getdefaultencoding())

    data = spark.sql('''
        SELECT officialpoiid, status, content, sourcepoiid
        FROM ods_youdb.ods_gscrawlerbigdatadb_sourcepoi_gspoi_process
        WHERE d = current_date
        AND status is not null
        ORDER BY datachange_lasttime DESC
        LIMIT 5000
    ''')
    data_poi = spark.sql('''
            SELECT officialpoiid, districtname, poiname, poiaddress
                , concat_ws(',', telephone) AS telephone
                , concat_ws(',', tagname) AS tagname, businessstatus
            FROM dw_youdb.edw_gs_poi_statistics_df
            WHERE d = current_date
                AND poitype IN (3, 66)
        ''')

    # 处理爬虫success数据，调用mapping接口，获取mapstatus并处理mapstatus为1、4的数据，入库
    df_map3_result = data.repartition(100).rdd.map(map_status3).toDF()
    # 获取GSPoi信息
    data_source_gs = df_map3_result.join(data_poi, on='officialpoiid', how='inner')
    # 处理mapstatus为2、3的数据，入ES
    df_map3_map23_result = data_source_gs.filter(data_source_gs.status == 3).filter('mapstatus == 2 or mapstatus == 3')\
        .repartition(100).rdd.map(map_status3_23).toDF()
    # 处理爬虫404、Failed数据，入ES
    df_map12_result = data_source_gs.filter('status == 1 or status == 2').repartition(100).rdd.map(map_status12).toDF()
    # Action，存临时表，每次全量覆盖
    df_map12_result.union(df_map3_map23_result).union(df_map3_result.filter('mapstatus == 1 or mapstatus == 4')) \
        .write.saveAsTable('tmp_youdb.tmp_matt_0728', mode='overwrite')
