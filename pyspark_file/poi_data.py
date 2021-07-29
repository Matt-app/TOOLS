import datetime
import re
from collections import defaultdict

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from pyspark.sql.functions import lit, col, expr, lower, collect_list, struct
from pyspark.sql.types import StructType, StructField, Row, MapType, ArrayType, StringType, IntegerType

t = datetime.datetime.now()
TODAY = '%d-%02d-%02d' % (t.year, t.month, t.day)
yd = t - datetime.timedelta(days=1)
YESTERDAY = '%d-%02d-%02d' % (yd.year, yd.month, yd.day)
POI_TEXT = ['eaddress', 'localname', 'tickettype', 'tips', 'playspendunit', 'playspendmin', 'playspendmax',
            'playspendsource', 'shortfeature', 'hasticketgoods', 'ownership', 'businessdes', 'features', 'transit',
            'ticketdesc', 'introduction', 'currency', 'avgcostper', 'isglobalbuy', 'servicefacility', 'weibo',
            'twitter', 'wechat', 'facebook', 'pricedesc', 'tickethasmoreprice', 'officialsite', 'availablecurrency',
            'guide', 'effecttime', 'expiretime', 'effectivetimesource', 'email', 'ticketprice', 'michelinrecommendstar',
            'sighttokentype', 'michelinenvironmentlevel', 'businessremark', 'bookingtipsrich', 'bookingtips',
            'officallevel', 'ugcshortfeature', 'formattimetips', 'hasformattime', 'sightneedgateticket',
            'ticketrestriction', 'custommapfield', 'locatedpark', 'shopguidemap', 'shopguidepdf', 'panoramagram',
            'airportplan', 'hotelpoiid', 'shortaddress', 'shortname', 'hotelrecommend', 'hotelparentid', 'hotelmetro',
            'hotelsort', 'shortename', 'poisubtype', 'flightid', 'airline', 'crafttype', 'displayname', 'airporttext',
            'terminalext', 'terminalname', 'terminalandthreecode', 'trainstationid', 'sightannouncement',
            'businesssuspendedtime', 'businessstarttime', 'preferentialpolicies', 'storetype', 'isvirtual', 'plump',
            'shopservicedesc', 'elevationlevel', 'itemcategory', 'address2', 'needappointment', 'inadvance',
            'appointmentway', 'checkinpark', 'threecode']
LANG_LIST = ['zh-HK', 'en-US', 'en-GB', 'en-HK', 'zh-TW', 'ko-KR', 'ja-JP', 'en-SG', 'en-AU', 'de-DE', 'fr-FR', 'es-ES',
             'it-IT', 'ru-RU', 'th-TH', 'id-ID', 'ms-MY', 'en-MY', 'tl-PH', 'vi-VN', 'ar-XX', 'pt-PT', 'pt-BR', 'tr-TR']
POI_TEXT_COLS = ['officialpoiid',
                 'value_map["eaddress"] as eaddress',
                 'value_map["localname"] as localname',
                 'value_map["tickettype"] as tickettype',
                 'value_map["tips"] as tips',
                 'value_map["playspendunit"] as playspendunit',
                 'value_map["playspendmin"] as playspendmin',
                 'value_map["playspendmax"] as playspendmax',
                 'value_map["playspendsource"] as playspendsource',
                 'value_map["shortfeature"] as shortfeature',
                 'value_map["hasticketgoods"] as hasticketgoods',
                 'value_map["ownership"] as ownership',
                 'value_map["businessdes"] as businessdes',
                 'value_map["features"] as features',
                 'value_map["transit"] as transit',
                 'value_map["ticketdesc"] as ticketdesc',
                 'value_map["introduction"] as introduction',
                 'value_map["currency"] as currency',
                 'value_map["avgcostper"] as avgcostper',
                 'value_map["isglobalbuy"] as isglobalbuy',
                 'value_map["servicefacility"] as servicefacility',
                 'value_map["weibo"] as weibo',
                 'value_map["twitter"] as twitter',
                 'value_map["wechat"] as wechat',
                 'value_map["facebook"] as facebook',
                 'value_map["pricedesc"] as pricedesc',
                 'value_map["tickethasmoreprice"] as tickethasmoreprice',
                 'value_map["officialsite"] as officialsite',
                 'value_map["availablecurrency"] as availablecurrency',
                 'value_map["guide"] as guide',
                 'value_map["effecttime"] as effecttime',
                 'value_map["expiretime"] as expiretime',
                 'value_map["effectivetimesource"] as effectivetimesource',
                 'value_map["email"] as email',
                 'value_map["ticketprice"] as ticketprice',
                 'value_map["michelinrecommendstar"] as michelinrecommendstar',
                 'value_map["sighttokentype"] as sighttokentype',
                 'value_map["michelinenvironmentlevel"] as michelinenvironmentlevel',
                 'value_map["businessremark"] as businessremark',
                 'value_map["bookingtipsrich"] as bookingtipsrich',
                 'value_map["bookingtips"] as bookingtips',
                 'value_map["officallevel"] as officallevel',
                 'value_map["ugcshortfeature"] as ugcshortfeature',
                 'value_map["formattimetips"] as formattimetips',
                 'value_map["hasformattime"] as hasformattime',
                 'value_map["sightneedgateticket"] as sightneedgateticket',
                 'value_map["ticketrestriction"] as ticketrestriction',
                 'value_map["custommapfield"] as custommapfield',
                 'value_map["locatedpark"] as locatedpark',
                 'value_map["shopguidemap"] as shopguidemap',
                 'value_map["shopguidepdf"] as shopguidepdf',
                 'value_map["panoramagram"] as panoramagram',
                 'value_map["airportplan"] as airportplan',
                 'value_map["hotelpoiid"] as hotelpoiid',
                 'value_map["shortaddress"] as shortaddress',
                 'value_map["shortname"] as shortname',
                 'value_map["hotelrecommend"] as hotelrecommend',
                 'value_map["hotelparentid"] as hotelparentid',
                 'value_map["hotelmetro"] as hotelmetro',
                 'value_map["hotelsort"] as hotelsort',
                 'value_map["shortename"] as shortename',
                 'value_map["poisubtype"] as poisubtype',
                 'value_map["flightid"] as flightid',
                 'value_map["airline"] as airline',
                 'value_map["crafttype"] as crafttype',
                 'value_map["displayname"] as displayname',
                 'value_map["airporttext"] as airporttext',
                 'value_map["terminalext"] as terminalext',
                 'value_map["terminalname"] as terminalname',
                 'value_map["terminalandthreecode"] as terminalandthreecode',
                 'value_map["trainstationid"] as trainstationid',
                 'value_map["sightannouncement"] as sightannouncement',
                 'value_map["businesssuspendedtime"] as businesssuspendedtime',
                 'value_map["businessstarttime"] as businessstarttime',
                 'value_map["preferentialpolicies"] as preferentialpolicies',
                 'value_map["storetype"] as storetype',
                 'value_map["isvirtual"] as isvirtual',
                 'value_map["plump"] as plump',
                 'value_map["shopservicedesc"] as shopservicedesc',
                 'value_map["elevationlevel"] as elevationlevel',
                 'value_map["itemcategory"] as itemcategory',
                 'value_map["address2"] as address2',
                 'value_map["needappointment"] as needappointment',
                 'value_map["inadvance"] as inadvance',
                 'value_map["appointmentway"] as appointmentway',
                 'value_map["checkinpark"] as checkinpark',
                 'value_map["threecode"] as threecode']


def struct_to_map(row):
    _dict = row.asDict()
    _map = {}
    for sr in _dict['value_map']:
        _map.update(sr.asDict())
    _dict['value_map'] = _map
    return _dict


# 处理目的地信息
def district_path_format(row):
    _dict = row.asDict()
    _map_name = defaultdict(str)
    _map_id = defaultdict(str)
    _path_name = []
    if int(_dict.get('isinchina', 0)) == 1:
        _path = row['districtpath'].split('.')[1:-1]
        _name_list = {1: 'continent', 2: 'country', 3: 'provivce', 4: 'city', 5: 'county', 6: 'town', 7: 'scenic'}
        _broadcast = district_broadcast.value
        for sr in _path:
            _key = _broadcast.get(int(sr), ('', 0))
            _path_name.append(str(_key))
            _map_name[_name_list.get(_key[1], '')] = _key[0]
            _map_id[_name_list.get(_key[1], '')] = str(sr)
        return Row(district_name_map=dict(_map_name), district_id_map=dict(_map_id),
                   districtpath=str(_dict['districtpath']),
                   districtid=str(_dict['districtid']), isinchina=_dict['isinchina'],
                   districtname=_dict['districtname'], districtpath_name='.'.join(_path_name))
    else:
        return Row(district_name_map=None, district_id_map=None,
                   districtpath=str(_dict['districtpath']),
                   districtid=str(_dict['districtid']), isinchina=_dict['isinchina'],
                   districtname=_dict['districtname'], districtpath_name=None)


# 多行合并为一个value
def struct_to_map_combine(row):
    _dict = row.asDict()
    _map = defaultdict(list)
    for sr in _dict['value_map']:
        _map[sr[0]].append(sr[1])
    return Row(officialpoiid=_dict['officialpoiid'], value_map=dict(_map))


def struct_to_map_poi_text(row):
    _dict = row.asDict()
    _map = {}
    for sr in _dict['value_map']:
        _sd = sr.asDict()
        if _sd['ename'] == 'introduction':
            _map[_sd['ename']] = clear_html_re(_sd['poivalue'])
        else:
            _map[_sd['ename']] = _sd['poivalue']
    _dict['value_map'] = _map
    return _dict


def clear_html_re(src_html):
    try:
        content = re.sub(r'</?(.+?)>', '', src_html)  # 去除标签
        content = re.sub(r'&nbsp;', '', content)
        dst_html = re.sub(r'\s+', '', content)  # 去除空白字符
        dst_html = ILLEGAL_CHARACTERS_RE.sub(r'', dst_html)
        return dst_html
    except:
        return src_html


if __name__ == '__main__':
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName('poi_data').enableHiveSupport().getOrCreate()
    spark.conf.set('spark.default.parallelism', '1000')
    spark.conf.set('spark.sql.shuffle.partitions', '200')  # 200比1000快了将近四分之一
    # poi主表
    df_poi = spark.sql('''
        SELECT officialpoiid, poitype, poiname, pinyin, ename
            , poiaddress, glat, glon, gglat, gglon
            , blat, blon, sourcetype, districtid, businessid
            , rating, score, isgsshow, iscalibration, platform
            , businessstatus, commentcount, datachange_createtime, datachange_lasttime, editor
            , auditor, remark, creator
        FROM dw_youdb.fact_gspoidb_officialpoi
        ''').where('d = current_date').where('publishstatus = 6').repartition(1000)

    # poi扩展字段，struct-dict
    df_poi_text = spark.table('dw_youdb.fact_gspoidb_poitext').repartition(1000) \
        .select('officialpoiid', struct(lower(col('ename')).alias('ename'), 'poivalue').alias('value_struct')) \
        .fillna('').where(col('ename').isin(POI_TEXT)).groupBy('officialpoiid') \
        .agg(collect_list('value_struct').alias('value_map'))
    df_poi_text_result = df_poi_text.rdd.map(struct_to_map_poi_text).toDF().repartition(1000).selectExpr(POI_TEXT_COLS)

    # 多语言，collect&struct-dict
    df_global = spark.table('dw_youdb.fact_gspoidb_globalizationresource') \
        .selectExpr('resourceid as officialpoiid', 'lang', 'content', 'lower(fieldname) as fieldname') \
        .fillna('').where(col('lang').isin(LANG_LIST)).where('fieldname in ("poiname", "poiaddress")') \
        .where('nvl(content, "") <> ""')
    df_global_name = df_global.filter(df_global.fieldname == 'poiname').groupBy('officialpoiid') \
        .agg(collect_list(struct(expr('lower(lang)').alias('lang'), 'content')).alias('value_map'))
    # df_global_name.show(10, truncate=False) 在这里改了位置
    df_global_name_result = df_global_name.rdd.map(struct_to_map_combine).toDF() \
        .withColumnRenamed('value_map', 'global_names')
    df_global_dir = df_global.filter(df_global.fieldname == 'poiaddress').groupBy('officialpoiid') \
        .agg(collect_list(struct(expr('lower(lang)').alias('lang'), 'content')).alias('value_map'))
    # df_global_dir.show(10, truncate=False)
    df_global_dir_result = df_global_dir.rdd.map(struct_to_map_combine).toDF() \
        .withColumnRenamed('value_map', 'global_dirs')

    # poi推荐表-封面图
    df_cover_image = spark.sql('''
        SELECT poiid as officialpoiid, displaycoverimageid
            FROM dw_youdb.poi_extrec
        ''').where('d = current_date')

    # poi标签
    df_tag_mapping = spark.sql('''
        SELECT poiid as officialpoiid, tagid
            FROM dw_youdb.gssimplepoidb_poi_tag
        ''').where('d = current_date').where('publishstatus = 6')
    df_tag = spark.sql('''
        SELECT tagid, tagname
                FROM dw_youdb.fact_gsdestdb_tags
        ''').where('publishstatus = 6')
    df_tags = df_tag_mapping.repartition(100).join(df_tag, on='tagid') \
        .select(df_tag_mapping.officialpoiid, df_tag_mapping.tagid, df_tag.tagname).groupBy('officialpoiid') \
        .agg(collect_list('tagid').alias('tagids'), collect_list('tagname').alias('tagnames'))

    # poi目的地表，broadcast
    df_district = spark.table('dw_youdb.fact_gsdestdb_districtinfo') \
        .select('districttype', 'districtid', 'districtpath', col('name').alias('districtname'), 'isinchina')
    district_collect = df_district.select('districtid', 'districtname', 'districttype') \
        .where('isinchina=1 or districttype=1').collect()
    district_broadcast = spark.sparkContext.broadcast(
        {key['districtid']: (key['districtname'], key['districttype']) for key in district_collect})

    df_district_result = df_district.repartition(1000).rdd.map(district_path_format) \
        .toDF(StructType([StructField('district_name_map', MapType(StringType(), StringType()), True),
                          StructField('district_id_map', MapType(StringType(), StringType()), True),
                          StructField('districtid', StringType(), True),
                          StructField('isinchina', StringType(), True),
                          StructField('districtname', StringType(), True),
                          StructField('districtpath', StringType(), True),
                          StructField('districtpath_name', StringType(), True)]))

    # 匹配关系，-多语言
    df_poi_mapping = spark.table('dw_youdb.poi_mapping') \
        .select(col('gspoiid').alias('officialpoiid'), struct('sourcetype', 'sourcepoiid').alias('value_struct')) \
        .where('maptype in (1, 4)').where('d = current_date').where('publishstatus = 6').where('officialpoiid <> 0') \
        .fillna('').groupBy('officialpoiid') \
        .agg(collect_list('value_struct').alias('value_map'))
    df_poi_mapping_result = df_poi_mapping.repartition(1000).rdd.map(struct_to_map_combine) \
        .toDF(StructType([StructField('officialpoiid', StringType(), True),
                          StructField('value_map', MapType(StringType(), ArrayType(StringType())), True)])) \
        .withColumnRenamed('value_map', 'poi_mapping')

    # POI质量
    # df_poi_admintags = spark.table('dw_youdb.poiadmintags') \
    #     .select(col('poiid').alias('officialpoiid'), struct('type', 'value').alias('value_struct')) \
    #     .where('type in (1, 2)').groupBy('officialpoiid').agg(collect_list('value_struct').alias('value_map'))
    # df_poi_admintags_result = df_poi_admintags.rdd.map(struct_to_map) \
    #     .toDF(StructType([StructField('officialpoiid', StringType(), True),
    #                       StructField('value_map', MapType(StringType(), StringType(), True))])) \
    #     .selectExpr('officialpoiid', 'value_map as admin_tags')

    df_poi_admintags_result = spark.sql('''
     SELECT poiid as officialpoiid
        , str_to_map(concat_ws('', collect_set(concat_ws('', type, nvl(`value`, '')))), '', '') AS value_dict
    FROM dw_youdb.poiadmintags
    WHERE type IN (1, 2)
    GROUP BY poiid
    ''').selectExpr('officialpoiid', 'value_dict["1"] as quality', 'value_dict["2"] as uv_level')

    # POI UV-真的就可以直接写sql
    df_poi_uv = spark.sql('''
        SELECT poiid as officialpoiid, COUNT(DISTINCT clientcode) AS uv90, COUNT(clientcode) AS pv90
        FROM dw_youdb.poi_pv_uv_orginal
        WHERE d >= date_sub(current_date, 90)
            AND d <= current_date
        GROUP BY poiid
    ''')

    # 连接，有必要每次都repartition吗
    df_r = df_poi.repartition(1000) \
        .join(df_poi_mapping_result, on='officialpoiid', how='left').repartition(1000) \
        .join(df_poi_text_result, on='officialpoiid', how='left').repartition(1000) \
        .join(df_cover_image, on='officialpoiid', how='left').repartition(1000) \
        .join(df_tags, on='officialpoiid', how='left').repartition(1000) \
        .join(df_district_result, on='districtid', how='left').repartition(1000) \
        .join(df_global_name_result, on='officialpoiid', how='left').repartition(1000) \
        .join(df_global_dir_result, on='officialpoiid', how='left').repartition(1000) \
        .join(df_poi_admintags_result, on='officialpoiid', how='left').repartition(1000) \
        .join(df_poi_uv, on='officialpoiid', how='left').repartition(1000)
    # df_r.explain()
    df_r.createOrReplaceTempView('out_put')
    spark.sql('''
    insert into dw_youdb.cdm_bigtagle_poi_df partition(d=current_date) 
    select
        officialpoiid,districtid,
        poitype,poiname,pinyin,ename,poiaddress,glat,glon,gglat,gglon,blat,blon,sourcetype,businessid,rating,score,
        isgsshow,iscalibration,platform,businessstatus,commentcount,datachange_createtime,datachange_lasttime,editor,
        auditor,remark,creator,poi_mapping,eaddress,localname,tickettype,tips,playspendunit,playspendmin,playspendmax,
        playspendsource,shortfeature,hasticketgoods,ownership,businessdes,features,transit,ticketdesc,introduction,
        currency,avgcostper,isglobalbuy,servicefacility,weibo,twitter,wechat,facebook,pricedesc,tickethasmoreprice,
        officialsite,availablecurrency,guide,effecttime,expiretime,effectivetimesource,email,ticketprice,
        michelinrecommendstar,sighttokentype,michelinenvironmentlevel,businessremark,bookingtipsrich,bookingtips,
        officallevel,ugcshortfeature,formattimetips,hasformattime,sightneedgateticket,ticketrestriction,custommapfield,
        locatedpark,shopguidemap,shopguidepdf,panoramagram,airportplan,hotelpoiid,shortaddress,shortname,hotelrecommend,
        hotelparentid,hotelmetro,hotelsort,shortename,poisubtype,flightid,airline,crafttype,displayname,airporttext,
        terminalext,terminalname,terminalandthreecode,trainstationid,sightannouncement,businesssuspendedtime,
        businessstarttime,preferentialpolicies,storetype,isvirtual,plump,shopservicedesc,elevationlevel,itemcategory,
        address2,needappointment,inadvance,appointmentway,checkinpark,threecode,displaycoverimageid,tagids,tagnames,
        district_name_map,district_id_map,isinchina,districtname,districtpath,districtpath_name,global_names,global_dirs,
        quality,uv_level,uv90,pv90 
    from out_put
    ''')
    # 存临时表
    df_r.write.saveAsTable(name='tmp_youdb.tmp_matt_0701', mode='overwrite')
