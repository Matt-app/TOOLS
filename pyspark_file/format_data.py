import datetime
import json
import re
import sys

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType, MapType

FIELD = ['eventid', 'poiid', 'poitype', 'ownership', 'eventtype', 'content', 'value_dict', 'creator', 'creator_group',
         'editor', 'editor_group', 'datachange_lastday']
CHECKSTR = ['', '健康码', '行程码', '核酸检测报告', '体温测量', '佩戴口罩']
SIGHTTT = ['收费', '免费', '未知']


def clear_html_re(src_html):
    try:
        content = re.sub(r'</?(.+?)>', '', src_html)  # 去除标签
        content = re.sub(r'&nbsp;', '', content)
        dst_html = re.sub(r'\s+', '', content)  # 去除空白字符
        dst_html = ILLEGAL_CHARACTERS_RE.sub(r'', dst_html)
        return dst_html
    except Exception:
        return src_html


def format_time(_data):
    return ''


def do(row):
    _data = dict(row['value_dict'])
    try:
        if 'tips' in _data.keys():
            _data['tips'] = clear_html_re(_data.get('tips', ''))
        if 'telephone' in _data.keys():
            _data['telephone'] = ','.join((str(x) for x in json.loads(_data.get('telephone', ''))))
        if 'sightannouncement' in _data.keys():
            _d = json.loads(_data.get('sightannouncement', ''))
            _data['sightannouncement'] = '\r\n'.join(
                ['标题:' + _d.get('title', ''), '上线时间' + _d.get('start', ''), '下线时间' + _d.get('end', '')
                    , '内容' + clear_html_re(_d.get('content', ''))])
        if 'sighttickettype' in _data.keys() or 'pricedescription' in _data.keys():
            try:
                _st = SIGHTTT[int(_data.get('sighttickettype', 2))]
                _data['sighttickettype&pricedescription'] = _st + '+' + str(_data.get(
                    'pricedescription', ''))
            except Exception as e:
                _data['sighttickettype&pricedescription'] = e
        if 'preferentialpolicies' in _data.keys():
            _d = json.loads(_data.get('preferentialpolicies', ''))
            _data['preferentialpolicies'] = '\r\n'.join(
                ['-'.join([str(x[y]) for y in x if y in ['customdesc', 'description', 'pricedesc']]) for x in
                 _d.get('peoplegrouptype', {})])
        if 'poinamemultilanguage' in _data.keys() or 'poiName' in _data.keys():
            try:
                _d = json.loads(_data.get('poinamemultilanguage', '[]'))
                if 'poiName' in _data.keys():
                    _d.extend(json.loads(_data.get('poiName', '[]')))
                _data['poinamemultilanguage&poiName'] = '\r\n'.join(
                    [':'.join((str(y) for y in x.values())) for x in _d])
            except Exception as e:
                _data['poinamemultilanguage&poiName'] = e
        if 'poiaddressmultilanguage' in _data.keys() or 'poiAddress' in _data.keys():
            try:
                _d = json.loads(_data.get('poiaddressmultilanguage', '[]'))
                if 'poiName' in _data.keys():
                    _d.extend(json.loads(_data.get('poiAddress', '[]')))
                _data['poiaddressmultilanguage&poiAddress'] = '\r\n'.join(
                    [':'.join((str(y) for y in x.values())) for x in _d])
            except Exception as e:
                _data['poiaddressmultilanguage&poiAddress'] = e
        if 'needappointment' in _data.keys() or 'InAdvance' in _data.keys() or 'appointmentWay' in _data.keys():
            try:
                _advance = json.loads(_data.get('InAdvance', '{}'))
                _str_advance = ''.join(['提前', str(_advance.get['time']), str(_advance.get['unit']), '预约'])
                _appointment = json.loads(_data.get('appointmentWay', '[]'))
                _str_appointment = \
                    '\r\n'.join(
                        ['-'.join([str(x[y]) for y in x if y in ['way', 'channel', 'detail']]) for x in _appointment])
                _data['needappointment&InAdvance&appointmentWay'] = \
                    '\r\n'.join([str(_data.get('needappointment', '')), _str_advance, _str_appointment])
            except Exception as e:
                _data['needappointment&InAdvance&appointmentWay'] = e
        if 'checkinpark' in _data.keys():
            _checkinpark = json.loads(_data.get('checkinpark', '{}'))
            _checkList = _checkinpark.get('checkList', [])
            _checkListStr = ';'.join([CHECKSTR[x] for x in _checkList])
            _tips = _checkinpark.get('tips', '')
            _data['checkinpark'] = '\r\n'.join([_checkListStr, clear_html_re(_tips)])
        if 'hasformattime' in _data.keys() or 'formattime' in _data.keys() or 'formattimetips' in _data.keys()\
                or 'OpenTimeDescription' in _data.keys():
            try:
                _has_format_time = '结构化' if _data.get('hasformattime', '0') == '1' or 'formattime' in _data.keys()\
                    else '非结构化'
                _data['hasformattime&formattime&formattimetips'] = '\r\n'.join(
                    [_has_format_time, _data.get('formattimetips', ''),
                     clear_html_re(_data.get('OpenTimeDescription', ''))])
            except Exception as e:
                _data['hasformattime&formattime&formattimetips'] = e
        if 'introduction' in _data.keys():
            _introduction = clear_html_re(_data.get('introduction', ''))
            _data['introduction'] = _introduction
        if 'pinYin' in _data.keys():
            _introduction = _data.get('pinYin', '')
            _data['pinYin'] = _introduction
        if 'businesssuspendedtime' in _data.keys() or 'businessstarttime' in _data.keys() :
            _businesssuspendedtime = _data.get('businesssuspendedtime', '')
            _businessstarttime = _data.get('businessstarttime', '')
            _data['businesssuspendedtime&businessstarttime'] = '\r\n'.join(['暂停营业开始时间'+str(_businesssuspendedtime),
                                                                            '暂停营业结束时间'+str(_businessstarttime)])
    except:
        print('error')
    return row['eventid']\
        , row['poiid']\
        , row['poitype']\
        , row['ownership']\
        , row['eventtype']\
        , row['content'] \
        , row['value_dict']\
        , row['creator']\
        , row['creator_group']\
        , row['editor']\
        , row['editor_group'] \
        , str(row['datachange_day']), _data


if __name__ == '__main__':
    spark = SparkSession.builder.appName('run_detect_lang').enableHiveSupport().getOrCreate()
    spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')
    print('++++++++++++++++++++++++++++++++++++++++ begin +++++++++++++++++++++++++++++++++++++++++++++++++')
    print('spark version', spark.version)
    print('start spark ')
    print('')
    print('Python version', sys.version_info[0])
    print('encoding', sys.getdefaultencoding())
    t = datetime.datetime.now()
    TODAY = '%d-%02d-%02d' % (t.year, t.month, t.day)
    a = int(sys.argv[1])
    b = int(sys.argv[2])
    data = spark.table('tmp_youdb.adm_poi_audit_event_info_sight'). \
        select('eventid', 'poiid', 'poitype', 'ownership',
               'eventtype', 'content', 'value_dict',
               'creator', 'creator_group', 'editor',
               'editor_group', 'datachange_day') \
        .fillna('')
    # 在df或者rdd之后进行repartition看起来效果是一样的，总的来说需要在map前进行分区。
    rd_result = data.repartition(int(a)).rdd.map(do).persist(storageLevel=StorageLevel.MEMORY_ONLY)
    fields = [StructField(str(field_name), StringType(), True) for field_name in FIELD]
    fields.append(StructField('data', MapType(StringType(), StringType()), True))
    df_result = rd_result.toDF(schema=StructType(fields))
    df_result.repartition(1).write.saveAsTable('tmp_youdb.matt_0520', mode='overwrite')
    # df_result.repartition(1).write.insertInto('dw_youdb.adm_poi_audit_event_info_sight_df', overwrite=True)
