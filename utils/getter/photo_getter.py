import json

import pandas
from pandas import DataFrame

from utils.jieba_tools.fortest import postData_noHeader

URL = 'http://poi.image.aesthetic.score.group.ctripcorp.com/api/imageAesthetic/'


def get_score(row):
    try:
        photo_url = row['gsurl'].replace('https://dimg04.c-ctrip.com/images', 'http://images4.fx.ctripcorp.com/target')
    except:
        photo_url = ''
    data = {
        "appId": "1010",
        "bizType": "imagequality",
        "service": "imgservice",
        "inputType": "url",
        "clientKey": "123456",
        "imEntities": [{
            "imageId": "0",
            "image": photo_url
        }]
    }
    r = postData_noHeader(URL, data)
    print(r)
    try:
        row['score'] = json.loads(json.loads(r).get('result').replace('\'', '"'))[0].get('score', '0')
    except:
        row['score'] = '0'
    return row


if __name__ == '__main__':
    data = pandas.read_excel('dzdp_photo.xlsx', keep_default_na=False)
    result = data.apply(get_score, axis=1)
    result.to_excel('result.xlsx', index=False)
