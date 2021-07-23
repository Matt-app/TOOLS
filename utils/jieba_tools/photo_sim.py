import json
import time
import requests

from utils.jieba_tools.fortest import soa2_post_json


def image_similarity(photoid_1, url_1, photoid_2, url_2):
    domain = 'http://imgsimilarity.hotel.ctripcorp.com/api/image_similarity'
    _data = {
        "appId": "100031442",
        "bizType": "hotel",
        "service": "imgservice",
        "clientKey": "123456",
        "inputType": "url",
        "imageId_1": str(photoid_1),
        "imageData_1": url_1,
        "imageId_2": str(photoid_2),
        "imageData_2": url_2
    }
    try:
        r1 = requests.post(url=domain, json=_data).text
        r = json.loads(r1)
        _msg = r.get('return_msg', '')
        if _msg == 'success!':
            _simi = float(r.get('similarity', '0'))
            if _simi > 244:
                return do(photoid_1, photoid_2, url_1, url_2)
            else:
                return _simi
        else:
            return r1
    except Exception as e:
        return e


def post_head(url, headers):
    try:
        req = requests.get(url, data=None, headers=headers)

        resp_body = req.text

        if resp_body is None:
            return "this is none"

        return resp_body
    except Exception as e:
        return e


def postData_eachdata(url, postdata, headers):
    try:
        req = requests.post(url, bytes(postdata, encoding='UTF-8'), headers=headers)
        resp_body = req.text

        return resp_body
    except Exception as e:
        print(e)

    return ""


def getpoi(datalist):
    headers = {
        'Authorization': 'Basic ZWxhc3RpYzplOFp3OFI5RTlaeExmdDI1NUdzNTQ5SE8=',
        'Content-Type': 'application/json;charset=UTF-8',
    }
    url = "http://poidatasearch-application.rb.es.gs.ctripcorp.com/infocheck/infocheck/_bulk"

    response = send(url, datalist, headers)
    if response == "timed out":
        print('timed out')
        response = send(url, datalist, headers)
    return response
    # print(response)


def send(url, datalist, headers):
    resp = postData_eachdata(url, datalist, headers)
    return resp


def cut_image(url, width, height):
    if (".mp4" in url):
        return url

    cut = "_D_{0}_{1}".format(width, height)
    newurl = url.replace(".jpg", cut + ".jpg").replace(".png", cut + ".png").replace(".gif", cut + ".gif")

    return newurl


def cut_image_w(url, width, height):
    if (".mp4" in url):
        return url

    cut = "_W_{0}_{1}".format(width, height)
    newurl = url.replace(".jpg", cut + ".jpg").replace(".png", cut + ".png").replace(".gif", cut + ".gif")

    return newurl


def suppimageurl(str):
    if (str.startswith("http")):
        return str
    else:
        if (str.startswith("//") or str.startswith("/")):
            return "http://images4.fx.ctripcorp.com/target" + str
        else:
            return "http://images4.fx.ctripcorp.com/target/" + str


def getcurr(indexid):
    headers = {
        'Authorization': 'Basic ZWxhc3RpYzplOFp3OFI5RTlaeExmdDI1NUdzNTQ5SE8=',
        'Content-Type': 'application/json;charset=UTF-8',
    }
    url = "http://poidatasearch-application.rb.es.gs.ctripcorp.com/infocheck/infocheck/" + indexid

    resp = post_head(url, headers)

    if resp == '':
        return ""

    return json.loads(resp).get('_source', {}).get('checkdetail', '')


def do(old, new, old_url, new_url):
    _id = old + new
    batch = "1012202106"
    indexid = str(batch) + str(_id)
    try:
        note = "photoid_1:{} photoid_2:{}".format(str(old), str(new))

        data1 = {"index": {"_id": indexid}}
        data2 = {
            "infoid": _id,
            "note": note,
            "value": "{0}|{1}".format(cut_image_w(old_url, 1200, 0), cut_image_w(new_url, 1200, 0)),
            "batch": batch,
            "id": _id,
            "domain": "",
            "type": 1012,
            "checkresult": "",
            "datachange_lasttime": time.time() * 1000
        }

        datalist = json.dumps(data1) + "\r\n" + json.dumps(data2) + "\r\n"
        res = getpoi(datalist)
        return res
    except Exception as e:
        print('doeach: ', str(e))
        return str(e)


def get_simi_photo():
    r2 = requests \
        .get(url='http://qmqgateway.arc.ctripcorp.com/pull?{}&{}&{}&{}&{}'
             .format('subject=gs.photo.spark.simi', 'group=photo_sim', 'timeout=3000', 'batch=100', 'tags=photo_sim')) \
        .text
    print(r2)
    r_json = json.loads(r2)['data']
    r_l = []
    for r in r_json:
        result = r['attrs']
        a = image_similarity(result.get('photoid_1'), result.get('url_1'), result.get('photoid_2'), result.get('url_2'))
        r_l.append(a)
    return r_l


def image_similarity_test(photoid_1, url_1, photoid_2, url_2):
    domain = 'http://imgsimilarity.hotel.ctripcorp.com/api/image_similarity'
    _data = {
        "appId": "100031442",
        "bizType": "hotel",
        "service": "imgservice",
        "clientKey": "123456",
        "inputType": "url",
        "imageId_1": str(photoid_1),
        "imageData_1": url_1,
        "imageId_2": str(photoid_2),
        "imageData_2": url_2
    }
    try:
        r1 = soa2_post_json(domain, _data)
        r = r1['data']
        _msg = r.get('return_msg', '')
        if _msg == 'success!':
            _simi = float(r.get('similarity', '0'))
            return str(_simi)
        else:
            return r1
    except Exception as e:
        return e


if __name__ == '__main__':
    r2 = requests \
        .get(url='http://qmqgateway.arc.ctripcorp.com/pull?{}&{}&{}&{}&{}'
             .format('subject=gs.photo.spark.simi', 'group=photo_sim', 'timeout=3000', 'batch=10', 'tags=photo_sim'))
    r_json = json.loads(r2.text)['data']
    r_l = []
    for r in r_json:
        result = r['attrs']
        a = image_similarity_test(result.get('photoid_1'), result.get('url_1'), result.get('photoid_2'), result.get('url_2'))
        print(a)

