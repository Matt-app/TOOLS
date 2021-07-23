import json
import urllib.request
import urllib.response

def postData_noHeader(url, postdata):
    try:
        data = bytes(json.dumps(postdata), encoding='utf8')
        headers11 = {'Content-Type': 'application/json'}
        req = urllib.request.Request(url, data, headers11)
        resp = urllib.request.urlopen(req)

        resp_body = resp.read().decode('UTF-8')

        return resp_body
    except Exception as e:
        print(e)

    return ""


def soa2_post_json(domain, data):
    headers = {'Content-Type': 'application/json', 'SOA20-Client-AppId': '100008526'}
    postdata = {
        "url": domain,
        "postdata": json.dumps(data),
        "headers": json.dumps(headers)
    }
    response = postData_noHeader('http://poiviewdata.you.ctripcorp.com/frankdata_headers', postdata)
    obj = json.loads(response)
    return obj
