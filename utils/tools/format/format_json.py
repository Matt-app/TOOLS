import pandas as pd

COLUMNS = ['airport', 'airportBuildingID', 'buildingName', 'smsBuildingName', 'recordStatus']
COLUMNS_NEED_SPLIT = []


def json_apply(_x):
    # _x = _x[0]
    _result_list = []
    for c in COLUMNS:
        if c in COLUMNS_NEED_SPLIT and c in _x.keys():
            _result_list.append(','.join(_x[c]) if c in _x.keys() else '')
        else:
            _result_list.append(_x[c] if c in _x.keys() else '')
    return _result_list


def json_to_excel(io, rest):
    mode, = rest
    _type = 'series' if '0' == mode else 'frame'
    data = pd.read_json(io, encoding='utf-8', typ=_type)
    # result_data = data.apply(json_apply, axis=1, result_type='expand')
    # result_data.columns = COLUMNS
    # result_data = result_data.set_index(['updateTime', 'poiId'])['imageUrlList']\
    #     .str.split(',', expand=True).stack()\
    #     .reset_index(level=2, drop=True)\
    #     .reset_index(name='imageUrlList')
    return pd.DataFrame(data).T if '0' == mode else data.T


# data = pd.read_json('test.json', encoding='utf-8', typ='series').T
# print(1)
pass
