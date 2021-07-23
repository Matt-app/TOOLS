from collections import namedtuple, deque

import jieba
import jieba.posseg as pseg
from utils import CITY_NAME, RE_PUNCTUATION


class JieBaWorker:
    def __init__(self, content=None):
        if content is None:
            content = []
        self.result = []
        self.loc_list = {}
        self.city_name = CITY_NAME
        self.word_list = []
        self.R = namedtuple('R', ['location', 'label', 'word'])
        self.content_list = content
        self.tmp_content_cache = deque(maxlen=3)
        self.tmp_brackets = []
        self.if_brackets = False
        pass

    def set_location(self, data):
        _data = data.replace('市', '')
        if _data in self.city_name:
            self.loc_list.setdefault(_data, 0)
            self.loc_list[_data] += 1
        return

    def set_result(self, word, label):
        for w in [i for i in RE_PUNCTUATION.split(word) if i != '']:
            _r = self.R(location='', label=label, word=w)
            self.result.append(_r)
        return

    def context(self, _word, _label):
        if _word in self.content_list:
            try:
                if self.tmp_content_cache[0][1] in ('v', 'n', 'ns', 'nt', 'nw', 'nz', 'LOC', 'ORG', 'p') and \
                        self.tmp_content_cache[1][1] in ('v', 'n', 'ns', 'nt', 'nw', 'nz', 'LOC', 'ORG', 'p'):
                    _out_put = ''.join([x[0] for x in self.tmp_content_cache])
                    self.set_result(_out_put, 'new_c')
                # elif self.tmp_content_cache[0][1] in ('v', 'n', 'ns', 'nt', 'nw', 'nz', 'LOC', 'ORG', 'p'):
                #     _out_put = '999'.join([x[0] for x in self.tmp_content_cache][1:])
                #     self.set_result(_out_put, 'new_c')
            except Exception:
                pass
            self.tmp_content_cache.clear()
        return

    def brackets(self, _word):
        if _word in [']', ']', '】', '」'] and self.if_brackets:
            self.if_brackets = False
        if self.if_brackets and len(self.tmp_brackets) < 4:
            self.tmp_brackets.append(_word)
            pass
        elif self.if_brackets and len(self.tmp_brackets) >= 4:
            self.if_brackets = [False, 0]
            self.tmp_brackets.clear()
        elif not self.if_brackets and len(self.tmp_brackets) > 0:
            if self.tmp_brackets[-1] != 'R':
                self.set_result(''.join(self.tmp_brackets), 'new_b')
            self.tmp_brackets.clear()
        if _word in ['[', '{', '【', '「']:
            self.if_brackets = [True, 0]
        return

    def fount_new_word(self, _word, _label):
        self.brackets(_word)
        if not self.if_brackets and len(self.content_list) > 0:
            self.context(_word, _label)
            pass
        pass

    def work(self, data):
        jieba.enable_paddle()
        data_list = pseg.cut(data, use_paddle=True)
        _result = []
        _loc = ''
        for _word, _label in data_list:
            self.tmp_content_cache.append((_word, _label))
            # n普通名词,f方位名词,s处所名词,t时间,ns地名,nt机构名,nw作品名,nz其他专名,vd动副词,vn名动词,a形容词,ad副形词,PER人名,LOC地名,ORG机构名,
            if _label in ['ns']:
                self.set_location(_word)
            if _label in ['v', 'x', 'n', 'ns', 'nt', 'nw', 'nz', 'LOC', 'ORG', 'p'] or self.if_brackets:
                self.fount_new_word(_word, _label)
                if _label in ['ns', 'nt', 'nw', 'nz', 'LOC', 'ORG']:
                    self.set_result(_word, _label)
        if len(self.loc_list) > 0:
            _loc = max(self.loc_list.items(), key=lambda x: x[1])[0]

        for _r in self.result:
            try:
                _result = [[_loc, x.word, x.label] for x in self.result
                           if x.word != _loc]
            except Exception as e:
                print(e)
        return _result


# def test():
#     j = JieBaWorker()
#     a = j.work('☕️为期三天的上海静安世界咖啡文化节非常火爆，兴业太古汇的主场已经人山人海挤不进的感觉，选择久光分会场逛逛也不错。 '
#                ' ☕️地点设在二楼海外平台，M Stand咖啡店外的一片区域，人也不少，比之前来多了太多人，现场有各种咖啡摊位，'
#                '冰淇淋也不少，买了好几个吃。  🍦咖啡冰淇淋 ¥20一个，第二个半价 🍦北海道冰淇淋¥18一个 ☕️'
#                'Key Turbo Coffee ¥8.8一杯  ⏰静安咖啡节时间：2021.5.28-5.30 📍地址：上海南京西路1618号上海久光百货二楼室外平台 '
#                '🚇交通：地铁2/7号线静安寺站，🚌公交37路/21路/20路静安寺站 ⏰营业时间：10:00-22:00 🅿️停车信息：¥15/小时，'
#                '购物满¥300免费停车1小时，购物满¥600免费停车2小时，购物满¥1000免费停车3小时')
#     print(a)
#
#
# test()
