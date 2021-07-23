import json
import re
import traceback

import pandas as pd
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


def merge_excel_worker(io, rest: tuple):
    key_word, num = rest
    data = pd.read_excel(io, keep_default_na=False, sheet_name=0)
    for i in range(1, int(num)):
        data = data.merge(pd.read_excel(io, keep_default_na=False, sheet_name=i),
                          on=key_word, how='left')
    return data


def replace_excel_worker(io, rest: tuple):
    print(rest)
    word_old, word_new, index_start, index_end = rest
    data = pd.read_excel(io, keep_default_na=False, sheet_name=0)
    data.iloc[:, index_start-1: index_end] = data.iloc[:, index_start-1: index_end] \
        .applymap(lambda x: str(x).replace(word_old, word_new))
    return data


def excel_clear_html_worker(io, rest):
    column, = rest

    def clear_html_re(row):
        try:
            src_html = row['introduction']
            content = re.sub(r'</?(.+?)>', '', src_html)  # 去除标签
            content = re.sub(r'&nbsp;', '', content)
            dst_html = re.sub(r'\s+', '', content)  # 去除空白字符
            dst_html = ILLEGAL_CHARACTERS_RE.sub(r'', dst_html)
            row[column] = dst_html
            return row
        except Exception:
            row['{}_result'.format(column)] = ''
            return row

    data = pd.read_excel(io, keep_default_na=False, index_col=False)
    result_data = data.apply(clear_html_re, axis=1, result_type='expand')
    return result_data



