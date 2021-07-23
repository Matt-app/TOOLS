import json
import traceback

with open('data/log_test.txt', 'r+', encoding='utf-8') as f:
    lines = f.readlines()
    file_name = 'text.txt'
    result_list = []
    for l in lines:
        try:
            # if '松鼠动物园' in l:
            #     print(1)
            # if '水族馆' in l or '动物园' in l:
            #     if file_name != 'name missing':
            #         with open(file_name, 'a+') as f2:
            #             f2.write(','.join(result_list))
            #     file_name = l.replace('\n', '')
            #     result_list.clear()
            #     continue
            if ',' in l:
                datas = json.loads(l.replace('\n', ''))
            else:
                datas = [l.replace('"', '')[:-1]]
            for d in datas:
                if 'upload' in d:
                    result_list.append('\'' + d[0:-8] + '\'')
                elif 'exist' in d:
                    result_list.append('\'' + d[0:-7] + '\'')
                    pass
                pass
            pass
        except:
            traceback.print_exc()
            print(l)
            pass
        pass
    with open(file_name, 'a+') as f2:
        f2.write(','.join(result_list))
    pass
pass
