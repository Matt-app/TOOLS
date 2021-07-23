import io

import pandas as pd
from flask import make_response


def file_to_html(excel, *rest, function):
    file_name = excel.filename.split('.')[0].encode('utf-8').decode('latin1')
    e_io = io.BytesIO(excel.read())
    e_io.seek(0)
    df = function(e_io, rest)
    sio = io.StringIO()
    sio.write(df)
    sio.seek(0)  # 文件指针
    rv = make_response(sio.getvalue())
    sio.close()
    rv.headers['Content-Type'] = 'text/html'
    rv.headers['Cache-Control'] = 'no-cache'
    rv.headers['Content-Disposition'] = 'attachment; filename={}_result.html'.format(file_name)
    return rv


def file_to_xlsx(excel, *rest, function):
    file_name = excel.filename.split('.')[0].encode('utf-8').decode('latin1')
    e_io = io.BytesIO(excel.read())
    e_io.seek(0)
    df = function(e_io, rest)
    bio = io.BytesIO()
    writer = pd.ExcelWriter(bio)
    df.to_excel(writer, index=False, encoding='utf-8')
    writer.save()
    bio.seek(0)  # 文件指针
    rv = make_response(bio.getvalue())
    bio.close()
    rv.headers['Content-Type'] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    rv.headers["Cache-Control"] = "no-cache"
    rv.headers['Content-Disposition'] = 'attachment; filename={}_result.xlsx'.format(file_name)
    return rv

