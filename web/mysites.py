import traceback

from flask import Flask, jsonify, request, render_template

from utils import merge_excel_worker, excel_clear_html_worker, json_to_excel, HtmlMaker, replace_excel_worker,JieBaWorker
from web import file_to_xlsx, file_to_html
from conf import TEMPLATE_PATH, STATIC_PATH

app = Flask(__name__, template_folder=TEMPLATE_PATH, static_folder=STATIC_PATH)


@app.route("/version")
def version():
    return "v0301"


@app.route('/jiebatest')
def jieba_test():
    content = ""
    if request.args.get("content"):
        content = request.args.get("content")
    do = JieBaWorker().work(content)
    return jsonify(do)


# @app.route('/simi_photo')
# def simi_photo():
#     do = get_simi_photo()
#     return jsonify(do)


@app.route('/formatHtml')
def format_html():  # 用来生成相关联的URL
    return render_template('1/FormatHtml.html')


@app.route('/formatJSON')
def format_json():  # 用来生成相关联的URL
    return render_template('1/FormatJSON.html')


@app.route('/makeHtml')
def make_html():  # 用来生成相关联的URL
    return render_template('1/MakeHtml.html')


@app.route('/mergeExcel')
def merge_excel():  # 用来生成相关联的URL
    return render_template('1/MergeExcel.html')


@app.route('/replaceExcel')
def replace_excel():  # 用来生成相关联的URL
    return render_template('1/ReplaceExcel.html')


@app.route('/doMergeExcel', methods=['POST'])
def do_merge_excel():
    error = None
    if request.method == 'POST':
        excel = request.files['excel_data']
        key_word = request.form['key_word']
        num = request.form['num']
        try:
            rv = file_to_xlsx(excel, key_word, num, function=merge_excel_worker)
            return rv
        except:
            traceback.print_exc()
            error = 'error'
    return render_template('1/MergeExcel.html', error=error)


@app.route('/doReplaceExcel', methods=['POST'])
def do_replace_excel():
    error = None
    if request.method == 'POST':
        excel = request.files['excel_data']
        word_old, word_new, index_start, index_end \
            = request.form['word_old'], request.form['word_new'], \
              request.form['index_start'], request.form['index_end']

        try:
            rv = file_to_xlsx(excel, word_old, word_new, int(index_start), int(index_end),
                              function=replace_excel_worker)
            return rv
        except:
            traceback.print_exc()
            error = 'error'
    return render_template('1/ReplaceExcel.html', error=error)


@app.route('/doMakeHtml', methods=['POST'])
def do_make_html():
    error = None
    if request.method == 'POST':
        excel = request.files['excel_data']
        columns = request.form['title']
        try:
            hm = HtmlMaker()
            rv = file_to_html(excel, columns.split(','), function=hm.make_static_html)
            return rv
        except:
            traceback.print_exc()
            error = 'error'
    return render_template('1/MakeHtml.html', error=error)


@app.route('/doFormatHtml', methods=['POST'])
def do_format_html():
    error = None
    if request.method == 'POST':
        excel = request.files['excel_data']
        columns = request.form['title']
        try:
            rv = file_to_xlsx(excel, columns, function=excel_clear_html_worker)
            return rv
        except:
            traceback.print_exc()
            error = 'error'
    return render_template('1/FormatHtml.html', error=error)


@app.route('/doFormatJSON', methods=['POST'])
def do_format_json():
    error = None
    if request.method == 'POST':
        excel = request.files['excel_data']
        mode = request.form['mode']
        try:
            rv = file_to_xlsx(excel, mode, function=json_to_excel)
            return rv
        except:
            traceback.print_exc()
            error = "error"
    return render_template('1/FormatHtml.html', error=error)


@app.route('/hellow')
def index():  # 用来生成相关联的URL
    return render_template('Hello.html')


@app.route('/')
def hello_world():
    return 'Hello World!'
