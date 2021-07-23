import pandas as pd


class HtmlMaker:
    def __init__(self, count=10000):
        self.T = {
            'START_AND_TEXT': '<tr><td>',
            'START_AND_PICTURE': '<tr><td><img src="',
            'TEXT_AND_END': '</td></tr>',
            'PICTURE_AND_END': '"></td></tr>',
            'TEXT_AND_TEXT': '</td><td>',
            'TEXT_AND_PICTURE': '</td><td><img src="',
            'PICTURE_AND_TEXT': '"></td><td>',
            'PICTURE_AND_PICTURE': '"></td><td><img src="',
        }
        self.HTML = ['<!DOCTYPE html>',
                     '<html lang="en">',
                     '<head>',
                     '    <meta charset="UTF-8">',
                     '    <title>Title</title>',
                     '</head>',
                     '<style>',
                     '    img{',
                     '     height:200px;',
                     '     }',
                     '    table, th, td {',
                     '      border: 1px solid black;',
                     '      border-collapse: collapse;',
                     '    }',
                     '    th{',
                     '      padding: 15px;',
                     '    }',
                     '    th {',
                     '      background-color: black;',
                     '      color: white;',
                     '    }',
                     '    td {',
                     '      width: 15px;',
                     '    }',
                     '</style>',
                     '<body>',
                     '<table width:100%;>',
                     '    <caption>photo information</caption>']
        self.TABLE_COLUMNS = ['<tr>']
        self.TABLE_PICTURES = []
        self.HTML_END = ['</table>'
                         '</body>'
                         '</html>']
        self.count = count

    def make_static_html(self, io, rest):
        data = pd.read_excel(io, keep_default_na=False, index_col=False, nrows=10000)
        columns, = rest

        def combine_columns(row):
            output = ''
            pre_label = 'START'
            if len(self.TABLE_COLUMNS) == 1:
                self.TABLE_COLUMNS.extend(''.join(['<th>', x, '</th>']) for x in row.index)
                self.TABLE_COLUMNS.append('</tr>')
            for i in row.index:
                label = 'PICTURE' if i in columns else 'TEXT'
                output = self.T.get('_AND_'.join([pre_label, label])).join([output, str(row[i])])
                pre_label = label
            if self.count:
                self.TABLE_PICTURES.append(output + self.T.get('_AND_'.join([pre_label, 'END'])))
                self.count -= 1
            pass

        data.apply(combine_columns, axis=1, result_type='expand')
        self.HTML.extend(self.TABLE_COLUMNS)
        self.HTML.extend(self.TABLE_PICTURES)
        self.HTML.extend(self.HTML_END)
        return '\n'.join(self.HTML)
