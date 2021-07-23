import os

CURRENT_PATH = os.path.join(os.path.dirname(__file__), os.path.pardir)
PROJECT_PATH = os.path.abspath(CURRENT_PATH)
FILE_PATH = os.path.join(PROJECT_PATH, 'FILE')
CONF_PATH = os.path.join(PROJECT_PATH, 'CONF')
RESULT_PATH = os.path.join(PROJECT_PATH, 'RESULT')
TEMPLATE_PATH = os.path.join(PROJECT_PATH, 'templates')
STATIC_PATH = os.path.join(PROJECT_PATH, 'static')