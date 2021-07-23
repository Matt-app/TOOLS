import os


os.environ.setdefault("env", "UAT")


from web.mysites import app


app.run(host="0.0.0.0", port="8000")
