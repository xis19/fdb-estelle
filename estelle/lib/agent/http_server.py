"""http_server.py

Replies HTTP endpoints for kubelet probes
"""

from flask import Flask

app = Flask("http_server")


@app.route("/probe/liveness", methods=["GET"])
def liveness_response():
    pass
