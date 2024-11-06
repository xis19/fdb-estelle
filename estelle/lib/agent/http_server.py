"""http_server.py

Replies HTTP endpoints for kubelet probes
"""

import flask

_http_server = flask.app()

@_http_server.route("/probe/liveness", methods=["GET"])
def liveness_response():
    pass