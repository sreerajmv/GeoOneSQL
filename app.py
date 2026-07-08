from flask import (
    Flask,
    jsonify,
    request,
    render_template,
    session,
    redirect,
    url_for,
    abort,
)
from routes import register_blueprints
from flask_cors import CORS  # type: ignore
from setting.db_connections import ms_query_db
# import os

app = Flask(__name__)

app.secret_key = "super_secret_development_key"

VALID_USERNAME = "admin"
VALID_PASSWORD = "secure123"

# Define the list of allowed IP addresses here.
# 127.0.0.1 allows you to test it locally.
ALLOWED_IPS = ["127.0.0.1", "10.10.0.204", "10.10.0.218", "10.10.0.231", "10.10.0.123"]

CORS(app)
register_blueprints(app)


@app.route("/")
def index():
    # print(f"Request from IP: {request.remote_addr}")
    return jsonify({"message": "Hello, World!"})



if __name__ == "__main__":
    app.run(debug=True, port=5001)
