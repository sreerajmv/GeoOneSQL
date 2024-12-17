from flask import Flask, jsonify
from routes import register_blueprints
from flask_cors import CORS
app = Flask(__name__)


CORS(app)
register_blueprints(app)

@app.route("/")
def index():
    return jsonify({"message": "Hello, World!"})

if __name__ == "__main__":
    app.run(debug=True)