from flask import Flask, request, jsonify
import os
from werkzeug.utils import secure_filename

from views import process_file, sanitize

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.route("/upload", methods=["POST"])
def upload():

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]

    filename = os.path.basename(file.filename)
    filename = secure_filename(filename) 

    path = os.path.join(UPLOAD_FOLDER, filename)
    
    file.save(path)

    table = sanitize(os.path.splitext(filename)[0])

    result = process_file(path, table)

    os.remove(path)

    return jsonify({"message": result})


@app.route("/")
def home():
    return "API Running Successfully"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)