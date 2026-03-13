from flask import Flask, request, jsonify, render_template
import os
from werkzeug.utils import secure_filename

from views import process_file, sanitize, get_table_data

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


#Fetch Data

app = Flask(__name__)

@app.route("/data/<table>", methods=["GET"])
def get_data_limit(table):

    limit = request.args.get("limit", default=10, type=int)
    page = request.args.get("page", default=1, type=int)

    result = get_table_data(table, page, limit)

    return render_template(
        "show-data.html",
        data=result["data"],
        columns=result["columns"],
        table=result["table"],
        limit=result["limit"],
        page=result["page"],
        db_name=result["table"]
)





# @app.route("/data/<table>", methods=["GET"])
# def get_data_limit(table):
#     db_name=table
#     # print(db_name)
#     limit = request.args.get("limit", default=10, type=int)
#     page = request.args.get("page", default=1, type=int)
    
#     offset = (page - 1) * limit
#     # table = request.args.get(table)


#     conn = get_conn()
#     cursor = conn.cursor()

#     query = f'SELECT * FROM "{sanitize(table)}" LIMIT %s OFFSET %s'
#     cursor.execute(query, (limit, offset))

#     columns = [desc[0] for desc in cursor.description]
#     rows = cursor.fetchall()

#     data = [dict(zip(columns, row)) for row in rows]

#     cursor.close()
#     conn.close()

#     return render_template(
#         "show-data.html",
#         data=data,
#         columns=columns,
#         table=table,
#         limit=limit,
#         page=page,
#         db_name=db_name
#     )



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)