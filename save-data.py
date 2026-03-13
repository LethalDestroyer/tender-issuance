from flask import Flask, request, jsonify
import psycopg2
import pandas as pd
import os
import re
from werkzeug.utils import secure_filename

app = Flask(__name__)

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


# DB CONNECTION
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# SANITIZE NAME
def sanitize(name):
    name = str(name).strip()
    name = re.sub(r"\W+", "_", name)
    return name.lower()


# REMOVE NULL BYTES
def clean_null_bytes(path):

    cleaned = path + "_clean"

    with open(path, "rb") as f:
        data = f.read()

    data = data.replace(b"\x00", b"")

    with open(cleaned, "wb") as f:
        f.write(data)

    return cleaned


# CREATE TABLE
def create_table(cursor, table, columns):

    cols = ", ".join([f'"{c}" TEXT' for c in columns])

    query = f"""
    CREATE TABLE IF NOT EXISTS "{table}" (
        {cols}
    );
    """

    cursor.execute(query)


# INSERT DATAFRAME
def insert_dataframe(df, table):

    conn = get_conn()
    cursor = conn.cursor()

    df.columns = [sanitize(c) for c in df.columns]

    create_table(cursor, table, df.columns)

    for _, row in df.iterrows():

        row = [str(x).replace("\x00", "") for x in row]

        placeholders = ", ".join(["%s"] * len(row))
        columns = ", ".join([f'"{c}"' for c in df.columns])

        query = f'INSERT INTO "{table}" ({columns}) VALUES ({placeholders})'

        cursor.execute(query, tuple(row))

    conn.commit()

    cursor.close()
    conn.close()


# PROCESS FILE
def process_file(file_path, table):

    ext = file_path.split(".")[-1].lower()

    cleaned_path = clean_null_bytes(file_path)

    try:

        if ext == "csv":

            df = pd.read_csv(
                cleaned_path,
                encoding="latin1",
                engine="python",
                sep=None,
                on_bad_lines="skip"
            )

        elif ext in ["xlsx", "xls", 'csv']:

            df = pd.read_excel(cleaned_path)

        else:

            return "Unsupported file type"

        insert_dataframe(df, table)

        return f"Upload successful → table '{table}'"

    except Exception as e:

        return f"Upload failed: {str(e)}"


# API ROUTE
@app.route("/upload", methods=["POST"])
def upload():

    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]

    filename = secure_filename(file.filename)

    path = os.path.join(UPLOAD_FOLDER, filename)

    file.save(path)

    table = sanitize(os.path.splitext(filename)[0])

    result = process_file(path, table)

    os.remove(path)

    return jsonify({"message": result})


# HEALTH CHECK
@app.route("/")
def home():

    try:

        conn = get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return f"PostgreSQL Connected: {version}"

    except Exception as e:

        return str(e)

# fetch data 
@app.route("/postgres/products_100/limit/10", methods=["GET"])
def get_data_limit(table, limit):

    conn = get_conn()
    cursor = conn.cursor()

    query = f'SELECT * FROM "{sanitize(table)}" LIMIT %s'
    print(query)
    cursor.execute(query, (limit,))

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    data = [dict(zip(columns, row)) for row in rows]
    print(data)

    cursor.close()
    conn.close()

    return jsonify(data)        




# RUN SERVER
if __name__ == "__main__":

    app.run(host="0.0.0.0", port=8000, debug=True)

