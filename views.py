import psycopg2
import pandas as pd
import os
import re
from config import DB_CONFIG


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def sanitize(name):
    name = str(name).strip()
    name = re.sub(r"\W+", "_", name)
    return name.lower()


def clean_null_bytes(path):

    cleaned = path + "_clean"

    with open(path, "rb") as f:
        data = f.read()

    data = data.replace(b"\x00", b"")

    with open(cleaned, "wb") as f:
        f.write(data)

    return cleaned


def create_table(cursor, table, columns):

    cols = ", ".join([f'"{c}" TEXT' for c in columns])

    query = f"""
    CREATE TABLE IF NOT EXISTS "{table}" (
        {cols}
    );
    """

    cursor.execute(query)


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

        elif ext in ["xlsx", "xls"]:

            df = pd.read_excel(cleaned_path)

        else:
            return "Unsupported file type"

        insert_dataframe(df, table)

        return f"Upload successful → table '{table}'"

    except Exception as e:

        return f"Upload failed: {str(e)}"