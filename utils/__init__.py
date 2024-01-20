import os
import time
import logging

from jinja2 import Template


# Global Variables
HTML_TEMPLATE_ANALYTICS = os.path.join("templates", "analytics_template.html")
HTML_ANALYTICS_FILE = "analytics.html"
FOLDER_CONFIG = "config"
DUCKDB_DATA_FOLDER = "data"
FIELD_TYPE_MAPPING = {
    "boolean": "BOOLEAN",
    "int": "INTEGER",
    "long": "INTEGER",
    "float": "DOUBLE",
    "double": "DOUBLE",
    "bytes": "BLOB",
    "string": "VARCHAR",
}

def create_html_file(template, file_name, context):
    with open(template, "r") as f:
        jinja_template = Template(f.read())
    with open(file_name, "w") as f:
        f.write(jinja_template.render(**context))


def map_field(field_type):
    return FIELD_TYPE_MAPPING.get(field_type, "VARCHAR")


def generate_table_schema(avro_schema):
    field_names = ["__ts", "__key"]
    table_schema = ["__ts TIMESTAMP", "__key VARCHAR"]
    for field in avro_schema.get("fields", list()):
        field_name = field.get("name")
        field_type = field.get("type")
        field_names.append(field_name)
        if isinstance(field_type, str):
            table_schema.append(f"{field_name} {map_field(field_type)}")
        elif isinstance(field_type, list):
            for f in field_type:
                if f != "null":
                    table_schema.append(f"{field_name} {map_field(f)}")
                    break
    return (
        field_names,
        ", ".join(table_schema),
    )


def create_db_table(conn, table_name, table_schema):
    conn.sql(f"""CREATE TABLE IF NOT EXISTS '{table_name}' ({table_schema});""")

def empty_table(conn, table_name):
    conn.sql(f"""DELETE FROM '{table_name}';""")


def insert_and_export(conn, folder, num_records, schema_field_names, records, chunks=25):
    # Inserting records
    logging.info(f"Adding {num_records} record(s) into the database...")
    for topic, rows in records.items():
        field_names = schema_field_names[topic]
        for i in range(0, len(rows), chunks):
            conn.executemany(f"""INSERT INTO '{topic}' ({','.join(field_names)}) VALUES ({('?,'*len(field_names))[:-1]});""", records[topic][i:i + chunks])

    # Exporting to Parquet
    file_name = f"database_{int(time.time())}"
    full_file_name = os.path.join(folder, file_name)
    logging.info(f"Exporting data to: {full_file_name}...")
    conn.sql(f"""EXPORT DATABASE '{full_file_name}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);""")
    logging.info("Completed!")
    
    # Emptying table
    for topic in records.keys():
        empty_table(conn, topic)


def aggregate_by_symbol_side(conn, table_name):
    return conn.execute(f"""
        SELECT
            symbol,
            side,
            AVG(price) AS avg_price,
            SUM(quantity) AS total_quantity,
            SUM(quantity * price) AS total_cost
        FROM '{table_name}'
        GROUP BY
            symbol, side
        ---
        UNION ALL
        SELECT
            symbol,
            '~~~~' AS side,
            CASE WHEN SUM(quantity * (CASE WHEN side='BUY' then -1 else 1 END))=0 THEN 0 ELSE SUM(CASE WHEN side='BUY' THEN -1 * (quantity * price) ELSE (quantity * price) END) / SUM(quantity * (CASE WHEN side='BUY' then -1 else 1 END)) END AS avg_price,
            SUM(quantity * (CASE WHEN side='BUY' then -1 else 1 END)) AS total_quantity,
            SUM(CASE WHEN side='BUY' THEN -1 * (quantity * price) ELSE (quantity * price) END) AS total_cost
        FROM '{table_name}'
        GROUP BY
            symbol, '~~~~'
        ORDER BY
            symbol, side;""")


def aggregate_by_sku(conn, table_name):
    return conn.execute(f"""
        SELECT
            storeid,
            sku,
            SUM(quantity) AS total_quantity,
            AVG(price) AS avg_price,
            SUM(quantity * price) AS total_cost
        FROM '{table_name}'
        GROUP BY
            storeid, sku
        ---
        UNION ALL
        SELECT
            storeid,
            '~~~~' AS sku,
            SUM(quantity) AS total_quantity,
            AVG(price) AS avg_price,
            SUM(quantity * price) AS total_cost
        FROM '{table_name}'
        GROUP BY
            storeid, '~~~~'
        ORDER BY
            storeid, sku;""")
