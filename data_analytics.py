import os
import re
import json
import glob
import time
import duckdb
import logging
import threading
import webbrowser
import http.server

import utils


# Global Variables
ROOT_FOLDER = os.path.dirname(os.path.realpath(__file__))
DUCKDB_STOCK_TABLE = "stock_trade"
DUCKDB_PURCHASE_TABLE = "purchase"


class quietServer(http.server.SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

def run_analytics():
    # Start local web server
    with open(utils.HTML_ANALYTICS_FILE, "w") as f:
        f.write("Loading...")
    with open(utils.UPDATE_FLAG_FILE, "w") as f:
        f.write("")

    server = http.server.ThreadingHTTPServer(
        (utils.LOCALWEB_HOST, utils.LOCALWEB_PORT),
        quietServer,
    )
    ANALYTICS_ADDRESS = f"http://{utils.LOCALWEB_HOST}:{utils.LOCALWEB_PORT}"
    logging.info(f"Starting WebServer thread ({ANALYTICS_ADDRESS})")
    threading.Thread(target = server.serve_forever, daemon=True).start()

    folders_processed = set()
    tables_created = list()
    table_print = False
    file_open = False
    with duckdb.connect() as conn:
        while True:
            folders = (
                set(glob.glob(os.path.join(utils.DUCKDB_DATA_FOLDER, "*")))
                - folders_processed
            )

            for folder in sorted(folders):
                table_print = True
                logging.info(f"Processing folder '{folder}'...")

                schema_file = os.path.join(folder, "schema.sql")
                if os.path.exists(schema_file):
                    with open(schema_file) as f:
                        for line in f:
                            if line.strip():
                                table_name = re.findall(
                                    "CREATE TABLE '*(.*?)'*\s*\(", line
                                )
                                if table_name:
                                    table_name = table_name[0]
                                    if table_name not in tables_created:
                                        conn.execute(line)
                                        tables_created.append(table_name)

                load_file = os.path.join(folder, "load.sql")
                if len(tables_created) > 0 and os.path.exists(load_file):
                    with open(load_file) as f:
                        for line in f:
                            if line.strip():
                                conn.execute(line)
                    folders_processed.add(folder)

            if table_print:
                if DUCKDB_STOCK_TABLE in tables_created:
                    logging.info(f"Updating table '{DUCKDB_STOCK_TABLE}'...")

                    # Aggregated data
                    data = utils.aggregate_by_symbol_side(conn, DUCKDB_STOCK_TABLE)
                    data_table_stock = ""
                    for row in data.fetchall():
                        if row[1] == "~~~~":
                            data_table_stock += "<tr class='table-warning'>"
                            data_table_stock += (
                                f"<th scope='row' colspan=2>Sub-Total</th>"
                            )
                            tds = "th scope='row'"
                            tde = "th"
                        else:
                            data_table_stock += "<tr>"
                            data_table_stock += f"<td>{row[0]}</td><td>{row[1]}</td>"
                            tds = "td"
                            tde = "td"
                        data_table_stock += f"<{tds} class='text-right {'text-danger' if row[3]<0 else ''}'>{row[3]:,.0f}</{tde}>"
                        data_table_stock += f"<{tds} class='text-right {'text-danger' if row[2]<0 else ''}'>{row[2]:,.2f}</{tde}>"
                        data_table_stock += f"<{tds} class='text-right {'text-danger' if row[4]<0 else ''}'>{row[4]:,.2f}</{tde}>"
                        data_table_stock += "</tr>"

                    # Latest
                    data = utils.get_latest_stock(conn, DUCKDB_STOCK_TABLE)
                    data_table_stock_latest = ""
                    for row in data.fetchall():
                        data_table_stock_latest += "<tr>"
                        data_table_stock_latest += (
                            f"<td>{row[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:23]}</td>"
                        )
                        data_table_stock_latest += f"<td>{row[1]}</td>"
                        data_table_stock_latest += f"<td>{row[2]}</td>"
                        data_table_stock_latest += f"<td>{row[3]}</td>"
                        data_table_stock_latest += f"<td>{row[4]}</td>"
                        data_table_stock_latest += f"<td>{row[5]}</td>"
                        data_table_stock_latest += (
                            f"<td class='text-right'>{row[6]:,.0f}</td>"
                        )
                        data_table_stock_latest += (
                            f"<td class='text-right'>{row[7]:,.2f}</td>"
                        )
                        data_table_stock_latest += "</tr>"

                    # Pie chart
                    array_data_stock = json.dumps([["Symbol", "Buy", "Sell"]] + list(utils.get_piechart_data_stock(
                        conn,
                        DUCKDB_STOCK_TABLE,
                    ).fetchall()))

                else:
                    data_table_stock = ""
                    data_table_stock_latest = ""
                    array_data_stock = ""

                if DUCKDB_PURCHASE_TABLE in tables_created:
                    logging.info(f"Updating table '{DUCKDB_PURCHASE_TABLE}'...")
                    data = utils.aggregate_by_sku(conn, DUCKDB_PURCHASE_TABLE)

                    # Aggregated data
                    data_table_purchase = ""
                    for row in data.fetchall():
                        if row[1] == "~~~~":
                            data_table_purchase += "<tr class='table-warning'>"
                            data_table_purchase += (
                                f"<th scope='row' colspan=2>Sub-Total</th>"
                            )
                            tds = "th scope='row'"
                            tde = "th"
                        else:
                            data_table_purchase += "<tr>"
                            data_table_purchase += f"<td>{row[0]}</td><td>{row[1]}</td>"
                            tds = "td"
                            tde = "td"
                        data_table_purchase += (
                            f"<{tds} class='text-right'>{row[2]:,.0f}</{tde}>"
                        )
                        data_table_purchase += (
                            f"<{tds} class='text-right'>{row[3]:,.2f}</{tde}>"
                        )
                        data_table_purchase += (
                            f"<{tds} class='text-right'>{row[4]:,.2f}</{tde}>"
                        )
                        data_table_purchase += "</tr>"

                    # Latest
                    data = utils.get_latest_purchase(conn, DUCKDB_PURCHASE_TABLE)
                    data_table_purchase_latest = ""
                    for row in data.fetchall():
                        data_table_purchase_latest += "<tr>"
                        data_table_purchase_latest += (
                            f"<td>{row[0].strftime('%Y-%m-%d %H:%M:%S.%f')[:23]}</td>"
                        )
                        data_table_purchase_latest += f"<td>{row[1]}</td>"
                        data_table_purchase_latest += f"<td>{row[2]}</td>"
                        data_table_purchase_latest += f"<td>{row[3]}</td>"
                        data_table_purchase_latest += (
                            f"<td class='text-right'>{row[4]:,.0f}</td>"
                        )
                        data_table_purchase_latest += (
                            f"<td class='text-right'>{row[5]:,.2f}</td>"
                        )
                        data_table_purchase_latest += "</tr>"

                    # Pie chart
                    array_data_store = json.dumps([["Store", "sku_0", "sku_1", "sku_2", "sku_3", "sku_4", "sku_5", "sku_6", "sku_7", "sku_8"]] + list(utils.get_piechart_data_store(
                        conn,
                        DUCKDB_PURCHASE_TABLE,
                    ).fetchall()))

                else:
                    data_table_purchase = ""
                    data_table_purchase_latest = ""
                    array_data_store = ""

                utils.create_html_file(
                    utils.HTML_TEMPLATE_ANALYTICS,
                    utils.HTML_ANALYTICS_FILE,
                    {
                        "data_table_stock": data_table_stock,
                        "data_table_purchase": data_table_purchase,
                        "data_table_stock_latest": data_table_stock_latest,
                        "data_table_purchase_latest": data_table_purchase_latest,
                        "array_data_stock": array_data_stock,
                        "array_data_store": array_data_store,
                    },
                )

                table_print = False
                html_file = f"file://{ROOT_FOLDER}/{utils.HTML_ANALYTICS_FILE}"
                logging.info(f"Analytics ready at: {html_file}")

                if not file_open:
                    webbrowser.open(ANALYTICS_ADDRESS)
                    file_open = True

            time.sleep(0.5)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    run_analytics()
