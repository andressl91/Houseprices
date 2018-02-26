import sqlite3

from datetime import date
from random import randint

from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, DateFormatter, TableColumn
from bokeh.io import output_file, show

def make_table():
    output_file("./html_products/data_table.html")
    data = dict(
            dates=[date(2014, 3, i+1) for i in range(10)],
            downloads=[randint(0, 100) for i in range(10)],
        )
    source = ColumnDataSource(data)

    columns = [
            TableColumn(field="dates", title="Date", formatter=DateFormatter()),
            TableColumn(field="downloads", title="Downloads"),
        ]
    data_table = DataTable(source=source, columns=columns, width=400, height=280)
    show(data_table)
    #show(vform(data_table))

def show_db():
    con = sqlite3.connect("../harvester/data/finn.db")

    with con:
        con.row_factory = sqlite3.Row

        cur = con.cursor()
        cur.execute("SELECT * FROM realestate")

        rows = cur.fetchall()

        address = []
        price = []

        for row in rows:
            # All colums must not be mentioned
            address.append(row["address"])
            price.append(row["price"])
            print(f"""{row["Id"]} {row["finn_id"]} {row["address"]} {row["sq_meter"]} {row["price"]}""")

    print()

    data = dict(
        price=price,
        address=address,
    )
    source = ColumnDataSource(data)

    #cur = con.cursor()
    #cur.execute("SELECT * FROM realestate")
    #rows = cur.fetchall()
    #con.close()
    #return rows


print(show_db())