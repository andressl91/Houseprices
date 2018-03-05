import sqlite3

from datetime import date
from random import randint

from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, DateFormatter, TableColumn
from bokeh.io import output_file, show
from bokeh.plotting import figure

from bokeh.layouts import gridplot
from bokeh.models import CDSView, GroupFilter

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
        sq_meter = []
        finn_id = []

        for row in rows:
            # All colums must not be mentioned
            #price.append(10)
            price.append(row["price"])
            sq_meter.append(row["sq_meter"])
            address.append(row["address"])
            finn_id.append(row["finn_id"])
            #print(f"""{row["Id"]} {row["finn_id"]} {row["address"]} {row["sq_meter"]} {row["price"]}""")

    output_file("./html_products/finn_table.html")
    data = dict(prices=price, sq_meters=sq_meter, address=address, finn_id=finn_id)

    source = ColumnDataSource(data)
    columns = [
        TableColumn(field="prices", title="Price"),
        TableColumn(field="sq_meters", title="SqMeter"),
        TableColumn(field="address", title="Address"),
        TableColumn(field="finn_id", title="FinnId")
    ]
    data_table = DataTable(source=source, columns=columns, width=400, height=280, selectable=True)
    show(data_table)

def plot2():
    con = sqlite3.connect("../harvester/data/finn.db")

    with con:
        con.row_factory = sqlite3.Row

        cur = con.cursor()
        cur.execute("SELECT * FROM realestate")

        rows = cur.fetchall()

        address = []
        price = []
        sq_meter = []
        finn_id = []

        for row in rows:
            # All colums must not be mentioned
            #price.append(10)
            price.append(row["price"])
            sq_meter.append(row["sq_meter"])
            address.append(row["address"])
            finn_id.append(row["finn_id"])
            #print(f"""{row["Id"]} {row["finn_id"]} {row["address"]} {row["sq_meter"]} {row["price"]}""")

    # Mode inline spawns full html with graphs without internet! :D
    output_file("./html_products/test.html", mode='inline')
    data = dict(prices=price, sq_meters=sq_meter, address=address, finn_id=finn_id)

    source = ColumnDataSource(data)
    #view1 = CDSView(source=source, filters=[GroupFilter(column_name='species', group='versicolor')])

    plot_size_and_tools = {'plot_height': 300, 'plot_width': 300,
                           'tools': ['box_select', 'reset', 'help']}

    p1 = figure(title="Full data set", **plot_size_and_tools)
    p1.circle(x='prices', y='sq_meters', source=source, color='black')

    #p2 = figure(title="Setosa only", x_range=p1.x_range, y_range=p1.y_range, **plot_size_and_tools)
    #p2.circle(x='petal_length', y='petal_width', source=source, view=view1, color='red')

    show(gridplot([[p1]]))

#make_table()
#show_db()
plot2()