import dash
import dash_core_components as dcc
import dash_html_components as html

import pandas as pd
import sqlite3

def lazy_sqlite_db_fetch():
    # Write CSV with new lines
    db_con = sqlite3.connect(database='/home/andreas/Desktop/Houseprices/ap/harvester/data/finn_table.db')
    cursor = db_con.cursor()
    ##cursor.execute("SELECT * FROM finn_info;")

    cursor.execute("PRAGMA table_info(realestate)")

    ##print()

    table = cursor.fetchall()
    colum_headers = " ".join([t[1] + "," for t in table])[:-1]
    print(colum_headers)

    # to export as csv file
    with open("./finn_table.csv", "wb") as write_file:
        cursor = db_con.cursor()
        write_file.write(colum_headers.encode())
        write_file.write("\n".encode())
        for row in cursor.execute("SELECT * FROM realestate"):
            writeRow = " ".join([str(i) + "," for i in row])[:-1]
            print(writeRow)
            write_file.write(writeRow.encode())
            write_file.write("\n".encode())


def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )


if __name__ == '__main__':
    lazy_sqlite_db_fetch()
    #df = pd.read_csv("./finn_table.csv")
    #app = dash.Dash()
#
    #app.layout = html.Div(children=[
    #    html.H4(children='US Agriculture Exports (2011)'),
    #    generate_table(df)
    #])
#
    #app.run_server(debug=True)