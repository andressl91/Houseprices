import os
from ap.utilities.sql_interface import SqlTsDb, SqlTable


def test_make_send_table():
    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn.db')

    sta = SqlTsDb(db_path=db_path, category="price", sql_type="INT")
    sta.create_table()
    sta.send_data(table_name='REALESTATE', data_value=20)
    # print table with fetcahll implement!!!


def test_table_exist():
    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn.db')

    sta = SqlTsDb(db_path=db_path, category="price", sql_type="INT")
    table_name = "finn_code_12"
    assert sta.does_table_exist(table_name=table_name) is False
    sta.send_data(table_name=table_name, data_value=10)
    assert sta.does_table_exist(table_name=table_name) is True
    sta.send_data(table_name=table_name, data_value=15)

def test_sql_table():
    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn_table.db')

    categories = {"price": "INT", "sq_m": "INT"}

    sql_table = SqlTable(db_path=db_path)
    sql_table.create_table(table_name="today", categories=categories)

    #sql_table.does_table_exist(table_name="today")
    #sql_table.get_categories(table_name="today")
    data_set = {"price": 10, "sq_m": 20}
    sql_table.write_to_table(values=data_set)