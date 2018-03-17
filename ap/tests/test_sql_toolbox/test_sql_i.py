import os
from ap.sql_toolbox.sql_interface import SqlDb, SqlTsDb, SqlTable


def test_make_ts_db():
    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn_ts.db')
    if os.path.exists(db_path):
        os.remove(db_path)

    sql_db = SqlTsDb(db_path=db_path, category="price", sql_type="INT")
    sql_db.send_data(table_name="test", data_value=12)

def test_db_pather():
    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn.db')
    if os.path.exists(db_path):
        os.remove(db_path)

    sql_db = SqlTable(db_path=db_path)
    categories = {"sq_m": "INT", "price": "INT"}
    sql_db.create_table(table_name="test", categories=categories, primary_key="price")
    assert sql_db.check_valid_keys({"sq_m": 10, "price": 23})
    assert sql_db.check_valid_keys({"sq_m": 10, "dollar": 23}) is False
    sql_db.write_to_table({"sq_m": 10, "price": 123})

    csv_path = os.path.join(path_to_folder, 'data', 'test.csv')
    sql_db.write_to_csv(path=csv_path, table="test")