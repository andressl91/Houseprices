import os
from ap.utilities.sql_interface import SqlDb, SqlTsDb


def test_make_table():
    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn.db')

    sta = SqlTsDb(db_path=db_path)
    sta.create_table(table_name='test1')

    # print table with fetcahll implement!!!

test_make_table()