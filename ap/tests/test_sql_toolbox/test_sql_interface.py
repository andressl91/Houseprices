import os
from ap.sql_toolbox.sql_interface import SqlTsDb


def test_make_table():
    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn.db')

    sta = SqlTsDb(db_path=db_path, category="price", sql_type="INT")
    sta.create_table(table_name='')

    # print table with fetcahll implement!!!

