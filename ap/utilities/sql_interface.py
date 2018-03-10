import sqlite3
from typing import Tuple, Dict, Any
from abc import abstractmethod
from enum import Enum

"""Make API for simpler sqlite operations for TimeSeries specific"""
"""Make same for retrievting data"""


# Storage classes and B
# Epoch time SELECT strftime('%s', 'now')

class Feature(Enum):
    ID = "id_number"
    PRICE = "price_nok"
    SQ_M = "square_meter"
    ZIP_CODE = "zip_code"


class SqlTsAdapter:

    @abstractmethod
    def connect_db(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def close_db(self):
        pass

    @abstractmethod
    def get_cursor(self):
        pass


class SqlDb(SqlTsAdapter):

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.sql_db = None

    def connect_db(self):
        self.sql_db = sqlite3.connect(self.db_path)

    def get_cursor(self):
        if not self.sql_db:
            print("No sql_DB")

        return self.sql_db.cursor()

    def commit(self):
        self.sql_db.commit()

    def close_db(self):
        self.sql_db.close()

    def routine(self, str):
        # Make try
        self.connect_db()
        self.get_cursor().execute(str)
        self.commit()
        self.close_db()


class SqlTsDb(SqlDb):
    """Sqlinterface, for storing TS in Sqlite.

    Intened for Activities.
    """
    def __init__(self, db_path: str, category: str, sql_type):
        super().__init__(db_path=db_path)
        self.category = category
        self.sql_type = sql_type

    def does_table_exist(self, table_name):
        self.connect_db()
        t_table = 'table'
        cursor = self.get_cursor()
        #cursor.execute(f"""SELECT count(*) FROM sqlite_master WHERE type='table' AND name='REALD' """)
        cursor.execute("SELECT count(*) FROM sqlite_master where type='table' AND name=?", (table_name,))
        # reads all records into memory, and then returns that list.
        # TODO: COnsider fetchall as flag into self.routine
        exist = cursor.fetchall()[0][0]
        if exist == 1:
            return True

        return False
        #self.commit()
        self.close_db()


    def create_table(self, table_name):

        self.routine(f"""CREATE TABLE {table_name}(id INTEGER PRIMARY KEY, 
                           time INT, {self.category} {self.sql_type})""")


    def send_data(self, input_time: int=None, table_name: str=None, data_value: float=None):

        if self.does_table_exist(table_name=table_name):
            if input_time:
                self.routine(f"""INSERT INTO 
                             {table_name}(TIME, PRICE) VALUES ({input_time}, {data_value})""")
            else:
                self.routine(f"""INSERT INTO 
                             {table_name}(TIME, PRICE) VALUES (strftime('%s', 'now'), {data_value})""")

        else:
            self.create_table(table_name=table_name)
            self.send_data(input_time=input_time, table_name=table_name, data_value=data_value)

# TODO: Make subclass for storing daily market info, to be used with
# TODO: Bokeh or ML. Initiated in harvester