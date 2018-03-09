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

    def __init__(self, db_path: str):
        super().__init__(db_path=db_path)


    def create_table(self, table_name: str):
        #self.connect_db()

        #self.get_cursor().execute(f'''CREATE TABLE realestate(id INTEGER PRIMARY KEY,
        #               time INT, price INT)''')

        self.routine(f'''CREATE TABLE {table_name}(id INTEGER PRIMARY KEY, 
                       time INT, price INT)''')
        #self.sql_db.commit()
        #self.close_db()

    def send_data(self):
        cursor.execute('''INSERT
        INTO PROSPECT(ID, TIME, PRICE) VALUES(1, strftime('%s', 'now'), 10)''')


"""
try:
    sql_db = sqlite3.connect(self.db_path)
    cursor = sql_db.cursor()
    cursor.execute('''CREATE TABLE realestate(id INTEGER PRIMARY KEY, finn_id TEXT,
                    address TEXT, sq_meter TEXT, price TEXT, price_pr_sqm TEXT)
                 ''')
    sql_db.commit()

except Exception as inst:
    print(type(inst))  # the exception instance
    print(inst.args)  # arguments stored in .args
    print(inst)  # __str__ allows args to be printed directly

finally:
    sql_db.close()

"""