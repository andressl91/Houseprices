import sqlite3
from typing import Tuple, Dict, Any
from abc import abstractmethod
from enum import Enum

"""Make API for simpler sqlite operations for TimeSeries specific"""
"""Make same for retrievting data"""

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

    def does_table_exist(self, table_name):
        self.connect_db()
        cursor = self.get_cursor()
        #cursor.execute(f"""SELECT count(*) FROM sqlite_master WHERE type='table' AND name='REALD' """)
        cursor.execute("SELECT count(*) FROM sqlite_master where type='table' AND name=?", (table_name,))
        # reads all records into memory, and then returns that list.
        exist = cursor.fetchall()[0][0]
        if exist == 1:
            return True

        return False

        self.close_db()

    def get_categories(self, table_name: str):
        self.connect_db()
        cursor = self.get_cursor()
        cursor.execute(f"""SELECT sql FROM sqlite_master
        WHERE tbl_name = {table_name} AND type = 'table'""")
        output = cursor.fetchall()
        self.close_db()
        return output

    def write_to_csv(self, path: str, table: str):
        # to export as csv file
        # WRITE TO CSV IKKE HELT GOD DA "," I ADRESSEN F* UP CSV
        # MULIG PREPROSSESEROMG I AKTIVITET ER LØSNINGEN!
        self.connect_db()
        cursor = self.get_cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        table_info = cursor.fetchall()
        colum_headers = " ".join([t[1] + "," for t in table_info])[:-1]

        with open(path, "wb") as write_file:
            write_file.write(colum_headers.encode())
            write_file.write("\n".encode())
            for row in cursor.execute(f"SELECT * FROM {table}"):
                writeRow = " ".join([str(i) + "," for i in row])[:-1]
                write_file.write(writeRow.encode())
                write_file.write("\n".encode())
        self.close_db()


class SqlTsDb(SqlDb):
    """Sqlinterface, for storing TS in Sqlite.

    Intened for Activities.
    """
    def __init__(self, db_path: str, category: str, sql_type):
        super().__init__(db_path=db_path)
        self.category = category
        self.sql_type = sql_type


    def create_table(self, table_name):

        self.routine(f"""CREATE TABLE {table_name}(id INTEGER PRIMARY KEY, 
                           time INT, {self.category} {self.sql_type})""")


    def send_data(self, input_time: int=None, table_name: str=None, data_value: float=None):

        if self.does_table_exist(table_name=table_name):

            if input_time:
                self.routine(f"""INSERT INTO 
                             {table_name}(TIME, {self.category}) VALUES ({input_time}, {data_value})""")
            else:
                self.routine(f"""INSERT INTO 
                             {table_name}(TIME, {self.category}) VALUES (strftime('%s', 'now'), {data_value})""")

        else:
            self.create_table(table_name=table_name)
            self.send_data(input_time=input_time, table_name=table_name, data_value=data_value)

# TODO: Make subclass for storing daily market info, to be used with
# TODO: Bokeh or ML. Initiated in harvester


class SqlTable(SqlDb):

    def __init__(self, db_path: str):
        super().__init__(db_path=db_path)
        self.categories = None
        self.table_name = None

    def create_table(self, table_name, categories: Dict, primary_key: str=None):

        if not self.does_table_exist(table_name=table_name):

            if primary_key:
                text = f"""CREATE TABLE {table_name} (id INT"""
                for cat, sql_type in categories.items():
                    text += f", {cat} {sql_type}"

                text += f", PRIMARY KEY({primary_key}) )"
                self.routine(text)
                self.categories = categories
                self.table_name = table_name

            else:

                text = f"""CREATE TABLE {table_name} (id INTEGER PRIMARY KEY"""
                for cat, sql_type in categories.items():
                    text += f", {cat} {sql_type}"

                text += ")"
                self.routine(text)
                self.categories = categories
                self.table_name = table_name

        else:
            self.table_name = table_name
            self.categories = categories
            self.table_name = table_name

            print("Table exist")

    def check_valid_keys(self, values: Dict):
        for i in self.categories:
            if i not in values:
                return False

        return True

    def write_to_table(self, values: Dict):

        if self.check_valid_keys(values=values):
            text = f"INSERT INTO {self.table_name}"
            cat_tuple = "(" + ",".join(i for i in list(values)) + ")"

            #val_tuple = "(" + ",".join(str(i) for i in list(values.values())) + ")"
            val_tuple = tuple([i for i in list(values.values())])
            nr_tuple = "(" + ",".join("?" for i in range(len(values))) + ")"

            text += cat_tuple + "VALUES" + nr_tuple

            self.connect_db()
            cursor = self.get_cursor()
            cursor.execute(text, val_tuple)
            self.commit()
            self.close_db()