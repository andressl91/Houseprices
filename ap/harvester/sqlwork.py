import sqlite3
import os

from typing import Tuple


class RealEstate:

    def __init__(self, finn_id: str, address: str, sq_meters: str, price: str, price_pr_sqm:str):
        self.finn_id = finn_id
        self.address = address
        self.sq_meters = sq_meters
        self.price = price
        self.price_pr_sqm = price_pr_sqm

    def spoonfeed_sql(self) -> Tuple:
        return (self.finn_id, self.address, self.sq_meters, self.price, self.price_pr_sqm)

class SqlLiteClient:

    def __init__(self, db_path: str):
        self.db_path = db_path

    def create_table(self):
        #TODO: Conditional, check if Realestate db exists, else create
        # If flag, then create
        #TODO: Make sure instance only created once for each activity
        try:
            sql_db = sqlite3.connect(self.db_path)
            cursor = sql_db.cursor()
            cursor.execute('''CREATE TABLE realestate(id INTEGER PRIMARY KEY, finn_id TEXT,
                            address TEXT, sq_meter INT, price INT, price_pr_sqm INT)
                         ''')
            sql_db.commit()

        except Exception as inst:
            print(type(inst))  # the exception instance
            print(inst.args)  # arguments stored in .args
            print(inst)  # __str__ allows args to be printed directly

        finally:
            sql_db.close()

    def persist_realestate(self, real_estate: RealEstate):
        try:
            sql_db = sqlite3.connect(self.db_path)
            cursor = sql_db.cursor()

            sql_input = real_estate.spoonfeed_sql()

            cursor.execute('''INSERT INTO realestate(finn_id, address, sq_meter, price, price_pr_sqm)
                              VALUES(?,?,?,?,?)''', sql_input)
            print('First user inserted')

            sql_db.commit()

        except sqlite3.OperationalError:
            print("Exists")

        finally:
            sql_db.close()


if __name__ == "__main__":

    path_to_folder = os.path.dirname(__file__)
    db_path = os.path.join(path_to_folder, 'data', 'finn_table.db')

    prospect = RealEstate(finn_id='123', address='home2', sq_meters=44, price=1234, price_pr_sqm=2222)

    sql_client = SqlLiteClient(db_path=db_path)
    # RUN THIS TO MAKE REALESTATE DB
    sql_client.create_table()
    sql_client.persist_realestate(prospect)


    """
    cursor.execute('''INSERT INTO users(name, phone, email, password)
                      VALUES(:name,:phone, :email, :password)''',
                   {'name': name1, 'phone': phone1, 'email': email1, 'password': password1})
    
    users = [(name1, phone1, email1, password1),
             (name2, phone2, email2, password2),
             (name3, phone3, email3, password3)]
    cursor.executemany(''' INSERT INTO users(name, phone, email, password) VALUES(?,?,?,?)''', users)
    db.commit()
    
    """