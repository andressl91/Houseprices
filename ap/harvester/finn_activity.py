from bs4 import BeautifulSoup, element
import urllib.request
import os
import time

from typing import Optional
from threading import Event
import logging

from ap.harvester.sqlwork import RealEstate
from ap.harvester.harvester import ActivityABC
from ap.sql_toolbox.sql_interface import SqlTsDb, SqlTable

class NotValidSqM(Exception):
    pass

class FinnActivity(ActivityABC):
    """Mirrors reservoir volumehistory from smg-database to shyft-container.

    The data pull and push operations are conducted by a collector, where ReservoirActivity is
    a activity of the collector. When a activity thread is initiated, the
    startup(), cleanup(), wait_for(), and action() methods are only called from the
    current thread executing the activity. See the collector module.

    Args:
        dtss_adr: Address (``hostname:port``) of the DTSS server to write to.
        wait_first: Whether to wait before the first call to action.
        wakeup_freq: Optional frequency in seconds (or fractions thereof) to wakeup during
            long wait phases.
        exit_event: Event to signal the thread to exit.
        logger: Parent logger object. The activity creates a child logger from the instance.
            The child logger name is created by using the name property with all spaces
            replaced with ``_``.

    Attributes:

    """

    def __init__(self, *,
                 wait_first: bool, wakeup_freq: Optional[float],
                 exit_event: Event, logger, stair_case: bool = True) -> None:
        super().__init__(
            exit_event=exit_event, logger=logger,
            wait_first=wait_first, wakeup_freq=wakeup_freq)

        self.tags = None
        self._smg_tsr = None
        self.module_map = None

        self.module_map = None
        self.sql_ts_db = None
        self.sql_table = None

    @property
    def name(self) -> str:
        return FinnActivity.__name__

    def startup(self) -> None:
        """Perform needed startup actions before the activity polling loop."""
        path_to_folder = os.path.dirname(__file__)
        db_path = os.path.join(path_to_folder, 'data', 'finn_ts.db')
        table_path = os.path.join(path_to_folder, 'data', 'finn_table.db')

        self.sql_ts_db = SqlTsDb(db_path=db_path, category="price", sql_type="INT")
        self.sql_table = SqlTable(db_path=table_path)
        categories = {"finn_id": "INT", "address": "VARCHAR(30)", "price": "INT", "sq_m": "INT"}
        self.sql_table.create_table(table_name="finn_info", categories=categories)

    def cleanup(self, started: bool, graceful: bool) -> None:
        """Perform needed cleanup actions when thread performing activity polling exits."""
        self.logger.info("Cleanup")
        self.logger.info('Cleanup finished')

    def wait_for(self):
        return 20

    def get_soup(self):
        with urllib.request.urlopen('https://www.finn.no/realestate/homes/search.html?location=0.20003') as html_code:
            read_html = html_code.read()

        soup = BeautifulSoup(read_html, "html.parser")
        return soup


    def persist_realestate(self, prospect: RealEstate):
        self.sql_client().persist_realestate(prospect)

    def table_name_template(self, finn_id: str):
        return "finn_code_" + finn_id

    def get_address(self, addr: element.Tag):
        return addr.find("div", class_="licorice valign-middle").string

    def get_sq_m(self, sq_m: element.Tag):
        sq_m = sq_m.find("p", {"class": "t5 word-break mhn"}).get_text().split('\n')[1]
        return sq_m.split('m')[0]

    def get_price_nok(self, price_nok: element.Tag):
        price_nok = price_nok.find("p", {"class": "t5 word-break mhn"}).get_text().split('\n')[2]
        return price_nok.split(',')[0].replace(' ', '')

    def soup_alchemy(self):
        t1 = time.time()
        soup = self.get_soup()
        print(f"Get soup took {time.time() - t1}")
        data = []
        # All realestates on one page
        all_realestate_boxes = soup.find_all("div", class_="unit flex align-items-stretch result-item")

        # TODO: asyncio for I/O
        # TODO: get logging up
        data = []
        for i in all_realestate_boxes:

            finn_id = i.find("a")['id']
            address = self.get_address(i)
            sq_m = self.get_sq_m(i)

            if "-" in sq_m:
                continue

            price_nok = self.get_price_nok(i)
            if "-" in price_nok:
                # Range of sq_m ex. 45 - 60, 4-6Mill indicates to general Realestate
                continue

            price_nok = price_nok.replace('\xa0', '')
            price_pr_sqm = "%.0f" % (float(price_nok) / int(sq_m))
            # print("Description: \n%s" % i.find("h3").string)
            data_realestate = {"finn_id": int(finn_id),
                        "address": address,
                        "price": int(price_nok),
                        "sq_m": int(sq_m)}
            data.append(data_realestate)


        return data

    def action(self):

            #self.sql_db.send_data(table_name=self.table_name_template(finn_id), data_value=price_nok)
            data_set = self.soup_alchemy()
            for data in data_set:
                self.sql_table.write_to_table(data)
                self.sql_ts_db.send_data(table_name="Finn_" + str(data["finn_id"]),
                                         data_value=data["price"])

            #JUST TO TEST WRITE CSV
            path_to_csv = os.path.dirname(__file__)
            path_to_csv = os.path.join(path_to_csv, 'data', 'finn_ts.csv')

            self.sql_table.write_to_csv(path=path_to_csv,table=self.sql_table.table_name)

if __name__ == "__main__":

    activity = FinnActivity(wait_first=False, wakeup_freq=5,
                            exit_event=None, logger=logging.getLogger("Reservoir"))
    activity.startup()
    activity.action()

    # TODO: Store data outside project
    # TODO: Clean m² out of storage in DB
    # TODO: Move RealEstate out of sqlwork.py, and OOP in individual .py