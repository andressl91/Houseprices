from bs4 import BeautifulSoup
import urllib.request
import csv
import os

from typing import Optional
from abc import abstractmethod
from threading import Event
from numpy import isnan
import logging

from ap.harvester.sqlwork import RealEstate, SqlLiteClient


from ap.harvester.harvester import ActivityABC

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
        self._dtss_client = None
        self.module_map = None

    @property
    def name(self) -> str:
        return FinnActivity.__name__

    def startup(self) -> None:
        """Perform needed startup actions before the activity polling loop."""

    def cleanup(self, started: bool, graceful: bool) -> None:
        """Perform needed cleanup actions when thread performing activity polling exits."""
        self.logger.info("Cleanup")
        self.logger.info('Cleanup finished')

    def wait_for(self):
        return 60*60

    def get_soup(self):
        with urllib.request.urlopen('https://www.finn.no/realestate/homes/search.html?location=0.20003') as html_code:
            read_html = html_code.read()

        soup = BeautifulSoup(read_html, "html.parser")
        return soup

    def sql_client(self):
        path_to_folder = os.path.dirname(__file__)
        db_path = os.path.join(path_to_folder, 'data', 'finn.db')
        sql_client = SqlLiteClient(db_path)
        return sql_client

    def persist_realestate(self, prospect: RealEstate):
        self.sql_client().persist_realestate(prospect)


    def action(self):
        soup = self.get_soup()
        data = []
        # All realestates on one page
        all_realestate_boxes = soup.find_all("div", class_="unit flex align-items-stretch result-item")

        # TODO: Consider Object programming of a site with attributes adress, finn_id... in constructor f.eks
        # TODO: Consider aggreate methods, to aggregate all realastate according to some option, f.eks price range or sq_m > 50 ..

        for i in all_realestate_boxes:
            # Adresses
            address = i.find("div", class_="licorice valign-middle").string
            finn_id = i.find("a")['id']
            sq_m = i.find("p", {"class": "t5 word-break mhn"}).get_text().split('\n')[1]
            price_nok = i.find("p", {"class": "t5 word-break mhn"}).get_text().split('\n')[2]
            print("Description: \n%s" % i.find("h3").string)

            print("Done one realestate unit \n")
            data.append((finn_id, address, sq_m, price_nok))

        folder = os.path.dirname(__file__)
        p = os.path.join(folder, 'data', 'index.csv')

        for finn_id, address, sq_m, price_nok in data:
            prospect = RealEstate(finn_id=finn_id, address=address, sq_meters=sq_m, price=price_nok)
            self.persist_realestate(prospect)


if __name__ == "__main__":

    activity = FinnActivity(wait_first=False, wakeup_freq=5,
                            exit_event=None, logger=logging.getLogger("Reservoir"))
    activity.action()