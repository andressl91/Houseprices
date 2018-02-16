from typing import Optional
from abc import abstractmethod
from threading import Event
from numpy import isnan

from harvester.harvester import ActivityABC

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




    def startup(self) -> None:
        """Perform needed startup actions before the activity polling loop."""

    def cleanup(self, started: bool, graceful: bool) -> None:
        """Perform needed cleanup actions when thread performing activity polling exits."""
        self.logger.info("Cleanup")
        self.logger.info('Cleanup finished')

    def wait(self):
        return 15

    def action(self):
        print("HELLO")