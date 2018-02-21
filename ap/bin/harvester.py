import logging


from ap.harvester.manager2 import CollectorManager
from ap.harvester.manager2 import Collectors
from ap.harvester.log_handler import log_handler


def mirror_smg(log_path: str = ''):
    """Main script for mirroring smg database.

    The purpose of this script is to setup collector activities for collecting timeseries
    from the smg database, and storing these locally on a shyft-container.
    Each of the activities is managed by the CollectorManager, initiating which activates
    mirroring of the smg database. The CollectorManager initiates a thread for each activity, defined as
    the process to mirroring data from the smg database. The data is the marked with a specific url-path
    to a local shyft-container, and stored accordingly.

    Returns:

    """
    cm = CollectorManager(
        monitor_poll_freq=8.,
        logger=None,
        default_wait_first=False,
        default_wakeup_freq=None,

        collectors={
            Collectors.FINN_REALESTATE: dict(
                wait_first=False, wakeup_freq=5,
                exit_event=None, logger=logging.getLogger("Reservoir")
            ),

        }
    )

    try:
        cm.start()
        log_handler(cm, log_path)
        while True:
            cm.block_on_monitor(timeout=10)
    finally:
        if cm.is_running():
            cm.exit()
            cm.block_on_monitor()

if __name__ == "__main__":
    mirror_smg()