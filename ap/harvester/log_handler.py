import os
import logging

from logging.handlers import TimedRotatingFileHandler
from ap.harvester.manager2 import CollectorManager


class ActivityFilter(logging.Filter):
    """Clean up terminal output from loggers.

    For a cleaner terminal output, logging messages from CollectorManager and CollectorMonitor
    are filtered out from the StreamHandler.
    """

    LOGGERS = ["CollectorManager.FinnActivity"
               ]

    def filter(self, record):
        # print(record.level)
        # print(self.name)
        if record.name in ActivityFilter.LOGGERS:
            return True
        if record.name not in ActivityFilter.LOGGERS and record.levelname != "INFO":
            return True


def log_handler(cm: CollectorManager, log_path: str = '') -> None:
    """Specify path for logging activity related log-outputs.

    A logging.StreamHandler is set ut by default for printing CollectorManager outsputs in
    terminal. A log path can be specified if user wants to store the acitivty logging outputs, individually.

    Args:
        cm: CollectorManager, used to get the parentlogger (cm.get_logger().
        log_path: the path to where to put the generated logfiles, if ''/default log console only

    Returns:

    """

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream = logging.StreamHandler()
    stream.setLevel(logging.INFO)
    stream.setFormatter(formatter)
    stream.addFilter(ActivityFilter())

    cm.get_logger().setLevel(logging.INFO)
    cm.get_logger().addHandler(stream)

    log_path = log_path
    if log_path:
        res_handler = TimedRotatingFileHandler(os.path.join(log_path, 'RESERVOIR_MAGVOLUME.log'),
                                               when="w1", interval=2)
        res_handler.setLevel(logging.INFO)
        res_handler.setFormatter(formatter)

        res_logger = logging.getLogger("CollectorManager.ReservoirActivity")
        res_logger.addHandler(res_handler)

        station_handler = TimedRotatingFileHandler(os.path.join(log_path, 'STATION_PRODUCTION.log'),
                                                   when="w1", interval=2)
        station_handler.setLevel(logging.INFO)
        station_handler.setFormatter(formatter)

        station_logger = logging.getLogger("CollectorManager.StationActivity")
        station_logger.addHandler(station_handler)

        forecast_handler = TimedRotatingFileHandler(os.path.join(log_path, 'INFLOW_FORECAST.log'),
                                                    when="w1", interval=2)
        forecast_handler.setLevel(logging.INFO)
        forecast_handler.setFormatter(formatter)

        forecast_logger = logging.getLogger("CollectorManager.InflowForecastActivity")
        forecast_logger.addHandler(forecast_handler)


        filling_handler = TimedRotatingFileHandler(os.path.join(log_path, 'OWN_FILLING.log'),
                                                    when="w1", interval=2)
        filling_handler.setLevel(logging.INFO)
        filling_handler.setFormatter(formatter)

        filling_logger = logging.getLogger("CollectorManager.OwnFillingActivity")
        filling_logger.addHandler(forecast_handler)

        info_handler = TimedRotatingFileHandler(os.path.join(log_path, 'INFO.log'),
                                                when="w1", interval=2)
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)
        cm.get_logger().addHandler(info_handler)