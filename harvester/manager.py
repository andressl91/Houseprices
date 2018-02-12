"""Collector service for polling data sources and persisting new data to a DTSS server.
"""

from typing import Any, Deque, Dict, List, Optional, Union

import logging
from collections import deque, namedtuple
from copy import copy
from enum import Enum
from queue import Queue
from threading import Event, Thread
from time import sleep

# noinspection PyUnresolvedReferences
from shyft.api import (
    DtsServer, DtsClient,
    TsVector, TimeSeries,
    Calendar, UtcPeriod, utctime_now
)

# from statkraft.data_collection.collectors.activity import ActivityABC  # not needed as we cannot type Queue
from statkraft.ltm.application.input.utilities.inflow_forecast_activity import InflowForecastActivity
from statkraft.ltm.application.input.utilities.reservoir_mag_activity import ReservoirActivity
from statkraft.ltm.application.input.utilities.station_prod_activity import StationActivity
from statkraft.ltm.application.input.utilities.own_filling_activity import OwnFillingActivity
CollectorTuple = namedtuple('CollectorTuple', ['collector', 'thread', 'activity', 'args'])


class CollectorError(RuntimeError):
    """Error raised by the collector service."""
    pass


class Collectors(Enum):
    """Enumeration of known collector instances."""
    INFLOW_FORECAST = InflowForecastActivity
    RESERVOIR_MAGVOLUME = ReservoirActivity
    STATION_PRODUCTION = StationActivity
    RES_FILLING = OwnFillingActivity

class CollectorManager:
    """Manager for controlling collector activities.
    Args:
        collectors: Dictionary of collectors to start together with the arguments needed
            to start each collector. The argument dictionaries are copied as arguments may
            be added by the manager.
            The arguments below will be automatically added using the defaults passed
            to CollectorManager:
             * ``wait_first``: Wait before first execution of the activity.
             * ``wakeup_freq``: Frequency between collector activity wakeups.
            While the arguments below will be ignored if added:
             * ``exit_event``: Event used to signal the collector activity to exit.
             * ``logger``: Collector activity parent logger instance.
            The collector activities are initialized in kwargs style using the appropriate
            value dictionary. If a collector doesn't take arguments, use an empty dictionary.
        monitor_poll_freq: Frequency in seconds or fractions thereof for the monitor to perform
            its activities.
        default_wait_first: Whether to wait before the first call to action.
        default_wakeup_freq: Frequency in seconds (or fractions thereof) to wakeup activities
            during long wait phases. Pass None to disable.
        logger: Logger instance. The manager creates a new child logger named after
            the class. When None, the manager creates a new logger named after the class.
    """
    def __init__(self, *,
                 collectors: Dict[Collectors, Dict[str, Any]],
                 monitor_poll_freq: float,
                 default_wait_first: bool,
                 default_wakeup_freq: Union[float, None],
                 logger: Union[logging.Logger, None]) -> None:

        # collectors to start with arguments
        # - only copy arguments
        self._collector_args = {}
        for c in collectors:
            local_args = self._collector_args[c] = copy(collectors[c])
            # add if missing
            if 'wait_first' not in local_args:
                local_args['wait_first'] = default_wait_first
            if 'wakeup_freq' not in local_args:
                local_args['wakeup_freq'] = default_wakeup_freq
            # remove if present
            if 'exit_event' in local_args:
                del local_args['exit_event']
            if 'logger' in local_args:
                del local_args['logger']

        # logging
        if logger is None:
            self._logger = logging.getLogger(CollectorManager.__name__)
        else:
            self._logger = logger.getChild(CollectorManager.__name__)

        # manager and monitor state
        self._exit_event = Event()
        self._monitor_poll_freq = monitor_poll_freq
        self._monitor: Union[CollectorMonitor, None] = None
        self._monitor_thread: Union[Thread, None] = None

    def get_logger(self) -> logging.Logger:
        """Return the logger used by the manager."""
        return self._logger

    def start(self) -> None:
        """Start all activities.
        Raises:
            CollectorError: If the any activities managed by self are already running.
        """
        if self.is_running():
            raise CollectorError('Collector already running')
        elif len(self._collector_args) == 0:
            raise CollectorError('No collectors to start')

        # cleanup
        self._exit_event.clear()

        # start monitor
        self._monitor = CollectorMonitor(
            poll_freq=self._monitor_poll_freq,
            exit_event=self._exit_event, logger=self._logger,
        )
        self._monitor_thread = Thread(name=f'{CollectorMonitor.__name__}', target=self._monitor, daemon=True)
        self._monitor_thread.start()

        # start all currently registered collectors
        for collector, args in self._collector_args.items():
            self._monitor.add_collector(collector=collector, args=args)

    def is_running(self, *, collector: Optional[Collectors] = None) -> bool:
        """Query if either the manager or specific collector activities are running.
        Args:
            collector: Optional collector to check for. If None check if the manager is
                running. Defaults to None.
        """
        if collector is not None:
            return self._monitor is not None and self._monitor.is_collector_running(collector)
        return self._monitor_thread is not None and self._monitor_thread.is_alive()

    def block_on_monitor(self, *, timeout: float = None) -> None:
        """Block execution until the CollectorMonitor monitoring the collector activities exits.
        Args:
            timeout: Timeout in seconds or fractions thereof before returning. There is no
                exception or other error if the timeout happened, to determine what caused
                the method to return you should call is_running() without arguments to
                determine if the monitor is still running.
        Raises:
            CollectorError: If the manager have not been started or have exited.
        """
        if not self.is_running():
            raise CollectorError('Not running')

        self._monitor_thread.join(timeout=timeout)

    def exit(self, *, collector: Optional[Collectors]=None) -> None:
        """Signal the manager or specific collector to exit, then return.
        This method is non-blocking. If you wish to wait for the background
        threads to exit follow with a call to block_on_monitor().
        Args:
            collector: Optional collector managed by this manager to exit.
                If this argument is not None only the specified collector will
                exit instead of all. The default value is None.
        Raises:
            CollectorError: If no collector activities are running.
        """
        if not self.is_running(collector=collector):
            raise CollectorError('Not running')

        if collector is None:
            self._exit_event.set()
        else:
            self._monitor.drop_collector(collector=collector)


class CollectorMonitor:
    """Monitor for supervising collector activities.
    The monitor regularly polls all threads registered with it, and recreate and restart them
    if necessary. If managed threads error out the error and traceback is logged to
    the provided logger.
    Note:
        The external interface for modifying the monitor is _NOT_ necessarily thread-safe.
        Only one thread should use the methods add_collector(), drop_collector(),
        is_collector_running(), running_collectors(), died_collectors().
        Additionally, they are never used from the manager.
    Args:
        poll_freq: Frequency in seconds or fractions thereof to poll activities and check
            for new activities to start or drop.
        exit_event: External event to set to signal the monitor and the activities to exit.
        logger: Parent logger object. The monitor requests a child logger with getChild(),
            and the logger is also passed on to started activities.
    """
    def __init__(self, *,
                 poll_freq: float,
                 exit_event: Event, logger: logging.Logger) -> None:
        # monitor arguments
        assert poll_freq > 0, f'{CollectorMonitor.__name__} should use a strictly positive poll frequency'
        self._poll_freq = poll_freq

        # activity common arguments
        self._exit_event = exit_event

        # loggers
        self._parent_logger = logger  # logger to pass to activities
        self._logger: logging.Logger = logger.getChild(CollectorMonitor.__name__)

        # monitor state
        self._collectors: Dict[Collectors, CollectorTuple] = {}
        self._to_drop_queue: Deque[Collectors] = deque()  # queue of collector to remove once exited
        self._register_queue = Queue()  # element type: Tuple[Collector, Dict[str, Any]]
        self._deregister_queue = Queue()  # element type: Collector

    def add_collector(self, *, collector: Collectors, args: Dict[str, Any]) -> None:
        """Register a collector activity in the monitor.
        This method queues the collector to be started. The collector is started from the thread
        running the monitor. Until the activity is removed by a call to drop_collector(),
        the monitor will keep the collector alive and restart it if necessary.
        Args:
            collector: The collector type to register.
            args: The arguments to start the collector activity with.
        Note:
            The external interface to the monitor can only see a collector once it is started.
            Therefore it is possible to add the same collector multiple times if the monitor
            is not able to process the added collectors fast enough. If the an already added
            collector is encountered in the internal startup code it is logged as an error,
            and the duplicate collector is left unstarted and dropped.
            Thus it is advisable to take care not to add duplicate collectors.
        Raises:
            ValueError: If a collector with the same type is already registered and started.
                See the above note.
        """
        if collector in self._collectors:
            # TODO consider more robust _collector check
            #      This is safe because of the GIL and that we are using threads
            raise ValueError(f'Collector {collector.name} already registered')
        self._register_queue.put((collector, args))

    def drop_collector(self, collector: Collectors) -> None:
        """De-register a started collector from the monitor.
        Args:
            collector: The collector to deregister.
        Note:
            The external interface to the monitor can only see a collector once it is started.
            Therefore it is possible that drop_collector() raises ValueError even though
            the collector have been added if the thread running the monitor have failed to add
            the collector between the calls to add_collector() and drop_collector().
        Raises:
            ValueError: If no such collector have been registered _and_ started.
                See the above note.
        """
        if collector not in self._collectors:
            # TODO consider more robust _collector check
            #      This is safe because of the GIL and that we are using threads
            raise ValueError(f'Collector {collector.name} is not registered')
        self._deregister_queue.put(collector)

    def is_collector_running(self, collector: Collectors) -> bool:
        """Query if a specific collector is running."""
        # TODO consider more robust _collector check
        #      This is safe because of the GIL and that we are using threads
        return collector in self._collectors and self._collectors[collector].thread.is_alive()

    def running_collectors(self) -> List[Collectors]:
        """Return a list of the running collector types."""
        # TODO consider more robust _collector retrieval
        #      This is safe because of the GIL and that we are using threads
        return list(collector
                    for collector in self._collectors
                    if self._collectors[collector].thread.is_alive())

    def died_collectors(self) -> List[Collectors]:
        """Return a list of collector types that is not running, yet still registered."""
        # TODO consider more robust _collector retrieval
        #      This is safe because of the GIL and that we are using threads
        return list(collector
                    for collector in self._collectors
                    if not self._collectors[collector].thread.is_alive())

    def __call__(self) -> None:
        """Monitor execution loop."""
        self._exit_event.clear()  # reset
        while not self._should_exit():
            self._logger.info('Performing activity monitoring')
            self._monitoring_step()
            sleep(self._poll_freq)

        self._logger.info(f'Monitor exiting')

    def _should_exit(self) -> bool:
        """Determines if the monitor should exit."""
        if self._exit_event.is_set():
            if len(self._collectors) > 0:
                self._logger.warning('Waiting for collectors to exit')
            else:
                return True
        return False

    def _monitoring_step(self) -> None:
        """Perform the monitoring tasks."""
        # if there are collectors waiting to start -> start them
        while not self._register_queue.empty():
            collector, args = self._register_queue.get()
            self._start_collector(collector, args)

        sleep(0.)  # force synchronization and context switch

        # if there are collectors that are requested to exit -> signal them to exit
        while not self._deregister_queue.empty():
            collector = self._deregister_queue.get()
            self._drop_collector(collector)

        sleep(0.)  # force synchronization and context switch

        # if there are collectors we plan to remove -> try to drop them
        retry_queue = deque()
        while len(self._to_drop_queue) > 0:
            collector = self._to_drop_queue.pop()
            self._do_drop_collector(collector, retry_queue)
        self._to_drop_queue.extend(retry_queue)

        sleep(0.)  # force synchronization and context switch

        # poll each collector and check check if they are alive
        for collector in self._collectors:
            self._poll_collector(collector)

    def _start_collector(self, collector: Collectors, args: Dict[str, Any], *, restart: bool=False) -> None:
        """Start and register a collector in the monitor."""
        if not restart:
            self._logger.info(f'Starting collector activity: {collector.name}')
        else:
            self._logger.info(f'Restarting collector activity: {collector.name}')

        # only start a collector once!
        if not restart and collector in self._collectors:
            self._logger.error(f'Trying to start collector {collector.name}, but it is already started')
            return
        elif restart and collector not in self._collectors:
            self._logger.error(f'Trying to restart collector {collector.name}, but it is not already registered')
            return

        # startup logic
        activity = collector.value(
            **args,
            exit_event=self._exit_event, logger=self._parent_logger,
        )
        thread = Thread(name=activity.name, target=activity, daemon=True)
        self._collectors[collector] = CollectorTuple(
            collector=collector, thread=thread,
            activity=activity, args=args,
        )
        thread.start()

    def _drop_collector(self, collector: Collectors) -> None:
        """Signal a collector to stop. Actual removal is done in _do_drop_collector()."""

        self._logger.info(f'Signalling collector activity to exit: {collector.name}')

        if collector not in self._collectors:
            self._logger.error(f'Requesting to drop not registered collector: {collector.name}')
            return

        if self._collectors[collector].activity.exiting():
            self._logger.warning(f'Collector already requested to exit: {collector.name}')

        # add the collector to the list of collectors to remove, and signal it to exit
        self._to_drop_queue.append(collector)
        self._collectors[collector].activity.exit()

    def _do_drop_collector(self, collector: Collectors, retry_queue: Deque[Collectors]) -> None:
        """Query if the collector activity is running, and drop it if it is dead."""

        self._logger.info(f'Attempting to exit collector activity: {collector.name}')

        if collector not in self._collectors:
            self._logger.error(f'Attempting to drop not started collector: {collector.name}')
            return

        collector_tup = self._collectors[collector]
        if not collector_tup.thread.is_alive():
            # it is dead -> remove it
            del self._collectors[collector]
        else:
            self._logger.warning(f'Dropped collector have not yet exited: {collector.name}')
            retry_queue.append(collector)

    def _poll_collector(self, collector: Collectors) -> None:
        """Poll collectors and check if they are alive."""

        self._logger.info(f'Polling collector activity: {collector.name}')

        collector_tup: CollectorTuple = self._collectors[collector]
        if not collector_tup.activity.exiting():
            if not collector_tup.thread.is_alive():
                # log error
                message = f'Collector activity exited unexpectedly: {collector.name}'
                error: Exception = collector_tup.activity.get_error()
                if error is not None:
                    message += f'\nWith error:\n{str(error)}'
                else:
                    message += f'\nNo error available.'
                self._logger.error(message)

                # attempt to restart
                self._start_collector(collector_tup.collector, collector_tup.args, restart=True)
        elif not collector_tup.thread.is_alive():
            self._to_drop_queue.append(collector)