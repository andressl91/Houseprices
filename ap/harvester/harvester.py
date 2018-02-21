"""Defines setup for collector activities.
"""

from typing import Optional, Union

from abc import ABC, abstractmethod
from logging import Logger
from threading import Event
from time import sleep


class ActivityABC(ABC):
    """Skeleton of a collector activity.

    Args:
        wait_first: Whether to wait before the first call to action.
        wakeup_freq: Optional frequency in seconds (or fractions thereof) to wakeup during
            long wait phases.
        exit_event: Event to signal the thread to exit.
        logger: Parent logger object. The activity creates a child logger from the instance.
            The child logger name is created by using the name property with all spaces
            replaced with ``_``.
    The methods startup(), cleanup(), wait_for(), and action() is only called from the thread
    executing the activity, the remaining methods may be called from multiple threads.
    """

    def __init__(self, *,
                 wait_first: bool, wakeup_freq: Optional[float],
                 exit_event: Event, logger: Logger) -> None:

        self._wakeup_freq = wakeup_freq
        self._wait_first = wait_first

        self._external_exit_event = exit_event
        self._logger = logger.getChild(self.name.replace(' ', '_'))

        self.__stored_error: Optional[Exception] = None
        self.__activity_exit_event = Event()

    @property
    @abstractmethod
    def name(self) -> str:
        """Name given to the thread running the activity."""
        pass

    @abstractmethod
    def startup(self) -> None:
        """Perform startup actions needed before the activity polling loop."""
        pass

    @abstractmethod
    def wait_for(self) -> float:
        """Return the seconds (or fractions thereof) to wait until performing action() again.
        Note:
            This method is only called once between actions, so it should return the total
            duration in seconds (or fractions thereof) to the next time action() should be
            called.
        """
        pass

    @abstractmethod
    def action(self) -> None:
        """Perform the actions this activity is intended for."""
        pass

    @abstractmethod
    def cleanup(self, started: bool, graceful: bool) -> None:
        """Perform cleanup actions needed before the activity exits.
        If an error occurred it can be retrieved with get_error().
        This method is always called.
        Args:
            started: True if the activity got past startup; False otherwise.
            graceful: True if the activity exited normally; False otherwise.
        """
        pass

    @property
    def logger(self) -> Logger:
        """The activity logger."""
        return self._logger

    def exit(self) -> None:
        """Signal the activity to exit."""
        self.__activity_exit_event.set()

    def exiting(self) -> bool:
        """Query if the activity is exiting."""
        return self._external_exit_event.is_set() or self.__activity_exit_event.is_set()

    def get_error(self) -> Union[Exception, None]:
        """Get a stored error."""
        return self.__stored_error

    def __store_error(self, error: Exception) -> None:
        """Store an exception that aborted the activity, it can be later retrieved by get_error()."""
        self.__stored_error = error

    def __do_wait(self, duration: float) -> float:
        """Perform the wait action, waiting either until duration or self._wakeup_freq seconds
        have passed."""
        # if the wakeup frequency is less than the current wait duration, wait for the wakeup frequency instead
        # update duration to the new wait duration
        self.logger.debug(f'Should wait for {duration} s')
        if self._wakeup_freq is not None and duration > self._wakeup_freq:
            to_wait = self._wakeup_freq
            duration -= self._wakeup_freq
        else:
            to_wait = duration
            duration = 0.

        self.logger.debug(f'Waiting for {to_wait} s')
        if to_wait > 0.:
            sleep(to_wait)

        # return the remainder of the wait duration
        return duration

    def __call__(self) -> None:
        """Run the activity. This is called automatically and does not exit until the activity exits."""
        # state
        started = False
        graceful = False

        # reset the activity event
        self.__activity_exit_event.clear()

        try:
            self.startup()
            started = True

            # wait before the first action()?
            if self._wait_first:
                wait_for = self.wait_for()
            else:
                wait_for = 0.

            while not self.exiting():
                # do the action if the wait is over
                if wait_for == 0.:
                    self.action()  # do the intended action of the activity
                    wait_for = self.wait_for()  # get next wait duration
                else:
                    wait_for = self.__do_wait(wait_for)

            # nothing went wrong!
            graceful = True
        except Exception as e:
            self.__store_error(error=e)
        finally:
            self.cleanup(started=started, graceful=graceful)