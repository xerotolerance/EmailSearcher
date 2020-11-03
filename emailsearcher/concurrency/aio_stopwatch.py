#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>

from datetime import datetime, timedelta
from . import Union, Any, asyncio, sleep, Event


class Stopwatch:
    """Multi-threaded Stopwatch capabilities implemented using asyncio"""

    def __init__(self, name: str = f"TASK_'{datetime.now()}'", endtrigger: Event = Event(), display: bool = True):
        """
        Initializes an instance of Stopwatch.

        :param name: Name of timer
        :param endtrigger: Event used to stop the watch
        :param display: Live display of elapsed time will be printed to stdout if True
        """
        self.endtrigger = endtrigger
        self.endtrigger.clear()  # Make sure endtrigger.is_set() is False before _keep_time is called
        self.name = name
        self.display = display
        self.execution_time = None

    def start(self):
        """
        Runs _keep_time in a separate thread, but under the control of this thread's Event Loop.

        :return: None
        """
        self.execution_time = asyncio.get_event_loop().run_in_executor(None, self._keep_time)

    async def __aenter__(self):
        """
        Starts the clock immediately if Stopwatch is created via async with

        :return: This instance of Stopwatch
        """
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Automatically stops clock when scope of async with is left.

        :return: Time elapsed since clock start in Hours:Mins:Seconds.xxx format.
        """
        return await self.end()

    def _keep_time(self) -> Union[timedelta, int]:
        """
        Tracks amount of time elapsed since function was called, optionally displaying a live count on stdout.

        :return: Time elapsed since clock start in Hours:Mins:Seconds.xxx format.
        """
        print(f'[{self.name}] > Clock started at {(start := datetime.now())}')
        runtime = 0
        while not self.endtrigger.is_set():
            sleep(1 / 10 ** 6)
            if self.display:
                print(f'\r[{self.name}] > Time Elapsed: {(runtime := datetime.now() - start)}... ', end='')
        print(f'\r[{self.name}] > Clock stopped at {datetime.now()}')
        return runtime if runtime else datetime.now() - start

    async def end(self) -> Union[timedelta, int]:
        """
        Stops the Clock.

        :return: Time elapsed since clock start in Hours:Mins:Seconds.xxx format.
        """
        self.endtrigger.set()
        print(f'"{self.name}" Completed in {(elapsed := await self.execution_time)}s')
        return elapsed


def async_init():
    """
    Creates and sets a new asyncio Event Loop in the current thread

    :return: None (Running Event loop set in calling thread as side effect)
    """
    asyncio.set_event_loop(asyncio.new_event_loop())


def async_exec(aio_future: asyncio.Future) -> Any:
    """
    Runs an awaitable in this thread's ending the loop upon completion

    :param aio_future: An asyncio awaitable (ie. Futures, Tasks, & Coroutines)
    :return: The result of the awaitable
    """
    return asyncio.get_event_loop().run_until_complete(aio_future)
