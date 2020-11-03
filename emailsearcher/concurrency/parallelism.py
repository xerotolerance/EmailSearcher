#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool


class Threaded:
    """An Abstract Class for providing instance-wide Multithreading Capability"""
    def __init__(self):
        self._threads = ThreadPool()

    def __del__(self):
        self._threaded_shutdown(hard=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._threaded_shutdown()

    def _threaded_shutdown(self, hard=False):
        """
        Clean up function to assure proper release of ThreadPool resources.
        :param hard: Forces running threads to join immediately if True; else allow them to finish their tasks before joining.
        :return: None
        """
        # print('Shutting down threadpool')
        try:
            if hard:
                self._threads.terminate()
            else:
                self._threads.close()
            self._threads.join()
        except Exception as e:
            print(e)


class MultiProcess:
    """An Abstract Class for providing instance-wide Multi-processing Capability"""
    def __init__(self):
        self._processes = Pool()

    def __del__(self):
        self._multiprocess_shutdown(hard=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._multiprocess_shutdown()

    def _multiprocess_shutdown(self, hard=False):
        """
        Clean up function to assure proper release of multiprocessing.Pool resources.
        :param hard: Forces running processes to terminate immediately if True; else allow them to finish their tasks before closing.
        :return: None
        """
        # print('Shutting down processpool')
        try:
            if hard:
                self._processes.terminate()
            else:
                self._processes.close()
            self._processes.join()
        except Exception as e:
            print(e)

