#  Copyright (c) 2020. Christopher J Maxwell <contact@christopherjmaxwell.com>
"""
A **dummy package** containing **common imports** for general use across emailsearcher.
"""

from string import punctuation
from pathlib import Path
from functools import partial
from abc import ABC, abstractmethod
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool, Manager, RLock, Event, cpu_count
from threading import Lock as ThreadingLock
import tarfile as tar
import os
import asyncio
from typing import Tuple, Optional, Set, Generator, Any, Union
from time import sleep
from datetime import datetime


